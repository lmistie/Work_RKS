"""
Сервер приёма больших массивов данных от устройств (клиентов).

Архитектура:
    - asyncio, один поток событий;
    - для каждого подключения читает кадры протокола (см. protocol.py);
    - payload каждого MSG_DATA передаётся в отдельный процесс-воркер
      (multiprocessing.Pool), чтобы парсинг/аналитика больших массивов
      не блокировали сетевой цикл сервера;
    - статистика (количество пакетов, байт, пропускная способность)
      выводится периодически и по завершении.

Запуск:
    python server.py --host 0.0.0.0 --port 9000

Тест без устройства-иммитатора:
    python server.py &   # затем
    python client_sim.py --packets 200 --packet-size 1MB
"""

import argparse
import asyncio
import os
import struct
import sys
import time
from multiprocessing import Pool

import numpy as np

from protocol import (
    HEADER_SIZE, parse_header, parse_data_payload, parse_meta_payload,
    MSG_HANDSHAKE, MSG_DATA, MSG_META, MSG_BYE, MSG_ACK, encode_frame,
)


# ---------------------------------------------------------------------------
# Воркер: обработка большого массива в отдельном процессе.
# Здесь имитируется «реальная» аналитика: конвертация bytes -> ndarray,
# вычисление статистик (среднее, максимум, RMS).
# ---------------------------------------------------------------------------
def process_chunk(seq, ts, dtype_str, raw):
    arr = np.frombuffer(raw, dtype=np.dtype(dtype_str))
    if arr.size == 0:
        return seq, 0.0, 0.0, 0.0
    mean = float(arr.mean())
    peak = float(np.abs(arr).max())
    rms = float(np.sqrt((arr.astype(np.float64) ** 2).mean()))
    return seq, mean, peak, rms


class ServerStats:
    def __init__(self):
        self.packets = 0
        self.bytes_recv = 0
        self.sessions = 0
        self.t0 = time.monotonic()

    def snapshot(self):
        dt = max(time.monotonic() - self.t0, 1e-9)
        mbps = self.bytes_recv * 8 / dt / 1e6
        return (f"пакетов={self.packets}  "
                f"данных={self.bytes_recv / 1e6:.1f} MB  "
                f"скорость={mbps:.1f} Mbit/s")


STATS = ServerStats()


async def read_exactly(reader: asyncio.StreamReader, n: int) -> bytes:
    """Прочитать ровно n байт или упасть с исключением при обрыве."""
    data = await reader.readexactly(n)
    return data


async def handle_client(reader: asyncio.StreamReader,
                        writer: asyncio.StreamWriter,
                        pool: Pool, loop: asyncio.AbstractEventLoop):
    peer = writer.get_extra_info("peername")
    STATS.sessions += 1
    print(f"[+] Подключение: {peer}")

    device_id = None
    meta = None            # (total_packets, total_bytes, dtype)
    session_bytes = 0
    session_pkts = 0
    last_ack_at = time.monotonic()

    try:
        while True:
            try:
                header = await read_exactly(reader, HEADER_SIZE)
                msg_type, length = parse_header(header)
                payload = await read_exactly(reader, length) if length else b""
            except asyncio.IncompleteReadError:
                break  # клиент штатно отключился

            if msg_type == MSG_HANDSHAKE:
                device_id = struct.unpack("!I", payload[:4])[0]
                name_len = payload[4]
                name = payload[5:5 + name_len].decode("utf-8")
                print(f"    handshake: устройство #{device_id} '{name}'")
                writer.write(encode_frame(MSG_ACK, struct.pack("!I", device_id)))
                await writer.drain()

            elif msg_type == MSG_META:
                dev, total_pkts, total_bytes, dtype_str = parse_meta_payload(payload)
                meta = (total_pkts, total_bytes, dtype_str)
                print(f"    meta: device=#{dev}, пакетов={total_pkts}, "
                      f"байт={total_bytes/1e6:.1f} MB, тип={dtype_str}")

            elif msg_type == MSG_DATA:
                seq, ts, chunk = parse_data_payload(payload)
                STATS.packets += 1
                STATS.bytes_recv += len(chunk)
                session_pkts += 1
                session_bytes += len(chunk)

                # Отдаём большой массив в процесс-воркер, не блокируя event loop.
                # apply_async ставит задачу в очередь пула; callback приходит
                # через inotify-фишку multiprocessing, интегрированную с asyncio.
                dtype_str = meta[2] if meta else "float32"

                def on_done(res, peer_=peer):
                    try:
                        s, mean, peak, rms = res
                        # выборочный лог первых и каждых N пакетов
                        if s < 3 or s % 200 == 0:
                            print(f"    [{peer_}] пакет #{s}: "
                                  f"mean={mean:.4f} peak={peak:.4f} rms={rms:.4f}")
                    except Exception as e:
                        print(f"    ошибка обработки пакета: {e}", file=sys.stderr)

                pool.apply_async(process_chunk, (seq, ts, dtype_str, chunk),
                                 callback=on_done,
                                 error_callback=lambda e: print(f"    ошибка пула: {e}",
                                                                file=sys.stderr))

                # Периодический ACK и лог прогресса — не чаще раза в секунду
                now = time.monotonic()
                if now - last_ack_at > 1.0:
                    writer.write(encode_frame(MSG_ACK, struct.pack("!IQ", 0, STATS.packets)))
                    await writer.drain()
                    last_ack_at = now
                    print(f"    [{peer}] {STATS.snapshot()}")

            elif msg_type == MSG_BYE:
                print(f"    [{peer}] завершение сессии: пакетов={session_pkts}, "
                      f"байт={session_bytes/1e6:.1f} MB")
                break

    except ConnectionResetError:
        print(f"[!] Обрыв соединения: {peer}")
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        print(f"[-] Соединение закрыто: {peer}. Итого: {STATS.snapshot()}")


async def main_async(host: str, port: int, workers: int):
    pool = Pool(processes=workers)
    loop = asyncio.get_running_loop()

    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, pool, loop), host, port)

    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    print(f"Сервер слушает на {addrs} (воркеров: {workers})")

    async with server:
        await server.serve_forever()


def main():
    ap = argparse.ArgumentParser(description="Сервер приёма больших массивов данных")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) // 2),
                    help="Число процессов-обработчиков")
    args = ap.parse_args()
    try:
        asyncio.run(main_async(args.host, args.port, args.workers))
    except KeyboardInterrupt:
        print("\nОстановка сервера.")


if __name__ == "__main__":
    main()
