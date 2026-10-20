"""
Имитатор устройства-клиента, отправляющего на сервер БОЛЬШИЕ массивы данных.

Генерирует синтетический сигнал (синусоида + шум) в формате float32,
упаковывает в кадры протокола (protocol.py) и потоково отправляет по TCP.

Примеры:
    # 100 пакетов по 1 МБ (~100 МБ данных)
    python client_sim.py --packets 100 --packet-size 1MB

    # 4 «устройства» одновременно, всего ~1 ГБ
    python client_sim.py --clients 4 --packets 64 --packet-size 4MB

    # с искусственной паузой между пакетами (имитация реальной скорости АЦП)
    python client_sim.py --packets 50 --packet-size 2MB --interval 0.05
"""

import argparse
import asyncio
import os
import re
import struct
import sys
import time

import numpy as np

from protocol import (
    make_handshake, make_meta_packet, make_data_packet, make_bye,
    HEADER_SIZE, parse_header, MSG_ACK,
)


def human_to_bytes(s: str) -> int:
    """'1MB', '512KB', '100b', '4.5MB' -> число байт."""
    m = re.fullmatch(r"\s*([\d.]+)\s*(B|KB|MB|GB)?\s*", s, re.IGNORECASE)
    if not m:
        raise argparse.ArgumentTypeError(f"Неверный размер: {s!r}")
    val = float(m.group(1))
    unit = (m.group(2) or "B").upper()
    mult = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}[unit]
    return int(val * mult)


def gen_signal_chunk(n_samples: int, seq: int, fs: float = 1e6) -> bytes:
    """Синтетический сигнал: сумма гармоник + белый шум, float32."""
    t = (np.arange(n_samples, dtype=np.float32) + seq * n_samples) / fs
    sig = (np.sin(2 * np.pi * 1000.0 * t)
           + 0.5 * np.sin(2 * np.pi * 7500.0 * t)
           + 0.1 * np.random.randn(n_samples).astype(np.float32))
    return sig.tobytes()


async def run_device(device_id: int, host: str, port: int, packets: int,
                     packet_bytes: int, interval: float, verbose: bool):
    reader, writer = await asyncio.open_connection(host, port)
    samples = packet_bytes // 4  # float32

    total_bytes = packets * packet_bytes
    writer.write(make_handshake(device_id, name=f"sim-device-{device_id}"))
    writer.write(make_meta_packet(device_id, packets, total_bytes, "float32"))
    await writer.drain()

    t0 = time.monotonic()
    sent_bytes = 0
    last_progress = t0

    for seq in range(packets):
        chunk = gen_signal_chunk(samples, seq)
        ts_ns = time.time_ns()
        writer.write(make_data_packet(seq, ts_ns, chunk))
        sent_bytes += len(chunk) + HEADER_SIZE + 12

        if interval > 0:
            await asyncio.sleep(interval)

        # не блокируемся на ACK — читаем, если он уже пришёл
        try:
            hdr = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=0)
            _, length = parse_header(hdr)
            await reader.readexactly(length)
        except (asyncio.TimeoutError, asyncio.IncompleteReadError):
            pass

        # дrenaж буфера при переполнении (backpressure)
        if writer.transport.get_write_buffer_size() > 64 * 1024 * 1024:
            await writer.drain()

        now = time.monotonic()
        if verbose and now - last_progress >= 1.0:
            dt = now - t0
            mbps = sent_bytes * 8 / dt / 1e6
            print(f"  [dev {device_id}] {seq+1}/{packets} пакетов, "
                  f"{sent_bytes/1e6:.1f} MB, {mbps:.1f} Mbit/s")
            last_progress = now

    await writer.drain()
    writer.write(make_bye())
    await writer.drain()
    await asyncio.sleep(0.05)
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass

    dt = time.monotonic() - t0
    print(f"[dev {device_id}] Готово: {packets} пакетов, "
          f"{sent_bytes/1e6:.1f} MB за {dt:.2f} c "
          f"({sent_bytes*8/dt/1e6:.1f} Mbit/s)")
    return sent_bytes, dt


async def main_async(args):
    tasks = []
    for i in range(args.clients):
        tasks.append(run_device(
            device_id=args.device_id + i,
            host=args.host, port=args.port,
            packets=args.packets,
            packet_bytes=args.packet_size,
            interval=args.interval,
            verbose=not args.quiet,
        ))
    t0 = time.monotonic()
    results = await asyncio.gather(*tasks)
    total = sum(r[0] for r in results)
    dt = time.monotonic() - t0
    print("=" * 60)
    print(f"ВСЕГО отправлено: {total/1e6:.1f} MB за {dt:.2f} c "
          f"=> суммарная скорость {total*8/dt/1e6:.1f} Mbit/s")


def main():
    ap = argparse.ArgumentParser(description="Имитатор устройства с большими данными")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--device-id", type=int, default=1)
    ap.add_argument("--clients", type=int, default=1, help="Число устройств одновременно")
    ap.add_argument("--packets", type=int, default=100, help="Пакетов на устройство")
    ap.add_argument("--packet-size", type=human_to_bytes, default="1MB",
                    help="Размер payload пакета: 512KB, 1MB, 4MB ...")
    ap.add_argument("--interval", type=float, default=0.0,
                    help="Пауза между пакетами, сек (0 = максимально быстро)")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    if args.packet_size % 4 != 0:
        print("packet-size должен быть кратен 4 (float32)", file=sys.stderr)
        sys.exit(1)

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\nПрервано пользователем.")


if __name__ == "__main__":
    main()
