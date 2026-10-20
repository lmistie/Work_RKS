"""
Общий протокол обмена «устройство -> сервер» больших массивов данных.

Формат кадра (TLV, big-endian):
    +--------+--------+----------------+------------------+
    | MAGIC  | TYPE   | LENGTH (4 байта)| PAYLOAD          |
    | 2 б    | 1 б    | uint32         | LENGTH байт      |
    +--------+--------+----------------+------------------+

Типы сообщений:
    MSG_HANDSHAKE (0x01) - устройство представляется, сервер подтверждает
    MSG_DATA      (0x02) - пакет данных (большой массив)
    MSG_META      (0x03) - метаданные о потоке (сколько пакетов всего и т.п.)
    MSG_ACK       (0x81) - подтверждение приёма от сервера
    MSG_BYE       (0x04) - завершение сессии

Payload MSG_DATA:
    seq (uint32) | timestamp_ns (uint64) | chunk (сырые данные)

Payload MSG_META:
    device_id (uint32) | total_packets (uint32) | total_bytes (uint64) | dtype (utf-8 str)
"""

import struct

MAGIC = b"\xAA\x55"

MSG_HANDSHAKE = 0x01
MSG_DATA      = 0x02
MSG_META      = 0x03
MSG_BYE       = 0x04
MSG_ACK       = 0x81

HEADER_FMT = "!2sBI"          # magic, type, length
HEADER_SIZE = struct.calcsize(HEADER_FMT)   # 7 байт

DATA_BODY_FMT = "!IQ"         # seq, timestamp_ns
DATA_BODY_SIZE = struct.calcsize(DATA_BODY_FMT)  # 12 байт

META_BODY_FMT = "!IIQ"        # device_id, total_packets, total_bytes
META_BODY_SIZE = struct.calcsize(META_BODY_FMT)  # 16 байт


def encode_frame(msg_type: int, payload: bytes) -> bytes:
    """Собрать кадр из типа сообщения и полезной нагрузки."""
    return struct.pack(HEADER_FMT, MAGIC, msg_type, len(payload)) + payload


def make_data_packet(seq: int, timestamp_ns: int, chunk: bytes) -> bytes:
    payload = struct.pack(DATA_BODY_FMT, seq, timestamp_ns) + chunk
    return encode_frame(MSG_DATA, payload)


def make_meta_packet(device_id: int, total_packets: int, total_bytes: int,
                     dtype: str = "float32") -> bytes:
    dtype_b = dtype.encode("utf-8")
    payload = struct.pack(META_BODY_FMT, device_id, total_packets, total_bytes) \
              + struct.pack("!B", len(dtype_b)) + dtype_b
    return encode_frame(MSG_META, payload)


def make_handshake(device_id: int, name: str = "sensor-device") -> bytes:
    name_b = name.encode("utf-8")
    payload = struct.pack("!I", device_id) + struct.pack("!B", len(name_b)) + name_b
    return encode_frame(MSG_HANDSHAKE, payload)


def make_bye() -> bytes:
    return encode_frame(MSG_BYE, b"")


def parse_header(header: bytes):
    """Разобрать заголовок кадра. Возвращает (magic, msg_type, length)."""
    magic, msg_type, length = struct.unpack(HEADER_FMT, header)
    if magic != MAGIC:
        raise ValueError(f"Неверный MAGIC кадра: {magic!r} (поток повреждён?)")
    return msg_type, length


def parse_data_payload(payload: bytes):
    """Вернуть (seq, timestamp_ns, chunk)."""
    seq, ts = struct.unpack(DATA_BODY_FMT, payload[:DATA_BODY_SIZE])
    return seq, ts, payload[DATA_BODY_SIZE:]


def parse_meta_payload(payload: bytes):
    device_id, total_packets, total_bytes = struct.unpack(
        META_BODY_FMT, payload[:META_BODY_SIZE])
    dlen = struct.unpack("!B", payload[META_BODY_SIZE:META_BODY_SIZE + 1])[0]
    dtype = payload[META_BODY_SIZE + 1:META_BODY_SIZE + 1 + dlen].decode("utf-8")
    return device_id, total_packets, total_bytes, dtype
