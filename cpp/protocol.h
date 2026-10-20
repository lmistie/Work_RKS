#pragma once
// ============================================================================
//  protocol.h — двоичный TLV-протокол обмена "устройство -> сервер"
//  Кадр:  MAGIC(2) | TYPE(1) | LENGTH(quint32, big-endian) | PAYLOAD(LENGTH)
//  Заголовок = 7 байт. LENGTH может быть большим (пакеты по 1..16 МБ).
// ============================================================================
#include <QtGlobal>
#include <QByteArray>
#include <QString>
#include <cstring>

namespace proto {

constexpr quint16 MAGIC      = 0xA55A;
constexpr int     HEADER_LEN = 7;

enum FrameType : quint8 {
    Handshake = 0x01, // устройство представляется: devId|model|fw|sampleRate|channels|format
    Meta      = 0x02, // метаданные потока (json)
    Data      = 0x03, // полезная нагрузка: seq(quint32 BE) + float32 samples (LE)
    Bye       = 0x04, // завершение передачи
    Ack       = 0x05, // ответ сервера: принятые пакеты/байты
};

// ---------------- разбор handshake ----------------
struct DeviceInfo {
    QString deviceId;
    QString model;
    QString firmware;
    double  sampleRateHz = 0.0;
    quint16 channels     = 1;
    quint8  format       = 0; // 1 = float32 LE
};

inline QByteArray buildHandshake(const QString &devId, const QString &model,
                                 const QString &fw, double sampleRateHz,
                                 quint16 channels, quint8 format)
{
    QByteArray p;
    auto putStr = [&p](const QString &s) {
        const QByteArray b = s.toUtf8();
        quint16 n = static_cast<quint16>(b.size());
        p.append(static_cast<char>(n >> 8));
        p.append(static_cast<char>(n & 0xFF));
        p.append(b);
    };
    putStr(devId);
    putStr(model);
    putStr(fw);
    quint64 srBits = 0;
    std::memcpy(&srBits, &sampleRateHz, sizeof(srBits));
    for (int i = 7; i >= 0; --i) p.append(static_cast<char>((srBits >> (8 * i)) & 0xFF));
    p.append(static_cast<char>(channels >> 8));
    p.append(static_cast<char>(channels & 0xFF));
    p.append(static_cast<char>(format));
    return p;
}

inline bool parseHandshake(const QByteArray &payload, DeviceInfo &out)
{
    int off = 0;
    auto getStr = [&](QString &s) -> bool {
        if (off + 2 > payload.size()) return false;
        int n = (static_cast<quint8>(payload[off]) << 8) | static_cast<quint8>(payload[off + 1]);
        off += 2;
        if (off + n > payload.size()) return false;
        s = QString::fromUtf8(payload.constData() + off, n);
        off += n;
        return true;
    };
    if (!getStr(out.deviceId)) return false;
    if (!getStr(out.model))   return false;
    if (!getStr(out.firmware)) return false;
    if (off + 8 > payload.size()) return false;
    quint64 srBits = 0;
    for (int i = 0; i < 8; ++i) srBits = (srBits << 8) | static_cast<quint8>(payload[off + i]);
    out.sampleRateHz = *reinterpret_cast<const double*>(&srBits);
    off += 8;
    if (off + 3 > payload.size()) return false;
    out.channels = (static_cast<quint8>(payload[off]) << 8) | static_cast<quint8>(payload[off + 1]);
    out.format   = static_cast<quint8>(payload[off + 2]);
    return true;
}

// ---------------- сборка кадра ----------------
inline QByteArray buildFrame(quint8 type, const QByteArray &payload)
{
    QByteArray f;
    f.reserve(HEADER_LEN + payload.size());
    f.append(static_cast<char>(MAGIC >> 8));
    f.append(static_cast<char>(MAGIC & 0xFF));
    f.append(static_cast<char>(type));
    quint32 len = static_cast<quint32>(payload.size());
    f.append(static_cast<char>((len >> 24) & 0xFF));
    f.append(static_cast<char>((len >> 16) & 0xFF));
    f.append(static_cast<char>((len >> 8) & 0xFF));
    f.append(static_cast<char>(len & 0xFF));
    f.append(payload);
    return f;
}

// Данные пакета: seq(4, BE) + float32 samples (native/little-endian)
inline QByteArray buildDataPayload(quint32 seq, const float *samples, qint64 count)
{
    QByteArray p;
    p.resize(static_cast<int>(4 + count * static_cast<qint64>(sizeof(float))));
    char *d = p.data();
    d[0] = static_cast<char>((seq >> 24) & 0xFF);
    d[1] = static_cast<char>((seq >> 16) & 0xFF);
    d[2] = static_cast<char>((seq >> 8) & 0xFF);
    d[3] = static_cast<char>(seq & 0xFF);
    std::memcpy(d + 4, samples, static_cast<size_t>(count) * sizeof(float));
    return p;
}

inline quint32 readSeq(const QByteArray &payload)
{
    const uchar *d = reinterpret_cast<const uchar*>(payload.constData());
    return (quint32(d[0]) << 24) | (quint32(d[1]) << 16) | (quint32(d[2]) << 8) | d[3];
}

// ---------------- потоковый парсер кадров ----------------
// Буферизует приходящие байты и выдаёт целые кадры по одному.
class FrameParser {
public:
    enum Result { NeedMore, BadMagic, FrameReady };

    void append(const QByteArray &data) { m_buf.append(data); }

    Result next(quint8 &type, QByteArray &payload)
    {
        if (m_buf.size() < HEADER_LEN) return NeedMore;
        const uchar *h = reinterpret_cast<const uchar*>(m_buf.constData());
        quint16 magic = (h[0] << 8) | h[1];
        if (magic != MAGIC) return BadMagic;
        quint8 t = h[2];
        quint32 len = (quint32(h[3]) << 24) | (quint32(h[4]) << 16) | (quint32(h[5]) << 8) | h[6];
        if (len > MAX_PAYLOAD) return BadMagic; // защита от "мусора"
        if (m_buf.size() < HEADER_LEN + int(len)) return NeedMore;
        type = t;
        payload = m_buf.mid(HEADER_LEN, int(len));
        m_buf.remove(0, HEADER_LEN + int(len));
        return FrameReady;
    }

    void reset() { m_buf.clear(); }

private:
    static constexpr quint32 MAX_PAYLOAD = 64u * 1024u * 1024u; // 64 МБ на кадр
    QByteArray m_buf;
};

} // namespace proto
