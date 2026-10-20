// ============================================================================
//  server.cpp — тестовый сервер на Qt6 (C++) для приёма БОЛЬШИХ массивов данных
//  от клиента-устройства по TCP.
//
//  Возможности:
//   * слушает порт (--port, по умолчанию 9000);
//   * принимает несколько подключений одновременно;
//   * читает поток байтами (readAll) и парсит TLV-кадры протокола protocol.h —
//     кадры Data могут быть размером в единицы/десятки МБ;
//   * для каждого устройства хранит статистику: пакеты, байты, скорость;
//   * выполняет "аналитику" принятого float32-массива (mean/min/max/RMS)
//     без копирования payload (используется data() напрямую);
//   * periodically отправляет клиенту ACK с принятием;
//   * пишет всё в лог (qInfo).
//
//  Сборка: см. CMakeLists.txt / server.pro
//  Запуск: ./device_server --port 9000 [--ack-interval-ms 1000] [--analyze]
// ============================================================================
#include <QCoreApplication>
#include <QTcpServer>
#include <QTcpSocket>
#include <QCommandLineParser>
#include <QElapsedTimer>
#include <QTimer>
#include <QTextStream>
#include <QDateTime>

#include <atomic>
#include <cmath>
#include <vector>

#include "protocol.h"

static QTextStream out(stdout);

// --------------------------- аналитика массива ------------------------------
struct Analysis {
    double mean = 0, minV = 0, maxV = 0, rms = 0;
    qint64 samples = 0;
};

static Analysis analyzeFloat32(const QByteArray &payload)
{
    // payload = seq(4) + float32[]
    const char *base = payload.constData() + 4;
    const auto *s = reinterpret_cast<const float *>(base);
    const qint64 n = (payload.size() - 4) / static_cast<qint64>(sizeof(float));
    Analysis a;
    a.samples = n;
    if (n <= 0) return a;
    double sum = 0, sq = 0;
    float mn = s[0], mx = s[0];
    // простое скалярное прохождение; для продакшена тут можно QtConcurrent/SIMD
    for (qint64 i = 0; i < n; ++i) {
        const float v = s[i];
        sum += v;
        sq += double(v) * double(v);
        if (v < mn) mn = v;
        if (v > mx) mx = v;
    }
    a.mean = sum / n;
    a.minV = mn;
    a.maxV = mx;
    a.rms  = std::sqrt(sq / n);
    return a;
}

// ------------------------------- сессия клиента -----------------------------
class ClientSession : public QObject {
    Q_OBJECT
public:
    ClientSession(QTcpSocket *sock, bool analyze, int ackIntervalMs)
        : m_sock(sock), m_analyze(analyze)
    {
        m_sock->setSocketOption(QAbstractSocket::LowDelayOption, 0); // нэгл-дагл off — выше throughput
        m_sock->setSocketOption(QAbstractSocket::KeepAliveOption, 1);

        connect(m_sock, &QTcpSocket::readyRead, this, &ClientSession::onReadyRead);
        connect(m_sock, &QTcpSocket::disconnected, this, &ClientSession::onDisconnected);

        m_timer.start();
        m_ackTimer.setInterval(ackIntervalMs);
        connect(&m_ackTimer, &QTimer::timeout, this, &ClientSession::sendAck);
        m_ackTimer.start();

        out << "[" << m_sock->peerAddress().toString() << "] подключение\n"; out.flush();
    }

signals:
    void finished(ClientSession *self);

private slots:
    void onReadyRead()
    {
        // читаем ВСЁ доступное — так обрабатываются многомегабайтные потоки
        const QByteArray chunk = m_sock->readAll();
        m_rawBytes += chunk.size();
        m_parser.append(chunk);

        quint8 type;
        QByteArray payload;
        while (true) {
            proto::FrameParser::Result r = m_parser.next(type, payload);
            if (r == proto::FrameParser::NeedMore) break;
            if (r == proto::FrameParser::BadMagic) {
                out << m_deviceId << ": ошибка протокола (битый кадр), закрываю сессию\n"; out.flush();
                m_sock->disconnectFromHost();
                return;
            }
            handleFrame(type, payload);
        }
    }

    void handleFrame(quint8 type, const QByteArray &payload)
    {
        using namespace proto;
        switch (type) {
        case Handshake: {
            DeviceInfo info;
            if (parseHandshake(payload, info)) {
                m_info = info;
                m_deviceId = info.deviceId;
                out << QString("%1: handshake model=%2 fw=%3 fs=")
                           .arg(info.deviceId, info.model, info.firmware)
                        << QString::number(info.sampleRateHz, 'f', 0)
                        << QString(" Hz ch=%1").arg(info.channels) << "\n";
                out.flush();
            }
            break;
        }
        case Meta:
            out << m_deviceId << ": META " << QString::fromUtf8(payload) << "\n"; out.flush();
            break;
        case Data: {
            ++m_packets;
            m_payloadBytes += payload.size();
            if (m_analyze && payload.size() > 4) {
                Analysis a = analyzeFloat32(payload);
                if (m_packets % 50 == 1 || m_packets <= 3) {
                    out << QString("%1: DATA seq=%2 n=%3 mean=")
                               .arg(m_deviceId)
                               .arg(readSeq(payload))
                               .arg(a.samples)
                            << QString::number(a.mean, 'f', 4)
                            << QString(" min=%1").arg(a.minV, 0, 'f', 3)
                            << QString(" max=%1").arg(a.maxV, 0, 'f', 3)
                            << QString(" rms=%1").arg(a.rms, 0, 'f', 4) << "\n";
                    out.flush();
                }
            }
            break;
        }
        case Bye: {
            printSummary("получено BYE");
            m_sock->disconnectFromHost();
            break;
        }
        default:
            break;
        }
    }

    void sendAck()
    {
        if (m_sock->state() != QTcpSocket::ConnectedState) return;
        if (m_packets == m_lastAckPackets) return; // тишина — нечего подтверждать
        m_lastAckPackets = m_packets;
        QByteArray p;
        auto put64 = [&p](quint64 v) {
            for (int i = 7; i >= 0; --i) p.append(static_cast<char>((v >> (8 * i)) & 0xFF));
        };
        put64(m_packets);
        put64(m_payloadBytes);
        m_sock->write(proto::buildFrame(proto::Ack, p));
    }

    void onDisconnected()
    {
        printSummary("отключилось");
        emit finished(this);
    }

private:
    void printSummary(const char *reason)
    {
        const double sec = std::max(0.001, m_timer.elapsed() / 1000.0);
        const double mbps = m_payloadBytes / 1e6;
        out << QString("%1: %2. пакетов=%3, данные=")
                   .arg(m_deviceId.isEmpty() ? "?" : m_deviceId, reason)
                   .arg(m_packets)
                << QString::number(mbps, 'f', 1)
                << QString(" МБ, raw=") + QString::number(m_rawBytes / 1e6, 'f', 1)
                << QString(" МБ, скорость=") + QString::number(mbps * 8.0 / sec, 'f', 0)
                << " Мбит/с\n";
        out.flush();
    }

    QTcpSocket      *m_sock = nullptr;
    proto::FrameParser m_parser;
    proto::DeviceInfo  m_info;
    QString m_deviceId;
    bool    m_analyze = false;
    QElapsedTimer m_timer;
    QTimer  m_ackTimer;
    quint64 m_packets = 0;
    quint64 m_payloadBytes = 0;
    quint64 m_rawBytes = 0;
    quint64 m_lastAckPackets = 0;
};

// ---------------------------------- сервер ----------------------------------
int main(int argc, char *argv[])
{
    QCoreApplication app(argc, argv);
    QCoreApplication::setApplicationName("device_server");
    QCoreApplication::setApplicationVersion("1.0");

    QCommandLineParser parser;
    parser.setApplicationDescription(
        "Тестовый сервер Qt для приёма больших массивов данных от устройства.");
    parser.addHelpOption();
    parser.addVersionOption();
    QCommandLineOption optPort({"p", "port"}, "Порт TCP.", "port", "9000");
    QCommandLineOption optAck({"a", "ack-interval-ms"}, "Интервал ACK, мс.", "ms", "1000");
    QCommandLineOption optAnalyze({"x", "analyze"}, "Анализировать каждый пакет (медленнее).");
    parser.addOption(optPort);
    parser.addOption(optAck);
    parser.addOption(optAnalyze);
    parser.process(app);

    const quint16 port = parser.value(optPort).toUShort();
    const int ackMs    = parser.value(optAck).toInt();
    const bool analyze = parser.isSet(optAnalyze);

    QTcpServer server;
    QList<ClientSession*> sessions;

    QObject::connect(&server, &QTcpServer::newConnection, [&]() {
        while (server.hasPendingConnections()) {
            QTcpSocket *sock = server.nextPendingConnection();
            auto *sess = new ClientSession(sock, analyze, ackMs);
            sessions.append(sess);
            QObject::connect(sess, &ClientSession::finished, [&, sess]() {
                sessions.removeAll(sess);
                sess->deleteLater();
            });
        }
    });

    if (!server.listen(QHostAddress::Any, port)) {
        out << "Не удалось слушать порт " << port << ": " << server.errorString() << "\n";
        return 1;
    }
    out << QString("Сервер запущен на порту %1 (ACK каждые %2 мс, анализ=%3)")
               .arg(port).arg(ackMs).arg(analyze ? "вкл" : "выборочно") << "\n";
    out.flush();

    return app.exec();
}

#include "server.moc"
