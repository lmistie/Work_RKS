// ============================================================================
//  client_sim.cpp — имитатор устройства-клиента на Qt6 (C++).
//  Генерирует БОЛЬШИЕ массивы данных (синтетический float32-сигнал:
//  гармоники + шум, как с АЦП) и потоково отправляет их серверу по TCP.
//
//  Параметры командной строки:
//    --host HOST            адрес сервера (по умолчанию 127.0.0.1)
//    --port PORT            порт сервера (по умолчанию 9000)
//    --packet-size SIZE     размер payload пакета: 1MB, 4MB, 512KB ... (1MB)
//    --packets N            сколько пакетов отправить (100)
//    --clients N            сколько "устройств" одновременно (1)
//    --interval MS          задержка между пакетами, имитация темпа АЦП (0)
//    --sample-rate HZ       частота дискретизации в handshake (1000000)
//
//  Сборка: см. CMakeLists.txt / client_sim.pro
//  Запуск: ./client_sim --packets 200 --packet-size 4MB --clients 3
// ============================================================================
#include <QCoreApplication>
#include <QTcpSocket>
#include <QCommandLineParser>
#include <QCommandLineOption>
#include <QElapsedTimer>
#include <QThread>
#include <QRandomGenerator>
#include <QTextStream>
#include <cmath>
#include <vector>
#include <atomic>

#include "protocol.h"

static std::atomic<quint64> g_totalBytes{0};
static QTextStream sout(stdout);

// ------------------------- генерация "сигнала с АЦП" ------------------------
// Один пакет = окно float32-отсчётов: сумма гармоник + гауссов шум.
static void fillSignal(float *buf, qint64 n, double t0sec, double fsHz)
{
    const double f1 = 1000.0, f2 = 7500.0, f3 = 23000.0;
    // быстрый LCG для воспроизводимого шума (быстрее QRandomGenerator на миллионах)
    quint64 st = 0x1234ABCDULL + quint64(t0sec * 1e6);
    auto frand = [&st]() -> double {
        st = st * 6364136223846793005ULL + 1442695040888963407ULL;
        return double(st >> 11) / double(1ULL << 53); // [0,1)
    };
    for (qint64 i = 0; i < n; ++i) {
        const double t = t0sec + double(i) / fsHz;
        double v = 0.8 * std::sin(2 * M_PI * f1 * t)
                 + 0.3 * std::sin(2 * M_PI * f2 * t + 0.7)
                 + 0.1 * std::sin(2 * M_PI * f3 * t + 1.3);
        v += 0.05 * (frand() + frand() + frand() - 1.5); // шум ~U(-0.075..0.075)
        buf[i] = float(v);
    }
}

static qint64 parseSize(const QString &s)
{
    QString t = s.toUpper().trimmed();
    qint64 mult = 1;
    if (t.endsWith("KB")) { mult = 1024; t.chop(2); }
    else if (t.endsWith("MB")) { mult = 1024 * 1024; t.chop(2); }
    else if (t.endsWith("B"))  { t.chop(1); }
    return t.toLongLong() * mult;
}

// ------------------------------ поток отправки ------------------------------
class Sender : public QObject {
    Q_OBJECT
public:
    Sender(QString host, quint16 port, int id, qint64 packetBytes,
           int packets, int intervalMs, double fsHz)
        : m_host(host), m_port(port), m_id(id),
          m_packetBytes(packetBytes), m_packets(packets),
          m_intervalMs(intervalMs), m_fs(fsHz) {}

signals:
    void done(int id);

public slots:
    void run()
    {
        QTcpSocket sock;
        sock.connectToHost(m_host, m_port);
        if (!sock.waitForConnected(3000)) {
            m_err = QString("Device-%1: не подключился: %2").arg(m_id).arg(sock.errorString());
            emit done(m_id);
            return;
        }

        // 1) handshake + meta
        sock.write(proto::buildFrame(proto::Handshake,
            proto::buildHandshake(QString("DEV-%1").arg(m_id, 3, 10, QChar('0')),
                                   "DAQ-4200", "fw-2.1.3", m_fs, 1, 1)));
        sock.write(proto::buildFrame(proto::Meta,
            QByteArray("{\"channel\":\"CH0\",\"unit\":\"V\",\"encoding\":\"float32-le\"}")));

        const qint64 samples = (m_packetBytes - 4) / qint64(sizeof(float));
        std::vector<float> buf(static_cast<size_t>(samples));
        QElapsedTimer timer; timer.start();
        quint64 sentBytes = 0;

        // 2) пакеты данных
        for (int p = 0; p < m_packets; ++p) {
            fillSignal(buf.data(), samples, double(p) * samples / m_fs, m_fs);
            QByteArray frame = proto::buildFrame(proto::Data,
                                                 proto::buildDataPayload(quint32(p), buf.data(), samples));
            // write+flush; при переполнении буфера сокета Qt сам дожимает через waitForBytesWritten
            sock.write(frame);
            while (!sock.flush()) {
                if (sock.state() != QTcpSocket::ConnectedState) break;
                sock.waitForBytesWritten(1000);
            }
            sentBytes += quint64(frame.size());
            if (m_intervalMs > 0) QThread::msleep(m_intervalMs);

            if ((p + 1) % 25 == 0 || p + 1 == m_packets) {
                double sec = std::max(0.001, timer.elapsed() / 1000.0);
                out << QString("Device-%1: отправлено %2/%3 пакетов (")
                           .arg(m_id).arg(p + 1).arg(m_packets)
                        << QString::number(sentBytes / 1e6, 'f', 1)
                        << QString(" МБ, ") + QString::number(sentBytes * 8.0 / sec / 1e6, 'f', 0)
                        << " Мбит/с)\n";
                out.flush();
            }
        }

        // 3) bye и читаем финальные ACK
        sock.write(proto::buildFrame(proto::Bye, QByteArray()));
        sock.waitForBytesWritten(2000);
        if (sock.waitForReadyRead(1500)) {
            proto::FrameParser fp;
            fp.append(sock.readAll());
            quint8 type; QByteArray pl;
            while (fp.next(type, pl) == proto::FrameParser::FrameReady) {
                if (type == proto::Ack && pl.size() >= 16) {
                    quint64 pk = 0, by = 0;
                    const uchar *d = reinterpret_cast<const uchar*>(pl.constData());
                    for (int i = 0; i < 8; ++i) pk = (pk << 8) | d[i];
                    for (int i = 8; i < 16; ++i) by = (by << 8) | d[i];
                    out << QString("Device-%1: сервер подтвердил packets=%2 bytes=%3")
                               .arg(m_id).arg(pk).arg(by) << "\n";
                    out.flush();
                }
            }
        }
        sock.disconnectFromHost();
        if (sock.state() != QTcpSocket::UnconnectedState)
            sock.waitForDisconnected(1000);

        g_totalBytes += sentBytes;
        double sec = std::max(0.001, timer.elapsed() / 1000.0);
        out << QString("Device-%1: готово. %2 пакетов, ")
                   .arg(m_id).arg(m_packets)
                << QString::number(sentBytes / 1e6, 'f', 1)
                << QString(" МБ, ") + QString::number(sentBytes * 8.0 / sec / 1e6, 'f', 0)
                << QString(" Мбит/с")
                << (m_err.isEmpty() ? QString() : (" | ОШИБКА: " + m_err)) << "\n";
        out.flush();
        emit done(m_id);
    }

private:
    QString m_host; quint16 m_port; int m_id;
    qint64 m_packetBytes; int m_packets; int m_intervalMs; double m_fs;
    QString m_err;
    static QTextStream out;
};
QTextStream Sender::out{stdout};

// ----------------------------------- main -----------------------------------
int main(int argc, char *argv[])
{
    QCoreApplication app(argc, argv);
    QCoreApplication::setApplicationName("client_sim");
    QCoreApplication::setApplicationVersion("1.0");

    QCommandLineParser parser;
    parser.setApplicationDescription(
        "Имитатор устройства: генерирует большие массивы данных и шлёт серверу.");
    parser.addHelpOption();
    QCommandLineOption oHost({"H", "host"}, "Адрес сервера.", "host", "127.0.0.1");
    QCommandLineOption oPort({"p", "port"}, "Порт сервера.", "port", "9000");
    QCommandLineOption oSize ({"s", "packet-size"}, "Размер пакета (1MB, 4MB, 512KB).", "size", "1MB");
    QCommandLineOption oCnt  ({"n", "packets"}, "Число пакетов.", "N", "100");
    QCommandLineOption oCli  ({"c", "clients"}, "Число одновременных устройств.", "N", "1");
    QCommandLineOption oIntv ({"i", "interval"}, "Пауза между пакетами, мс.", "ms", "0");
    QCommandLineOption oFs   ({"f", "sample-rate"}, "Частота дискретизации, Гц.", "Hz", "1000000");
    for (auto &o : {oHost, oPort, oSize, oCnt, oCli, oIntv, oFs}) parser.addOption(o);
    parser.process(app);

    const QString host = parser.value(oHost);
    const quint16 port = parser.value(oPort).toUShort();
    const qint64 pktSz = parseSize(parser.value(oSize));
    const int packets  = parser.value(oCnt).toInt();
    const int clients  = parser.value(oCli).toInt();
    const int interval = parser.value(oIntv).toInt();
    const double fs    = parser.value(oFs).toDouble();

    if (pktSz < 1024 || packets <= 0 || clients <= 0) {
        { QTextStream e(stderr); e << "Некорректные параметры.\n"; e.flush(); }
        return 2;
    }

    sout << QString("Запуск: %1 устройств x %2 пакетов x %3 байт -> %4:%5")
                       .arg(clients).arg(packets).arg(pktSz).arg(host).arg(port) << "\n";
    sout.flush();

    QList<QThread*> threads;
    int remaining = clients;
    bool hadError = false;

    for (int i = 0; i < clients; ++i) {
        QThread *th = new QThread;
        Sender *snd = new Sender(host, port, i + 1, pktSz, packets, interval, fs);
        snd->moveToThread(th);
        QObject::connect(th, &QThread::started, snd, &Sender::run);
        QObject::connect(snd, &Sender::done, [&](int id) {
            Q_UNUSED(id);
            if (--remaining == 0) QCoreApplication::quit();
        });
        QObject::connect(snd, &Sender::done, snd, &QObject::deleteLater);
        QObject::connect(th, &QThread::finished, th, &QObject::deleteLater);
        threads.append(th);
        th->start();
    }

    app.exec();
    for (QThread *th : threads) if (th->isRunning()) th->quit();

    sout << QString("Итого отправлено: %1 МБ\n").arg(g_totalBytes / 1e6, 0, 'f', 1);
    sout.flush();
    return hadError ? 1 : 0;
}

#include "client_sim.moc"
