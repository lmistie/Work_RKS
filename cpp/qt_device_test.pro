QT -= gui
CONFIG += c++17 console release
CONFIG -= app_bundle

TARGET = qt_device_test
TEMPLATE = subdirs

SUBDIRS += device_server client_sim

device_server.SOURCES = server.cpp
device_server.HEADERS = protocol.h
device_server.QT += network
device_server.DESTDIR = $$PWD/build_qmake

client_sim.SOURCES = client_sim.cpp
client_sim.HEADERS = protocol.h
client_sim.QT += network
client_sim.DESTDIR = $$PWD/build_qmake
