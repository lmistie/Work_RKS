cmake_minimum_required(VERSION 3.16)
project(qt_device_test LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_AUTOMOC ON)          # moc для классов с Q_OBJECT (server.cpp / client_sim.cpp)

find_package(Qt6 REQUIRED COMPONENTS Core Network)

add_executable(device_server server.cpp protocol.h)
target_link_libraries(device_server PRIVATE Qt6::Core Qt6::Network)

add_executable(client_sim client_sim.cpp protocol.h)
target_link_libraries(client_sim PRIVATE Qt6::Core Qt6::Network)
