// include/Socket.hpp
#pragma once

#include <cstdint>
#include <string>

namespace apigateway {

class Socket {
public:
    static int createListeningSocket(uint16_t port, int backlog = 128);
    static int createConnectingSocket(const std::string& ip, uint16_t port);
    static void setNonBlocking(int fd);
    static void disableSigPipe(int fd) noexcept;
    static void closeSocket(int fd) noexcept;
};

} 