#pragma once

#include <cstdint>

namespace apigateway {

class Socket {
public:
    static int  createListeningSocket(uint16_t port, int backlog = 128);
    static void setNonBlocking(int fd);
    static void disableSigPipe(int fd) noexcept;
    static void closeSocket(int fd) noexcept;
};

} // namespace apigateway
