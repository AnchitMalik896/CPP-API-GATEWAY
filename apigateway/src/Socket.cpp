#include "Socket.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <string>   
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace apigateway {

namespace {

[[noreturn]] void throwErrno(const char* what) {
    const int err = errno;
    throw std::runtime_error(std::string(what) + ": " + std::strerror(err));
}

} 

int Socket::createListeningSocket(uint16_t port, int backlog) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throwErrno("socket() failed");
    }

    const int reuse = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        const int err = errno;
        ::close(fd);
        errno = err;
        throwErrno("setsockopt(SO_REUSEADDR) failed");
    }

    const int noDelay = 1;
    (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &noDelay, sizeof(noDelay));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    if (::bind(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        const int err = errno;
        ::close(fd);
        errno = err;
        throwErrno("bind() failed");
    }

    if (::listen(fd, backlog) < 0) {
        const int err = errno;
        ::close(fd);
        errno = err;
        throwErrno("listen() failed");
    }

    setNonBlocking(fd);
    disableSigPipe(fd);

    return fd;
}

void Socket::setNonBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        throwErrno("fcntl(F_GETFL) failed");
    }
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throwErrno("fcntl(F_SETFL, O_NONBLOCK) failed");
    }
}

void Socket::disableSigPipe(int fd) noexcept {
#ifdef SO_NOSIGPIPE
    const int enable = 1;
    (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enable, sizeof(enable));
#else
    (void)fd;
#endif
}

void Socket::closeSocket(int fd) noexcept {
    if (fd >= 0) {
        ::close(fd);
    }
}

} 
