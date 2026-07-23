#include "HttpServer.hpp"
#include "Socket.hpp"

#include <array>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>

namespace apigateway {

namespace {
constexpr size_t kReadChunkSize = 8192;
} // namespace

HttpServer::HttpServer(uint16_t port, Router& router, RateLimiter& rateLimiter)
    : port_(port)
    , listenFd_(Socket::createListeningSocket(port))
    , router_(router)
    , rateLimiter_(rateLimiter) {
}

HttpServer::~HttpServer() {
    for (auto& [fd, conn] : connections_) {
        (void)conn;
        Socket::closeSocket(fd);
    }
    Socket::closeSocket(listenFd_);
}

void HttpServer::run() {
    eventLoop_.addReadWatch(listenFd_, [this](int fd) { onListenReadable(fd); });
    eventLoop_.run();
}

void HttpServer::stop() noexcept {
    eventLoop_.stop();
}

void HttpServer::onListenReadable(int /*fd*/) {
    for (;;) {
        sockaddr_in clientAddr{};
        socklen_t   clientAddrLen = sizeof(clientAddr);

        const int clientFd = ::accept(
            listenFd_,
            reinterpret_cast<sockaddr*>(&clientAddr),
            &clientAddrLen);

        if (clientFd < 0) { break; }

        Socket::setNonBlocking(clientFd);
        Socket::disableSigPipe(clientFd);

        auto conn  = std::make_unique<Connection>();
        conn->fd   = clientFd;
        connections_[clientFd] = std::move(conn);

        eventLoop_.addReadWatch(clientFd, [this](int fd) { onConnectionReadable(fd); });
    }
}

void HttpServer::onConnectionReadable(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) { return; }
    Connection& conn = *it->second;

    std::array<char, kReadChunkSize> buffer{};

    for (;;) {
        const ssize_t bytesRead = ::recv(fd, buffer.data(), buffer.size(), 0);

        if (bytesRead > 0) {
            const auto len      = static_cast<size_t>(bytesRead);
            const bool complete = conn.parser.feed(std::string_view(buffer.data(), len));

            if (conn.parser.hasError()) {
                conn.writeBuffer = buildResponse(400, "Bad Request", "Malformed HTTP request");
                conn.writeOffset = 0;
                eventLoop_.removeAll(fd);
                eventLoop_.addWriteWatch(fd, [this](int wfd) { onConnectionWritable(wfd); });
                return;
            }

            if (complete) {
                const HttpRequest request = conn.parser.takeRequest();
                dispatchRequest(conn, request);
                eventLoop_.removeAll(fd);
                eventLoop_.addWriteWatch(fd, [this](int wfd) { onConnectionWritable(wfd); });
                return;
            }

            if (len < buffer.size()) { return; }
            continue;
        }

        if (bytesRead == 0) { closeConnection(fd); return; }

        if (errno == EAGAIN || errno == EWOULDBLOCK) { return; }
        if (errno == EINTR)  { continue; }

        closeConnection(fd);
        return;
    }
}

void HttpServer::onConnectionWritable(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) { return; }
    Connection& conn = *it->second;

    while (conn.writeOffset < conn.writeBuffer.size()) {
        const char* data      = conn.writeBuffer.data() + conn.writeOffset;
        const size_t remaining = conn.writeBuffer.size() - conn.writeOffset;

        const ssize_t bytesSent = ::send(fd, data, remaining, 0);

        if (bytesSent > 0) {
            conn.writeOffset += static_cast<size_t>(bytesSent);
            continue;
        }

        if (bytesSent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { return; }
        if (bytesSent < 0 && errno == EINTR) { continue; }

        closeConnection(fd);
        return;
    }

    closeConnection(fd);
}

void HttpServer::closeConnection(int fd) noexcept {
    eventLoop_.removeAll(fd);
    Socket::closeSocket(fd);
    connections_.erase(fd);
}

void HttpServer::dispatchRequest(Connection& conn, const HttpRequest& request) {
    if (!rateLimiter_.tryAcquire()) {
        conn.writeBuffer = buildResponse(
            429, "Too Many Requests", "Rate limit exceeded. Please try again later.");
        conn.writeOffset = 0;
        return;
    }

    const RouteMatch match = router_.match(request.method, request.path);

    if (!match.found) {
        conn.writeBuffer = buildResponse(404, "Not Found", "No route matches this path/method.");
        conn.writeOffset = 0;
        return;
    }

    if (match.handler) {
        match.handler(match.params);
    }

    std::ostringstream body;
    body << "{\"status\":\"ok\",\"path\":\"" << request.path << "\",\"params\":{";
    bool first = true;
    for (const auto& [key, value] : match.params) {
        if (!first) { body << ","; }
        body << "\"" << key << "\":\"" << value << "\"";
        first = false;
    }
    body << "}}";

    conn.writeBuffer = buildResponse(200, "OK", body.str(), "application/json");
    conn.writeOffset = 0;
}

std::string HttpServer::buildResponse(int              statusCode,
                                       std::string_view statusText,
                                       std::string_view body,
                                       std::string_view contentType) {
    std::ostringstream response;
    response << "HTTP/1.1 " << statusCode << " " << statusText << "\r\n"
              << "Content-Type: "   << contentType   << "\r\n"
              << "Content-Length: " << body.size()   << "\r\n"
              << "Connection: close\r\n"
              << "\r\n"
              << body;
    return response.str();
}

} // namespace apigateway
