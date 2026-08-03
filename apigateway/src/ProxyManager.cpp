#include "ProxyManager.hpp"

#include "AsyncLogger.hpp"
#include "HttpClient.hpp"
#include "Socket.hpp"

#include <cctype>
#include <cerrno>
#include <chrono>
#include <poll.h>
#include <sstream>

namespace apigateway {

namespace {

bool caseInsensitiveEquals(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size()) {
        return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

bool isHopByHopHeader(std::string_view name) noexcept {
    return caseInsensitiveEquals(name, "host") ||
           caseInsensitiveEquals(name, "content-length") ||
           caseInsensitiveEquals(name, "connection") ||
           caseInsensitiveEquals(name, "transfer-encoding") ||
           caseInsensitiveEquals(name, "keep-alive") ||
           caseInsensitiveEquals(name, "proxy-connection");
}

std::string buildUpstreamTarget(std::string_view path, std::string_view query) {
    std::string target(path);
    if (!query.empty()) {
        target.push_back('?');
        target.append(query);
    }
    return target;
}

} // namespace

ProxyManager::ProxyManager(ConnectionPool& connectionPool,
                            std::chrono::milliseconds connectTimeout,
                            std::chrono::milliseconds ioTimeout) noexcept
    : connectionPool_(connectionPool)
    , connectTimeout_(connectTimeout)
    , ioTimeout_(ioTimeout) {
}

std::string ProxyManager::hostKeyFor(const ProxyTarget& target) {
    return target.host + ":" + std::to_string(target.port);
}

bool ProxyManager::waitForWritable(int fd, std::chrono::milliseconds timeout) const noexcept {
    struct pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLOUT;

    for (;;) {
        const int result = ::poll(&pfd, 1, static_cast<int>(timeout.count()));
        if (result > 0) {
            return true;
        }
        if (result == 0) {
            return false; // timed out
        }
        if (errno == EINTR) {
            continue;
        }
        return false;
    }
}

bool ProxyManager::waitForReadable(int fd, std::chrono::milliseconds timeout) const noexcept {
    struct pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLIN;

    for (;;) {
        const int result = ::poll(&pfd, 1, static_cast<int>(timeout.count()));
        if (result > 0) {
            return true;
        }
        if (result == 0) {
            return false; // timed out
        }
        if (errno == EINTR) {
            continue;
        }
        return false;
    }
}

std::string ProxyManager::buildErrorResponse(int statusCode,
                                              std::string_view statusText,
                                              std::string_view body) {
    std::ostringstream response;
    response << "HTTP/1.1 " << statusCode << " " << statusText << "\r\n"
              << "Content-Type: text/plain; charset=utf-8\r\n"
              << "Content-Length: " << body.size() << "\r\n"
              << "Connection: close\r\n"
              << "\r\n"
              << body;
    return response.str();
}

std::string ProxyManager::forward(const ProxyTarget& target,
                                   HttpMethod method,
                                   std::string_view path,
                                   std::string_view query,
                                   const std::unordered_map<std::string, std::string>& headers,
                                   std::string_view body,
                                   const std::string& requestId) {
    const auto startTime = std::chrono::steady_clock::now();
    const std::string hostKey = hostKeyFor(target);

    AsyncLogger::instance().info(
        "Proxy request started upstream=" + hostKey + " path=" + std::string(path));

    HttpClient client;
    bool reused = false;

    const int leasedFd = connectionPool_.lease(hostKey);
    if (leasedFd >= 0) {
        reused = client.adoptConnection(leasedFd, hostKey);
        if (!reused) {
            Socket::closeSocket(leasedFd);
        } else {
            AsyncLogger::instance().info("Proxy reused pooled connection upstream=" + hostKey);
        }
    }

    if (!reused) {
        try {
            client.connect(target.host, target.port);
        } catch (const std::exception&) {
            return buildErrorResponse(502, "Bad Gateway", "Failed to reach upstream host");
        }

        if (!waitForWritable(client.fd(), connectTimeout_)) {
            AsyncLogger::instance().warn(
                "Proxy request failed (connect timeout) upstream=" + hostKey);
            return buildErrorResponse(504, "Gateway Timeout", "Timed out connecting to upstream");
        }

        if (!client.completeConnect()) {
            AsyncLogger::instance().warn(
                "Proxy request failed (connect error) upstream=" + hostKey);
            return buildErrorResponse(502, "Bad Gateway", "Failed to connect to upstream host");
        }

        AsyncLogger::instance().info("Proxy connected upstream=" + hostKey);
    }

    std::unordered_map<std::string, std::string> upstreamHeaders;
    upstreamHeaders.reserve(headers.size() + 1);
    for (const auto& [key, value] : headers) {
        if (isHopByHopHeader(key)) {
            continue;
        }
        upstreamHeaders.emplace(key, value);
    }
    upstreamHeaders["Connection"] = "keep-alive";

    const std::string upstreamTarget = buildUpstreamTarget(path, query);

    try {
        client.prepareRequest(method, upstreamTarget, upstreamHeaders, body);
    } catch (const std::exception&) {
        return buildErrorResponse(502, "Bad Gateway", "Internal proxy error preparing request");
    }

    for (;;) {
        if (!client.send()) {
            AsyncLogger::instance().warn(
                "Proxy request failed (send error) upstream=" + hostKey);
            return buildErrorResponse(502, "Bad Gateway", "Failed to send request to upstream");
        }
        if (client.state() == HttpClientState::Receiving) {
            break; // fully sent
        }
        if (!waitForWritable(client.fd(), ioTimeout_)) {
            AsyncLogger::instance().warn(
                "Proxy request failed (send timeout) upstream=" + hostKey);
            return buildErrorResponse(504, "Gateway Timeout", "Timed out sending request to upstream");
        }
    }

    for (;;) {
        if (!client.receive()) {
            AsyncLogger::instance().warn(
                "Proxy request failed (receive error) upstream=" + hostKey);
            return buildErrorResponse(502, "Bad Gateway", "Upstream connection closed unexpectedly");
        }
        if (client.state() == HttpClientState::Complete) {
            break;
        }
        if (!waitForReadable(client.fd(), ioTimeout_)) {
            AsyncLogger::instance().warn(
                "Proxy request failed (receive timeout) upstream=" + hostKey);
            return buildErrorResponse(504, "Gateway Timeout", "Timed out waiting for upstream response");
        }
    }

    const HttpClientResponse& upstreamResponse = client.response();

    AsyncLogger::instance().info(
        "Proxy response received upstream=" + hostKey +
        " status=" + std::to_string(upstreamResponse.statusCode));

    bool upstreamKeepAlive = true;
    for (const auto& [key, value] : upstreamResponse.headers) {
        if (caseInsensitiveEquals(key, "connection")) {
            upstreamKeepAlive = caseInsensitiveEquals(value, "keep-alive");
            break;
        }
    }

    std::ostringstream out;
    out << "HTTP/1.1 " << upstreamResponse.statusCode << " "
        << (upstreamResponse.reasonPhrase.empty() ? "OK" : upstreamResponse.reasonPhrase) << "\r\n";
    for (const auto& [key, value] : upstreamResponse.headers) {
        if (isHopByHopHeader(key)) {
            continue;
        }
        out << key << ": " << value << "\r\n";
    }
    out << "Content-Length: " << upstreamResponse.body.size() << "\r\n";
    out << "Connection: close\r\n"; // the gateway always closes the downstream connection
    out << "\r\n";
    out << upstreamResponse.body;

    if (upstreamKeepAlive) {
        const int releasedFd = client.releaseFd();
        connectionPool_.release(hostKey, releasedFd);
    }
    const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - startTime)
                                .count();

    AsyncLogger::instance().info(
        "Proxy request complete upstream=" + hostKey +
        " status=" + std::to_string(upstreamResponse.statusCode) +
        " latencyMs=" + std::to_string(elapsedMs) +
        " reused=" + (reused ? std::string("true") : std::string("false")) +
        " requestId=" + requestId);

    return out.str();
}

} 