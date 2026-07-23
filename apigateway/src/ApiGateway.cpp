// src/ApiGateway.cpp
#include "ApiGateway.hpp"
#include "Socket.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cctype>
#include <cstring>
#include <netinet/in.h>
#include <sstream>
#include <stdexcept>
#include <sys/event.h>
#include <sys/socket.h>
#include <unistd.h>

namespace apigateway {

namespace {

constexpr size_t kReadChunkSize = 16 * 1024;
constexpr size_t kMaxHeaderSectionLength = 32 * 1024;
constexpr size_t kMaxBodyLength = 10 * 1024 * 1024;
constexpr int kMaxEventsPerPoll = 256;
constexpr long kPollTimeoutNanos = 250'000'000L; 

constexpr uintptr_t kWakeIdent = 1;

std::string_view trimView(std::string_view s) noexcept {
    size_t begin = 0;
    while (begin < s.size() && std::isspace(static_cast<unsigned char>(s[begin])) != 0) {
        ++begin;
    }
    size_t end = s.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(s[end - 1])) != 0) {
        --end;
    }
    return s.substr(begin, end - begin);
}

} 


ApiGateway::ApiGateway(uint16_t port, size_t threadPoolSize)
    : port_(port)
    , kq_(kqueue())
    , listenFd_(-1)
    , rateLimiter_(/*capacity=*/500, /*refillRatePerSecond=*/250)
    , threadPool_(threadPoolSize) {
    if (kq_ < 0) {
        throw std::runtime_error(std::string("kqueue() failed: ") + std::strerror(errno));
    }

    try {
        listenFd_ = Socket::createListeningSocket(port_);
    } catch (...) {
        ::close(kq_);
        throw;
    }

    setupReactorWakeChannel();
    registerEvent(listenFd_, EVFILT_READ, EV_ADD | EV_ENABLE);
}

ApiGateway::~ApiGateway() {
    for (auto& [fd, conn] : connections_) {
        (void)conn;
        Socket::closeSocket(fd);
    }
    Socket::closeSocket(listenFd_);
    if (kq_ >= 0) {
        ::close(kq_);
    }
}

Router& ApiGateway::router() noexcept {
    return router_;
}

RateLimiter& ApiGateway::rateLimiter() noexcept {
    return rateLimiter_;
}

void ApiGateway::setupReactorWakeChannel() {
    struct kevent change{};
    EV_SET(&change, kWakeIdent, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
    if (::kevent(kq_, &change, 1, nullptr, 0, nullptr) < 0) {
        throw std::runtime_error(
            std::string("kevent() failed to register wake channel: ") + std::strerror(errno));
    }
}

void ApiGateway::triggerWake() noexcept {
    struct kevent change{};
    EV_SET(&change, kWakeIdent, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
    (void)::kevent(kq_, &change, 1, nullptr, 0, nullptr);
}

void ApiGateway::registerEvent(int fd, int16_t filter, uint16_t flags) noexcept {
    struct kevent change{};
    
    EV_SET(&change, fd, filter, flags | EV_CLEAR, 0, 0, nullptr);
    (void)::kevent(kq_, &change, 1, nullptr, 0, nullptr);
}


void ApiGateway::run() {
    running_.store(true, std::memory_order_relaxed);

    std::vector<struct kevent> events(static_cast<size_t>(kMaxEventsPerPoll));
    const struct timespec timeout{
        .tv_sec = 0,
        .tv_nsec = kPollTimeoutNanos,
    };

    while (running_.load(std::memory_order_relaxed)) {
        const int numEvents = ::kevent(
            kq_, nullptr, 0, events.data(), static_cast<int>(events.size()), &timeout);

        if (numEvents < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::runtime_error(std::string("kevent() wait failed: ") + std::strerror(errno));
        }

        for (int i = 0; i < numEvents; ++i) {
            const struct kevent& ev = events[static_cast<size_t>(i)];

            if (ev.filter == EVFILT_USER) {
                drainCompletionQueue();
                continue;
            }

            const int fd = static_cast<int>(ev.ident);

            if (fd == listenFd_ && ev.filter == EVFILT_READ) {
                onListenReadable();
                continue;
            }

            if (ev.filter == EVFILT_READ) {
                onConnectionReadable(fd);
            } else if (ev.filter == EVFILT_WRITE) {
                onConnectionWritable(fd);
            }
        }

        drainCompletionQueue();
    }
}

void ApiGateway::stop() noexcept {
    running_.store(false, std::memory_order_relaxed);
    triggerWake();
}


void ApiGateway::onListenReadable() {
    for (;;) {
        sockaddr_in clientAddr{};
        socklen_t clientAddrLen = sizeof(clientAddr);

        const int clientFd = ::accept(
            listenFd_, reinterpret_cast<sockaddr*>(&clientAddr), &clientAddrLen);

        if (clientFd < 0) {
            break;
        }

        Socket::setNonBlocking(clientFd);
        Socket::disableSigPipe(clientFd);

        auto conn = std::make_unique<Connection>();
        conn->fd = clientFd;
        connections_[clientFd] = std::move(conn);

        registerEvent(clientFd, EVFILT_READ, EV_ADD | EV_ENABLE);
    }
}


void ApiGateway::onConnectionReadable(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) {
        return;
    }
    Connection& conn = *it->second;

    if (conn.awaitingCompletion) {
        return;
    }

    std::array<char, kReadChunkSize> buffer{};

    for (;;) {
        const ssize_t bytesRead = ::recv(fd, buffer.data(), buffer.size(), 0);

        if (bytesRead > 0) {
            conn.readBuffer.append(buffer.data(), static_cast<size_t>(bytesRead));

            if (conn.readBuffer.size() > kMaxHeaderSectionLength + kMaxBodyLength) {
                conn.writeBuffer = buildResponse(413, "Payload Too Large",
                                                  "Request exceeds maximum allowed size",
                                                  "text/plain; charset=utf-8");
                conn.writeOffset = 0;
                registerEvent(fd, EVFILT_READ, EV_DELETE);
                registerEvent(fd, EVFILT_WRITE, EV_ADD | EV_ENABLE);
                return;
            }

            size_t consumedBytes = 0;
            std::optional<ParsedRequestView> parsed;
            try {
                parsed = tryParseRequest(conn.readBuffer, consumedBytes);
            } catch (const std::invalid_argument&) {
                conn.writeBuffer = buildResponse(400, "Bad Request",
                                                  "Malformed HTTP request",
                                                  "text/plain; charset=utf-8");
                conn.writeOffset = 0;
                registerEvent(fd, EVFILT_READ, EV_DELETE);
                registerEvent(fd, EVFILT_WRITE, EV_ADD | EV_ENABLE);
                return;
            }

            if (parsed.has_value()) {

                conn.awaitingCompletion = true;
                registerEvent(fd, EVFILT_READ, EV_DELETE);

                std::string ownedBuffer = std::move(conn.readBuffer);
                conn.readBuffer.clear();

                dispatchToThreadPool(fd, *parsed, std::move(ownedBuffer));
                return;
            }

            if (static_cast<size_t>(bytesRead) < buffer.size()) {
                return;
            }
            continue;
        }

        if (bytesRead == 0) {
            closeConnection(fd);
            return;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        }
        if (errno == EINTR) {
            continue;
        }

        closeConnection(fd);
        return;
    }
}



bool ApiGateway::caseInsensitiveEquals(std::string_view a, std::string_view b) noexcept {
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

std::string_view ApiGateway::findHeader(const ParsedRequestView& request,
                                         std::string_view name) noexcept {
    for (const auto& [key, value] : request.headers) {
        if (caseInsensitiveEquals(key, name)) {
            return value;
        }
    }
    return {};
}

std::optional<ApiGateway::ParsedRequestView> ApiGateway::tryParseRequest(
    std::string_view raw, size_t& outConsumedBytes) {
    const size_t headerEnd = raw.find("\r\n\r\n");
    if (headerEnd == std::string_view::npos) {
        return std::nullopt; 
    }

    const std::string_view headerSection = raw.substr(0, headerEnd);
    size_t lineStart = 0;
    const size_t firstLineEnd = headerSection.find("\r\n");
    const std::string_view requestLine =
        (firstLineEnd == std::string_view::npos)
            ? headerSection
            : headerSection.substr(0, firstLineEnd);

    const size_t firstSpace = requestLine.find(' ');
    if (firstSpace == std::string_view::npos) {
        throw std::invalid_argument("Missing method/target separator in request line");
    }
    const size_t secondSpace = requestLine.find(' ', firstSpace + 1);
    if (secondSpace == std::string_view::npos) {
        throw std::invalid_argument("Missing target/version separator in request line");
    }

    const std::string_view methodStr = requestLine.substr(0, firstSpace);
    const std::string_view target =
        requestLine.substr(firstSpace + 1, secondSpace - firstSpace - 1);
    const std::string_view versionStr = requestLine.substr(secondSpace + 1);

    if (target.empty() || target.front() != '/') {
        throw std::invalid_argument("Request target must be an absolute path");
    }

    ParsedRequestView request;
    try {
        request.method = httpMethodFromString(methodStr);
    } catch (const std::invalid_argument&) {
        throw;
    }

    const size_t queryPos = target.find('?');
    if (queryPos == std::string_view::npos) {
        request.path = target;
    } else {
        request.path = target.substr(0, queryPos);
        request.query = target.substr(queryPos + 1);
    }
    request.version = versionStr;

    lineStart = (firstLineEnd == std::string_view::npos) ? headerSection.size()
                                                           : firstLineEnd + 2;

    while (lineStart < headerSection.size()) {
        const size_t lineEnd = headerSection.find("\r\n", lineStart);
        const std::string_view line =
            (lineEnd == std::string_view::npos)
                ? headerSection.substr(lineStart)
                : headerSection.substr(lineStart, lineEnd - lineStart);

        if (!line.empty()) {
            const size_t colon = line.find(':');
            if (colon == std::string_view::npos) {
                throw std::invalid_argument("Malformed header line (missing colon)");
            }
            const std::string_view key = trimView(line.substr(0, colon));
            const std::string_view value = trimView(line.substr(colon + 1));
            if (key.empty()) {
                throw std::invalid_argument("Malformed header line (empty key)");
            }
            request.headers.emplace(key, value);
        }

        if (lineEnd == std::string_view::npos) {
            break;
        }
        lineStart = lineEnd + 2;
    }

   
    size_t contentLength = 0;
    const std::string_view contentLengthHeader = findHeader(request, "content-length");
    if (!contentLengthHeader.empty()) {
        for (char c : contentLengthHeader) {
            if (std::isdigit(static_cast<unsigned char>(c)) == 0) {
                throw std::invalid_argument("Invalid Content-Length value");
            }
        }
        try {
            contentLength = static_cast<size_t>(std::stoul(std::string(contentLengthHeader)));
        } catch (const std::exception&) {
            throw std::invalid_argument("Invalid Content-Length value");
        }
        if (contentLength > kMaxBodyLength) {
            throw std::invalid_argument("Content-Length exceeds maximum allowed body size");
        }
    }

    const size_t bodyStart = headerEnd + 4; 
    if (raw.size() - bodyStart < contentLength) {
        return std::nullopt; 
    }

    request.body = raw.substr(bodyStart, contentLength);
    outConsumedBytes = bodyStart + contentLength;
    return request;
}



void ApiGateway::dispatchToThreadPool(int fd, ParsedRequestView request, std::string ownedBuffer) {
  
    const std::string_view originalRaw(ownedBuffer);
    const size_t pathOffset = static_cast<size_t>(request.path.data() - originalRaw.data());
    const size_t pathLen = request.path.size();
    const size_t queryOffset =
        request.query.empty() ? 0 : static_cast<size_t>(request.query.data() - originalRaw.data());
    const size_t queryLen = request.query.size();
    const size_t bodyOffset =
        request.body.empty() ? originalRaw.size()
                              : static_cast<size_t>(request.body.data() - originalRaw.data());
    const size_t bodyLen = request.body.size();
    const HttpMethod method = request.method;

    threadPool_.enqueue([this, fd, method, pathOffset, pathLen, queryOffset, queryLen,
                          bodyOffset, bodyLen, buffer = std::move(ownedBuffer)]() mutable {
        const std::string_view raw(buffer);
        const std::string_view path = raw.substr(pathOffset, pathLen);
        const std::string_view query = (queryLen > 0) ? raw.substr(queryOffset, queryLen)
                                                       : std::string_view{};
        const std::string_view body = (bodyLen > 0) ? raw.substr(bodyOffset, bodyLen)
                                                     : std::string_view{};
        (void)query;
        (void)body;

        std::string responseBytes;

        if (!rateLimiter_.tryAcquire()) {
            responseBytes = buildResponse(429, "Too Many Requests",
                                           "Rate limit exceeded. Please try again later.",
                                           "text/plain; charset=utf-8");
        } else {
   
            const RouteMatch match = router_.match(method, std::string(path));

            if (!match.found) {
                responseBytes = buildResponse(404, "Not Found",
                                               "No route matches this path/method.",
                                               "text/plain; charset=utf-8");
            } else {
                if (match.handler) {
                    match.handler(match.params);
                }

                std::ostringstream json;
                json << "{\"status\":\"ok\",\"path\":\"" << path << "\",\"params\":{";
                bool first = true;
                for (const auto& [key, value] : match.params) {
                    if (!first) {
                        json << ",";
                    }
                    json << "\"" << key << "\":\"" << value << "\"";
                    first = false;
                }
                json << "}}";

                responseBytes = buildResponse(200, "OK", json.str(), "application/json");
            }
        }

        {
            std::lock_guard<std::mutex> lock(completionMutex_);
            completionQueue_.push(CompletionResult{fd, std::move(responseBytes)});
        }
        triggerWake();
    });
}

void ApiGateway::drainCompletionQueue() {
    for (;;) {
        CompletionResult result;
        {
            std::lock_guard<std::mutex> lock(completionMutex_);
            if (completionQueue_.empty()) {
                return;
            }
            result = std::move(completionQueue_.front());
            completionQueue_.pop();
        }

        auto it = connections_.find(result.fd);
        if (it == connections_.end()) {
          
            continue;
        }

        Connection& conn = *it->second;
        conn.writeBuffer = std::move(result.responseBytes);
        conn.writeOffset = 0;
        conn.awaitingCompletion = false;

        registerEvent(result.fd, EVFILT_WRITE, EV_ADD | EV_ENABLE);
    }
}



void ApiGateway::onConnectionWritable(int fd) {
    auto it = connections_.find(fd);
    if (it == connections_.end()) {
        return;
    }
    Connection& conn = *it->second;

    while (conn.writeOffset < conn.writeBuffer.size()) {
        const char* data = conn.writeBuffer.data() + conn.writeOffset;
        const size_t remaining = conn.writeBuffer.size() - conn.writeOffset;

        const ssize_t bytesSent = ::send(fd, data, remaining, 0);

        if (bytesSent > 0) {
            conn.writeOffset += static_cast<size_t>(bytesSent);
            continue;
        }

        if (bytesSent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return; 
        }
        if (bytesSent < 0 && errno == EINTR) {
            continue;
        }

        closeConnection(fd);
        return;
    }

   
    closeConnection(fd);
}

void ApiGateway::closeConnection(int fd) noexcept {
    registerEvent(fd, EVFILT_READ, EV_DELETE);
    registerEvent(fd, EVFILT_WRITE, EV_DELETE);
    Socket::closeSocket(fd);
    connections_.erase(fd);
}



std::string ApiGateway::buildResponse(int statusCode,
                                       std::string_view statusText,
                                       std::string_view body,
                                       std::string_view contentType) {
    std::ostringstream response;
    response << "HTTP/1.1 " << statusCode << " " << statusText << "\r\n"
              << "Content-Type: " << contentType << "\r\n"
              << "Content-Length: " << body.size() << "\r\n"
              << "Connection: close\r\n"
              << "\r\n"
              << body;
    return response.str();
}

} 