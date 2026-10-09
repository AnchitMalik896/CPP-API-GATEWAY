// src/ApiGateway.cpp
#include "ApiGateway.hpp"
#include "AsyncLogger.hpp"
#include "HttpRequest.hpp"
#include "HttpResponse.hpp"
#include "JsonEscape.hpp"
#include "Socket.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstring>
#include <netinet/in.h>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <sys/event.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace apigateway {

namespace {

constexpr size_t kReadChunkSize = 16 * 1024;
constexpr size_t kMaxRequestLineLength = 8 * 1024;
constexpr size_t kMaxHeaderSectionLength = 32 * 1024;
constexpr size_t kMaxBodyLength = 10 * 1024 * 1024;

// A request violating a size limit. Derives from invalid_argument so any
// caller that only knows about malformed requests still maps it to 400.
struct RequestRejected : std::invalid_argument {
    RequestRejected(int statusCode, std::string_view statusPhrase,
                    std::string_view responseBody, const char* reason)
        : std::invalid_argument(reason)
        , status(statusCode)
        , phrase(statusPhrase)
        , body(responseBody) {}

    int status;
    std::string_view phrase;
    std::string_view body;
};

[[noreturn]] void throwRequestLineTooLong() {
    throw RequestRejected(414, "URI Too Long",
                           "Request line exceeds maximum allowed length",
                           "request line too long");
}

[[noreturn]] void throwHeaderSectionTooLarge() {
    throw RequestRejected(431, "Request Header Fields Too Large",
                           "Request headers exceed maximum allowed size",
                           "header section too large");
}

[[noreturn]] void throwBodyTooLarge() {
    throw RequestRejected(413, "Payload Too Large",
                           "Request exceeds maximum allowed size",
                           "Content-Length exceeds maximum allowed body size");
}

void fillTextResponse(HttpResponse& response, int statusCode, std::string_view reasonPhrase,
                      std::string_view body) {
    response.setStatus(statusCode, reasonPhrase)
        .setHeader("Content-Type", "text/plain; charset=utf-8")
        .setBody(std::string(body));
}

HttpResponse textResponse(int statusCode, std::string_view reasonPhrase, std::string_view body) {
    HttpResponse response;
    fillTextResponse(response, statusCode, reasonPhrase, body);
    return response;
}

// Status code of a serialized "HTTP/1.x NNN ..." response, or 0 if it is not one.
int statusFromWire(std::string_view wire) noexcept {
    if (wire.size() < 12 || wire.substr(0, 5) != "HTTP/" || wire[8] != ' ') {
        return 0;
    }
    int code = 0;
    for (size_t i = 9; i < 12; ++i) {
        if (wire[i] < '0' || wire[i] > '9') {
            return 0;
        }
        code = code * 10 + (wire[i] - '0');
    }
    return code >= 100 && code <= 599 ? code : 0;
}

// Builds the owned request from the buffer the reactor handed over. Path and
// query are copied; the body is the buffer itself, trimmed in place, so no
// second allocation is made for it.
HttpRequest makeRequest(HttpMethod method, std::string buffer,
                        size_t pathOffset, size_t pathLen,
                        size_t queryOffset, size_t queryLen,
                        size_t bodyOffset, size_t bodyLen,
                        std::unordered_map<std::string, std::string> headers,
                        std::string requestId) {
    HttpRequest request;
    request.method = method;
    request.path.assign(buffer, pathOffset, pathLen);
    request.query.assign(buffer, queryOffset, queryLen);
    request.headers = std::move(headers);
    request.requestId = std::move(requestId);

    if (bodyLen > 0) {
        buffer.erase(0, bodyOffset);
        buffer.resize(bodyLen);
        request.body = std::move(buffer);
    }
    return request;
}
constexpr int kMaxEventsPerPoll = 256;
constexpr long kPollTimeoutNanos = 250'000'000L;

constexpr uintptr_t kWakeIdent = 1;

constexpr int kShutdownSignals[] = {SIGINT, SIGTERM};

// Pooled upstream connections: how many idle connections we keep around
// per upstream host:port, and how long an idle connection may sit
// before ConnectionPool::reapStale() (if wired up by the caller) or a
// health probe on lease() considers it stale.
constexpr size_t kProxyMaxIdlePerHost = 16;
constexpr std::chrono::seconds kProxyIdleTimeout{60};

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

const char* signalName(int signalNumber) noexcept {
    switch (signalNumber) {
        case SIGINT:  return "SIGINT";
        case SIGTERM: return "SIGTERM";
        default:      return "signal";
    }
}

}


ApiGateway::ApiGateway(uint16_t port, size_t threadPoolSize)
    : port_(port)
    , kq_(kqueue())
    , listenFd_(-1)
    , rateLimiter_(/*capacity=*/500, /*refillRatePerSecond=*/250)
    , connectionPool_(kProxyMaxIdlePerHost, kProxyIdleTimeout)
    , proxyManager_(connectionPool_)
    , threadPool_(std::make_unique<ThreadPool>(threadPoolSize)) {
    if (kq_ < 0) {
        throw std::runtime_error(std::string("kqueue() failed: ") + std::strerror(errno));
    }

    try {
        listenFd_ = Socket::createListeningSocket(port_);
        setupReactorWakeChannel();
        registerEvent(listenFd_, EVFILT_READ, EV_ADD | EV_ENABLE);
        setupSignalHandling();
    } catch (...) {
        restoreSignalDispositions();
        Socket::closeSocket(listenFd_);
        ::close(kq_);
        throw;
    }
}

ApiGateway::~ApiGateway() {
    teardown();
}

// Deterministic teardown. Order matters:
//   1. restore default signal dispositions so a second Ctrl-C can force-kill
//      a gateway that is stuck draining;
//   2. tell not-yet-started queued tasks to skip their work;
//   3. destroy the ThreadPool, which joins every worker. Tasks already running
//      finish normally (they may still use kq_, the completion queue, the
//      connection pool, the proxy manager and the logger, all alive here);
//   4. only now, with no thread able to touch them, close the connections,
//      the listen socket and the kqueue.
void ApiGateway::teardown() noexcept {
    restoreSignalDispositions();
    running_.store(false, std::memory_order_relaxed);
    abandoningWork_.store(true, std::memory_order_release);

    try {
        AsyncLogger::instance().info("ApiGateway tearing down: joining worker threads");
    } catch (...) {
    }

    threadPool_.reset();

    for (auto& [fd, conn] : connections_) {
        (void)conn;
        Socket::closeSocket(fd);
    }
    connections_.clear();

    Socket::closeSocket(listenFd_);
    listenFd_ = -1;

    if (kq_ >= 0) {
        ::close(kq_);
        kq_ = -1;
    }
}

Router& ApiGateway::router() noexcept {
    return router_;
}

RateLimiter& ApiGateway::rateLimiter() noexcept {
    return rateLimiter_;
}

void ApiGateway::use(Middleware middleware) {
    if (running_.load(std::memory_order_relaxed)) {
        throw std::logic_error("middleware must be registered before run()");
    }
    middleware_.add(std::move(middleware));
}

void ApiGateway::setupReactorWakeChannel() {
    struct kevent change{};
    EV_SET(&change, kWakeIdent, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
    if (::kevent(kq_, &change, 1, nullptr, 0, nullptr) < 0) {
        throw std::runtime_error(
            std::string("kevent() failed to register wake channel: ") + std::strerror(errno));
    }
}

// EVFILT_SIGNAL observes signals even when their disposition is SIG_IGN, so
// no handler is installed at all: the kernel queues the event on the kqueue
// and the reactor handles it on its own thread.
void ApiGateway::setupSignalHandling() {
    for (const int sig : kShutdownSignals) {
        if (std::signal(sig, SIG_IGN) == SIG_ERR) {
            throw std::runtime_error(
                std::string("signal(SIG_IGN) failed for ") + signalName(sig) + ": " +
                std::strerror(errno));
        }

        struct kevent change{};
        EV_SET(&change, static_cast<uintptr_t>(sig), EVFILT_SIGNAL,
               EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
        if (::kevent(kq_, &change, 1, nullptr, 0, nullptr) < 0) {
            throw std::runtime_error(
                std::string("kevent() failed to register ") + signalName(sig) + ": " +
                std::strerror(errno));
        }
    }
}

void ApiGateway::restoreSignalDispositions() noexcept {
    for (const int sig : kShutdownSignals) {
        (void)std::signal(sig, SIG_DFL);
    }
}

void ApiGateway::onShutdownSignal(int signalNumber) {
    AsyncLogger::instance().info(
        std::string("received ") + signalName(signalNumber) + ", shutting down");
    running_.store(false, std::memory_order_relaxed);
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

            if (ev.filter == EVFILT_SIGNAL) {
                onShutdownSignal(static_cast<int>(ev.ident));
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
    try {
        AsyncLogger::instance().info("ApiGateway shutdown requested");
    } catch (...) {
    }
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
        conn->requestId = generateRequestId();
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

            std::optional<ParsedRequestView> parsed;
            try {
                if (hasEnoughBytesToParse(conn)) {
                    size_t requestBytes = 0;
                    parsed = tryParseRequest(conn.readBuffer, requestBytes);
                    if (!parsed.has_value() && requestBytes > 0) {
                        conn.headerParsed = true;
                        conn.expectedRequestBytes = requestBytes;
                    }
                }
            } catch (const RequestRejected& ex) {
                rejectRequest(conn, ex.status, ex.phrase, ex.body, ex.what());
                return;
            } catch (const std::invalid_argument& ex) {
                rejectRequest(conn, 400, "Bad Request", "Malformed HTTP request", ex.what());
                return;
            }

            if (parsed.has_value()) {

                conn.awaitingCompletion = true;
                registerEvent(fd, EVFILT_READ, EV_DELETE);

                const std::string_view originalRaw(conn.readBuffer);
                const size_t pathOffset =
                    static_cast<size_t>(parsed->path.data() - originalRaw.data());
                const size_t pathLen = parsed->path.size();
                const size_t queryOffset =
                    parsed->query.empty()
                        ? 0
                        : static_cast<size_t>(parsed->query.data() - originalRaw.data());
                const size_t queryLen = parsed->query.size();
                const size_t bodyOffset =
                    parsed->body.empty()
                        ? conn.readBuffer.size()
                        : static_cast<size_t>(parsed->body.data() - originalRaw.data());
                const size_t bodyLen = parsed->body.size();
                const HttpMethod method = parsed->method;

                // Copy the headers out into owned storage *before* the
                // buffer they view into is moved below. This is the only
                // extra state the proxy path needs: everything else
                // (path/query/body) is still reachable via offsets into
                // the moved buffer.
                std::unordered_map<std::string, std::string> headersOwned;
                headersOwned.reserve(parsed->headers.size());
                for (const auto& [key, value] : parsed->headers) {
                    headersOwned.emplace(std::string(key), std::string(value));
                }

                std::string ownedBuffer = std::move(conn.readBuffer);
                conn.readBuffer.clear();

                dispatchToThreadPool(fd, conn.requestId, method,
                                      pathOffset, pathLen, queryOffset, queryLen,
                                      bodyOffset, bodyLen, std::move(ownedBuffer),
                                      std::move(headersOwned));
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

bool ApiGateway::hasEnoughBytesToParse(Connection& conn) {
    if (conn.headerParsed) {
        return conn.readBuffer.size() >= conn.expectedRequestBytes;
    }

    const std::string& buffer = conn.readBuffer;
    const size_t resumeFrom = conn.headerScanOffset >= 3 ? conn.headerScanOffset - 3 : 0;
    if (buffer.find("\r\n\r\n", resumeFrom) != std::string::npos) {
        return true;
    }
    conn.headerScanOffset = buffer.size();

    if (buffer.size() > kMaxRequestLineLength + 1 && buffer.find("\r\n") == std::string::npos) {
        throwRequestLineTooLong();
    }
    if (buffer.size() >= kMaxHeaderSectionLength) {
        throwHeaderSectionTooLarge();
    }
    return false;
}

void ApiGateway::rejectRequest(Connection& conn, int statusCode, std::string_view statusText,
                                std::string_view body, std::string_view reason) {
    ScopedRequestId scopedId(conn.requestId);
    AsyncLogger::instance().warn(
        "rejecting request, fd=" + std::to_string(conn.fd) +
        " status=" + std::to_string(statusCode) + " reason=" + std::string(reason));

    conn.writeBuffer = textResponse(statusCode, statusText, body).toBytes();
    conn.writeOffset = 0;
    registerEvent(conn.fd, EVFILT_READ, EV_DELETE);
    registerEvent(conn.fd, EVFILT_WRITE, EV_ADD | EV_ENABLE);
}

std::optional<ApiGateway::ParsedRequestView> ApiGateway::tryParseRequest(
    std::string_view raw, size_t& outRequestBytes) {
    outRequestBytes = 0;

    const size_t headerEnd = raw.find("\r\n\r\n");
    if (headerEnd == std::string_view::npos) {
        return std::nullopt;
    }

    if (headerEnd + 4 > kMaxHeaderSectionLength) {
        throwHeaderSectionTooLarge();
    }

    const std::string_view headerSection = raw.substr(0, headerEnd);
    size_t lineStart = 0;
    const size_t firstLineEnd = headerSection.find("\r\n");
    const std::string_view requestLine =
        (firstLineEnd == std::string_view::npos)
            ? headerSection
            : headerSection.substr(0, firstLineEnd);

    if (requestLine.size() > kMaxRequestLineLength) {
        throwRequestLineTooLong();
    }

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
        } catch (const std::out_of_range&) {
            throwBodyTooLarge();
        } catch (const std::exception&) {
            throw std::invalid_argument("Invalid Content-Length value");
        }
        if (contentLength > kMaxBodyLength) {
            throwBodyTooLarge();
        }
    }

    const size_t bodyStart = headerEnd + 4;
    outRequestBytes = bodyStart + contentLength;
    if (raw.size() < outRequestBytes) {
        return std::nullopt;
    }

    request.body = raw.substr(bodyStart, contentLength);
    return request;
}



void ApiGateway::routeRequest(HttpRequest& request, HttpResponse& response,
                              std::optional<std::string>& proxiedBytes) {
    RouteMatch match = router_.match(request.method, request.path);

    if (!match.found) {
        AsyncLogger::instance().info("no route matched, path=" + request.path);
        fillTextResponse(response, 404, "Not Found", "No route matches this path/method.");
        return;
    }

    request.params = std::move(match.params);

    if (match.proxyTarget.has_value()) {
        proxiedBytes = proxyManager_.forward(*match.proxyTarget, request.method, request.path,
                                             request.query, request.headers, request.body,
                                             request.requestId);
        if (const int code = statusFromWire(*proxiedBytes); code != 0) {
            response.setStatus(code);
        }
        AsyncLogger::instance().info("response ready (proxied), path=" + request.path);
        return;
    }

    if (match.handler != nullptr) {
        (*match.handler)(request, response);
    }

    if (match.echoResponse) {
        std::string json = "{\"status\":\"ok\",\"path\":\"";
        appendJsonEscaped(json, request.path);
        json += "\",\"params\":{";
        bool first = true;
        for (const auto& [key, value] : request.params) {
            if (!first) {
                json += ',';
            }
            json += '"';
            appendJsonEscaped(json, key);
            json += "\":\"";
            appendJsonEscaped(json, value);
            json += '"';
            first = false;
        }
        json += "}}";

        response.setStatus(200)
            .setHeader("Content-Type", "application/json")
            .setBody(std::move(json));
    }

    AsyncLogger::instance().info("response ready, path=" + request.path +
                                 " status=" + std::to_string(response.status()));
}

void ApiGateway::dispatchToThreadPool(int fd, std::string requestId, HttpMethod method,
                                       size_t pathOffset, size_t pathLen,
                                       size_t queryOffset, size_t queryLen,
                                       size_t bodyOffset, size_t bodyLen,
                                       std::string ownedBuffer,
                                       std::unordered_map<std::string, std::string> headers) {

    threadPool_->enqueue([this, fd, method, pathOffset, pathLen, queryOffset, queryLen,
                          bodyOffset, bodyLen, requestId = std::move(requestId),
                          buffer = std::move(ownedBuffer),
                          headers = std::move(headers)]() mutable {
        // Tasks still queued when teardown begins are dropped without doing
        // any work: the reactor has already left its loop, so nobody could
        // deliver the response anyway.
        if (abandoningWork_.load(std::memory_order_acquire)) {
            return;
        }

        ScopedRequestId scopedId(requestId);

        // Last-resort 500. If even building it fails, an empty response is
        // returned: the reactor then closes the connection instead of
        // leaving it stuck in awaitingCompletion forever.
        const auto internalError = [](std::string_view what) {
            try {
                AsyncLogger::instance().error(
                    "unhandled exception while processing request: " + std::string(what));
            } catch (...) {
            }
            try {
                return textResponse(500, "Internal Server Error", "Internal server error")
                    .toBytes();
            } catch (...) {
                return std::string{};
            }
        };

        std::string responseBytes;

        try {
            HttpRequest request = makeRequest(method, std::move(buffer),
                                               pathOffset, pathLen, queryOffset, queryLen,
                                               bodyOffset, bodyLen,
                                               std::move(headers), std::move(requestId));

            if (!rateLimiter_.tryAcquire()) {
                AsyncLogger::instance().warn("rate limit exceeded, path=" + request.path);
                responseBytes = textResponse(429, "Too Many Requests",
                                              "Rate limit exceeded. Please try again later.")
                                    .toBytes();
            } else {
                HttpResponse response;
                std::optional<std::string> proxiedBytes;
                auto terminal = [this, &proxiedBytes](HttpRequest& req, HttpResponse& res) {
                    routeRequest(req, res, proxiedBytes);
                };
                middleware_.run(request, response, terminal);
                responseBytes = proxiedBytes ? std::move(*proxiedBytes) : response.toBytes();
            }
        } catch (const std::exception& ex) {
            responseBytes = internalError(ex.what());
        } catch (...) {
            responseBytes = internalError("non-standard exception");
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

}
