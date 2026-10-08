#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <unordered_map>

#include "ConnectionPool.hpp"
#include "ProxyManager.hpp"
#include "RateLimiter.hpp"
#include "Router.hpp"
#include "ThreadPool.hpp"

namespace apigateway {
class ApiGateway {
public:
    ApiGateway(uint16_t port, size_t threadPoolSize);

    // Must not run concurrently with run(). Joins every worker thread
    // before any state the workers use is destroyed or closed.
    ~ApiGateway();

    ApiGateway(const ApiGateway&) = delete;
    ApiGateway& operator=(const ApiGateway&) = delete;
    ApiGateway(ApiGateway&&) = delete;
    ApiGateway& operator=(ApiGateway&&) = delete;
    Router& router() noexcept;
    RateLimiter& rateLimiter() noexcept;

    // Runs the reactor until stop() is called or SIGINT/SIGTERM is
    // delivered through the kqueue (EVFILT_SIGNAL).
    void run();

    // Thread-safe; not async-signal-safe (it logs).
    void stop() noexcept;

private:
    struct Connection {
        int fd = -1;
        std::string readBuffer; 
        std::string writeBuffer; 
        size_t writeOffset = 0;
        bool awaitingCompletion = false; 
        size_t headerScanOffset = 0;
        bool headerParsed = false;
        size_t expectedRequestBytes = 0;
        std::string requestId;
    };

    
    struct CompletionResult {
        int fd = -1;
        std::string responseBytes;
    };

  
    struct ParsedRequestView {
        HttpMethod method = HttpMethod::GET;
        std::string_view path;
        std::string_view query;
        std::string_view version;
        std::unordered_map<std::string_view, std::string_view> headers;
        std::string_view body;
    };

    void setupReactorWakeChannel();
    void setupSignalHandling();
    void restoreSignalDispositions() noexcept;
    void onShutdownSignal(int signalNumber);
    void teardown() noexcept;
    void registerEvent(int fd, int16_t filter, uint16_t flags) noexcept;
    void triggerWake() noexcept;

    void onListenReadable();
    void onConnectionReadable(int fd);
    void onConnectionWritable(int fd);
    void drainCompletionQueue();
    void closeConnection(int fd) noexcept;

    // Cheap per-chunk gate: resumes the header terminator search where the
    // previous call stopped and enforces the request-line and header-section
    // limits while the headers are still arriving. Throws on a violation.
    static bool hasEnoughBytesToParse(Connection& conn);

    // Parses a buffer whose header terminator has arrived. Returns the request
    // when complete; otherwise nullopt, with outRequestBytes set to the total
    // size the request needs (header section + Content-Length).
    static std::optional<ParsedRequestView> tryParseRequest(std::string_view raw,
                                                             size_t& outRequestBytes);

    void rejectRequest(Connection& conn, int statusCode, std::string_view statusText,
                       std::string_view body, std::string_view reason);

    static bool caseInsensitiveEquals(std::string_view a, std::string_view b) noexcept;
    static std::string_view findHeader(const ParsedRequestView& request, std::string_view name) noexcept;

   
    void dispatchToThreadPool(int fd, std::string requestId, HttpMethod method,
                               size_t pathOffset, size_t pathLen,
                               size_t queryOffset, size_t queryLen,
                               size_t bodyOffset, size_t bodyLen,
                               std::string ownedBuffer,
                               std::unordered_map<std::string, std::string> headers);

    // Member order is a lifetime contract: members are destroyed in
    // reverse order, so everything a worker task touches (router_,
    // rateLimiter_, connectionPool_, proxyManager_, completionMutex_,
    // completionQueue_, kq_) is declared BEFORE threadPool_, which is
    // declared last and therefore destroyed (and its workers joined)
    // first. ~ApiGateway() additionally resets threadPool_ explicitly
    // before closing kq_ and the sockets.
    uint16_t port_;
    int kq_;
    int listenFd_;
    std::atomic<bool> running_{false};
    std::atomic<bool> abandoningWork_{false};

    Router router_;
    RateLimiter rateLimiter_;
    ConnectionPool connectionPool_;
    ProxyManager proxyManager_;

    std::unordered_map<int, std::unique_ptr<Connection>> connections_;

    std::mutex completionMutex_;
    std::queue<CompletionResult> completionQueue_;

    std::unique_ptr<ThreadPool> threadPool_;
};

}
