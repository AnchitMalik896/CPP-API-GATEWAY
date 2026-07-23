// include/ApiGateway.hpp
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <unordered_map>

#include "RateLimiter.hpp"
#include "Router.hpp"
#include "ThreadPool.hpp"

namespace apigateway {
class ApiGateway {
public:
    ApiGateway(uint16_t port, size_t threadPoolSize);
    ~ApiGateway();

    ApiGateway(const ApiGateway&) = delete;
    ApiGateway& operator=(const ApiGateway&) = delete;
    ApiGateway(ApiGateway&&) = delete;
    ApiGateway& operator=(ApiGateway&&) = delete;
    Router& router() noexcept;
    RateLimiter& rateLimiter() noexcept;
    void run();
    void stop() noexcept;

private:
    struct Connection {
        int fd = -1;
        std::string readBuffer; 
        std::string writeBuffer; 
        size_t writeOffset = 0;
        bool awaitingCompletion = false; 
        bool headerParsed = false;
        size_t headerBytesConsumed = 0;
        size_t contentLength = 0;
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
    void registerEvent(int fd, int16_t filter, uint16_t flags) noexcept;
    void triggerWake() noexcept;

    void onListenReadable();
    void onConnectionReadable(int fd);
    void onConnectionWritable(int fd);
    void drainCompletionQueue();
    void closeConnection(int fd) noexcept;

    static std::optional<ParsedRequestView> tryParseRequest(std::string_view raw,
                                                             size_t& outConsumedBytes);

    static bool caseInsensitiveEquals(std::string_view a, std::string_view b) noexcept;
    static std::string_view findHeader(const ParsedRequestView& request, std::string_view name) noexcept;

    static std::string buildResponse(int statusCode,
                                      std::string_view statusText,
                                      std::string_view body,
                                      std::string_view contentType);

    void dispatchToThreadPool(int fd, ParsedRequestView request, std::string ownedBuffer);

    uint16_t port_;
    int kq_;
    int listenFd_;
    std::atomic<bool> running_{false};

    Router router_;
    RateLimiter rateLimiter_;
    ThreadPool threadPool_;

    std::unordered_map<int, std::unique_ptr<Connection>> connections_;

    std::mutex completionMutex_;
    std::queue<CompletionResult> completionQueue_;
};

} 