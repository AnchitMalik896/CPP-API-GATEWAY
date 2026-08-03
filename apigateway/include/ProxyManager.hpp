// include/ProxyManager.hpp
#pragma once

#include <chrono>
#include <string>
#include <string_view>
#include <unordered_map>

#include "ConnectionPool.hpp"
#include "Router.hpp"

namespace apigateway {


class ProxyManager {
public:
    explicit ProxyManager(ConnectionPool& connectionPool,
                           std::chrono::milliseconds connectTimeout = std::chrono::milliseconds(2000),
                           std::chrono::milliseconds ioTimeout = std::chrono::milliseconds(5000)) noexcept;

    ProxyManager(const ProxyManager&) = delete;
    ProxyManager& operator=(const ProxyManager&) = delete;
    ProxyManager(ProxyManager&&) = delete;
    ProxyManager& operator=(ProxyManager&&) = delete;

    
    [[nodiscard]] std::string forward(const ProxyTarget& target,
                                       HttpMethod method,
                                       std::string_view path,
                                       std::string_view query,
                                       const std::unordered_map<std::string, std::string>& headers,
                                       std::string_view body,
                                       const std::string& requestId);

private:
    static std::string hostKeyFor(const ProxyTarget& target);

    [[nodiscard]] bool waitForWritable(int fd, std::chrono::milliseconds timeout) const noexcept;
    [[nodiscard]] bool waitForReadable(int fd, std::chrono::milliseconds timeout) const noexcept;

    static std::string buildErrorResponse(int statusCode,
                                           std::string_view statusText,
                                           std::string_view body);

    ConnectionPool& connectionPool_;
    std::chrono::milliseconds connectTimeout_;
    std::chrono::milliseconds ioTimeout_;
};

} 