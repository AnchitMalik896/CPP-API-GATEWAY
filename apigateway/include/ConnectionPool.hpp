// include/ConnectionPool.hpp
#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

namespace apigateway {

class ConnectionPool {
public:
    ConnectionPool(size_t maxIdlePerHost, std::chrono::seconds idleTimeout);
    ~ConnectionPool();

    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;
    ConnectionPool(ConnectionPool&&) = delete;
    ConnectionPool& operator=(ConnectionPool&&) = delete;

    int lease(const std::string& hostKey);

    void release(const std::string& hostKey, int fd);

    void discard(int fd) noexcept;

    void reapStale();

    void closeAll() noexcept;

   
    [[nodiscard]] size_t idleConnectionCount() const noexcept;

private:
    struct PooledConnection {
        int fd = -1;
        std::chrono::steady_clock::time_point lastUsed;
    };

    static bool isHealthy(int fd) noexcept;

    size_t maxIdlePerHost_;
    std::chrono::seconds idleTimeout_;

    std::unordered_map<std::string, std::vector<PooledConnection>> idleByHost_;
};

} 