// src/ConnectionPool.cpp
#include "ConnectionPool.hpp"
#include "Socket.hpp"

#include <algorithm>
#include <cerrno>
#include <sys/socket.h>

namespace apigateway {

ConnectionPool::ConnectionPool(size_t maxIdlePerHost, std::chrono::seconds idleTimeout)
    : maxIdlePerHost_(maxIdlePerHost)
    , idleTimeout_(idleTimeout) {
}

ConnectionPool::~ConnectionPool() {
    closeAll();
}

bool ConnectionPool::isHealthy(int fd) noexcept {
    char probe = 0;
    const ssize_t result = ::recv(fd, &probe, 1, MSG_PEEK);

    if (result > 0) {
        return false;
    }
    if (result == 0) {
        return false;
    }
    return errno == EAGAIN || errno == EWOULDBLOCK;
}

int ConnectionPool::lease(const std::string& hostKey) {
    auto it = idleByHost_.find(hostKey);
    if (it == idleByHost_.end()) {
        return -1;
    }

    std::vector<PooledConnection>& idle = it->second;

    while (!idle.empty()) {
        const PooledConnection candidate = idle.back();
        idle.pop_back();

        if (isHealthy(candidate.fd)) {
            return candidate.fd;
        }

        Socket::closeSocket(candidate.fd);
    }

    return -1;
}

void ConnectionPool::release(const std::string& hostKey, int fd) {
    if (fd < 0) {
        return;
    }

    std::vector<PooledConnection>& idle = idleByHost_[hostKey];

    if (idle.size() >= maxIdlePerHost_) {
       
        auto oldestIt = std::min_element(
            idle.begin(), idle.end(),
            [](const PooledConnection& a, const PooledConnection& b) {
                return a.lastUsed < b.lastUsed;
            });
        Socket::closeSocket(oldestIt->fd);
        idle.erase(oldestIt);
    }

    idle.push_back(PooledConnection{fd, std::chrono::steady_clock::now()});
}

void ConnectionPool::discard(int fd) noexcept {
    Socket::closeSocket(fd);
}

void ConnectionPool::reapStale() {
    const auto now = std::chrono::steady_clock::now();

    for (auto& [hostKey, idle] : idleByHost_) {
        (void)hostKey;
        idle.erase(
            std::remove_if(idle.begin(), idle.end(),
                            [&](const PooledConnection& conn) {
                                const bool stale = (now - conn.lastUsed) > idleTimeout_;
                                if (stale) {
                                    Socket::closeSocket(conn.fd);
                                }
                                return stale;
                            }),
            idle.end());
    }
}

void ConnectionPool::closeAll() noexcept {
    for (auto& [hostKey, idle] : idleByHost_) {
        (void)hostKey;
        for (const PooledConnection& conn : idle) {
            Socket::closeSocket(conn.fd);
        }
        idle.clear();
    }
}

size_t ConnectionPool::idleConnectionCount() const noexcept {
    size_t total = 0;
    for (const auto& [hostKey, idle] : idleByHost_) {
        (void)hostKey;
        total += idle.size();
    }
    return total;
}

} 