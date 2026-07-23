#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace apigateway {

class RateLimiter {
public:
    RateLimiter(size_t capacity, size_t refillRatePerSecond) noexcept;

    RateLimiter(const RateLimiter&) = delete;
    RateLimiter& operator=(const RateLimiter&) = delete;
    RateLimiter(RateLimiter&&) = delete;
    RateLimiter& operator=(RateLimiter&&) = delete;

    bool tryAcquire(size_t cost = 1) noexcept;

    [[nodiscard]] size_t availableTokens() const noexcept;
    [[nodiscard]] size_t capacity() const noexcept;
    [[nodiscard]] size_t refillRatePerSecond() const noexcept;

private:
    void refill() noexcept;
    static int64_t nowNanos() noexcept;

    const size_t capacity_;
    const size_t refillRatePerSecond_;

    std::atomic<size_t>  tokens_;
    std::atomic<int64_t> lastRefillTimeNs_;
};

} // namespace apigateway
