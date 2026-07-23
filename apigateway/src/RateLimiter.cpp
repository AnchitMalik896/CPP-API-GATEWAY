#include "RateLimiter.hpp"

#include <chrono>

namespace apigateway {

RateLimiter::RateLimiter(size_t capacity, size_t refillRatePerSecond) noexcept
    : capacity_(capacity)
    , refillRatePerSecond_(refillRatePerSecond)
    , tokens_(capacity)
    , lastRefillTimeNs_(nowNanos()) {
}

int64_t RateLimiter::nowNanos() noexcept {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
}

void RateLimiter::refill() noexcept {
    constexpr int64_t kNanosPerSecond = 1'000'000'000LL;

    if (refillRatePerSecond_ == 0) {
        return;
    }

    const int64_t currentTime = nowNanos();
    int64_t previousTime = lastRefillTimeNs_.load(std::memory_order_relaxed);

    for (;;) {
        const int64_t elapsedNs = currentTime - previousTime;
        if (elapsedNs <= 0) {
            return;
        }

        const auto elapsedNsUnsigned = static_cast<uint64_t>(elapsedNs);
        const uint64_t tokensToAdd =
            (elapsedNsUnsigned * static_cast<uint64_t>(refillRatePerSecond_)) /
            static_cast<uint64_t>(kNanosPerSecond);

        if (tokensToAdd == 0) {
            return;
        }

        const int64_t nsConsumed =
            static_cast<int64_t>((tokensToAdd * static_cast<uint64_t>(kNanosPerSecond)) /
                                  static_cast<uint64_t>(refillRatePerSecond_));
        const int64_t newTimestamp = previousTime + nsConsumed;

        if (lastRefillTimeNs_.compare_exchange_weak(
                previousTime, newTimestamp,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) {
            size_t currentTokens = tokens_.load(std::memory_order_relaxed);
            for (;;) {
                const size_t newTokens =
                    (currentTokens + tokensToAdd > capacity_)
                        ? capacity_
                        : currentTokens + static_cast<size_t>(tokensToAdd);

                if (tokens_.compare_exchange_weak(
                        currentTokens, newTokens,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    break;
                }
            }
            return;
        }
    }
}

bool RateLimiter::tryAcquire(size_t cost) noexcept {
    if (cost == 0) {
        return true;
    }

    refill();

    size_t currentTokens = tokens_.load(std::memory_order_relaxed);
    for (;;) {
        if (currentTokens < cost) {
            return false;
        }

        const size_t newTokens = currentTokens - cost;
        if (tokens_.compare_exchange_weak(
                currentTokens, newTokens,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) {
            return true;
        }
    }
}

size_t RateLimiter::availableTokens() const noexcept {
    return tokens_.load(std::memory_order_relaxed);
}

size_t RateLimiter::capacity() const noexcept {
    return capacity_;
}

size_t RateLimiter::refillRatePerSecond() const noexcept {
    return refillRatePerSecond_;
}

} // namespace apigateway
