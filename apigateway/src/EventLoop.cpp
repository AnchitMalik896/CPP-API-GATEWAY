#include "EventLoop.hpp"

#include <array>
#include <cstring>
#include <stdexcept>
#include <sys/event.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace apigateway {

namespace {
constexpr int  kMaxEventsPerPoll = 256;
constexpr long kPollTimeoutNanos = 250'000'000L;
} // namespace

EventLoop::EventLoop() : kq_(kqueue()) {
    if (kq_ < 0) {
        throw std::runtime_error(std::string("kqueue() failed: ") + std::strerror(errno));
    }
}

EventLoop::~EventLoop() {
    if (kq_ >= 0) {
        ::close(kq_);
    }
}

void EventLoop::applyChange(int fd, int16_t filter, uint16_t flags) noexcept {
    struct kevent change{};
    EV_SET(&change, fd, filter, flags, 0, 0, nullptr);
    (void)::kevent(kq_, &change, 1, nullptr, 0, nullptr);
}

void EventLoop::addReadWatch(int fd, IOCallback callback) {
    const bool wasRegistered = readCallbacks_.contains(fd);
    readCallbacks_[fd] = std::move(callback);
    if (!wasRegistered) {
        applyChange(fd, EVFILT_READ, EV_ADD | EV_ENABLE);
    }
}

void EventLoop::addWriteWatch(int fd, IOCallback callback) {
    const bool wasRegistered = writeCallbacks_.contains(fd);
    writeCallbacks_[fd] = std::move(callback);
    if (!wasRegistered) {
        applyChange(fd, EVFILT_WRITE, EV_ADD | EV_ENABLE);
    }
}

void EventLoop::removeWriteWatch(int fd) noexcept {
    if (writeCallbacks_.erase(fd) > 0) {
        applyChange(fd, EVFILT_WRITE, EV_DELETE);
    }
}

void EventLoop::removeAll(int fd) noexcept {
    if (readCallbacks_.erase(fd) > 0) {
        applyChange(fd, EVFILT_READ, EV_DELETE);
    }
    if (writeCallbacks_.erase(fd) > 0) {
        applyChange(fd, EVFILT_WRITE, EV_DELETE);
    }
}

void EventLoop::run() {
    running_.store(true, std::memory_order_relaxed);

    std::vector<struct kevent> events(static_cast<size_t>(kMaxEventsPerPoll));
    const struct timespec timeout {
        .tv_sec  = 0,
        .tv_nsec = kPollTimeoutNanos,
    };

    while (running_.load(std::memory_order_relaxed)) {
        const int numEvents = ::kevent(
            kq_, nullptr, 0, events.data(), static_cast<int>(events.size()), &timeout);

        if (numEvents < 0) {
            if (errno == EINTR) { continue; }
            throw std::runtime_error(
                std::string("kevent() wait failed: ") + std::strerror(errno));
        }

        for (int i = 0; i < numEvents; ++i) {
            const struct kevent& ev = events[static_cast<size_t>(i)];
            const int fd = static_cast<int>(ev.ident);

            if (ev.filter == EVFILT_READ) {
                auto it = readCallbacks_.find(fd);
                if (it != readCallbacks_.end()) {
                    it->second(fd);
                }
            } else if (ev.filter == EVFILT_WRITE) {
                auto it = writeCallbacks_.find(fd);
                if (it != writeCallbacks_.end()) {
                    it->second(fd);
                }
            }
        }
    }
}

void EventLoop::stop() noexcept {
    running_.store(false, std::memory_order_relaxed);
}

} // namespace apigateway
