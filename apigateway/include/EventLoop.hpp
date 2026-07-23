#pragma once

#include <atomic>
#include <functional>
#include <unordered_map>

namespace apigateway {

class EventLoop {
public:
    using IOCallback = std::function<void(int fd)>;

    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;
    EventLoop(EventLoop&&) = delete;
    EventLoop& operator=(EventLoop&&) = delete;

    void addReadWatch(int fd, IOCallback callback);
    void addWriteWatch(int fd, IOCallback callback);
    void removeWriteWatch(int fd) noexcept;
    void removeAll(int fd) noexcept;

    void run();
    void stop() noexcept;

private:
    void applyChange(int fd, int16_t filter, uint16_t flags) noexcept;

    int kq_;
    std::atomic<bool> running_{false};

    std::unordered_map<int, IOCallback> readCallbacks_;
    std::unordered_map<int, IOCallback> writeCallbacks_;
};

} // namespace apigateway
