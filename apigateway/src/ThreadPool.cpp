// src/ThreadPool.cpp
#include "ThreadPool.hpp"

namespace apigateway {

ThreadPool::ThreadPool(size_t threadCount) {
    if (threadCount == 0) {
        threadCount = 1;
    }

    workers_.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i) {
        workers_.emplace_back([this]() { workerLoop(); });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        stopping_ = true;
    }
    condition_.notify_all();

    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void ThreadPool::workerLoop() {
    for (;;) {
        std::function<void()> task;

        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            condition_.wait(lock, [this]() { return stopping_ || !tasks_.empty(); });

            if (stopping_ && tasks_.empty()) {
                return;
            }

            task = std::move(tasks_.front());
            tasks_.pop();
        }

       
        task();
    }
}

size_t ThreadPool::workerCount() const noexcept {
    return workers_.size();
}

size_t ThreadPool::pendingTasks() const {
    std::lock_guard<std::mutex> lock(queueMutex_);
    return tasks_.size();
}

}