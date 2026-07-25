// src/AsyncLogger.cpp
#include "AsyncLogger.hpp"

#include <ctime>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>

namespace apigateway {

namespace {

constexpr const char* kHexDigits = "0123456789abcdef";
constexpr size_t kRequestIdLength = 8;

thread_local std::string t_currentRequestId;

} // namespace

std::string_view logLevelToString(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::INFO:  return "INFO";
        case LogLevel::WARN:  return "WARN";
        case LogLevel::ERROR: return "ERROR";
    }
    return "UNKNOWN";
}

std::string generateRequestId() {
    // thread_local RNG: each producer thread (the reactor thread, and
    // each ThreadPool worker) owns its own generator, so id generation
    // never needs a lock and never contends across threads.
    thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<int> dist(0, 15);

    std::string id;
    id.reserve(kRequestIdLength);
    for (size_t i = 0; i < kRequestIdLength; ++i) {
        id.push_back(kHexDigits[dist(rng)]);
    }
    return id;
}

ScopedRequestId::ScopedRequestId(std::string requestId) noexcept
    : previous_(std::move(t_currentRequestId)) {
    t_currentRequestId = std::move(requestId);
}

ScopedRequestId::~ScopedRequestId() {
    t_currentRequestId = std::move(previous_);
}

const std::string& ScopedRequestId::current() noexcept {
    return t_currentRequestId;
}


AsyncLogger& AsyncLogger::instance() {
    static AsyncLogger logger;
    return logger;
}

AsyncLogger::AsyncLogger() {
    worker_ = std::thread([this]() { backgroundLoop(); });
}

AsyncLogger::~AsyncLogger() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    condition_.notify_all();

    if (worker_.joinable()) {
        worker_.join();
    }

    std::vector<LogRecord> remaining0;
    std::vector<LogRecord> remaining1;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        remaining0 = std::move(buffers_[0]);
        remaining1 = std::move(buffers_[1]);
        buffers_[0].clear();
        buffers_[1].clear();
    }
    writeRecords(remaining0);
    writeRecords(remaining1);
}

void AsyncLogger::setLogFile(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (path.empty()) {
        logFile_.close();
        return;
    }
    logFile_.open(path, std::ios::out | std::ios::app);
}

void AsyncLogger::log(LogLevel level, std::string_view message) {
    LogRecord record;
    record.timestamp = std::chrono::system_clock::now();
    record.level      = level;
    record.requestId  = t_currentRequestId;
    record.message.assign(message.data(), message.size());

    {
        std::lock_guard<std::mutex> lock(mutex_);
        buffers_[activeIndex_].push_back(std::move(record));
    }
    condition_.notify_one();
}

void AsyncLogger::flush() {
    std::unique_lock<std::mutex> lock(mutex_);
    flushRequested_ = true;
    condition_.notify_one();
    condition_.wait(lock, [this]() { return !flushRequested_; });
}

void AsyncLogger::backgroundLoop() {
    using namespace std::chrono_literals;

    for (;;) {
        std::vector<LogRecord> toWrite;
        bool stopNow = false;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            // Wake on new data / flush / shutdown, or at worst every
            // 200ms so buffered records don't sit around indefinitely
            // under light load.
            condition_.wait_for(lock, 200ms, [this]() {
                return stopping_ || flushRequested_ || !buffers_[activeIndex_].empty();
            });

            const int drained = activeIndex_;
            activeIndex_ = 1 - activeIndex_;
            toWrite.swap(buffers_[drained]);
            stopNow = stopping_;
        }

        if (!toWrite.empty()) {
            writeRecords(toWrite);
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (flushRequested_ && buffers_[0].empty() && buffers_[1].empty()) {
                flushRequested_ = false;
                condition_.notify_all();
            }
        }

        if (stopNow) {
            return;
        }
    }
}

void AsyncLogger::writeRecords(const std::vector<LogRecord>& records) {
    if (records.empty()) {
        return;
    }

    // Formatting and I/O both happen here, on the background thread only
    // -- never on the reactor thread or a ThreadPool worker.
    std::ostringstream out;
    for (const auto& record : records) {
        out << '[' << formatTimestamp(record.timestamp) << "] "
            << '[' << logLevelToString(record.level) << "] ";
        if (!record.requestId.empty()) {
            out << "[req_" << record.requestId << "] ";
        }
        out << record.message << '\n';
    }

    const std::string batch = out.str();

    std::cout << batch;
    std::cout.flush();

    if (logFile_.is_open()) {
        logFile_ << batch;
        logFile_.flush();
    }
}

std::string AsyncLogger::formatTimestamp(std::chrono::system_clock::time_point tp) {
    using namespace std::chrono;

    const auto sinceEpoch = tp.time_since_epoch();
    const auto seconds    = duration_cast<std::chrono::seconds>(sinceEpoch);
    const auto micros     = duration_cast<microseconds>(sinceEpoch - seconds);

    const auto timeT = static_cast<std::time_t>(seconds.count());
    std::tm tmBuffer{};
#if defined(_WIN32)
    gmtime_s(&tmBuffer, &timeT);
#else
    gmtime_r(&timeT, &tmBuffer);
#endif

    std::ostringstream out;
    out << std::put_time(&tmBuffer, "%Y-%m-%d %H:%M:%S")
        << '.' << std::setfill('0') << std::setw(6) << micros.count();
    return out.str();
}

} // namespace apigateway