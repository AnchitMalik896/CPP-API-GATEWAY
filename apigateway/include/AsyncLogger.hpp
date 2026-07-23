// include/AsyncLogger.hpp
#pragma once

#include <chrono>
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace apigateway {

enum class LogLevel {
    INFO,
    WARN,
    ERROR
};

std::string_view logLevelToString(LogLevel level) noexcept;

// Generates a short, non-cryptographic unique request identifier (8 hex
// characters, e.g. "a1b2c3d4"). Safe to call concurrently from multiple
// threads -- each caller thread gets its own random engine, so this never
// contends on a lock.
std::string generateRequestId();

// RAII guard that sets the thread-local "current request id" for the
// duration of its scope. AsyncLogger::log() reads this automatically, so
// call sites (including Router route handlers, which have no request
// context parameter) don't need any signature change to get their log
// lines correlated to a request -- they just need to be on the call stack
// below a ScopedRequestId.
//
// Guards nest correctly: the previous value (if any) is restored when the
// guard goes out of scope.
class ScopedRequestId {
public:
    explicit ScopedRequestId(std::string requestId) noexcept;
    ~ScopedRequestId();

    ScopedRequestId(const ScopedRequestId&) = delete;
    ScopedRequestId& operator=(const ScopedRequestId&) = delete;
    ScopedRequestId(ScopedRequestId&&) = delete;
    ScopedRequestId& operator=(ScopedRequestId&&) = delete;

    // The request id currently in scope on this thread, or an empty
    // string if none has been set.
    [[nodiscard]] static const std::string& current() noexcept;

private:
    std::string previous_;
};

// Thread-safe, double-buffered async logger.
//
// Producers (the kqueue reactor thread, ThreadPool worker threads) call
// info()/warn()/error(). Each call builds its record on the caller's
// stack, then only holds the mutex long enough to push_back it into the
// active buffer -- no formatting and no I/O happens on the caller's
// thread, so the reactor loop and worker threads are never blocked on
// console/file writes.
//
// A single dedicated background thread periodically swaps the active and
// standby buffers under the lock (an O(1) vector swap), then formats and
// writes the standby buffer's contents to stdout (and optionally a file)
// without holding the lock.
class AsyncLogger {
public:
    static AsyncLogger& instance();

    AsyncLogger(const AsyncLogger&) = delete;
    AsyncLogger& operator=(const AsyncLogger&) = delete;
    AsyncLogger(AsyncLogger&&) = delete;
    AsyncLogger& operator=(AsyncLogger&&) = delete;

    // Optionally mirror log output to a file, in addition to stdout.
    // Intended to be called once, from main(), before the reactor starts
    // (i.e. before any other thread may be logging concurrently). Pass an
    // empty path to disable file output again.
    void setLogFile(const std::string& path);

    void log(LogLevel level, std::string_view message);

    void info(std::string_view message)  { log(LogLevel::INFO, message);  }
    void warn(std::string_view message)  { log(LogLevel::WARN, message);  }
    void error(std::string_view message) { log(LogLevel::ERROR, message); }

    // Blocks until every record accepted before this call has been
    // written out. Not required for correctness at process exit (the
    // destructor drains everything), but useful if a caller wants a
    // synchronization point, e.g. in tests.
    void flush();

private:
    struct LogRecord {
        std::chrono::system_clock::time_point timestamp;
        LogLevel level;
        std::string requestId;
        std::string message;
    };

    AsyncLogger();
    ~AsyncLogger();

    void backgroundLoop();
    void writeRecords(const std::vector<LogRecord>& records);
    static std::string formatTimestamp(std::chrono::system_clock::time_point tp);

    std::mutex mutex_;
    std::condition_variable condition_;
    std::vector<LogRecord> buffers_[2];
    int activeIndex_ = 0;
    bool stopping_ = false;
    bool flushRequested_ = false;

    std::ofstream logFile_;
    std::thread worker_;
};

} // namespace apigateway
