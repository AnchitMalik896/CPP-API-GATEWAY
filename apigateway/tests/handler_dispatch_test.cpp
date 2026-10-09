#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "ApiGateway.hpp"

using namespace apigateway;

namespace {

int g_failures = 0;
int g_checks = 0;

#define CHECK(cond)                                                                   \
    do {                                                                              \
        ++g_checks;                                                                   \
        if (!(cond)) {                                                                \
            ++g_failures;                                                             \
            std::fprintf(stderr, "FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond);      \
        }                                                                             \
    } while (false)

struct Reply {
    int status = 0;
    std::string head;
    std::string body;

    [[nodiscard]] bool hasHeader(const std::string& line) const {
        return head.find("\r\n" + line + "\r\n") != std::string::npos;
    }
};

uint16_t freePort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (fd < 0 || ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        throw std::runtime_error("cannot reserve a port");
    }
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    const uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

int connectTo(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (fd >= 0 && ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
        return fd;
    }
    if (fd >= 0) {
        ::close(fd);
    }
    return -1;
}

Reply request(uint16_t port, const std::string& raw) {
    Reply reply;
    const int fd = connectTo(port);
    if (fd < 0) {
        return reply;
    }
    timeval timeout{5, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    size_t sent = 0;
    while (sent < raw.size()) {
        const ssize_t n = ::send(fd, raw.data() + sent, raw.size() - sent, 0);
        if (n <= 0) {
            break;
        }
        sent += static_cast<size_t>(n);
    }

    std::string data;
    char buffer[4096];
    for (;;) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0) {
            break;
        }
        data.append(buffer, static_cast<size_t>(n));
    }
    ::close(fd);

    if (data.size() >= 12 && data.compare(0, 5, "HTTP/") == 0) {
        reply.status = std::stoi(data.substr(9, 3));
    }
    const size_t split = data.find("\r\n\r\n");
    if (split != std::string::npos) {
        reply.head = data.substr(0, split + 2);
        reply.body = data.substr(split + 4);
    }
    return reply;
}

std::string get(const std::string& target) {
    return "GET " + target + " HTTP/1.1\r\nHost: t\r\n\r\n";
}

std::string post(const std::string& target, const std::string& body) {
    return "POST " + target + " HTTP/1.1\r\nHost: t\r\nContent-Length: " +
           std::to_string(body.size()) + "\r\n\r\n" + body;
}

// One-shot upstream that answers every request with a fixed reply.
class Upstream {
public:
    Upstream() : port_(freePort()) {
        listenFd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int on = 1;
        ::setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(port_);
        if (::bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
            ::listen(listenFd_, 8) != 0) {
            throw std::runtime_error("upstream cannot listen");
        }
        thread_ = std::thread([this] { serve(); });
    }

    ~Upstream() {
        stop_ = true;
        const int wake = connectTo(port_);
        if (wake >= 0) {
            ::close(wake);
        }
        thread_.join();
        ::close(listenFd_);
    }

    [[nodiscard]] uint16_t port() const { return port_; }

private:
    void serve() {
        static const std::string reply =
            "HTTP/1.1 202 Accepted\r\nContent-Type: text/plain\r\nContent-Length: 8\r\n"
            "Connection: close\r\n\r\nupstream";
        while (!stop_) {
            const int fd = ::accept(listenFd_, nullptr, nullptr);
            if (fd < 0) {
                continue;
            }
            char buffer[4096];
            std::string seen;
            while (seen.find("\r\n\r\n") == std::string::npos) {
                const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
                if (n <= 0) {
                    break;
                }
                seen.append(buffer, static_cast<size_t>(n));
            }
            if (!stop_) {
                ::send(fd, reply.data(), reply.size(), 0);
            }
            ::close(fd);
        }
    }

    uint16_t port_;
    int listenFd_ = -1;
    std::atomic<bool> stop_{false};
    std::thread thread_;
};

class Events {
public:
    void add(std::string event) {
        const std::lock_guard<std::mutex> lock(mutex_);
        events_.push_back(std::move(event));
    }
    std::vector<std::string> take() {
        const std::lock_guard<std::mutex> lock(mutex_);
        return std::exchange(events_, {});
    }

private:
    std::mutex mutex_;
    std::vector<std::string> events_;
};

void testRegistrationRules() {
    Router router;
    router.get("/a", [](const HttpRequest&, HttpResponse&) {});
    router.get("/p", [](const RouteParams&) {});

    const auto throws = [](auto&& fn) {
        try {
            fn();
        } catch (const std::invalid_argument&) {
            return true;
        }
        return false;
    };

    CHECK(throws([&] { router.get("/a", [](const HttpRequest&, HttpResponse&) {}); }));
    CHECK(throws([&] { router.get("/a", [](const RouteParams&) {}); }));
    CHECK(throws([&] { router.get("/p", [](const HttpRequest&, HttpResponse&) {}); }));
    CHECK(throws([&] { router.get("/e", RouteHandler{}); }));
    CHECK(throws([&] { router.get("/e", ParamsHandler{}); }));
    CHECK(!throws([&] { router.post("/a", [](const HttpRequest&, HttpResponse&) {}); }));

    RouteMatch typed = router.match(HttpMethod::GET, "/a");
    CHECK(typed.found && typed.handler != nullptr && !typed.echoResponse);
    RouteMatch legacy = router.match(HttpMethod::GET, "/p");
    CHECK(legacy.found && legacy.handler != nullptr && legacy.echoResponse);
    CHECK(!router.match(HttpMethod::GET, "/missing").found);

    router.proxy(HttpMethod::GET, "/px", "127.0.0.1", 9);
    RouteMatch proxied = router.match(HttpMethod::GET, "/px");
    CHECK(proxied.found && proxied.handler == nullptr && proxied.proxyTarget.has_value());
}

void testGateway() {
    const uint16_t port = freePort();
    Upstream upstream;
    Events events;

    ApiGateway gateway(port, 2);
    Router& router = gateway.router();

    router.post("/typed", [&](const HttpRequest& req, HttpResponse& res) {
        events.add("handler");
        res.setStatus(201).setHeader("X-Custom", "value").setBody("created:" + req.body);
    });
    router.get("/typed/:a/:b", [](const HttpRequest& req, HttpResponse& res) {
        res.setHeader("Content-Type", "text/plain")
            .setBody("a=" + req.params.at("a") + ";b=" + req.params.at("b") +
                     ";rid=" + (req.requestId.empty() ? "missing" : "set"));
    });
    router.get("/untouched", [](const HttpRequest&, HttpResponse&) {});
    router.get("/boom", [](const HttpRequest&, HttpResponse& res) {
        res.setStatus(201).setBody("partial");
        throw std::runtime_error("handler failure");
    });
    router.get("/health", [](const RouteParams&) {});
    router.get("/api/v1/users/:id", [](const RouteParams&) {});
    router.proxy(HttpMethod::GET, "/proxy", "127.0.0.1", upstream.port());

    gateway.use([&](HttpRequest& req, HttpResponse& res, Next next) {
        events.add("before:" + req.path);
        res.setHeader("X-Pre", "1");
        next();
        events.add("after:" + std::to_string(res.status()));
    });

    std::thread reactor([&] { gateway.run(); });

    for (int i = 0; i < 200 && connectTo(port) < 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    events.take();

    {
        const Reply r = request(port, post("/typed", "abc"));
        CHECK(r.status == 201);
        CHECK(r.body == "created:abc");
        CHECK(r.hasHeader("X-Custom: value"));
        CHECK(r.hasHeader("X-Pre: 1"));
        CHECK(r.hasHeader("Content-Length: 11"));
        CHECK(r.hasHeader("Connection: close"));
        CHECK((events.take() ==
               std::vector<std::string>{"before:/typed", "handler", "after:201"}));
    }
    {
        const Reply r = request(port, get("/typed/x1/y%202"));
        CHECK(r.status == 200);
        CHECK(r.body == "a=x1;b=y%202;rid=set");
        CHECK(r.hasHeader("Content-Type: text/plain"));
        events.take();
    }
    {
        const Reply r = request(port, get("/untouched"));
        CHECK(r.status == 200);
        CHECK(r.body.empty());
        CHECK(r.hasHeader("Content-Length: 0"));
        events.take();
    }
    {
        const Reply r = request(port, get("/nope"));
        CHECK(r.status == 404);
        CHECK(r.body == "No route matches this path/method.");
        CHECK(r.hasHeader("Content-Type: text/plain; charset=utf-8"));
        CHECK(r.hasHeader("X-Pre: 1"));
        CHECK((events.take() ==
               std::vector<std::string>{"before:/nope", "after:404"}));
    }
    {
        const Reply r = request(port, get("/boom"));
        CHECK(r.status == 500);
        CHECK(r.body == "Internal server error");
        CHECK(!r.hasHeader("X-Pre: 1"));
        events.take();
        CHECK(request(port, get("/health")).status == 200);
        events.take();
    }
    {
        const Reply r = request(port, get("/health"));
        CHECK(r.status == 200);
        CHECK(r.body == R"({"status":"ok","path":"/health","params":{}})");
        CHECK(r.hasHeader("Content-Type: application/json"));
        events.take();
    }
    {
        const Reply r = request(port, get("/api/v1/users/42"));
        CHECK(r.status == 200);
        CHECK(r.body == R"({"status":"ok","path":"/api/v1/users/42","params":{"id":"42"}})");
        events.take();
    }
    {
        const Reply r = request(port, get("/proxy"));
        CHECK(r.status == 202);
        CHECK(r.body == "upstream");
        CHECK((events.take() ==
               std::vector<std::string>{"before:/proxy", "after:202"}));
    }

    gateway.stop();
    reactor.join();
}

}

int main() {
    testRegistrationRules();
    testGateway();
    std::printf("%d checks, %d failed\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
