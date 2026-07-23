#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "EventLoop.hpp"
#include "HttpParser.hpp"
#include "RateLimiter.hpp"
#include "Router.hpp"

namespace apigateway {

class HttpServer {
public:
    HttpServer(uint16_t port, Router& router, RateLimiter& rateLimiter);
    ~HttpServer();

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;
    HttpServer(HttpServer&&) = delete;
    HttpServer& operator=(HttpServer&&) = delete;

    void run();
    void stop() noexcept;

private:
    struct Connection {
        int         fd           = -1;
        HttpParser  parser;
        std::string writeBuffer;
        size_t      writeOffset  = 0;
    };

    void onListenReadable(int fd);
    void onConnectionReadable(int fd);
    void onConnectionWritable(int fd);
    void closeConnection(int fd) noexcept;

    void dispatchRequest(Connection& conn, const HttpRequest& request);

    static std::string buildResponse(int statusCode,
                                      std::string_view statusText,
                                      std::string_view body,
                                      std::string_view contentType =
                                          "text/plain; charset=utf-8");

    uint16_t     port_;
    int          listenFd_;
    Router&      router_;
    RateLimiter& rateLimiter_;
    EventLoop    eventLoop_;
    std::unordered_map<int, std::unique_ptr<Connection>> connections_;
};

} // namespace apigateway
