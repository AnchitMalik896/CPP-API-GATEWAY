// src/main.cpp
#include <csignal>
#include <cstdlib>
#include <string>
#include <thread>

#include "ApiGateway.hpp"
#include "AsyncLogger.hpp"

int main(int argc, char** argv) {
    std::signal(SIGPIPE, SIG_IGN);

    // SIGINT/SIGTERM are not handled here: ApiGateway registers them with
    // its kqueue (EVFILT_SIGNAL) and shuts down from the reactor thread.

    // Touch the logger first. Function-local statics are destroyed in
    // reverse order of construction, so the logger outlives the gateway
    // (a local below) and every worker thread the gateway owns.
    apigateway::AsyncLogger& logger = apigateway::AsyncLogger::instance();

    uint16_t port = 8080;
    if (argc > 1) {
        const int parsedPort = std::atoi(argv[1]);
        if (parsedPort <= 0 || parsedPort > 65535) {
            logger.error(std::string("Invalid port: ") + argv[1]);
            logger.flush();
            return EXIT_FAILURE;
        }
        port = static_cast<uint16_t>(parsedPort);
    }

    const unsigned int hwThreads = std::thread::hardware_concurrency();
    const size_t threadPoolSize = (hwThreads > 0) ? static_cast<size_t>(hwThreads) : 4;

    try {
        apigateway::ApiGateway gateway(port, threadPoolSize);

        apigateway::Router& router = gateway.router();

        router.get("/health", [](const apigateway::RouteParams&) {
        });

        router.get("/api/v1/users", [](const apigateway::RouteParams&) {
        });

        router.get("/api/v1/users/:id", [](const apigateway::RouteParams& params) {
            auto it = params.find("id");
            if (it != params.end()) {
                apigateway::AsyncLogger::instance().info(
                    "Fetching user id=" + it->second);
            }
        });

        router.post("/api/v1/users", [](const apigateway::RouteParams&) {
        });

        router.get("/api/v1/orders", [](const apigateway::RouteParams&) {
        });

        router.get("/api/v1/orders/:id", [](const apigateway::RouteParams& params) {
            auto it = params.find("id");
            if (it != params.end()) {
                apigateway::AsyncLogger::instance().info(
                    "Fetching order id=" + it->second);
            }
        });

        router.post("/api/v1/orders", [](const apigateway::RouteParams&) {
        });

        router.proxy(
            apigateway::HttpMethod::GET,
            "/proxy",
            "127.0.0.1",
            9000
        );
        logger.info(
            "API Gateway starting on port " + std::to_string(port) +
            " (thread pool size: " + std::to_string(threadPoolSize) + ")");

        gateway.run();
        // `gateway` is destroyed at the end of this block: workers are
        // joined, then sockets and the kqueue are closed.
    } catch (const std::exception& ex) {
        logger.error(std::string("Fatal error: ") + ex.what());
        logger.flush();
        return EXIT_FAILURE;
    }

    logger.info("API Gateway shut down cleanly.");
    logger.flush();
    return EXIT_SUCCESS;
}
