// src/main.cpp
#include <csignal>
#include <cstdlib>
#include <memory>
#include <thread>

#include "ApiGateway.hpp"
#include "AsyncLogger.hpp"

namespace {

std::unique_ptr<apigateway::ApiGateway> g_gateway;

extern "C" void handleShutdownSignal(int /*signal*/) {
    if (g_gateway) {
        g_gateway->stop();
    }
}

} 

int main(int argc, char** argv) {
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT, handleShutdownSignal);
    std::signal(SIGTERM, handleShutdownSignal);

    uint16_t port = 8080;
    if (argc > 1) {
        const int parsedPort = std::atoi(argv[1]);
        if (parsedPort <= 0 || parsedPort > 65535) {
            apigateway::AsyncLogger::instance().error(
                std::string("Invalid port: ") + argv[1]);
            return EXIT_FAILURE;
        }
        port = static_cast<uint16_t>(parsedPort);
    }

    const unsigned int hwThreads = std::thread::hardware_concurrency();
    const size_t threadPoolSize = (hwThreads > 0) ? static_cast<size_t>(hwThreads) : 4;

    try {
        g_gateway = std::make_unique<apigateway::ApiGateway>(port, threadPoolSize);

        apigateway::Router& router = g_gateway->router();

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
        apigateway::AsyncLogger::instance().info(
            "API Gateway starting on port " + std::to_string(port) +
            " (thread pool size: " + std::to_string(threadPoolSize) + ")");

        g_gateway->run();
    } catch (const std::exception& ex) {
        apigateway::AsyncLogger::instance().error(
            std::string("Fatal error: ") + ex.what());
        return EXIT_FAILURE;
    }

    apigateway::AsyncLogger::instance().info("API Gateway shut down cleanly.");
    return EXIT_SUCCESS;
}