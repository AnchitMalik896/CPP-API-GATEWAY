// src/main.cpp
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>

#include "ApiGateway.hpp"

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
            std::cerr << "Invalid port: " << argv[1] << "\n";
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
                std::cout << "[handler] Fetching user id=" << it->second << "\n";
            }
        });

        router.post("/api/v1/users", [](const apigateway::RouteParams&) {
        });

        router.get("/api/v1/orders", [](const apigateway::RouteParams&) {
        });

        router.get("/api/v1/orders/:id", [](const apigateway::RouteParams& params) {
            auto it = params.find("id");
            if (it != params.end()) {
                std::cout << "[handler] Fetching order id=" << it->second << "\n";
            }
        });

        router.post("/api/v1/orders", [](const apigateway::RouteParams&) {
        });

        std::cout << "API Gateway listening on port " << port
                  << " (thread pool size: " << threadPoolSize << ")\n";

        g_gateway->run();
    } catch (const std::exception& ex) {
        std::cerr << "Fatal error: " << ex.what() << "\n";
        return EXIT_FAILURE;
    }

    std::cout << "API Gateway shut down cleanly.\n";
    return EXIT_SUCCESS;
}