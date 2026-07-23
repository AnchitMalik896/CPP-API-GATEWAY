#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>

#include "HttpServer.hpp"
#include "RateLimiter.hpp"
#include "Router.hpp"

namespace {

std::unique_ptr<apigateway::HttpServer> g_server;

extern "C" void handleShutdownSignal(int /*signal*/) {
    if (g_server) {
        g_server->stop();
    }
}

} // namespace

int main(int argc, char** argv) {
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT,  handleShutdownSignal);
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

    apigateway::Router router;

    router.get("/health", [](const apigateway::RouteParams&) {
        // Liveness probe
    });

    router.get("/api/v1/users", [](const apigateway::RouteParams&) {
        // List users
    });

    router.get("/api/v1/users/:id", [](const apigateway::RouteParams& params) {
        auto it = params.find("id");
        if (it != params.end()) {
            std::cout << "[handler] Fetching user id=" << it->second << "\n";
        }
    });

    router.post("/api/v1/users", [](const apigateway::RouteParams&) {
        // Create user
    });

    // 100 token burst capacity, 50 tokens/second sustained refill
    apigateway::RateLimiter rateLimiter(100, 50);

    try {
        g_server = std::make_unique<apigateway::HttpServer>(port, router, rateLimiter);
        std::cout << "API Gateway listening on port " << port << "\n";
        g_server->run();
    } catch (const std::exception& ex) {
        std::cerr << "Fatal error: " << ex.what() << "\n";
        return EXIT_FAILURE;
    }

    std::cout << "API Gateway shut down cleanly.\n";
    return EXIT_SUCCESS;
}
