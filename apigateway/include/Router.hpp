// include/Router.hpp
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace apigateway {

struct HttpRequest;
class HttpResponse;

enum class HttpMethod {
    GET,
    POST,
    PUT,
    DELETE,
    PATCH,
    HEAD,
    OPTIONS
};

HttpMethod httpMethodFromString(std::string_view method);
std::string_view httpMethodToString(HttpMethod method) noexcept;

using RouteParams  = std::unordered_map<std::string, std::string>;

// Typed handler: reads the request, fills in the response.
using RouteHandler  = std::function<void(const HttpRequest&, HttpResponse&)>;

// Params-only handler. The gateway answers such a route with the JSON echo of
// the path and params once the handler returns.
using ParamsHandler = std::function<void(const RouteParams& params)>;

struct ProxyTarget {
    std::string host;
    uint16_t    port = 0;
};

struct RouteMatch {
    bool         found = false;
    // Owned by the router and valid while its routes are not modified, i.e.
    // for the whole time the gateway is running. Null for proxy routes.
    const RouteHandler* handler = nullptr;
    bool         echoResponse = false;
    RouteParams  params;
    std::optional<ProxyTarget> proxyTarget;
};

class Router {
public:
    Router();
    ~Router();

    Router(const Router&) = delete;
    Router& operator=(const Router&) = delete;
    Router(Router&&) noexcept;
    Router& operator=(Router&&) noexcept;

    void addRoute(HttpMethod method, const std::string& path, RouteHandler handler);
    void addRoute(HttpMethod method, const std::string& path, ParamsHandler handler);

    void get(const std::string& path, RouteHandler handler);
    void get(const std::string& path, ParamsHandler handler);
    void post(const std::string& path, RouteHandler handler);
    void post(const std::string& path, ParamsHandler handler);
    void put(const std::string& path, RouteHandler handler);
    void put(const std::string& path, ParamsHandler handler);
    void del(const std::string& path, RouteHandler handler);
    void del(const std::string& path, ParamsHandler handler);
    void patch(const std::string& path, RouteHandler handler);
    void patch(const std::string& path, ParamsHandler handler);

   
    void addProxyRoute(HttpMethod method, const std::string& path,
                        std::string upstreamHost, uint16_t upstreamPort);

    void proxy(HttpMethod method, const std::string& path,
               std::string upstreamHost, uint16_t upstreamPort);

    [[nodiscard]] RouteMatch match(HttpMethod method, const std::string& path) const;

private:
    struct TrieNode;

    static std::vector<std::string_view> splitPath(std::string_view path);

   
    TrieNode* resolveOrCreateNode(const std::string& path);

    std::unique_ptr<TrieNode> root_;
};

} 