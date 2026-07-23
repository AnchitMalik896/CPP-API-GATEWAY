#pragma once

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace apigateway {

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
using RouteHandler = std::function<void(const RouteParams& params)>;

struct RouteMatch {
    bool         found = false;
    RouteHandler handler;
    RouteParams  params;
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

    void get(const std::string& path, RouteHandler handler);
    void post(const std::string& path, RouteHandler handler);
    void put(const std::string& path, RouteHandler handler);
    void del(const std::string& path, RouteHandler handler);
    void patch(const std::string& path, RouteHandler handler);

    [[nodiscard]] RouteMatch match(HttpMethod method, const std::string& path) const;

private:
    struct TrieNode;

    static std::vector<std::string_view> splitPath(std::string_view path);

    std::unique_ptr<TrieNode> root_;
};

} // namespace apigateway
