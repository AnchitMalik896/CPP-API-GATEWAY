#pragma once

#include <string>
#include <string_view>
#include <unordered_map>

#include "Router.hpp"

namespace apigateway {

// A fully parsed request that owns all of its data. It is built once per
// request on the worker thread and never refers to the connection's read
// buffer, so it stays valid for as long as the caller keeps it.
struct HttpRequest {
    HttpMethod method = HttpMethod::GET;
    std::string path;
    std::string query;
    std::unordered_map<std::string, std::string> headers;  // names exactly as the client sent them
    RouteParams params;                                     // filled in once the route has matched
    std::string body;
    std::string requestId;

    [[nodiscard]] std::string_view methodName() const noexcept {
        return httpMethodToString(method);
    }
};

}
