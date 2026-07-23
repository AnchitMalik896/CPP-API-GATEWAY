#include "Router.hpp"

#include <algorithm>
#include <stdexcept>

namespace apigateway {

// ---------------------------------------------------------------------------
// HttpMethod helpers
// ---------------------------------------------------------------------------

HttpMethod httpMethodFromString(std::string_view method) {
    if (method == "GET")     return HttpMethod::GET;
    if (method == "POST")    return HttpMethod::POST;
    if (method == "PUT")     return HttpMethod::PUT;
    if (method == "DELETE")  return HttpMethod::DELETE;
    if (method == "PATCH")   return HttpMethod::PATCH;
    if (method == "HEAD")    return HttpMethod::HEAD;
    if (method == "OPTIONS") return HttpMethod::OPTIONS;
    throw std::invalid_argument("Unrecognized HTTP method: " + std::string(method));
}

std::string_view httpMethodToString(HttpMethod method) noexcept {
    switch (method) {
        case HttpMethod::GET:     return "GET";
        case HttpMethod::POST:    return "POST";
        case HttpMethod::PUT:     return "PUT";
        case HttpMethod::DELETE:  return "DELETE";
        case HttpMethod::PATCH:   return "PATCH";
        case HttpMethod::HEAD:    return "HEAD";
        case HttpMethod::OPTIONS: return "OPTIONS";
    }
    return "UNKNOWN";
}

// ---------------------------------------------------------------------------
// Trie node
// ---------------------------------------------------------------------------

struct Router::TrieNode {
    std::unordered_map<std::string, std::unique_ptr<TrieNode>> children;
    std::unique_ptr<TrieNode> paramChild;
    std::string               paramName;
    std::unordered_map<HttpMethod, RouteHandler> handlers;
};

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

Router::Router()  : root_(std::make_unique<TrieNode>()) {}
Router::~Router() = default;
Router::Router(Router&&) noexcept            = default;
Router& Router::operator=(Router&&) noexcept = default;

// ---------------------------------------------------------------------------
// Path splitting
// ---------------------------------------------------------------------------

std::vector<std::string_view> Router::splitPath(std::string_view path) {
    std::vector<std::string_view> segments;

    if (path.empty()) {
        throw std::invalid_argument("Route path must not be empty");
    }
    if (path.front() != '/') {
        throw std::invalid_argument("Route path must start with '/': " + std::string(path));
    }

    size_t start = 1;
    while (start <= path.size()) {
        size_t end = path.find('/', start);
        if (end == std::string_view::npos) {
            end = path.size();
        }

        if (end > start) {
            segments.emplace_back(path.substr(start, end - start));
        } else if (end == start && end != path.size()) {
            throw std::invalid_argument("Route path contains an empty segment: " + std::string(path));
        }

        start = end + 1;
    }

    return segments;
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

void Router::addRoute(HttpMethod method, const std::string& path, RouteHandler handler) {
    if (!handler) {
        throw std::invalid_argument("Route handler must not be empty for path: " + path);
    }

    const std::vector<std::string_view> segments = splitPath(path);

    TrieNode* current = root_.get();
    for (std::string_view segment : segments) {
        if (!segment.empty() && segment.front() == ':') {
            const std::string name(segment.substr(1));
            if (name.empty()) {
                throw std::invalid_argument("Parameter segment must have a name in path: " + path);
            }

            if (!current->paramChild) {
                current->paramChild           = std::make_unique<TrieNode>();
                current->paramChild->paramName = name;
            } else if (current->paramChild->paramName != name) {
                throw std::invalid_argument(
                    "Conflicting parameter name at same path position: expected ':" +
                    current->paramChild->paramName + "' but got ':" + name +
                    "' in path: " + path);
            }

            current = current->paramChild.get();
        } else {
            const std::string key(segment);
            auto [it, inserted] = current->children.try_emplace(key, nullptr);
            if (inserted) {
                it->second = std::make_unique<TrieNode>();
            }
            current = it->second.get();
        }
    }

    if (current->handlers.count(method) > 0) {
        throw std::invalid_argument(
            "Duplicate route registration for method " +
            std::string(httpMethodToString(method)) + " on path: " + path);
    }

    current->handlers.emplace(method, std::move(handler));
}

void Router::get(const std::string& path, RouteHandler handler) {
    addRoute(HttpMethod::GET,    path, std::move(handler));
}
void Router::post(const std::string& path, RouteHandler handler) {
    addRoute(HttpMethod::POST,   path, std::move(handler));
}
void Router::put(const std::string& path, RouteHandler handler) {
    addRoute(HttpMethod::PUT,    path, std::move(handler));
}
void Router::del(const std::string& path, RouteHandler handler) {
    addRoute(HttpMethod::DELETE, path, std::move(handler));
}
void Router::patch(const std::string& path, RouteHandler handler) {
    addRoute(HttpMethod::PATCH,  path, std::move(handler));
}

// ---------------------------------------------------------------------------
// Matching
// ---------------------------------------------------------------------------

RouteMatch Router::match(HttpMethod method, const std::string& path) const {
    RouteMatch result;

    std::vector<std::string_view> segments;
    try {
        segments = splitPath(path);
    } catch (const std::invalid_argument&) {
        return result;
    }

    const TrieNode* current = root_.get();
    RouteParams     params;

    for (std::string_view segment : segments) {
        const std::string key(segment);
        auto it = current->children.find(key);
        if (it != current->children.end()) {
            current = it->second.get();
            continue;
        }

        if (current->paramChild) {
            params.emplace(current->paramChild->paramName, std::string(segment));
            current = current->paramChild.get();
            continue;
        }

        return result;
    }

    auto handlerIt = current->handlers.find(method);
    if (handlerIt == current->handlers.end()) {
        return result;
    }

    result.found   = true;
    result.handler = handlerIt->second;
    result.params  = std::move(params);
    return result;
}

} // namespace apigateway
