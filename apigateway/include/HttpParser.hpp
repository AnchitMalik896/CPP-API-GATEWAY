#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>

#include "Router.hpp"

namespace apigateway {

struct HttpRequest {
    HttpMethod  method  = HttpMethod::GET;
    std::string path;
    std::string query;
    std::string version;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

class HttpParser {
public:
    HttpParser() = default;

    bool feed(std::string_view data);

    [[nodiscard]] bool hasError()    const noexcept;
    [[nodiscard]] bool isComplete()  const noexcept;

    HttpRequest takeRequest();
    void        reset();

private:
    enum class State {
        RequestLine,
        Headers,
        Body,
        Complete,
        Error
    };

    bool parseRequestLine(std::string_view line);
    bool parseHeaderLine(std::string_view line);

    std::string buffer_;
    State       state_         = State::RequestLine;
    HttpRequest request_;
    size_t      contentLength_ = 0;
};

} // namespace apigateway
