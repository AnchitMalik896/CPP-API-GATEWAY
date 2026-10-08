#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace apigateway {

class HttpResponse {
public:
    using Header = std::pair<std::string, std::string>;

    HttpResponse() = default;
    explicit HttpResponse(int statusCode);

    // Status must be 100-599. Without a reason phrase the standard one is used.
    HttpResponse& setStatus(int statusCode);
    HttpResponse& setStatus(int statusCode, std::string_view reasonPhrase);

    // Header names are matched case-insensitively; setting an existing header
    // replaces its value and keeps its position. Throws std::invalid_argument
    // for a name that is not an HTTP token or a value containing CR, LF or NUL.
    HttpResponse& setHeader(std::string_view name, std::string_view value);

    HttpResponse& setBody(std::string body);

    [[nodiscard]] int status() const noexcept { return status_; }
    [[nodiscard]] const std::string& reasonPhrase() const noexcept { return reason_; }
    [[nodiscard]] const std::vector<Header>& headers() const noexcept { return headers_; }
    [[nodiscard]] const std::string& body() const noexcept { return body_; }

    // HTTP/1.1 wire format: status line, headers in insertion order, then
    // Content-Length (always derived from the body; a Content-Length set with
    // setHeader is ignored), then "Connection: close" unless a Connection
    // header was set, a blank line and the body.
    [[nodiscard]] std::string toBytes() const;

private:
    int status_ = 200;
    std::string reason_ = "OK";
    std::vector<Header> headers_;
    std::string body_;
};

}
