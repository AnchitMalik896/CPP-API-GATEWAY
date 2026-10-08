// src/HttpResponse.cpp
#include "HttpResponse.hpp"

#include <cctype>
#include <stdexcept>

namespace apigateway {

namespace {

bool equalsIgnoreCase(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size()) {
        return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

bool isTokenChar(char c) noexcept {
    const auto byte = static_cast<unsigned char>(c);
    if (std::isalnum(byte) != 0) {
        return true;
    }
    switch (c) {
        case '!': case '#': case '$': case '%': case '&': case '\'': case '*':
        case '+': case '-': case '.': case '^': case '_': case '`': case '|': case '~':
            return true;
        default:
            return false;
    }
}

bool isValidHeaderName(std::string_view name) noexcept {
    if (name.empty()) {
        return false;
    }
    for (const char c : name) {
        if (!isTokenChar(c)) {
            return false;
        }
    }
    return true;
}

bool isValidHeaderValue(std::string_view value) noexcept {
    for (const char c : value) {
        if (c == '\r' || c == '\n' || c == '\0') {
            return false;
        }
    }
    return true;
}

std::string_view defaultReasonPhrase(int statusCode) noexcept {
    switch (statusCode) {
        case 200: return "OK";
        case 201: return "Created";
        case 202: return "Accepted";
        case 204: return "No Content";
        case 301: return "Moved Permanently";
        case 302: return "Found";
        case 304: return "Not Modified";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 408: return "Request Timeout";
        case 413: return "Payload Too Large";
        case 414: return "URI Too Long";
        case 429: return "Too Many Requests";
        case 431: return "Request Header Fields Too Large";
        case 500: return "Internal Server Error";
        case 502: return "Bad Gateway";
        case 503: return "Service Unavailable";
        case 504: return "Gateway Timeout";
        default:  return "Unknown";
    }
}

}

HttpResponse::HttpResponse(int statusCode) {
    setStatus(statusCode);
}

HttpResponse& HttpResponse::setStatus(int statusCode) {
    return setStatus(statusCode, defaultReasonPhrase(statusCode));
}

HttpResponse& HttpResponse::setStatus(int statusCode, std::string_view reasonPhrase) {
    if (statusCode < 100 || statusCode > 599) {
        throw std::invalid_argument("HTTP status code must be between 100 and 599");
    }
    if (!isValidHeaderValue(reasonPhrase)) {
        throw std::invalid_argument("HTTP reason phrase must not contain CR, LF or NUL");
    }
    status_ = statusCode;
    reason_.assign(reasonPhrase);
    return *this;
}

HttpResponse& HttpResponse::setHeader(std::string_view name, std::string_view value) {
    if (!isValidHeaderName(name)) {
        throw std::invalid_argument("invalid HTTP header name");
    }
    if (!isValidHeaderValue(value)) {
        throw std::invalid_argument("HTTP header value must not contain CR, LF or NUL");
    }

    for (Header& header : headers_) {
        if (equalsIgnoreCase(header.first, name)) {
            header.second.assign(value);
            return *this;
        }
    }
    headers_.emplace_back(std::string(name), std::string(value));
    return *this;
}

HttpResponse& HttpResponse::setBody(std::string body) {
    body_ = std::move(body);
    return *this;
}

std::string HttpResponse::toBytes() const {
    size_t estimate = 64 + reason_.size() + body_.size();
    for (const Header& header : headers_) {
        estimate += header.first.size() + header.second.size() + 4;
    }

    std::string out;
    out.reserve(estimate);

    out += "HTTP/1.1 ";
    out += std::to_string(status_);
    out += ' ';
    out += reason_;
    out += "\r\n";

    bool hasConnection = false;
    for (const Header& header : headers_) {
        if (equalsIgnoreCase(header.first, "Content-Length")) {
            continue;
        }
        if (equalsIgnoreCase(header.first, "Connection")) {
            hasConnection = true;
        }
        out += header.first;
        out += ": ";
        out += header.second;
        out += "\r\n";
    }

    out += "Content-Length: ";
    out += std::to_string(body_.size());
    out += "\r\n";
    if (!hasConnection) {
        out += "Connection: close\r\n";
    }
    out += "\r\n";
    out += body_;
    return out;
}

}
