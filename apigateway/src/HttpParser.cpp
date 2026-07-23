#include "HttpParser.hpp"

#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace apigateway {

namespace {

constexpr size_t kMaxRequestLineLength   = 8  * 1024;
constexpr size_t kMaxHeaderSectionLength = 32 * 1024;
constexpr size_t kMaxBodyLength          = 10 * 1024 * 1024;

std::string_view trim(std::string_view s) {
    size_t begin = 0;
    while (begin < s.size() &&
           std::isspace(static_cast<unsigned char>(s[begin])) != 0) {
        ++begin;
    }
    size_t end = s.size();
    while (end > begin &&
           std::isspace(static_cast<unsigned char>(s[end - 1])) != 0) {
        --end;
    }
    return s.substr(begin, end - begin);
}

std::string toLower(std::string_view s) {
    std::string result(s);
    std::transform(result.begin(), result.end(), result.begin(),
                    [](unsigned char c) {
                        return static_cast<char>(std::tolower(c));
                    });
    return result;
}

} // namespace

bool HttpParser::parseRequestLine(std::string_view line) {
    const size_t firstSpace = line.find(' ');
    if (firstSpace == std::string_view::npos) { return false; }

    const size_t secondSpace = line.find(' ', firstSpace + 1);
    if (secondSpace == std::string_view::npos) { return false; }

    const std::string_view methodStr  = line.substr(0, firstSpace);
    const std::string_view target     = line.substr(firstSpace + 1, secondSpace - firstSpace - 1);
    const std::string_view versionStr = line.substr(secondSpace + 1);

    if (target.empty() || target.front() != '/') { return false; }

    try {
        request_.method = httpMethodFromString(methodStr);
    } catch (const std::invalid_argument&) {
        return false;
    }

    const size_t queryPos = target.find('?');
    if (queryPos == std::string_view::npos) {
        request_.path  = std::string(target);
        request_.query.clear();
    } else {
        request_.path  = std::string(target.substr(0, queryPos));
        request_.query = std::string(target.substr(queryPos + 1));
    }

    request_.version = std::string(versionStr);
    return true;
}

bool HttpParser::parseHeaderLine(std::string_view line) {
    const size_t colon = line.find(':');
    if (colon == std::string_view::npos) { return false; }

    const std::string key   = toLower(trim(line.substr(0, colon)));
    const std::string value(trim(line.substr(colon + 1)));

    if (key.empty()) { return false; }

    request_.headers[key] = value;
    return true;
}

bool HttpParser::feed(std::string_view data) {
    if (state_ == State::Error)    { return false; }
    if (state_ == State::Complete) { return true;  }

    buffer_.append(data.data(), data.size());

    if (state_ == State::RequestLine) {
        const size_t pos = buffer_.find("\r\n");
        if (pos == std::string::npos) {
            if (buffer_.size() > kMaxRequestLineLength) { state_ = State::Error; }
            return false;
        }
        if (!parseRequestLine(std::string_view(buffer_).substr(0, pos))) {
            state_ = State::Error;
            return false;
        }
        buffer_.erase(0, pos + 2);
        state_ = State::Headers;
    }

    if (state_ == State::Headers) {
        for (;;) {
            const size_t pos = buffer_.find("\r\n");
            if (pos == std::string::npos) {
                if (buffer_.size() > kMaxHeaderSectionLength) { state_ = State::Error; }
                return false;
            }

            if (pos == 0) {
                buffer_.erase(0, 2);
                break;
            }

            if (!parseHeaderLine(std::string_view(buffer_).substr(0, pos))) {
                state_ = State::Error;
                return false;
            }
            buffer_.erase(0, pos + 2);
        }

        contentLength_ = 0;
        const auto it = request_.headers.find("content-length");
        if (it != request_.headers.end()) {
            try {
                const unsigned long parsed = std::stoul(it->second);
                if (parsed > kMaxBodyLength) {
                    state_ = State::Error;
                    return false;
                }
                contentLength_ = static_cast<size_t>(parsed);
            } catch (const std::exception&) {
                state_ = State::Error;
                return false;
            }
        }

        state_ = State::Body;
    }

    if (state_ == State::Body) {
        if (buffer_.size() < contentLength_) { return false; }
        request_.body = buffer_.substr(0, contentLength_);
        buffer_.erase(0, contentLength_);
        state_ = State::Complete;
        return true;
    }

    return false;
}

bool HttpParser::hasError()   const noexcept { return state_ == State::Error;    }
bool HttpParser::isComplete() const noexcept { return state_ == State::Complete; }

HttpRequest HttpParser::takeRequest() {
    HttpRequest result = std::move(request_);
    reset();
    return result;
}

void HttpParser::reset() {
    state_         = State::RequestLine;
    request_       = HttpRequest{};
    contentLength_ = 0;
}

} // namespace apigateway
