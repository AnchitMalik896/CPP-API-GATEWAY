// src/HttpClient.cpp
#include "HttpClient.hpp"
#include "AsyncLogger.hpp"
#include "Socket.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace apigateway {

namespace {

constexpr size_t kMaxHeaderSectionLength = 32 * 1024;
constexpr size_t kMaxBodySize = 10 * 1024 * 1024;
constexpr size_t kMaxResponseSize = kMaxHeaderSectionLength + kMaxBodySize;
constexpr size_t kReadChunkSize = 16 * 1024;



std::string_view trimView(std::string_view s) noexcept {
    size_t begin = 0;
    while (begin < s.size() && std::isspace(static_cast<unsigned char>(s[begin])) != 0) {
        ++begin;
    }
    size_t end = s.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(s[end - 1])) != 0) {
        --end;
    }
    return s.substr(begin, end - begin);
}

bool caseInsensitiveEquals(std::string_view a, std::string_view b) noexcept {
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

} 
HttpClient::HttpClient() = default;

HttpClient::~HttpClient() {
    close();
}

HttpClientState HttpClient::state() const noexcept {
    return state_;
}

int HttpClient::fd() const noexcept {
    return fd_;
}

const HttpClientResponse& HttpClient::response() const noexcept {
    return response_;
}

void HttpClient::close() noexcept {
    if (fd_ >= 0) {
        Socket::closeSocket(fd_);
        fd_ = -1;
    }
}

void HttpClient::fail(std::string_view reason) noexcept {
    state_ = HttpClientState::Failed;
    AsyncLogger::instance().error(reason);
}

int HttpClient::connect(const std::string& ip, uint16_t port) {
    if (state_ != HttpClientState::NotConnected) {
        throw std::logic_error("HttpClient::connect() called from an invalid state");
    }

    try {
        fd_ = Socket::createConnectingSocket(ip, port);
    } catch (const std::exception& ex) {
        AsyncLogger::instance().error(
            "HttpClient: failed to begin connecting to " + ip + ":" + std::to_string(port) +
            " - " + ex.what());
        state_ = HttpClientState::Failed;
        throw;
    }

    remoteHostHeader_ = ip + ":" + std::to_string(port);
    state_ = HttpClientState::Connecting;

    AsyncLogger::instance().info(
        "HttpClient: connecting to " + remoteHostHeader_ + " (fd=" + std::to_string(fd_) + ")");

    return fd_;
}

bool HttpClient::completeConnect() {
    if (state_ != HttpClientState::Connecting) {
        return false;
    }

    int socketError = 0;
    socklen_t errorLen = sizeof(socketError);

    if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &socketError, &errorLen) < 0) {
        fail("HttpClient: getsockopt(SO_ERROR) failed for " + remoteHostHeader_ +
             " - " + std::strerror(errno));
        return false;
    }

    if (socketError != 0) {
        fail("HttpClient: connect() to " + remoteHostHeader_ +
             " failed - " + std::strerror(socketError));
        return false;
    }

    state_ = HttpClientState::Connected;
    AsyncLogger::instance().info("HttpClient: connected to " + remoteHostHeader_);
    return true;
}

void HttpClient::prepareRequest(HttpMethod method,
                                 std::string_view path,
                                 const std::unordered_map<std::string, std::string>& headers,
                                 std::string_view body) {
    if (state_ != HttpClientState::Connected) {
        throw std::logic_error("HttpClient::prepareRequest() called from an invalid state");
    }

    std::ostringstream request;
    request << httpMethodToString(method) << ' ' << path << " HTTP/1.1\r\n";

    bool hasHost = false;
    bool hasContentLength = false;
    bool hasConnection = false;

    for (const auto& [key, value] : headers) {
        request << key << ": " << value << "\r\n";
        if (caseInsensitiveEquals(key, "host")) { hasHost = true; }
        if (caseInsensitiveEquals(key, "content-length")) { hasContentLength = true; }
        if (caseInsensitiveEquals(key, "connection")) { hasConnection = true; }
    }

    if (!hasHost) {
        request << "Host: " << remoteHostHeader_ << "\r\n";
    }
    if (!body.empty() && !hasContentLength) {
        request << "Content-Length: " << body.size() << "\r\n";
    }
    if (!hasConnection) {
        request << "Connection: close\r\n";
    }

    request << "\r\n";
    if (!body.empty()) {
        request << body;
    }

    writeBuffer_ = request.str();
    writeOffset_ = 0;
    state_ = HttpClientState::Sending;
}


bool HttpClient::send() {
    if (state_ != HttpClientState::Sending) {
        return state_ == HttpClientState::Receiving || state_ == HttpClientState::Complete;
    }

    while (writeOffset_ < writeBuffer_.size()) {
        const char* data = writeBuffer_.data() + writeOffset_;
        const size_t remaining = writeBuffer_.size() - writeOffset_;

        const ssize_t bytesSent = ::send(fd_, data, remaining, 0);

        if (bytesSent > 0) {
            writeOffset_ += static_cast<size_t>(bytesSent);
            continue;
        }

        if (bytesSent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return true; 
        }
        if (bytesSent < 0 && errno == EINTR) {
            continue;
        }

        fail("HttpClient: send() to " + remoteHostHeader_ + " failed - " + std::strerror(errno));
        return false;
    }

    writeBuffer_.clear();
    writeBuffer_.shrink_to_fit();
    state_ = HttpClientState::Receiving;
    return true;
}


bool HttpClient::receive() {
    if (state_ != HttpClientState::Receiving) {
        return state_ == HttpClientState::Complete;
    }

    std::array<char, kReadChunkSize> buffer{};

    for (;;) {
        const ssize_t bytesRead = ::recv(fd_, buffer.data(), buffer.size(), 0);

        if (bytesRead > 0) {
            readBuffer_.append(buffer.data(), static_cast<size_t>(bytesRead));

            if (readBuffer_.size() > kMaxResponseSize) {
                fail("HttpClient: response from " + remoteHostHeader_ +
                     " exceeded maximum allowed size");
                return false;
            }

            bool complete = false;
            try {
                complete = tryParseResponse();
            } catch (const std::invalid_argument& ex) {
                fail("HttpClient: malformed response from " + remoteHostHeader_ +
                     " - " + ex.what());
                return false;
            }

            if (complete) {
                state_ = HttpClientState::Complete;
                readBuffer_.clear();
                readBuffer_.shrink_to_fit();
                AsyncLogger::instance().info(
                    "HttpClient: response from " + remoteHostHeader_ +
                    " complete, status=" + std::to_string(response_.statusCode));
                return true;
            }

            if (static_cast<size_t>(bytesRead) < buffer.size()) {
                return true; 
            }
            continue;
        }

        if (bytesRead == 0) {
            fail("HttpClient: connection to " + remoteHostHeader_ +
                 " closed before response was fully received");
            return false;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return true;
        }
        if (errno == EINTR) {
            continue;
        }

        fail("HttpClient: recv() from " + remoteHostHeader_ + " failed - " + std::strerror(errno));
        return false;
    }
}

bool HttpClient::tryParseResponse() {
    const std::string_view raw(readBuffer_);
    const size_t headerEnd = raw.find("\r\n\r\n");
    if (headerEnd == std::string_view::npos) {
        if (raw.size() > kMaxHeaderSectionLength) {
            throw std::invalid_argument("response header section too large");
        }
        return false; 
    }

    const std::string_view headerSection = raw.substr(0, headerEnd);
    const size_t firstLineEnd = headerSection.find("\r\n");
    const std::string_view statusLine =
        (firstLineEnd == std::string_view::npos) ? headerSection
                                                   : headerSection.substr(0, firstLineEnd);

    const size_t firstSpace = statusLine.find(' ');
    if (firstSpace == std::string_view::npos) {
        throw std::invalid_argument("malformed status line (missing version/status separator)");
    }
    const size_t secondSpace = statusLine.find(' ', firstSpace + 1);
    const std::string_view statusCodeStr =
        (secondSpace == std::string_view::npos)
            ? statusLine.substr(firstSpace + 1)
            : statusLine.substr(firstSpace + 1, secondSpace - firstSpace - 1);
    const std::string_view reasonPhrase =
        (secondSpace == std::string_view::npos) ? std::string_view{}
                                                  : statusLine.substr(secondSpace + 1);

    if (statusCodeStr.empty() ||
        !std::all_of(statusCodeStr.begin(), statusCodeStr.end(), [](char c) {
            return std::isdigit(static_cast<unsigned char>(c)) != 0;
        })) {
        throw std::invalid_argument("malformed status code");
    }

    int statusCode = 0;
    try {
        statusCode = std::stoi(std::string(statusCodeStr));
    } catch (const std::exception&) {
        throw std::invalid_argument("malformed status code");
    }

    std::unordered_map<std::string, std::string> headers;
    size_t lineStart = (firstLineEnd == std::string_view::npos) ? headerSection.size()
                                                                  : firstLineEnd + 2;

    while (lineStart < headerSection.size()) {
        const size_t lineEnd = headerSection.find("\r\n", lineStart);
        const std::string_view line =
            (lineEnd == std::string_view::npos)
                ? headerSection.substr(lineStart)
                : headerSection.substr(lineStart, lineEnd - lineStart);

        if (!line.empty()) {
            const size_t colon = line.find(':');
            if (colon == std::string_view::npos) {
                throw std::invalid_argument("malformed response header line (missing colon)");
            }
            const std::string_view key = trimView(line.substr(0, colon));
            const std::string_view value = trimView(line.substr(colon + 1));
            if (key.empty()) {
                throw std::invalid_argument("malformed response header line (empty key)");
            }
            headers.emplace(std::string(key), std::string(value));
        }

        if (lineEnd == std::string_view::npos) {
            break;
        }
        lineStart = lineEnd + 2;
    }
    size_t contentLength = 0;
    for (const auto& [key, value] : headers) {
        if (caseInsensitiveEquals(key, "content-length")) {
            if (value.empty() ||
                !std::all_of(value.begin(), value.end(), [](char c) {
                    return std::isdigit(static_cast<unsigned char>(c)) != 0;
                })) {
                throw std::invalid_argument("invalid Content-Length value");
            }
            try {
                contentLength = static_cast<size_t>(std::stoul(value));
            } catch (const std::exception&) {
                throw std::invalid_argument("invalid Content-Length value");
            }
            if (contentLength > kMaxBodySize) {
                throw std::invalid_argument("Content-Length exceeds maximum allowed body size");
            }
            break;
        }
    }

    const size_t bodyStart = headerEnd + 4; 
    if (raw.size() - bodyStart < contentLength) {
        return false; 
    }

    response_.statusCode = statusCode;
    response_.reasonPhrase = std::string(trimView(reasonPhrase));
    response_.headers = std::move(headers);
    response_.body = std::string(raw.substr(bodyStart, contentLength));

    return true;
}

} 