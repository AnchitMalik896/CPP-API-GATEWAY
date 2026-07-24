// include/HttpClient.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

#include "Router.hpp" 

namespace apigateway {

enum class HttpClientState {
    NotConnected,
    Connecting,
    Connected,
    Sending,
    Receiving,
    Complete,
    Failed
};


struct HttpClientResponse {
    int statusCode = 0;
    std::string reasonPhrase;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};


class HttpClient {
public:
    HttpClient();
    ~HttpClient();

    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;
    HttpClient(HttpClient&&) = delete;
    HttpClient& operator=(HttpClient&&) = delete;

    [[nodiscard]] HttpClientState state() const noexcept;
    [[nodiscard]] int fd() const noexcept;

   
    int connect(const std::string& ip, uint16_t port);
    bool completeConnect();

    void prepareRequest(HttpMethod method,
                         std::string_view path,
                         const std::unordered_map<std::string, std::string>& headers,
                         std::string_view body = {});

    
    bool send();
    bool receive();

    [[nodiscard]] const HttpClientResponse& response() const noexcept;


    void close() noexcept;

private:
    bool tryParseResponse();
    void fail(std::string_view reason) noexcept;

    int fd_ = -1;
    HttpClientState state_ = HttpClientState::NotConnected;

    std::string remoteHostHeader_;

    std::string writeBuffer_;
    size_t writeOffset_ = 0;

    std::string readBuffer_;

    HttpClientResponse response_;
};

} 