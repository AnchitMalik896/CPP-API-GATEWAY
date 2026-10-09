#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <type_traits>
#include <vector>

#include "HttpRequest.hpp"
#include "HttpResponse.hpp"

namespace apigateway {

class MiddlewarePipeline;

namespace detail {
struct RunState;
}

// Continuation handed to a middleware. Calling it runs the rest of the chain
// (and finally the terminal step) synchronously and returns once that is done,
// so code after next() sees the finished response.
//
// Non-owning and trivially copyable: it points into the stack of the pipeline
// run that created it, so it must not be used after the middleware returns
// and must not be handed to another thread. Calling it twice throws
// std::logic_error before anything downstream runs a second time.
class Next {
public:
    void operator()() const;

private:
    friend class MiddlewarePipeline;
    Next(detail::RunState* run, std::size_t index, bool* advanced) noexcept
        : run_(run), index_(index), advanced_(advanced) {}

    detail::RunState* run_;
    std::size_t index_;
    bool* advanced_;
};

// A middleware may inspect or change the request, call next() to continue, and
// inspect or change the response afterwards. Returning without calling next()
// short-circuits: nothing later in the chain runs and the response as the
// middleware left it is sent.
using Middleware = std::function<void(HttpRequest&, HttpResponse&, Next)>;

// Non-owning reference to the callable that ends the chain. Binds to lvalues
// only, so a temporary cannot be captured by accident.
class TerminalRef {
public:
    template <class F, class = std::enable_if_t<!std::is_same_v<std::remove_cv_t<F>, TerminalRef>>>
    TerminalRef(F& terminal) noexcept
        : object_(const_cast<void*>(static_cast<const void*>(std::addressof(terminal)))),
          invoke_([](void* object, HttpRequest& request, HttpResponse& response) {
              (*static_cast<F*>(object))(request, response);
          }) {}

    void operator()(HttpRequest& request, HttpResponse& response) const {
        invoke_(object_, request, response);
    }

private:
    void* object_;
    void (*invoke_)(void*, HttpRequest&, HttpResponse&);
};

// Ordered middleware chain. Built before the reactor starts and read-only
// afterwards, so workers share it without locking. One run() per request, on
// the worker thread that owns that request.
class MiddlewarePipeline {
public:
    // Bounds the recursion depth of run(); the depth depends on the chain
    // length only, never on request data.
    static constexpr std::size_t kMaxMiddleware = 64;

    // Throws std::invalid_argument for an empty function and std::length_error
    // past kMaxMiddleware.
    void add(Middleware middleware);

    [[nodiscard]] bool empty() const noexcept { return middleware_.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return middleware_.size(); }

    // Runs the chain in registration order, then terminal(request, response)
    // if every middleware called next(). Exceptions propagate to the caller.
    // Allocation-free: no per-request or per-middleware heap use.
    void run(HttpRequest& request, HttpResponse& response, TerminalRef terminal) const;

private:
    friend class Next;
    void invoke(std::size_t index, detail::RunState& run) const;

    std::vector<Middleware> middleware_;
};

}
