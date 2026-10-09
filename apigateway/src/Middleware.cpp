#include "Middleware.hpp"

#include <stdexcept>
#include <utility>

namespace apigateway {

namespace detail {

struct RunState {
    const MiddlewarePipeline& pipeline;
    HttpRequest& request;
    HttpResponse& response;
    TerminalRef terminal;
};

}

void Next::operator()() const {
    if (*advanced_) {
        throw std::logic_error("middleware called next() more than once");
    }
    *advanced_ = true;
    run_->pipeline.invoke(index_, *run_);
}

void MiddlewarePipeline::add(Middleware middleware) {
    if (!middleware) {
        throw std::invalid_argument("middleware must not be empty");
    }
    if (middleware_.size() >= kMaxMiddleware) {
        throw std::length_error("too many middleware registered");
    }
    middleware_.push_back(std::move(middleware));
}

void MiddlewarePipeline::run(HttpRequest& request, HttpResponse& response,
                             TerminalRef terminal) const {
    detail::RunState state{*this, request, response, terminal};
    invoke(0, state);
}

void MiddlewarePipeline::invoke(std::size_t index, detail::RunState& run) const {
    if (index == middleware_.size()) {
        run.terminal(run.request, run.response);
        return;
    }
    bool advanced = false;
    middleware_[index](run.request, run.response, Next(&run, index + 1, &advanced));
}

}
