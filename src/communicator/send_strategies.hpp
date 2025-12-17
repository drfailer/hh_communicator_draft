#ifndef COMMUNICATOR_SEND_STRATEGIES
#define COMMUNICATOR_SEND_STRATEGIES
#include "protocol.hpp"
#include <vector>
#include <functional>
#include <memory>

namespace hh {

namespace comm {

namespace strategy {

template <typename T>
struct SendTo {
    std::vector<rank_t> dests;

    SendTo(auto ...ranks): dests({(rank_t)ranks...}) {}
    SendTo(std::function<bool(rank_t)> pred, size_t nbProcesses) {
        for (rank_t r = 0; r < nbProcesses; ++r) {
            if (pred(r)) {
                dests.push_back(r);
            }
        }
    }

    std::vector<rank_t> operator()(std::shared_ptr<T>) {
        return dests;
    }
};

}

} // end namespace comm

} // end namespace hh

#endif
