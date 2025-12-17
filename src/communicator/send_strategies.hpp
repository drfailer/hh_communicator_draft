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

template <typename T>
struct Scatter {
    std::vector<rank_t> dests;
    size_t processIdx = 0;

    Scatter(std::vector<rank_t> const &dests) : dests(dests) {}
    Scatter(std::vector<rank_t> &&dests) : dests(dests) {}
    Scatter(size_t nbProcesses) : dests(nbProcesses, 0) {
        for (size_t rank = 0; rank < nbProcesses; ++rank) {
            dests[rank] = rank;
        }
    }

    std::vector<rank_t> operator()(std::shared_ptr<T>) {
        rank_t idx = processIdx;
        processIdx = (processIdx + 1) % dests.size();
        return std::vector<rank_t>({dests[idx]});
    }
};

template <typename T>
struct Gather {
    rank_t base;

    Gather(rank_t base = 0) : base(base) {}

    std::vector<rank_t> operator()(std::shared_ptr<T>) {
        return std::vector<rank_t>({base});
    }
};

}

} // end namespace comm

} // end namespace hh

#endif
