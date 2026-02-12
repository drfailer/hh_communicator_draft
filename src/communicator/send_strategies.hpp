#ifndef COMMUNICATOR_SEND_STRATEGIES
#define COMMUNICATOR_SEND_STRATEGIES
#include "protocol.hpp"
#include <functional>
#include <memory>
#include <vector>

// could we add a ready flag for the data??? -> ready to send

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

/// @brief Definition of the `SendStrategy` type. The send strategy is just a
///        function that returns a list of destination ranks for a given data.
///
/// Strategies are used to determin the destination ranks for a given data:
///
/// @code
/// // create and setup the communicator
/// auto ct = std::make_shared<hh::comm::CommunicatorTask<Data1, Data2>>(&service, "example");
///
/// // set the send strategies for each type
///
/// // the send strategy is a function that returns a list of destination
/// // ranks, we can use a lambda to compute the destination depending on the
/// // input data
/// gatherTask->strategy<Data1>([&](auto data) {
///     hh::comm::rank_t rank = service.rank();
///     size_t           nbProcesses = service.nbProcesses();
///     return std::vector<hh::comm::rank_t>({(rank + 1) % nbProcesses});
/// });
/// // or use some of the generic strategies provided by the library
/// gatherTask->strategy<Data2>(hh::comm::strategy::SendTo(1, 2));
/// @endcode
///
/// @tparam T Type of the data that needs to be sent.
template <typename T>
using SendStrategy = std::function<std::vector<comm::rank_t>(std::shared_ptr<T>)>;

/// @brief Strategy namespace
namespace strategy {

/// @brief Send the data to specific destination.
/// @tparam Type of the data.
struct SendTo {
  std::vector<rank_t> dests; ///< Vector of destinations .

  /// @brief Variadic constructor.
  /// @param ranks Ranks of the destinations.
  SendTo(auto... ranks)
      : dests({(rank_t)ranks...}) {}

  /// @brief Constructor from a predicate (ex: send a data to all even ranks).
  /// @param pred        Predicate.
  /// @param nbProcesses Number of processes.
  SendTo(std::function<bool(rank_t)> pred, size_t nbProcesses) {
    for (rank_t r = 0; r < nbProcesses; ++r) {
      if (pred(r)) {
        dests.push_back(r);
      }
    }
  }

  /// @brief Function that returns the destination vector.
  /// @param data Unused.
  /// @result Destination ranks.
  std::vector<rank_t> operator()(auto) {
    return dests;
  }
};

/// @brief Scatter strategy (distributes evenly the data between the given destinations).
struct Scatter {
  std::vector<rank_t> dests;          ///< vector of destination
  size_t              processIdx = 0; ///< index used to distributes the data.

  /// @brief Constuctor from vector.
  /// @param dests Vector of destination ranks.
  Scatter(std::vector<rank_t> const &dests)
      : dests(dests) {}

  /// @brief Constuctor from vector (move).
  /// @param dests Vector of destination ranks.
  Scatter(std::vector<rank_t> &&dests)
      : dests(std::move(dests)) {}

  /// @brief Constructor from number of processes (scatter among all processes).
  /// @brief nbProcesses Number of processes.
  Scatter(size_t nbProcesses)
      : dests(nbProcesses, 0) {
    for (size_t rank = 0; rank < nbProcesses; ++rank) {
      dests[rank] = rank;
    }
  }

  /// @brief Functions that returns the destination rank.
  /// @param data Unused.
  /// @return Vector containing the rank of the destination.
  std::vector<rank_t> operator()(auto) {
    rank_t idx = processIdx;
    processIdx = (processIdx + 1) % dests.size();
    return std::vector<rank_t>({dests[idx]});
  }
};

/// @brief Gather strategy.
struct Gather {
  rank_t base; ///< Rank of the process that gathers the data.

  /// @brief Constructor from base rank.
  /// @parma base Base rank.
  Gather(rank_t base = 0)
      : base(base) {}

  /// @brief Function that returns the vector of destination rank.
  /// @param data Unused
  /// @return Vector containing the base rank.
  std::vector<rank_t> operator()(auto) {
    return std::vector<rank_t>({base});
  }
};

} // namespace strategy

} // end namespace comm

} // end namespace hh

#endif
