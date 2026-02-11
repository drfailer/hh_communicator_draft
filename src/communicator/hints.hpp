#ifndef COMMUNICATOR_HINTS
#define COMMUNICATOR_HINTS
#include "protocol.hpp"

// Hints are used to manually optimize transmission:
//
// commTask->hint(hints::recvFrom(source, poolCount));
// or is it better to do:
// commTask->hint().expectRecvFrom(source, poolCount);
// -> the communictor should make sure that there are always `poolCount` recv requests tward 0
// -> we may get rid of the package id system (even ucx orders messages on for the same tag)
//
// What is the hint type (if we need one)?
// - recvCountFrom (a certain number of request, but do not re-post)
// - continuousRecvFrom(count = 1) (count being a rate here)

namespace hh {
namespace comm {
namespace hint {

enum class HintType {
  RecvCountFrom,
  ContinuousRecvFrom,
};

struct Hint {
  HintType type;
  union {
    struct {
      rank_t source;
      size_t count;
    } recvCountFrom;
    struct {
      rank_t source;
      size_t poolSize;
    } continuousRecvFrom;
  } data;
};

inline Hint recvCountFrom(rank_t source, size_t count) {
  return Hint{
    .type = HintType::RecvCountFrom,
    .data = { .recvCountFrom = { .source = source, .count = count } },
  };
}

inline Hint continuousRecvFrom(rank_t source, size_t poolSize) {
  return Hint{
    .type = HintType::ContinuousRecvFrom,
    .data = { .continuousRecvFrom = { .source = source, .poolSize = poolSize } },
  };
}


} // end namespace hint
} // end namespace com
} // end namespace hh

#endif
