#ifndef COMMUNICATOR_PROTOCOL
#define COMMUNICATOR_PROTOCOL
#include "hedgehog/src/tools/meta_functions.h"
#include <memory>

namespace hh {

namespace comm {

struct Buffer {
  char  *mem;
  size_t len;
};

// Signal //////////////////////////////////////////////////////////////////////

enum class Signal : std::uint8_t {
  None,
  Data,
  Disconnect,
};

// Header //////////////////////////////////////////////////////////////////////

using channel_t = std::uint64_t;
using rank_t = std::uint64_t;
using signal_t = std::uint64_t;
using type_id_t = std::uint64_t;
using package_id_t = std::uint64_t;
using buffer_id_t = std::uint64_t;

struct Header {
  channel_t    channel;
  rank_t       source;
  signal_t     signal;
  type_id_t    typeId;
  package_id_t packageId;
  buffer_id_t  bufferId;

  Header() = default;
  Header(rank_t source, signal_t signal, type_id_t typeId, channel_t channel,
         package_id_t packageId, buffer_id_t bufferId)
      : channel(channel),
        source(source),
        signal(signal),
        typeId(typeId),
        packageId(packageId),
        bufferId(bufferId) {}

  enum Fields {
    CHANNEL = 0,
    SOURCE = 1,
    SIGNAL = 2,
    TYPE_ID = 3,
    PACKAGE_ID = 4,
    BUFFER_ID = 5,
  };

  struct FieldInfo {
    size_t        offset;
    std::uint64_t mask;
  };

  bool operator<(Header const &other) const {
    std::uint64_t thisVals[]
        = {this->source, this->signal, this->typeId, this->channel, this->packageId, this->bufferId};
    std::uint64_t otherVals[]
        = {other.source, other.signal, other.typeId, other.channel, other.packageId, other.bufferId};

    for (size_t i = 0; i < sizeof(thisVals) / sizeof(this->source); ++i) {
      if (thisVals[i] != otherVals[i]) {
        return thisVals[i] < otherVals[i];
      }
    }
    return false;
  }
};

} // end namespace comm

} // end namespace hh

#endif
