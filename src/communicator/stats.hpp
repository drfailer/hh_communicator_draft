#ifndef COMMUNICATOR_STATS
#define COMMUNICATOR_STATS
#include "package.hpp"
#include <chrono>

namespace hh {

namespace comm {

// Stats container /////////////////////////////////////////////////////////////

using time_t = std::chrono::time_point<std::chrono::system_clock>;
using delay_t = std::chrono::duration<long int, std::ratio<1, 1000000000>>;

struct StorageInfo {
  time_t       sendtp;
  time_t       recvtp;
  delay_t      packingTime;
  delay_t      unpackingTime;
  size_t       packingCount;
  size_t       unpackingCount;
  std::uint8_t typeId;
  size_t       dataSize;
};

// TODO: we may have to define a limit on the size of storageStats
struct CommTaskStats {
  std::map<StorageId, StorageInfo> storageStats = {};
  size_t                           maxSendOpsSize = 0;
  size_t                           maxRecvOpsSize = 0;
  size_t                           maxCreateDataQueueSize = 0;
  size_t                           maxSendStorageSize = 0;
  size_t                           maxRecvStorageSize = 0;
  std::mutex                       mutex;
};

} // end namespace comm

} // end namespace hh

#endif
