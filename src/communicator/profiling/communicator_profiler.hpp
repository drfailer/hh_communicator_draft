#ifndef COMMUNICATOR_PROFILING_COMMUNICATOR_PROFILER
#define COMMUNICATOR_PROFILING_COMMUNICATOR_PROFILER
#include "../tool/queue.hpp"
#include "profiling_tools.hpp"
#include <chrono>

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

struct PackageProfile {
  delay_t   packingTime;
  time_t    beginTp;
  rank_t    rank;
  type_id_t typeId;
  size_t    bufferCount;
};

struct ProfileInfo {
  delay_t packingTime;
  delay_t transmissionTime;
  rank_t  rank;
  size_t  packageSize;
};

enum class ProfiledSize : size_t {
  MaxSendOpsSize, ///< Max send queue size.
  MaxRecvOpsSize, ///< Max recv queue size.
  MaxCreateDataQueueSize, ///< Max data creation queue size.
  MaxSendStorageSize, ///< Max send storage size.
  MaxRecvStorageSize, ///< Max recv storage size.
  MaxSendQueueSize, ///< Max send queue size.
  COUNT_,
};

enum class ProfiledCounter : size_t {
  ProbedRequest,
  HintedRequest,
  COUNT_,
};

class CommunicatorProfiler {
public:
  CommunicatorProfiler(size_t typeCount, bool enabled)
      : enabled_(enabled) {
    if (!enabled) {
      return;
    }
    this->sendInfos_.resize(typeCount);
    this->recvInfos_.resize(typeCount);
  }

public:
  void updateProfiledSize(ProfiledSize size, size_t value) {
    if (!this->enabled_) {
      return;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    this->sizes_[size_t(size)] = std::max(this->sizes_[size_t(size)], value);
  }

  void incrementProfiledCounter(ProfiledCounter counter, size_t amout = 1) {
    if (!this->enabled_) {
      return;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    this->counters_[size_t(counter)] += amout;
  }

  size_t preSend(type_id_t typeId, rank_t dest, delay_t packingTime, size_t bufferCount) {
    if (!this->enabled_) {
      return 0;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    return this->profileQueue_.add(PackageProfile{
        .packingTime = packingTime,
        .beginTp = std::chrono::system_clock::now(),
        .rank = dest,
        .typeId = typeId,
        .bufferCount = bufferCount,
    });
  }

  void postSend(size_t queueIdx, size_t packageSize) {
    if (!this->enabled_) {
      return;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    auto                        &profile = this->profileQueue_.at(queueIdx);
    profile.bufferCount -= 1;

    if (profile.bufferCount == 0) {
      auto endTp = std::chrono::system_clock::now();
      this->sendInfos_[profile.typeId].emplace_back(ProfileInfo{
          .packingTime = profile.packingTime,
          .transmissionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(endTp - profile.beginTp),
          .rank = profile.rank,
          .packageSize = packageSize,
      });
    }
  }

  size_t preRecv(type_id_t typeId, rank_t source) {
    if (!this->enabled_) {
      return 0;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    return this->profileQueue_.add(PackageProfile{
        .packingTime = {},
        .beginTp = std::chrono::system_clock::now(),
        .rank = source,
        .typeId = typeId,
        .bufferCount = 0, // unused for the reception
    });
  }

  void postRecv(size_t queueIdx, delay_t unpackingTime, size_t packageSize) {
    if (!this->enabled_) {
      return;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    auto                        &profile = this->profileQueue_.at(queueIdx);
    auto                         endTp = std::chrono::system_clock::now();
    this->recvInfos_[profile.typeId].emplace_back(ProfileInfo{
        .packingTime = unpackingTime,
        .transmissionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(endTp - profile.beginTp),
        .rank = profile.rank,
        .packageSize = packageSize,
    });
  }

private:
  Queue<PackageProfile>                               profileQueue_;
  std::vector<std::vector<ProfileInfo>>               sendInfos_ = {};
  std::vector<std::vector<ProfileInfo>>               recvInfos_ = {};
  std::array<size_t, size_t(ProfiledSize::COUNT_)>    sizes_ = {0};
  std::array<size_t, size_t(ProfiledCounter::COUNT_)> counters_ = {0};
  std::mutex                                          mutex_; ///< Mutex for thread-safety.
  bool                                                enabled_ = false; ///< Statistic collection enabled flag.
};

} // end namespace comm

} // end namespace hh

#endif
