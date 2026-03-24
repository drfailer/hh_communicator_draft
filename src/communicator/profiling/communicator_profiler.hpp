#ifndef COMMUNICATOR_PROFILING_COMMUNICATOR_PROFILER
#define COMMUNICATOR_PROFILING_COMMUNICATOR_PROFILER
#include "../tool/queue.hpp"
#include "profiling_tools.hpp"
#include <chrono>
#include <hedgehog/hedgehog.h>
#include <sstream>

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

struct PackageProfile {
  time_unit_t packingTime;
  time_t      beginTp;
  rank_t      rank;
  type_id_t   typeId;
  size_t      bufferCount;
};

struct ProfileInfo {
  time_unit_t timestamp;
  time_unit_t packingTime;
  time_unit_t transmissionTime;
  size_t      packageSize;
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
  CommunicatorProfiler(size_t typeCount, size_t processesCount, rank_t rank, time_t startTime, bool enabled)
      : enabled_(enabled),
        typeCount_(typeCount),
        processesCount_(processesCount),
        rank_(rank),
        startTime_(startTime) {
    if (!enabled) {
      return;
    }
    this->sendInfos_.resize(typeCount * processesCount);
    this->recvInfos_.resize(typeCount * processesCount);
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
      this->sendInfos_[profile.typeId * processesCount_ + profile.rank].push_back(ProfileInfo{
          .timestamp = std::chrono::duration_cast<time_unit_t>(profile.beginTp - this->startTime_),
          .packingTime = profile.packingTime,
          .transmissionTime = std::chrono::duration_cast<time_unit_t>(endTp - profile.beginTp),
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
    this->recvInfos_[profile.typeId * processesCount_ + profile.rank].push_back(ProfileInfo{
        .timestamp = std::chrono::duration_cast<time_unit_t>(profile.beginTp - this->startTime_),
        .packingTime = unpackingTime,
        .transmissionTime = std::chrono::duration_cast<time_unit_t>(endTp - profile.beginTp),
        .packageSize = packageSize,
    });
  }

  template <typename TM>
  std::string print() const {
    std::ostringstream ss;

#define PRINT_ENUM_ARRAY_ELT(arr, enm, i) ss << #i << " = " << arr[(size_t)enm::i] << "\n"
    PRINT_ENUM_ARRAY_ELT(sizes_, ProfiledSize, MaxSendOpsSize);
    PRINT_ENUM_ARRAY_ELT(sizes_, ProfiledSize, MaxRecvOpsSize);
    PRINT_ENUM_ARRAY_ELT(sizes_, ProfiledSize, MaxCreateDataQueueSize);
    PRINT_ENUM_ARRAY_ELT(sizes_, ProfiledSize, MaxSendStorageSize);
    PRINT_ENUM_ARRAY_ELT(sizes_, ProfiledSize, MaxRecvStorageSize);
    PRINT_ENUM_ARRAY_ELT(sizes_, ProfiledSize, MaxSendQueueSize);

    PRINT_ENUM_ARRAY_ELT(counters_, ProfiledCounter, ProbedRequest);
    PRINT_ENUM_ARRAY_ELT(counters_, ProfiledCounter, HintedRequest);
#undef PRINT_ENUM_ARRAY_ELT

    // per type stats
    for (type_id_t typeId = 0; typeId < typeCount_; ++typeId) {
      std::vector<time_unit_t> packTimes, unpackTimes, sendTimes, recvTimes;
      std::vector<double>      bandWidths;

      ss << typeIdToStr<TM>(typeId) << ":\n";

      // per rank stats
      for (rank_t rank = 0; rank < processesCount_; ++rank) {
        if (rank == rank_) {
          continue;
        }
        auto                                   &sendInfos = sendInfos_[typeId * processesCount_ + rank];
        auto                                   &recvInfos = recvInfos_[typeId * processesCount_ + rank];
        std::function<time_unit_t(ProfileInfo)> getPackingTime = [](ProfileInfo pi) { return pi.packingTime; };
        std::function<time_unit_t(ProfileInfo)> getTransmissionTime
            = [](ProfileInfo pi) { return pi.transmissionTime; };
        std::function<double(ProfileInfo)> getBandWidth = [](ProfileInfo pi) {
          double sizeMB = (double)pi.packageSize / (1024. * 1024.);
          double transmissionTimeS = (double)pi.transmissionTime.count() / 1'000'000'000.;
          return sizeMB / transmissionTimeS;
        };
        auto [sendDurAvg, sendDurStd] = computeAvgDuration(sendInfos, getTransmissionTime);
        auto [recvDurAvg, recvDurStd] = computeAvgDuration(recvInfos, getTransmissionTime);
        auto [bandWidthAvg, bandWidthStd] = computeAvg(sendInfos, getBandWidth);

        // print per rank infos
        ss << "\t" << rank_ << " -> " << rank << ": received = " << recvInfos.size() << " ("
           << durationToString(recvDurAvg) << ")"
           << ", sent = " << sendInfos.size() << ", transmission time = " << durationToString(sendDurAvg) << " +- "
           << durationToString(sendDurStd) << ", band width = " << bandWidthAvg << "MB/s +- " << bandWidthStd << "MB/s"
           << "\n";

        // update the global stats arrays
        for (auto pi : sendInfos) {
          packTimes.push_back(getPackingTime(pi));
          sendTimes.push_back(getTransmissionTime(pi));
          bandWidths.push_back(getBandWidth(pi));
        }
        for (auto pi : recvInfos) {
          unpackTimes.push_back(pi.packingTime);
          recvTimes.push_back(pi.packingTime);
        }
      }

      // add the global stats
      auto [packTimeAvg, packTimeStd] = computeAvgDuration(packTimes);
      auto [unpackTimeAvg, unpackTimeStd] = computeAvgDuration(unpackTimes);
      auto [sendTimeAvg, sendTimeStd] = computeAvgDuration(sendTimes);
      auto [recvTimeAvg, recvTimeStd] = computeAvgDuration(recvTimes);
      auto [bandWidthAvg, bandWidthStd] = computeAvg(bandWidths);

      ss << "pack time: " << durationToString(packTimeAvg) << " +- " << durationToString(packTimeStd) << "\n"
         << "unpack time: " << durationToString(unpackTimeAvg) << " +- " << durationToString(unpackTimeStd) << "\n"
         << "send time: " << durationToString(sendTimeAvg) << " +- " << durationToString(sendTimeStd) << "\n"
         << "recv time: " << durationToString(recvTimeAvg) << " +- " << durationToString(recvTimeStd) << "\n"
         << "band width: " << bandWidthAvg << "MB/s +- " << bandWidthStd << "MB/s"
         << "\n"
         << "\n";
    }
    return ss.str();
  }

  /// @brief Generate a file that will contain the transmission delays per
  ///        package. The data is generated by channel and can be visualized
  ///        using the `plot_transmission.py` script. The result is text for
  ///        now.
  ///
  /// Format of the file: s/r;typename;channel;source;dest;timestamp,duration;...
  ///
  /// @param channel Channel id.
  template <typename TM>
  void generateTransmissionFile(channel_t channel) const {
    std::filesystem::create_directory("transmissions");
    std::ofstream file("transmissions/channel_" + std::to_string(channel) + "_" + std::to_string(this->rank_) + ".transmission");
    char          sep = ';';

    for (type_id_t typeId = 0; typeId < TM::size; ++typeId) {
      // send infos
      for (size_t dest = 0; dest < this->processesCount_; ++dest) {
        auto const &infos = this->sendInfos_[typeId * this->processesCount_ + dest];
        if (infos.empty()) {
          continue;
        }
        file << 0 << sep;
        TM::apply(typeId, [&]<typename T>() { file << hh::tool::typeToStr<T>() << sep; });
        file << channel << sep << this->rank_ << sep << dest;
        for (auto const &info : infos) {
          file << sep << info.timestamp.count() << ',' << info.transmissionTime.count();
        }
        file << '\n';
      }
      // recv infos
      for (size_t src = 0; src < this->processesCount_; ++src) {
        auto const &infos = this->recvInfos_[typeId * this->processesCount_ + src];
        if (infos.empty()) {
          continue;
        }
        file << 1 << sep;
        TM::apply(typeId, [&]<typename T>() { file << hh::tool::typeToStr<T>() << sep; });
        file << channel << sep << src << sep << this->rank_;
        for (auto const &info : infos) {
          file << sep << info.timestamp.count() << ',' << info.transmissionTime.count();
        }
        file << '\n';
      }
    }
  }

private:
  Queue<PackageProfile>                               profileQueue_;
  std::vector<std::vector<ProfileInfo>>               sendInfos_ = {};
  std::vector<std::vector<ProfileInfo>>               recvInfos_ = {};
  std::array<size_t, size_t(ProfiledSize::COUNT_)>    sizes_ = {0};
  std::array<size_t, size_t(ProfiledCounter::COUNT_)> counters_ = {0};
  std::mutex                                          mutex_; ///< Mutex for thread-safety.
  bool                                                enabled_ = false; ///< Statistic collection enabled flag.
  size_t                                              typeCount_ = 0; ///< Number of type managed by the communicator.
  size_t                                              processesCount_ = 0; ///< Number of processes.
  rank_t                                              rank_ = 0;
  time_t                                              startTime_;
};

} // end namespace comm

} // end namespace hh

#endif
