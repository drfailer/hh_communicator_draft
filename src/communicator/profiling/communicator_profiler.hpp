#ifndef COMMUNICATOR_PROFILING_COMMUNICATOR_PROFILER
#define COMMUNICATOR_PROFILING_COMMUNICATOR_PROFILER
#include "../tool/queue.hpp"
#include "profiling_tools.hpp"
#include <chrono>
#include <hedgehog.h>
#include <sstream>

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

/// @brief Profiled sizes categories.
enum class ProfiledSize : size_t {
  MaxSendOpsSize,         ///< Max send queue size.
  MaxRecvOpsSize,         ///< Max recv queue size.
  MaxCreateDataQueueSize, ///< Max data creation queue size.
  MaxSendStorageSize,     ///< Max send storage size.
  MaxRecvStorageSize,     ///< Max recv storage size.
  MaxSendQueueSize,       ///< Max send queue size.
  COUNT_,
};

/// @brief Profiled counters categories.
enum class ProfiledCounter : size_t {
  ProbedRequest, ///< Number of probed recv requests.
  HintedRequest, ///< Number of hinted recv requests
  COUNT_,
};

/// @brief Communicator profiler
class CommunicatorProfiler {
private:
  struct InTransitPackageInfo;
  struct ProfileInfo;
  struct CompiledProfileInfosPerRank;
  struct CompiledProfileInfosPerType;
  struct CompiledProfileInfos;
public:
  /// @brief Contructor.
  /// @param typeCount Number of types managed by the communicator.
  /// @param service Comm service.
  CommunicatorProfiler(size_t typeCount, CommService const *service)
      : enabled_(service->profilingEnabled()),
        nbTypes_(typeCount),
        nbProcesses_(service->nbProcesses()),
        rank_(service->rank()),
        startTime_(service->startTime()) {
    if (!enabled_) {
      return;
    }
    this->sendInfos_.resize(nbTypes_ * nbProcesses_);
    this->recvInfos_.resize(nbTypes_ * nbProcesses_);
  }

public:
  /// @brief Updated a profiled size value.
  /// @param size Category of the size to update.
  /// @param value Value to register.
  void updateProfiledSize(ProfiledSize size, size_t value) {
    if (!this->enabled_) {
      return;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    this->sizes_[size_t(size)] = std::max(this->sizes_[size_t(size)], value);
  }

  /// @brief Increment a profiled counter.
  /// @param counter Category of counter to increment.
  /// @param amout Amount to add to the counter.
  void incrementProfiledCounter(ProfiledCounter counter, size_t amout = 1) {
    if (!this->enabled_) {
      return;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    this->counters_[size_t(counter)] += amout;
  }

  /// @brief Register pre send information.
  /// @param typeId Id of the send type.
  /// @param dest Destination rank.
  /// @param packingTime Packing time.
  /// @param bufferCount Number of buffers in the package.
  /// @return Index of the entry in the profile queue.
  size_t preSend(type_id_t typeId, rank_t dest, delay_t packingTime, size_t bufferCount) {
    if (!this->enabled_) {
      return 0;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    return this->infosQueue_.add(InTransitPackageInfo{
        .packingTime = packingTime,
        .beginTp = std::chrono::system_clock::now(),
        .rank = dest,
        .typeId = typeId,
        .bufferCount = bufferCount,
    });
  }

  /// @brief Register post send information.
  /// @param queueIdx Index in the profile queue.
  /// @param packageSize Size of the package in bytes.
  void postSend(size_t queueIdx, size_t packageSize) {
    if (!this->enabled_) {
      return;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    auto                        &profile = this->infosQueue_.at(queueIdx);
    profile.bufferCount -= 1;

    if (profile.bufferCount == 0) {
      auto endTp = std::chrono::system_clock::now();
      this->sendInfos_[profile.typeId * nbProcesses_ + profile.rank].push_back(ProfileInfo{
          .timestamp = std::chrono::duration_cast<time_unit_t>(profile.beginTp - this->startTime_),
          .packingTime = profile.packingTime,
          .transmissionTime = std::chrono::duration_cast<time_unit_t>(endTp - profile.beginTp),
          .packageSize = packageSize,
      });
    }
  }

  /// @brief Register pre recv information.
  /// @param typeId Id of the received type.
  /// @param source Sender rank.
  /// @return Index in the profile queue.
  size_t preRecv(type_id_t typeId, rank_t source) {
    if (!this->enabled_) {
      return 0;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    return this->infosQueue_.add(InTransitPackageInfo{
        .packingTime = {},
        .beginTp = std::chrono::system_clock::now(),
        .rank = source,
        .typeId = typeId,
        .bufferCount = 0, // unused for the reception
    });
  }

  /// @brief Register post recv information.
  /// @param queueIdx Index in the profile queue.
  /// @param unpackingTime Unpacking time.
  /// @param packageSize Size of the package in bytes.
  void postRecv(size_t queueIdx, delay_t unpackingTime, size_t packageSize) {
    if (!this->enabled_) {
      return;
    }
    std::unique_lock<std::mutex> lock(this->mutex_);
    auto                        &profile = this->infosQueue_.at(queueIdx);
    auto                         endTp = std::chrono::system_clock::now();
    this->recvInfos_[profile.typeId * nbProcesses_ + profile.rank].push_back(ProfileInfo{
        .timestamp = std::chrono::duration_cast<time_unit_t>(profile.beginTp - this->startTime_),
        .packingTime = unpackingTime,
        .transmissionTime = std::chrono::duration_cast<time_unit_t>(endTp - profile.beginTp),
        .packageSize = packageSize,
    });
  }

  /// @brief Compile the profilign information (compute averages, ...).
  /// @tparam TM Type map of the communicator.
  template <typename TM>
  void compile() {
    this->compiledInfos_.infosPerType.clear();

    // per type stats
    for (type_id_t typeId = 0; typeId < nbTypes_; ++typeId) {
      std::vector<time_unit_t> packTimes, unpackTimes, sendTimes, recvTimes;
      std::vector<double>      bandWidth;
      std::string typeStr = typeIdToStr<TM>(typeId);

      this->compiledInfos_.infosPerType[typeStr] = {};
      auto &infosPerType = this->compiledInfos_.infosPerType[typeStr];
      infosPerType.infosPerRank.resize(nbProcesses_);

      // per rank stats
      for (rank_t rank = 0; rank < nbProcesses_; ++rank) {
        if (rank == rank_) { continue; }
        auto &infosPerRank = infosPerType.infosPerRank[rank];
        auto &sendInfos = sendInfos_[typeId * nbProcesses_ + rank];
        auto &recvInfos = recvInfos_[typeId * nbProcesses_ + rank];
        infosPerRank.sendTime = computeAvgDuration(sendInfos, this->getTransmissionTime_);
        infosPerRank.recvTime = computeAvgDuration(recvInfos, this->getTransmissionTime_);
        infosPerRank.bandWidth = computeAvg(sendInfos, this->getBandWidth_);
        infosPerRank.sendCount = sendInfos.size();
        infosPerRank.recvCount = recvInfos.size();

        // update the global stats arrays
        for (auto pi : sendInfos) {
          packTimes.push_back(pi.packingTime);
          sendTimes.push_back(pi.transmissionTime);
          bandWidth.push_back(this->getBandWidth_(pi));
        }
        for (auto pi : recvInfos) {
          unpackTimes.push_back(pi.packingTime);
          recvTimes.push_back(pi.packingTime);
        }
      }

      // add the global stats
      infosPerType.packTime = computeAvgDuration(packTimes);
      infosPerType.unpackTime = computeAvgDuration(unpackTimes);
      infosPerType.sendTime = computeAvgDuration(sendTimes);
      infosPerType.recvTime = computeAvgDuration(recvTimes);
      infosPerType.bandWidth = computeAvg(bandWidth);
      infosPerType.sendCount = sendTimes.size();
      infosPerType.recvCount = recvTimes.size();
    }
  }

  /// @brief Return the profiling information to print in the dot file (require compile).
  /// @return Sring containing the information to print.
  std::string extraPrintingInformation() const {
    std::ostringstream ss;

#define PRINT_ENUM_ARRAY_ELT(arr, enm, i) ss << #i << " = " << arr[(size_t)enm::i] << "\\l"
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
    for (auto const &[typeStr, infosPerType] : this->compiledInfos_.infosPerType) {
      print(ss, "\n=====================<", typeStr, ">=====================\\l");
      print(ss, "pack time: ", infosPerType.packTime, "\\l");
      print(ss, "unpack time: ", infosPerType.unpackTime, "\\l");
      print(ss, "send time: ", infosPerType.sendTime, "\\l");
      print(ss, "recv time: ", infosPerType.recvTime, "\\l");
      print(ss, "band width: ", infosPerType.bandWidth, " MB/s\\l");
      print(ss, "Transmission per rank:\\l");

      // per rank stats
      for (rank_t rank = 0; rank < nbProcesses_; ++rank) {
        if (rank == rank_) { continue; }
        auto const &infosPerRank = infosPerType.infosPerRank[rank];
        print(ss, "[", rank_, "][", rank, "]: ");
        print(ss, "send = ", infosPerRank.sendTime, " (count = ", infosPerRank.sendCount, ") | ");
        print(ss, "recv = ", infosPerRank.recvTime, " (count = ", infosPerRank.recvCount, ") | ");
        print(ss, "bandWidth = ", infosPerRank.bandWidth, " MB/s\\l");
      }
    }
    return ss.str();
  }

  /// @brief Return the profiling information to print in the jsonl file (require compile).
  /// @return Sring containing the information to print.
  std::string getInfos() {
    std::ostringstream ss;

    jsonl_add_entry(ss, "probed_request_count", this->counters_[(size_t)ProfiledCounter::ProbedRequest]);
    jsonl_add_entry(ss, "hinted_request_count", this->counters_[(size_t)ProfiledCounter::HintedRequest]);
    jsonl_add_entry(ss, "max_pending_send_count", this->sizes_[(size_t)ProfiledSize::MaxSendOpsSize]);
    jsonl_add_entry(ss, "max_pending_recv_count", this->sizes_[(size_t)ProfiledSize::MaxRecvOpsSize]);

    jsonl_begin_list(ss, "type_data");
    // per type stats
    size_t i = 0;
    for (auto const &[typeStr, infosPerType] : this->compiledInfos_.infosPerType) {
      jsonl_begin_obj(ss);
      {
        jsonl_add_entry(ss, "type", typeStr);
        jsonl_add_entry(ss, "send_count", infosPerType.sendCount);
        jsonl_add_entry(ss, "recv_count", infosPerType.recvCount);
        jsonl_add_entry(ss, "transmission_time.avg", infosPerType.sendTime.first);
        jsonl_add_entry(ss, "transmission_time.std", infosPerType.sendTime.second);
        jsonl_add_entry(ss, "pack_time.avg", infosPerType.packTime.first);
        jsonl_add_entry(ss, "pack_time.std", infosPerType.packTime.second);
        jsonl_add_entry(ss, "unpack_time.avg", infosPerType.unpackTime.first);
        jsonl_add_entry(ss, "unpack_time.std", infosPerType.unpackTime.second);
        jsonl_add_entry(ss, "bandWidth.avg", infosPerType.bandWidth.first);
        jsonl_add_entry(ss, "bandWidth.std", infosPerType.bandWidth.second);
      }
      jsonl_end_obj(ss, ++i == this->nbTypes_ ? ", ": "");
    }
    jsonl_end_list(ss, "");
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
      for (size_t dest = 0; dest < this->nbProcesses_; ++dest) {
        auto const &infos = this->sendInfos_[typeId * this->nbProcesses_ + dest];
        if (infos.empty()) { continue; }
        file << 0 << sep << typeIdToStr<TM>(typeId) << sep << channel << sep << this->rank_ << sep << dest;
        for (auto const &info : infos) {
          file << sep << info.timestamp.count() << ',' << info.transmissionTime.count();
        }
        file << '\n';
      }
      // recv infos
      for (size_t src = 0; src < this->nbProcesses_; ++src) {
        auto const &infos = this->recvInfos_[typeId * this->nbProcesses_ + src];
        if (infos.empty()) { continue; }
        file << 1 << sep << typeIdToStr<TM>(typeId) << sep << channel << sep << src << sep << this->rank_;
        for (auto const &info : infos) {
          file << sep << info.timestamp.count() << ',' << info.transmissionTime.count();
        }
        file << '\n';
      }
    }
  }

private: // type definitions

/// @brief Profiling information of a transiting package (common to sender and receiver).
struct InTransitPackageInfo {
  time_unit_t packingTime; ///< Packing/Upacking time.
  time_t      beginTp;     ///< Transmission start timestamp.
  rank_t      rank;        ///< Rank (sender or receiver).
  type_id_t   typeId;      ///< Type id.
  size_t      bufferCount; ///< Number of buffer in the package.
};

/// @brief Profiling information of a received/sent package (common to sender and receiver).
struct ProfileInfo {
  time_unit_t timestamp;        ///< Transmission timestamp.
  time_unit_t packingTime;      ///< Packing/Unpacking time.
  time_unit_t transmissionTime; ///< Transmission duration.
  size_t      packageSize;      ///< Package size in bytes.
};

/// @brief Compiled profiling information per rank.
struct CompiledProfileInfosPerRank {
  std::pair<time_unit_t, time_unit_t> sendTime;   ///< Send time (avg, std; sender only).
  std::pair<time_unit_t, time_unit_t> recvTime;   ///< Recv time (avg, std; receiver only).
  std::pair<double, double> bandWidth;            ///< Bandwidth (avg, std; sender only).
  size_t sendCount;                               ///< Number of packages sent.
  size_t recvCount;                               ///< Number of packages received.
};

/// @brief Compiled profiling information per type.
struct CompiledProfileInfosPerType {
  std::vector<CompiledProfileInfosPerRank> infosPerRank; ///< Infomation per rank.
  std::pair<time_unit_t, time_unit_t> packTime;          ///< Packing time (avg, std).
  std::pair<time_unit_t, time_unit_t> unpackTime;        ///< Unpacking time (avg, std).
  std::pair<time_unit_t, time_unit_t> sendTime;          ///< Send time (avg, std).
  std::pair<time_unit_t, time_unit_t> recvTime;          ///< Recv time (avg, std).
  std::pair<double, double> bandWidth;                   ///< Bandwidth (avg, std).
  size_t sendCount;                                      ///< Number of packages sent.
  size_t recvCount;                                      ///< Number of packages received.
};

/// @brief Compiled profiling information.
struct CompiledProfileInfos {
  std::map<std::string, CompiledProfileInfosPerType> infosPerType; ///< Information per type.
};

private: // data
  Queue<InTransitPackageInfo>                         infosQueue_;      ///< Profiling infos queue.
  std::vector<std::vector<ProfileInfo>>               sendInfos_ = {};  ///< Send infos.
  std::vector<std::vector<ProfileInfo>>               recvInfos_ = {};  ///< Recv infos.
  std::array<size_t, size_t(ProfiledSize::COUNT_)>    sizes_ = {0};     ///< Profiled sizes values.
  std::array<size_t, size_t(ProfiledCounter::COUNT_)> counters_ = {0};  ///< Profiled counters values.
  bool                                                enabled_ = false; ///< Statistic collection enabled flag.
  size_t                                              nbTypes_ = 0;     ///< Number of type managed by the communicator.
  size_t                                              nbProcesses_ = 0; ///< Number of processes.
  rank_t                                              rank_ = 0;        ///< Rank of the current process.
  time_t                                              startTime_;       ///< Start time (used to compute the timestamp).
  CompiledProfileInfos                                compiledInfos_;   ///< Struct that contains all the compiled profiling information to print.
  std::mutex                                          mutex_;           ///< Mutex for thread-safety.

private: // callbacks used for computing averages and standard deviation
  std::function<time_unit_t(ProfileInfo)> getPackingTime_ = [](ProfileInfo pi) { return pi.packingTime; };
  std::function<time_unit_t(ProfileInfo)> getTransmissionTime_ = [](ProfileInfo pi) { return pi.transmissionTime; };
  std::function<double(ProfileInfo)> getBandWidth_ = [](ProfileInfo pi) {
    double sizeMB = (double)pi.packageSize / (1024. * 1024.);
    double transmissionTimeS = (double)pi.transmissionTime.count() / 1'000'000'000.;
    return sizeMB / transmissionTimeS;
  };
};

} // end namespace comm

} // end namespace hh

#endif
