#ifndef COMMUNICATOR_STATS
#define COMMUNICATOR_STATS
#include "package.hpp"
#include <chrono>
#include <cmath>
#include <iomanip>
#include <fstream>
#include <serializer/serializer.hpp>

namespace hh {

namespace comm {

inline std::string durationToString(std::chrono::nanoseconds const &ns) {
  std::ostringstream oss;

  // Cast with precision loss
  auto s = std::chrono::duration_cast<std::chrono::seconds>(ns);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(ns);
  auto us = std::chrono::duration_cast<std::chrono::microseconds>(ns);

  if (s > std::chrono::seconds::zero()) {
    oss << s.count() << "." << std::setfill('0') << std::setw(3) << (ms - s).count() << "s";
  } else if (ms > std::chrono::milliseconds::zero()) {
    oss << ms.count() << "." << std::setfill('0') << std::setw(3) << (us - ms).count() << "ms";
  } else if (us > std::chrono::microseconds::zero()) {
    oss << us.count() << "." << std::setfill('0') << std::setw(3) << (ns - us).count() << "us";
  } else {
    oss << ns.count() << "ns";
  }
  return oss.str();
}

inline std::pair<std::chrono::nanoseconds, std::chrono::nanoseconds>
computeAvgDuration(std::vector<std::chrono::nanoseconds> nss) {
  if (nss.size() == 0) {
    return {std::chrono::nanoseconds::zero(), std::chrono::nanoseconds::zero()};
  }
  std::chrono::nanoseconds sum = std::chrono::nanoseconds::zero(), mean = std::chrono::nanoseconds::zero();
  double                   sd = 0;

  for (auto ns : nss) {
    sum += ns;
  }
  mean = sum / (nss.size());

  for (auto ns : nss) {
    auto diff = (double)(ns.count() - mean.count());
    sd += diff * diff;
  }
  return {mean, std::chrono::nanoseconds((int64_t)std::sqrt(sd / (double)nss.size()))};
}

inline std::pair<double, double> computeAvg(std::vector<double> const &values) {
  if (values.size() == 0) {
    return {0, 0};
  }
  double avg = 0;
  double stddev = 0;
  double nbElements = (double)values.size();

  for (double value : values) {
    avg += value;
  }
  avg /= nbElements;

  for (double value : values) {
    double diff = value - avg;
    stddev += diff * diff;
  }
  stddev = std::sqrt(stddev / nbElements);
  return {avg, stddev};
}

// Stats container /////////////////////////////////////////////////////////////

using time_t = std::chrono::time_point<std::chrono::system_clock>;
using delay_t = std::chrono::duration<long int, std::ratio<1, 1000000000>>;

struct TransmissionInfoId {
  rank_t       source;
  rank_t       dest;
  type_id_t    typeId;
  package_id_t packageId;

  bool operator<(TransmissionInfoId const &other) const {
    if (this->source == other.source) {
      if (this->dest == other.dest) {
        if (this->typeId == other.typeId) {
          return this->packageId < other.packageId;
        }
        return this->typeId < other.typeId;
      }
      return this->dest < other.dest;
    }
    return this->source < other.source;
  }
};

struct TransmissionInfo {
  time_t  tp;
  delay_t packingTime;
  size_t  dataSize;
};

struct TransmissionStats {
  void addSend(rank_t source, rank_t dest, type_id_t typeId, package_id_t packageId, TransmissionInfo info) {
    TransmissionInfoId id{source, dest, typeId, packageId};
    if (!this->sendInfos.contains(id)) {
      this->sendInfos.insert({id, {}});
    }
    this->sendInfos.at(id).push_back(info);
  }

  void addRecv(rank_t source, rank_t dest, type_id_t typeId, package_id_t packageId, TransmissionInfo info) {
    TransmissionInfoId id{source, dest, typeId, packageId};
    if (!this->recvInfos.contains(id)) {
      this->recvInfos.insert({id, {}});
    }
    this->recvInfos.at(id).push_back(info);
  }

  std::map<TransmissionInfoId, std::vector<TransmissionInfo>> sendInfos;
  std::map<TransmissionInfoId, std::vector<TransmissionInfo>> recvInfos;
};

// TODO: we may have to define a limit on the size of storageStats
struct CommTaskStats {
  TransmissionStats transmissionStats = {};
  size_t            maxSendOpsSize = 0;
  size_t            maxRecvOpsSize = 0;
  size_t            maxCreateDataQueueSize = 0;
  size_t            maxSendStorageSize = 0;
  size_t            maxRecvStorageSize = 0;
  std::mutex        mutex;
  bool              enabled = false;

  CommTaskStats(bool enabled = false)
      : enabled(enabled) {}

  void updateSendQueuesInfos(size_t nbOps, size_t storageSize) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    this->maxSendOpsSize = std::max(this->maxSendOpsSize, nbOps);
    this->maxSendStorageSize = std::max(this->maxSendStorageSize, storageSize);
  }

  void registerSendTimings(StorageId storageId, std::vector<rank_t> dests, delay_t packingTime,
                           size_t dataSize) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    for (auto dest : dests) {
      this->transmissionStats.addSend(storageId.source, dest, storageId.typeId, storageId.packageId,
                                      TransmissionInfo{
                                          .tp = std::chrono::system_clock::now(),
                                          .packingTime = packingTime,
                                          .dataSize = dataSize,
                                      });
    }
  }

  void updateCreateDataQueueInfos(size_t nbOps) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    this->maxCreateDataQueueSize = std::max(this->maxCreateDataQueueSize, nbOps);
  }

  void registerRecvTimings(StorageId storageId, size_t dest, delay_t unpackingTime, size_t dataSize) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    this->transmissionStats.addRecv(storageId.source, dest, storageId.typeId, storageId.packageId,
                                    TransmissionInfo{
                                        .tp = std::chrono::system_clock::now(),
                                        .packingTime = unpackingTime,
                                        .dataSize = dataSize,
                                    });
  }

  void updateRecvQueuesInfos(size_t nbOps, size_t storageSize) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    this->maxRecvOpsSize = std::max(this->maxRecvOpsSize, nbOps);
    this->maxRecvStorageSize = std::max(this->maxRecvStorageSize, storageSize);
  }

  void pack(std::vector<char> &buf) const {
    using Serializer = serializer::Serializer<std::vector<char>>;
    serializer::serialize<Serializer>(buf, 0, this->maxSendOpsSize, this->maxRecvOpsSize, this->maxCreateDataQueueSize,
                                      this->maxSendStorageSize, this->maxRecvStorageSize,
                                      this->transmissionStats.sendInfos, this->transmissionStats.recvInfos);
  }

  void unpack(std::vector<char> &buf) {
    using Serializer = serializer::Serializer<std::vector<char>>;
    serializer::deserialize<Serializer>(
        buf, 0, this->maxSendOpsSize, this->maxRecvOpsSize, this->maxCreateDataQueueSize, this->maxSendStorageSize,
        this->maxRecvStorageSize, this->transmissionStats.sendInfos, this->transmissionStats.recvInfos);
  }

  // static function for computing stats summary ///////////////////////////////

  struct TransmissionPerfs {
    std::vector<std::chrono::nanoseconds>              packingDelay;
    std::vector<std::chrono::nanoseconds>              unpackingDelay;
    std::vector<std::vector<std::chrono::nanoseconds>> transmissionDelays; // 2D array: delay [i] -> [j]
    std::vector<double>                                bandWidth;
  };
  using TransmissionPerfsPerType = std::map<type_id_t, TransmissionPerfs>; // stats organized per type

  template <typename TM>
  static TransmissionPerfsPerType computeTransmissionStats(std::vector<comm::CommTaskStats> const &stats,
                                                           size_t                                  nbProcesses) {
    TransmissionPerfsPerType transmissionStats;

    for (type_id_t tid = 0; tid < TM::size; ++tid) {
      transmissionStats.insert(
          {tid, TransmissionPerfs{
                    .packingDelay = {},
                    .unpackingDelay = {},
                    .transmissionDelays = std::vector<std::vector<std::chrono::nanoseconds>>(nbProcesses * nbProcesses),
                    .bandWidth = {},
                }});
    }

    for (size_t recvRank = 0; recvRank < nbProcesses; ++recvRank) {
      for (auto ri : stats[recvRank].transmissionStats.recvInfos) {
        assert(stats[ri.first.source].transmissionStats.sendInfos.contains(ri.first));
        auto id = ri.first;
        auto sendRank = id.source;
        auto sendInfos = stats[sendRank].transmissionStats.sendInfos.at(id);
        auto recvInfos = ri.second;

        assert(recvInfos.size() == sendInfos.size());
        for (size_t i = 0; i < recvInfos.size(); ++i) {
          auto   recvInfo = recvInfos[i];
          auto   sendInfo = sendInfos[i];
          auto   delay_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(recvInfo.tp - sendInfo.tp);
          double dataSizeMB = (double)sendInfo.dataSize / (1024. * 1024.);
          double delay_s = (double)delay_ns.count() / 1'000'000'000.;

          transmissionStats.at(id.typeId).packingDelay.push_back(sendInfo.packingTime);
          transmissionStats.at(id.typeId).unpackingDelay.push_back(recvInfo.packingTime);
          transmissionStats.at(id.typeId).transmissionDelays[sendRank * nbProcesses + recvRank].push_back(delay_ns);
          transmissionStats.at(id.typeId).bandWidth.push_back(dataSizeMB / delay_s);
        }
      }
    }
    return transmissionStats;
  }

  static void strAppend(std::string &str, auto const &...args) {
    std::ostringstream oss;
    (
        [&] {
          if constexpr (std::is_same_v<decltype(args), std::vector<double> const &>) {
            auto avg = computeAvg(args);
            oss << avg.first << " +- " << avg.second;
          } else if constexpr (std::is_same_v<decltype(args), std::vector<std::chrono::nanoseconds> const &>) {
            auto avg = computeAvgDuration(args);
            oss << durationToString(avg.first) << " +- " << durationToString(avg.second);
          } else {
            oss << args;
          }
        }(),
        ...);
    str.append(oss.str() + "\\l");
  }

  template <typename TM>
  static std::string extraPrintingInformation(std::vector<CommTaskStats> const &stats, channel_t channel, size_t nbProcesses) {
    std::string infos;

    // find the max
    size_t maxSendOpsSize = 0;
    size_t maxRecvOpsSize = 0;
    size_t maxCreateDataQueueSize = 0;
    size_t maxSendStorageSize = 0;
    size_t maxRecvStorageSize = 0;
    for (auto const &stat : stats) {
      maxSendOpsSize = std::max(maxSendOpsSize, stat.maxSendOpsSize);
      maxRecvOpsSize = std::max(maxRecvOpsSize, stat.maxRecvOpsSize);
      maxCreateDataQueueSize = std::max(maxCreateDataQueueSize, stat.maxCreateDataQueueSize);
      maxSendStorageSize = std::max(maxSendStorageSize, stat.maxSendStorageSize);
      maxRecvStorageSize = std::max(maxRecvStorageSize, stat.maxRecvStorageSize);
    }
    strAppend(infos, "maxSendOpsSize = " + std::to_string(maxSendOpsSize));
    strAppend(infos, "maxRecvOpsSize = " + std::to_string(maxRecvOpsSize));
    strAppend(infos, "maxCreateDataQueueSize = " + std::to_string(maxCreateDataQueueSize));
    strAppend(infos, "maxSendStorageSize = " + std::to_string(maxSendStorageSize));
    strAppend(infos, "maxRecvStorageSize = " + std::to_string(maxRecvStorageSize));

    // transmission stats
    auto transmissionStats = computeTransmissionStats<TM>(stats, nbProcesses);
    // TODO: this is not the correct place for doing this, however, the const methods make things difficult...
    generateTransmissionFile<TM>(transmissionStats, channel, nbProcesses);
    for (type_id_t typeId = 0; typeId < TM::size; ++typeId) {
      if (!transmissionStats.contains(typeId)) {
        continue;
      }
      assert(typeId < TM::size);
      TM::apply(typeId,
                [&]<typename T>() { infos.append("========== " + hh::tool::typeToStr<T>() + " ==========\n"); });
      auto transmissionDelays = transmissionStats.at(typeId).transmissionDelays;
      auto packingDelay = transmissionStats.at(typeId).packingDelay;
      auto unpackingDelay = transmissionStats.at(typeId).unpackingDelay;
      auto bandWidth = transmissionStats.at(typeId).bandWidth;
      strAppend(infos, "packing: ", packingDelay, ", (count = ", packingDelay.size(), ")");
      strAppend(infos, "unpacking: ", unpackingDelay, ", (count = ", unpackingDelay.size(), ")");
      strAppend(infos, "bandWidth: ", bandWidth, "MB/s");
      infos.append("transmission: {\\l");
      for (size_t sender = 0; sender < nbProcesses; ++sender) {
        for (size_t receiver = 0; receiver < nbProcesses; ++receiver) {
          if (sender == receiver || transmissionDelays[sender * nbProcesses + receiver].empty()) {
            continue;
          }
          strAppend(infos, "    [", sender, " -> ", receiver,
                    "] = ", transmissionDelays[sender * nbProcesses + receiver]);
        }
      }
      strAppend(infos, "}");
    }

    return infos;
  }

  // type,sender,receiver,times...
  template <typename TM>
  static void generateTransmissionFile(TransmissionPerfsPerType const &stats, channel_t channel, size_t nbProcesses) {
      std::ofstream file("transmissions_" + std::to_string(channel) + ".csv", std::ios_base::app);

      file << "type,sender,receiver,times\n";
      for (type_id_t typeId = 0; typeId < TM::size; ++typeId) {
          if (!stats.contains(typeId)) {
              continue;
          }
          for (size_t i = 0; i < nbProcesses; ++i) {
              if (i == channel) {
                  continue;
              }
              TM::apply(typeId, [&]<typename T>() { file << '"' << hh::tool::typeToStr<T>() << '"' << ','; });
              file << channel << ',' << i;
              for (auto delay : stats.at(typeId).transmissionDelays[channel * nbProcesses + i]) {
                  file << ',' << delay.count();
              }
              file << '\n';
          }
      }
  }
};

} // end namespace comm

} // end namespace hh

#endif
