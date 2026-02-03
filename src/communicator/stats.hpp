#ifndef COMMUNICATOR_STATS
#define COMMUNICATOR_STATS
#include "package.hpp"
#include "../log.hpp"
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
using time_unit_t = std::chrono::nanoseconds;
using delay_t = std::chrono::duration<long int, std::ratio<1, 1000000000>>;

struct TransmissionInfo {
  time_t  tp;
  delay_t packingTime;
  size_t  dataSize;

  TransmissionInfo() = default;

  TransmissionInfo(delay_t packingTime, size_t dataSize)
      : tp(std::chrono::system_clock::now()),
        packingTime(packingTime),
        dataSize(dataSize) {}
};
using TransmissionInfos = std::vector<TransmissionInfo>;

struct TransmissionStats {
  TransmissionStats() = default;

  TransmissionStats(size_t nbTypes, size_t nbProcesses)
    : nbProcesses(nbProcesses),
      sendInfos(nbTypes * nbProcesses),
      recvInfos(nbTypes * nbProcesses) {}

  TransmissionInfos &sendInfosAt(type_id_t typeId, rank_t dest) {
      return this->sendInfos[typeId * nbProcesses + dest];
  }

  TransmissionInfos const &sendInfosAt(type_id_t typeId, rank_t dest) const {
      return this->sendInfos[typeId * nbProcesses + dest];
  }

  TransmissionInfos &recvInfosAt(type_id_t typeId, rank_t source) {
      return this->recvInfos[typeId * nbProcesses + source];
  }

  TransmissionInfos const &recvInfosAt(type_id_t typeId, rank_t source) const {
      return this->recvInfos[typeId * nbProcesses + source];
  }

  void addSend(type_id_t typeId, rank_t dest, TransmissionInfo info) {
    sendInfosAt(typeId, dest).push_back(info);
  }

  void addRecv(type_id_t typeId, rank_t source, TransmissionInfo info) {
    recvInfosAt(typeId, source).push_back(info);
  }

  size_t nbProcesses;
  std::vector<TransmissionInfos> sendInfos; // [type * dest]
  std::vector<TransmissionInfos> recvInfos; // [type * source]
};

struct CommTaskStats {
  TransmissionStats transmissionStats;
  size_t            maxSendOpsSize = 0;
  size_t            maxRecvOpsSize = 0;
  size_t            maxCreateDataQueueSize = 0;
  size_t            maxSendStorageSize = 0;
  size_t            maxRecvStorageSize = 0;
  std::mutex        mutex;
  bool              enabled = false;

  CommTaskStats() = default;

  CommTaskStats(size_t nbTypes, size_t nbProcesses, bool enabled = false)
      : transmissionStats(nbTypes, nbProcesses), enabled(enabled) {}

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
      this->transmissionStats.addSend(storageId.typeId, dest, TransmissionInfo(packingTime, dataSize));
    }
  }

  void updateCreateDataQueueInfos(size_t nbOps) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    this->maxCreateDataQueueSize = std::max(this->maxCreateDataQueueSize, nbOps);
  }

  void registerRecvTimings(StorageId storageId, delay_t unpackingTime, size_t dataSize) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    this->transmissionStats.addRecv(storageId.typeId, storageId.source,
                                    TransmissionInfo(unpackingTime, dataSize));
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
                                      this->transmissionStats.nbProcesses, this->transmissionStats.nbProcesses,
                                      this->transmissionStats.sendInfos, this->transmissionStats.recvInfos);
  }

  void unpack(std::vector<char> &buf) {
    using Serializer = serializer::Serializer<std::vector<char>>;
    serializer::deserialize<Serializer>(
        buf, 0, this->maxSendOpsSize, this->maxRecvOpsSize, this->maxCreateDataQueueSize, this->maxSendStorageSize,
        this->maxRecvStorageSize, this->transmissionStats.nbProcesses, this->transmissionStats.nbProcesses,
        this->transmissionStats.sendInfos, this->transmissionStats.recvInfos);
  }

  // static function for computing stats summary ///////////////////////////////

  template <typename T>
  struct Mean {
      T value;
      T stddev;
  };

  struct TransmissionPerfs {
    std::vector<time_unit_t>              packingDelay;
    std::vector<time_unit_t>              unpackingDelay;
    std::vector<std::vector<time_unit_t>> transmissionDurations; // 2D array: delay [i] -> [j]
    std::vector<std::vector<time_unit_t>> transmissionTimestamps; // 2D array: delay [i] -> [j]
    std::vector<double>                   bandWidth;

    TransmissionPerfs(size_t nbProcesses)
        : transmissionDurations(nbProcesses * nbProcesses),
          transmissionTimestamps(nbProcesses * nbProcesses) {}
  };
  using TransmissionPerfsPerType = std::vector<TransmissionPerfs>; // stats organized per type

  static time_unit_t computeDuration(time_t const &begin, time_t const &end) {
      return std::chrono::duration_cast<time_unit_t>(end - begin);
  }

  template <typename TM>
  static TransmissionPerfsPerType computeTransmissionStats(std::vector<comm::CommTaskStats> const &stats,
                                                           time_t startTime, size_t nbProcesses) {
    auto transmissionStats = TransmissionPerfsPerType(TM::size, TransmissionPerfs(nbProcesses));

    for (type_id_t typeId = 0; typeId < TM::size; ++typeId) {
        auto &stat = transmissionStats[typeId];

        for (size_t sendRank = 0; sendRank < nbProcesses; ++sendRank) {
            for (size_t recvRank = 0; recvRank < nbProcesses; ++recvRank) {
                if (sendRank == recvRank) {
                    continue;
                }
                size_t sendRecvIdx = sendRank * nbProcesses + recvRank;
                auto sendInfos = stats[sendRank].transmissionStats.sendInfosAt(typeId, recvRank);
                auto recvInfos = stats[recvRank].transmissionStats.recvInfosAt(typeId, sendRank);

                assert(recvInfos.size() == sendInfos.size());
                for (size_t i = 0; i < sendInfos.size(); ++i) {
                    auto   delay_ns = computeDuration(sendInfos[i].tp, recvInfos[i].tp);
                    auto   timestamp = computeDuration(startTime, sendInfos[i].tp);
                    double dataSizeMB = (double)sendInfos[i].dataSize / (1024. * 1024.);
                    double delay_s = (double)delay_ns.count() / 1'000'000'000.;

                    stat.packingDelay.push_back(sendInfos[i].packingTime);
                    stat.unpackingDelay.push_back(recvInfos[i].packingTime);
                    stat.transmissionDurations[sendRecvIdx].push_back(delay_ns);
                    stat.transmissionTimestamps[sendRecvIdx].push_back(timestamp);
                    stat.bandWidth.push_back(dataSizeMB / delay_s);
                }
            }
        }
    }
    return transmissionStats;
  }

  static void strAppendStat(std::string &str, auto const &...args) {
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
  static std::string extraPrintingInformation(std::vector<CommTaskStats> const &stats, time_t startTime, channel_t channel, size_t nbProcesses) {
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
    strAppendStat(infos, "maxSendOpsSize = " + std::to_string(maxSendOpsSize));
    strAppendStat(infos, "maxRecvOpsSize = " + std::to_string(maxRecvOpsSize));
    strAppendStat(infos, "maxCreateDataQueueSize = " + std::to_string(maxCreateDataQueueSize));
    strAppendStat(infos, "maxSendStorageSize = " + std::to_string(maxSendStorageSize));
    strAppendStat(infos, "maxRecvStorageSize = " + std::to_string(maxRecvStorageSize));

    // transmission stats
    auto transmissionStats = computeTransmissionStats<TM>(stats, startTime, nbProcesses);
    // TODO: this is not the correct place for doing this, however, the const methods make things difficult...
    generateTransmissionFile<TM>(transmissionStats, channel, nbProcesses);
    for (type_id_t typeId = 0; typeId < TM::size; ++typeId) {
      assert(typeId < TM::size);
      TM::apply(typeId,
                [&]<typename T>() { infos.append("========== " + hh::tool::typeToStr<T>() + " ==========\n"); });
      auto transmissionDurations = transmissionStats.at(typeId).transmissionDurations;
      auto packingDelay = transmissionStats.at(typeId).packingDelay;
      auto unpackingDelay = transmissionStats.at(typeId).unpackingDelay;
      auto bandWidth = transmissionStats.at(typeId).bandWidth;
      strAppendStat(infos, "packing: ", packingDelay, ", (count = ", packingDelay.size(), ")");
      strAppendStat(infos, "unpacking: ", unpackingDelay, ", (count = ", unpackingDelay.size(), ")");
      strAppendStat(infos, "bandWidth: ", bandWidth, "MB/s");
      infos.append("transmission: {\\l");
      for (size_t sender = 0; sender < nbProcesses; ++sender) {
        for (size_t receiver = 0; receiver < nbProcesses; ++receiver) {
          if (sender == receiver || transmissionDurations[sender * nbProcesses + receiver].empty()) {
            continue;
          }
          strAppendStat(infos, "    [", sender, " -> ", receiver,
                    "] = ", transmissionDurations[sender * nbProcesses + receiver]);
        }
      }
      strAppendStat(infos, "}");
    }

    return infos;
  }

  // type,sender,receiver,times...
  template <typename TM>
  static void generateTransmissionFile(TransmissionPerfsPerType const &stats, channel_t channel, size_t nbProcesses) {
      std::ofstream file("transmissions_" + std::to_string(channel) + ".data", std::ios_base::app);
      char sep = ';';

      for (type_id_t typeId = 0; typeId < TM::size; ++typeId) {
          for (size_t i = 0; i < nbProcesses; ++i) {
              if (i == channel) {
                  continue;
              }
              TM::apply(typeId, [&]<typename T>() { file << hh::tool::typeToStr<T>() << sep; });
              file << channel << sep << i;
              auto durations = stats.at(typeId).transmissionDurations[channel * nbProcesses + i];
              auto timestamps = stats.at(typeId).transmissionTimestamps[channel * nbProcesses + i];
              assert(durations.size() == timestamps.size());
              for (size_t i = 0; i < durations.size(); ++i) {
                  file << sep << timestamps[i].count() << ',' << durations[i].count();
              }
              file << '\n';
          }
      }
  }
};

} // end namespace comm

} // end namespace hh

#endif
