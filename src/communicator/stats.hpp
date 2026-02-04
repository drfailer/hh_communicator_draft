#ifndef COMMUNICATOR_STATS
#define COMMUNICATOR_STATS
#include "../log.hpp"
#include "package.hpp"
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <cassert>

namespace hh {

namespace comm {

using time_t = std::chrono::time_point<std::chrono::system_clock>;
using time_unit_t = std::chrono::nanoseconds;
using delay_t = std::chrono::duration<long int, std::ratio<1, 1000000000>>;

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

inline std::pair<time_unit_t, time_unit_t> computeAvgDuration(std::vector<time_unit_t> nss) {
  if (nss.size() == 0) {
    return {time_unit_t::zero(), time_unit_t::zero()};
  }
  time_unit_t sum = time_unit_t::zero(), mean = time_unit_t::zero();
  double      sd = 0;

  for (auto ns : nss) {
    sum += ns;
  }
  mean = sum / (nss.size());

  for (auto ns : nss) {
    auto diff = (double)(ns.count() - mean.count());
    sd += diff * diff;
  }
  return {mean, time_unit_t((int64_t)std::sqrt(sd / (double)nss.size()))};
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

template <typename T>
size_t writeBytes(std::vector<char> &buf, T const &data) {
    size_t pos = buf.size();
    buf.resize(pos + sizeof(T));
    std::memcpy(&buf[pos], &data, sizeof(T));
    return buf.size();
}

template <typename T>
size_t writeVectorBytes(std::vector<char> &buf, std::vector<T> const &data) {
    size_t dataCount = data.size();
    size_t pos = buf.size();
    buf.resize(pos + sizeof(dataCount) + sizeof(T) * dataCount);
    std::memcpy(&buf[pos], &dataCount, sizeof(dataCount));
    pos += sizeof(dataCount);
    std::memcpy(&buf[pos], data.data(), sizeof(T) * dataCount);
    return buf.size();
}

template <typename T>
size_t readBytes(std::vector<char> const &buf, size_t pos, T &data) {
    assert(pos + sizeof(T) <= buf.size());
    memcpy(&data, &buf[pos], sizeof(T));
    return pos + sizeof(T);
}

template <typename T>
size_t readVectorBytes(std::vector<char> const &buf, size_t pos, std::vector<T> &data) {
    size_t dataCount = 0;
    pos = readBytes(buf, pos, dataCount);
    data.resize(dataCount);
    assert(pos + sizeof(T) * dataCount <= buf.size());
    std::memcpy(data.data(), &buf[pos], sizeof(T) * dataCount);
    return pos + sizeof(T) * dataCount;
}

// Stats container /////////////////////////////////////////////////////////////

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

  size_t                         nbProcesses;
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
      : transmissionStats(nbTypes, nbProcesses),
        enabled(enabled) {}

  void updateSendQueuesInfos(size_t nbOps, size_t storageSize) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    this->maxSendOpsSize = std::max(this->maxSendOpsSize, nbOps);
    this->maxSendStorageSize = std::max(this->maxSendStorageSize, storageSize);
  }

  void registerSendTimings(StorageId storageId, std::vector<rank_t> dests, delay_t packingTime, size_t dataSize) {
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
    this->transmissionStats.addRecv(storageId.typeId, storageId.source, TransmissionInfo(unpackingTime, dataSize));
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
    writeBytes(buf, this->maxSendOpsSize);
    writeBytes(buf, this->maxRecvOpsSize);
    writeBytes(buf, this->maxCreateDataQueueSize);
    writeBytes(buf, this->maxSendStorageSize);
    writeBytes(buf, this->maxRecvStorageSize);

    writeBytes(buf, this->transmissionStats.sendInfos.size());
    for (auto sendInfo : this->transmissionStats.sendInfos) {
        writeVectorBytes(buf, sendInfo);
    }
    writeBytes(buf, this->transmissionStats.recvInfos.size());
    for (auto recvInfo : this->transmissionStats.recvInfos) {
        writeVectorBytes(buf, recvInfo);
    }
  }

  void unpack(std::vector<char> const &buf) {
    size_t pos = 0;
    pos = readBytes(buf, pos, this->maxSendOpsSize);
    pos = readBytes(buf, pos, this->maxRecvOpsSize);
    pos = readBytes(buf, pos, this->maxCreateDataQueueSize);
    pos = readBytes(buf, pos, this->maxSendStorageSize);
    pos = readBytes(buf, pos, this->maxRecvStorageSize);

    size_t infoCount = 0;

    pos = readBytes(buf, pos, infoCount);
    this->transmissionStats.sendInfos.resize(infoCount);
    for (auto &sendInfo : this->transmissionStats.sendInfos) {
        pos = readVectorBytes(buf, pos, sendInfo);
    }
    pos = readBytes(buf, pos, infoCount);
    this->transmissionStats.recvInfos.resize(infoCount);
    for (auto &recvInfo : this->transmissionStats.recvInfos) {
        pos = readVectorBytes(buf, pos, recvInfo);
    }
  }

  // static function for computing stats summary ///////////////////////////////

  struct MergedStats {
    std::vector<time_unit_t>              packingDelay;
    std::vector<time_unit_t>              unpackingDelay;
    std::vector<std::vector<time_unit_t>> transmissionDurations; // 2D array: delay [i] -> [j]
    std::vector<std::vector<time_unit_t>> transmissionTimestamps; // 2D array: delay [i] -> [j]
    std::vector<double>                   bandWidth;

    MergedStats(size_t nbProcesses)
        : transmissionDurations(nbProcesses * nbProcesses),
          transmissionTimestamps(nbProcesses * nbProcesses) {}
  };
  using MergedStatsPerType = std::vector<MergedStats>; // stats organized per type

  static time_unit_t computeDuration(time_t const &begin, time_t const &end) {
    return std::chrono::duration_cast<time_unit_t>(end - begin);
  }

  template <typename TM>
  static MergedStatsPerType mergeCommTasksStats(std::vector<comm::CommTaskStats> const &stats, time_t startTime,
                                                size_t nbProcesses) {
    auto mergedStats = MergedStatsPerType(TM::size, MergedStats(nbProcesses));

    for (type_id_t typeId = 0; typeId < TM::size; ++typeId) {
      auto &stat = mergedStats[typeId];

      for (size_t sendRank = 0; sendRank < nbProcesses; ++sendRank) {
        for (size_t recvRank = 0; recvRank < nbProcesses; ++recvRank) {
          if (sendRank == recvRank) {
            continue;
          }
          size_t sendRecvIdx = sendRank * nbProcesses + recvRank;
          auto   sendInfos = stats[sendRank].transmissionStats.sendInfosAt(typeId, recvRank);
          auto   recvInfos = stats[recvRank].transmissionStats.recvInfosAt(typeId, sendRank);

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
    return mergedStats;
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
  static std::string extraPrintingInformation(std::vector<CommTaskStats> const &stats, time_t startTime,
                                              channel_t channel, size_t nbProcesses) {
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
    strAppendStat(infos, "maxSendOpsSize = ", maxSendOpsSize);
    strAppendStat(infos, "maxRecvOpsSize = ", maxRecvOpsSize);
    strAppendStat(infos, "maxCreateDataQueueSize = ", maxCreateDataQueueSize);
    strAppendStat(infos, "maxSendStorageSize = ", maxSendStorageSize);
    strAppendStat(infos, "maxRecvStorageSize = ", maxRecvStorageSize);

    // transmission stats
    auto mergedStats = mergeCommTasksStats<TM>(stats, startTime, nbProcesses);
    for (type_id_t typeId = 0; typeId < TM::size; ++typeId) {
      auto transmissionDurations = mergedStats.at(typeId).transmissionDurations;
      auto packingDelay = mergedStats.at(typeId).packingDelay;
      auto unpackingDelay = mergedStats.at(typeId).unpackingDelay;
      auto bandWidth = mergedStats.at(typeId).bandWidth;

      TM::apply(typeId,
                [&]<typename T>() { infos.append("========== " + hh::tool::typeToStr<T>() + " ==========\n"); });
      strAppendStat(infos, "packing: ", packingDelay, ", (count = ", packingDelay.size(), ")");
      strAppendStat(infos, "unpacking: ", unpackingDelay, ", (count = ", unpackingDelay.size(), ")");
      strAppendStat(infos, "bandWidth: ", bandWidth, "MB/s");
      strAppendStat(infos, "transmission: {");
      for (size_t sendRank = 0; sendRank < nbProcesses; ++sendRank) {
        for (size_t recvRank = 0; recvRank < nbProcesses; ++recvRank) {
          auto const &durations = transmissionDurations[sendRank * nbProcesses + recvRank];
          if (sendRank != recvRank && !durations.empty()) {
            strAppendStat(infos, "\t[", sendRank, " -> ", recvRank, "] = ", durations);
          }
        }
      }
      strAppendStat(infos, "}");
    }

    // TODO: this is not the correct place for doing this, however, the const methods make things difficult...
    generateTransmissionFile<TM>(mergedStats, channel, nbProcesses);

    return infos;
  }

  // type,sender,receiver,times...
  template <typename TM>
  static void generateTransmissionFile(MergedStatsPerType const &stats, channel_t channel, size_t nbProcesses) {
    std::ofstream file("transmissions_" + std::to_string(channel) + ".data", std::ios_base::app);
    char          sep = ';';

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
