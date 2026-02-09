#ifndef COMMUNICATOR_STATS
#define COMMUNICATOR_STATS
#include "../log.hpp"
#include "package.hpp"
#include <cassert>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

/// @brief Alias for time point type.
using time_t = std::chrono::time_point<std::chrono::system_clock>;
/// @brief Alias for the time unit type.
using time_unit_t = std::chrono::nanoseconds;
/// @brief Alias for the duration.
using delay_t = std::chrono::duration<long int, std::ratio<1, 1000000000>>;

/******************************************************************************/
/*                              helper functions                              */
/******************************************************************************/

/// @brief Generate the string corresponding to the given time.
/// @brief ns Time to transform to string.
/// @return Sring in which the time is writen.
inline std::string durationToString(time_unit_t const &ns) {
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

/// @brief Compute the average duration and the standard deviation of a list of durations.
/// @param nss List of durations.
/// @return Pair containing the mean and the standard deviation of the given list.
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

/// @brief Compute the mean and the standard deviation of a list of values.
/// @param values List of values.
/// @return Pair containing the mean and the standard deviation of the given list.
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

/// @brief Write data in a buffer of bytes.
/// @tparam T Type of the data.
/// @param buf  Buffer of bytes to write to.
/// @param data Data to write.
/// @return Size of the buffer after the write.
template <typename T>
size_t writeBytes(std::vector<char> &buf, T const &data) {
  size_t pos = buf.size();
  buf.resize(pos + sizeof(T));
  std::memcpy(&buf[pos], &data, sizeof(T));
  return buf.size();
}

/// @brief Write a vector of data to a buffer of bytes.
/// @tparam T Type of the data.
/// @param buf  Buffer of bytes to write to.
/// @param data Vector of data to write.
/// @return Size of the buffer after the write.
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

/// @brief Read some bytes into the given data starting at pos.
/// @tparam T Type of the data.
/// @param buf  Buffer to read.
/// @param pos  Position in the buffer to start reading.
/// @param data Data to read into.
/// @return Position of the next data in the buffer.
template <typename T>
size_t readBytes(std::vector<char> const &buf, size_t pos, T &data) {
  assert(pos + sizeof(T) <= buf.size());
  memcpy(&data, &buf[pos], sizeof(T));
  return pos + sizeof(T);
}

/// @brief Read some bytes into the given vector of data starting at pos.
/// @tparam T Type of the data.
/// @param buf  Buffer to read.
/// @param pos  Position in the buffer to start reading.
/// @param data Vector of data to read into.
/// @return Position of the next data in the buffer.
template <typename T>
size_t readVectorBytes(std::vector<char> const &buf, size_t pos, std::vector<T> &data) {
  size_t dataCount = 0;
  pos = readBytes(buf, pos, dataCount);
  data.resize(dataCount);
  assert(pos + sizeof(T) * dataCount <= buf.size());
  std::memcpy(data.data(), &buf[pos], sizeof(T) * dataCount);
  return pos + sizeof(T) * dataCount;
}

/******************************************************************************/
/*                              Stats container                               */
/******************************************************************************/

/// @brief Information about a package transmission.
struct TransmissionInfo {
  time_t  tp;          ///< send/recv time point.
  delay_t packingTime; ///< packing/unpacking time.
  size_t  dataSize;    ///< size of the transferred data.

  /// @brief Default constructor.
  TransmissionInfo() = default;

  /// @brief Constructor from packing time and data size.
  /// @param packingTime Value of the packing time.
  /// @param dataSize    Value of the data size.
  TransmissionInfo(delay_t packingTime, size_t dataSize)
      : tp(std::chrono::system_clock::now()),
        packingTime(packingTime),
        dataSize(dataSize) {}
};

/// @brief alias for the transmission info container type.
using TransmissionInfos = std::vector<TransmissionInfo>;

/// @brief Structure that stores all the transmission statistics.
struct TransmissionStats {
  size_t                         nbProcesses; ///< number of processes
  std::vector<TransmissionInfos> sendInfos;   ///< send transmission infos [type * dest]
  std::vector<TransmissionInfos> recvInfos;   ///< recv transmission infos [type * source]

  /// @brief Default constructor.
  TransmissionStats() = default;

  /// @brief Constructor from number of types and number of processes.
  /// @param nbTypes     Number of types managed by the corresponding communicator.
  /// @param nbProcesses Number of processes.
  TransmissionStats(size_t nbTypes, size_t nbProcesses)
      : nbProcesses(nbProcesses),
        sendInfos(nbTypes * nbProcesses),
        recvInfos(nbTypes * nbProcesses) {}

  /// @brief Gives the send transmission information for a given type and destination.
  /// @param typeId Id of the transferred data type.
  /// @param dest   Destination of the packages.
  /// @return Transmission info for the given type id and destination.
  TransmissionInfos &sendInfosAt(type_id_t typeId, rank_t dest) {
    return this->sendInfos[typeId * nbProcesses + dest];
  }

  /// @brief Gives the send transmission information for a given type and destination (const).
  /// @param typeId Id of the transferred data type.
  /// @param dest   Destination of the packages.
  /// @return Transmission info for the given type id and destination (const).
  TransmissionInfos const &sendInfosAt(type_id_t typeId, rank_t dest) const {
    return this->sendInfos[typeId * nbProcesses + dest];
  }

  /// @brief Gives the recv transmission information for a given type and source.
  /// @param typeId Id of the transferred data type.
  /// @param source Source of the transmission.
  /// @return Transmission info for the given type id and source.
  TransmissionInfos &recvInfosAt(type_id_t typeId, rank_t source) {
    return this->recvInfos[typeId * nbProcesses + source];
  }

  /// @brief Gives the recv transmission information for a given type and source (const).
  /// @param typeId Id of the transferred data type.
  /// @param source Source of the transmission.
  /// @return Transmission info for the given type id and source (const).
  TransmissionInfos const &recvInfosAt(type_id_t typeId, rank_t source) const {
    return this->recvInfos[typeId * nbProcesses + source];
  }

  /// @brief Add a send info.
  /// @param typeId Type of the transferred data.
  /// @param dest   Destination of the transmission.
  /// @param info   Transmission information to add.
  void addSend(type_id_t typeId, rank_t dest, TransmissionInfo info) {
    sendInfosAt(typeId, dest).push_back(info);
  }

  /// @brief Add a recv info.
  /// @param typeId Type of the transferred data.
  /// @param dest   Destination of the transmission.
  /// @param info   Transmission information to add.
  void addRecv(type_id_t typeId, rank_t source, TransmissionInfo info) {
    recvInfosAt(typeId, source).push_back(info);
  }
};

/// @brief Structure that stores the statistics about a communicator.
struct CommTaskStats {
  TransmissionStats transmissionStats;          ///< All the transmission statistics.
  size_t            maxSendOpsSize = 0;         ///< Max send queue size.
  size_t            maxRecvOpsSize = 0;         ///< Max recv queue size.
  size_t            maxCreateDataQueueSize = 0; ///< Max data creation queue size.
  size_t            maxSendStorageSize = 0;     ///< Max send storage size.
  size_t            maxRecvStorageSize = 0;     ///< Max recv storage size.
  std::mutex        mutex;                      ///< Mutex for thread-safety.
  bool              enabled = false;            ///< Statistic collection enabled flag.

  /// @brief Default constructor.
  CommTaskStats() = default;

  /// @brief Constructor from the number of types and processes.
  /// @param nbTypes     Number of types managed by the corresponding communicator.
  /// @param nbProcesses Number of processes.
  /// @param enabled     Enable flag.
  CommTaskStats(size_t nbTypes, size_t nbProcesses, bool enabled = false)
      : transmissionStats(nbTypes, nbProcesses),
        enabled(enabled) {}

  /// @brief Update the information about the communicator's send queue and storage.
  /// @param nbOps       Size of the queue.
  /// @param storageSize Size of the storage.
  void updateSendQueuesInfos(size_t nbOps, size_t storageSize) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    this->maxSendOpsSize = std::max(this->maxSendOpsSize, nbOps);
    this->maxSendStorageSize = std::max(this->maxSendStorageSize, storageSize);
  }

  /// @brief Register the send timings.
  /// @param storageId   Storage id.
  /// @param dests       Vector of destinations.
  /// @param packingTime Data packing time.
  /// @param dataSize    Size (in bytes) of the transferred data.
  void registerSendTimings(StorageId storageId, std::vector<rank_t> dests, delay_t packingTime, size_t dataSize) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    for (auto dest : dests) {
      this->transmissionStats.addSend(storageId.typeId, dest, TransmissionInfo(packingTime, dataSize));
    }
  }

  /// @brief Update the statistic for the create data queue.
  /// @param nbOps Number of operation in the queue.
  void updateCreateDataQueueInfos(size_t nbOps) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    this->maxCreateDataQueueSize = std::max(this->maxCreateDataQueueSize, nbOps);
  }

  /// @brief Register the timings when receiving a package.
  /// @param StorageId     Storage id.
  /// @param unpackingTime Data unpacking time.
  /// @parma dataSize      Size of the transferred data.
  void registerRecvTimings(StorageId storageId, delay_t unpackingTime, size_t dataSize) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    this->transmissionStats.addRecv(storageId.typeId, storageId.source, TransmissionInfo(unpackingTime, dataSize));
  }

  /// @brief Update the information about the recv queue.
  /// @param nbOps       Recv queue size.
  /// @param storageSize Size of the recv storage.
  void updateRecvQueuesInfos(size_t nbOps, size_t storageSize) {
    if (!enabled) {
      return;
    }
    std::lock_guard<std::mutex> lock(this->mutex);
    this->maxRecvOpsSize = std::max(this->maxRecvOpsSize, nbOps);
    this->maxRecvStorageSize = std::max(this->maxRecvStorageSize, storageSize);
  }

  /// @brief Serialize the statistics into buf.
  /// @param buf Buffer to write in.
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

  /// @brief Deserialize the statistics contained in buf.
  /// @parma buf Buffer that contains the data.
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

  /// @brief Structure that contains the merged statistics for all the processes.
  struct MergedStats {
    std::vector<time_unit_t>              packingDelay;           ///< all the packing times.
    std::vector<time_unit_t>              unpackingDelay;         ///< all the unpacking times.
    std::vector<std::vector<time_unit_t>> transmissionDurations;  ///< all the transmission duration (2D array: delay [source] -> [dest])
    std::vector<std::vector<time_unit_t>> transmissionTimestamps; ///< all the transmission timestamps (2D array: delay [source] -> [dest])
    std::vector<double>                   bandWidth;

    /// @brief Constructor from number of processes (allocates the 2D arrays).
    /// @param nbProcesses Number of processes.
    MergedStats(size_t nbProcesses)
        : transmissionDurations(nbProcesses * nbProcesses),
          transmissionTimestamps(nbProcesses * nbProcesses) {}
  };
  /// @brief Alias for the merged statistic container type (one MergedStats per type).
  using MergedStatsPerType = std::vector<MergedStats>;

  /// @brief Helper function to compute the duration between `begin` and `end`.
  /// @param begin Start time point.
  /// @param end   End time point.
  /// @return Duration in time_unit_t.
  static time_unit_t computeDuration(time_t const &begin, time_t const &end) {
    return std::chrono::duration_cast<time_unit_t>(end - begin);
  }

  /// @brief Merge the statistics of all the processes (done only by the master rank).
  /// @param stats       List of the statistics of all the processes.
  /// @param startTime   Start time of the application (creation of the service).
  /// @param nbProcesses Number of processes.
  /// @param nbType      Number of types managed by the corresponding communicator.
  static MergedStatsPerType mergeCommTasksStats(std::vector<comm::CommTaskStats> const &stats, time_t startTime,
                                                size_t nbProcesses, size_t nbTypes) {
    auto mergedStats = MergedStatsPerType(nbTypes, MergedStats(nbProcesses));

    for (type_id_t typeId = 0; typeId < nbTypes; ++typeId) {
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

            assert(sendInfos[i].tp < recvInfos[i].tp);
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

  /// @brief Helper function that appends `args` to the string. If args
  ///        contains a vector of values or duration, the average and mean will
  ///        be appended.
  /// @param str  String to append to.
  /// @param args Content to append to the string.
  static void strAppendStat(std::string &str, auto const &...args) {
    std::ostringstream oss;
    (
        [&] {
          if constexpr (std::is_same_v<decltype(args), std::vector<double> const &>) {
            auto avg = computeAvg(args);
            oss << avg.first << " +- " << avg.second;
          } else if constexpr (std::is_same_v<decltype(args), std::vector<time_unit_t> const &>) {
            auto avg = computeAvgDuration(args);
            oss << durationToString(avg.first) << " +- " << durationToString(avg.second);
          } else {
            oss << args;
          }
        }(),
        ...);
    str.append(oss.str() + "\\l");
  }

  /// @brief Give the string that contains all the profiling information to
  ///        print the dot file.
  /// @param stats       List of the statistics of all the processes.
  /// @param startTime   Start time of the application (creation of the service).
  /// @param channel     Channel id.
  /// @param nbProcesses Number of processes.
  /// @return String to add to the dot file.
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
    auto mergedStats = mergeCommTasksStats(stats, startTime, nbProcesses, TM::size);
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

  /// @brief Generate a file that will contain the transmission delays per
  ///        package. The data is generated by channel and can be visualized
  ///        using the `plot_transmission.py` script. The result is text for
  ///        now.
  ///
  /// Format of the file: typename;channel(sender);rank(receiver);(timestamp,duration)...
  ///
  /// @param stats       List of the statistics of all the processes.
  /// @param channel     Channel id.
  /// @param nbProcesses Number of processes.
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
