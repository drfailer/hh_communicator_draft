#ifndef COMMUNICATOR_PROFILING_PROFILING_TOOLS
#define COMMUNICATOR_PROFILING_PROFILING_TOOLS

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

/******************************************************************************/
/*                                   types                                    */
/******************************************************************************/

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

/// @brief Helper function that appends `args` to the string. If args
///        contains a vector of values or duration, the average and mean will
///        be appended.
/// @param str  String to append to.
/// @param args Content to append to the string.
inline void strAppend(std::string &str, auto const &...args) {
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

} // end namespace comm

} // end namespace hh

#endif
