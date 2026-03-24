#ifndef COMMUNICATOR_PROFILING_PROFILING_TOOLS
#define COMMUNICATOR_PROFILING_PROFILING_TOOLS
#include "../protocol.hpp"
#include <functional>

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
/// @tparam T Type of the elements.
/// @param values List of values.
/// @param get    Function that allow to get fields values for structs.
/// @return Pair containing the mean and the standard deviation of the given list.
template <typename T>
std::pair<time_unit_t, time_unit_t> computeAvgDuration(std::vector<T> const &values, std::function<time_unit_t(T)> get) {
  if (values.size() == 0) {
    return {time_unit_t::zero(), time_unit_t::zero()};
  }
  time_unit_t sum = time_unit_t::zero(), mean = time_unit_t::zero();
  double      sd = 0;

  for (auto value : values) {
    sum += get(value);
  }
  mean = sum / values.size();

  for (auto value : values) {
    auto diff = (double)(get(value).count() - mean.count());
    sd += diff * diff;
  }
  return {mean, time_unit_t((int64_t)std::sqrt(sd / (double)values.size()))};
}

/// @brief Compute the average duration and the standard deviation of a list of durations.
/// @param values List of values.
/// @return Pair containing the mean and the standard deviation of the given list.
inline std::pair<time_unit_t, time_unit_t> computeAvgDuration(std::vector<time_unit_t> const &values) {
  if (values.size() == 0) {
    return {time_unit_t::zero(), time_unit_t::zero()};
  }
  time_unit_t sum = time_unit_t::zero(), mean = time_unit_t::zero();
  double      sd = 0;

  for (auto value : values) {
    sum += value;
  }
  mean = sum / values.size();

  for (auto value : values) {
    auto diff = (double)(value.count() - mean.count());
    sd += diff * diff;
  }
  return {mean, time_unit_t((int64_t)std::sqrt(sd / (double)values.size()))};
}

/// @brief Compute the mean and the standard deviation of a list of values.
/// @tparam T Type of the elements.
/// @param values List of values.
/// @param get    Function that allow to get fields values for structs.
/// @return Pair containing the mean and the standard deviation of the given list.
template <typename T>
std::pair<double, double> computeAvg(std::vector<T> const &values, std::function<double(T)> get) {
  if (values.size() == 0) {
    return {0, 0};
  }
  double avg = 0;
  double stddev = 0;
  double nbElements = (double)values.size();

  for (auto value : values) {
    avg += get(value);
  }
  avg /= nbElements;

  for (auto value : values) {
    double diff = get(value) - avg;
    stddev += diff * diff;
  }
  stddev = std::sqrt(stddev / nbElements);
  return {avg, stddev};
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

  for (auto value : values) {
    avg += value;
  }
  avg /= nbElements;

  for (auto value : values) {
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
/// @param data  Data to write.
/// @param count Number of elements to write.
/// @return Size of the buffer after the write.
template <typename T>
size_t writeBytes(std::vector<char> &buf, T *data, size_t count) {
  assert(data != nullptr);
  size_t pos = buf.size();
  buf.resize(pos + sizeof(T) * count);
  std::memcpy(&buf[pos], data, sizeof(T) * count);
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

/// @brief Read count elements into the given buffer starting at pos.
/// @tparam T Type of the data.
/// @param buf  Buffer to read.
/// @param pos  Position in the buffer to start reading.
/// @param data  Data to read.
/// @param count Number of elements to read.
/// @return Position of the next data in the buffer.
template <typename T>
size_t readBytes(std::vector<char> const &buf, size_t pos, T *data, size_t count) {
  assert(data != nullptr);
  assert(pos + count * sizeof(T) <= buf.size());
  std::memcpy(data, &buf[pos], sizeof(T) * count);
  return pos + sizeof(T) * count;
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

template <typename TM>
std::string typeIdToStr(type_id_t typeId) {
  std::string result;
  TM::apply(typeId, [&]<typename T>() { result = hh::tool::typeToStr<T>(); });
  return result;
}

} // end namespace comm

} // end namespace hh

#endif
