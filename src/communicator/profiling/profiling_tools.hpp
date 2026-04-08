#ifndef COMMUNICATOR_PROFILING_PROFILING_TOOLS
#define COMMUNICATOR_PROFILING_PROFILING_TOOLS
#include "../protocol.hpp"
#include <functional>
#include <ostream>

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

inline std::ostream &operator<<(std::ostream &os, time_unit_t time) {
  os << durationToString(time);
  return os;
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

inline void print(std::ostream &os, auto const &...args) {
  (
    [&] {
      if constexpr (std::is_same_v<decltype(args), std::pair<double, double> const &>) {
        os << args.first << " +- " << args.second;
      } else if constexpr (std::is_same_v<decltype(args), std::pair<time_unit_t, time_unit_t> const &>) {
        os << args.first << " +- " << args.second;
      } else {
        os << args;
      }
    }(),
    ...);
}

template <typename TM>
std::string typeIdToStr(type_id_t typeId) {
  std::string result;
  TM::apply(typeId, [&]<typename T>() { result = hh::tool::typeToStr<T>(); });
  return result;
}

// JSONL generation helper functions

template <typename T>
inline void jsonl_add_entry(std::ostream &os, std::string const &name, T value, std::string const &sep = ", ") {
  os << '"' << name << '"' << ": " << value << sep;
}

inline void jsonl_begin_obj(std::ostream &os, std::string const &name = "") {
  if (name.empty()) {
    os << "{ ";
  } else {
    os << '"' << name << '"' << ": { ";
  }
}

inline void jsonl_end_obj(std::ostream &os, std::string const &sep = ", ") {
  os << " }" << sep;
}

inline void jsonl_begin_list(std::ostream &os, std::string const &name = "") {
  if (name.empty()) {
    os << "[ ";
  } else {
    os << '"' << name << '"' << ": [ ";
  }
}

inline void jsonl_end_list(std::ostream &os, std::string const &sep = ", ") {
  os << " ]" << sep;
}

} // end namespace comm

} // end namespace hh

#endif
