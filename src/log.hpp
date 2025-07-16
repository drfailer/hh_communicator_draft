/*
 * log.h is a minimalist library that provides various log functions for people
 * who like debug prints. Modify this file directly to customize log levels.
 */

#ifndef LOG_H
#define LOG_H
#include <iostream>
#include <type_traits>

/* config *********************************************************************/

// use preprocessor constants to toggle features

// log levels (comment to deactivate)
#define LOG
#define LOG_INFO
#define LOG_WARN
#define LOG_ERR
#define LOG_TODO
#define LOG_DBG

// if defined, the log module will contain functions to print containers
// (requires C++20 and usable only with the DBG macro)
#define DBG_CONTAINERS

// info groups active (groups that are in the list are activated)
// note: the identifier of the group is displayed in the message, use a custom
// define / enum when using the INFO_GRP macro if you want a more explicit name.
#define INFO_GRPS 0, 1

/******************************************************************************/

// colors and metafunctions
#if defined(LOG)
#define BRED "\033[1;31m"
#define BYEL "\033[1;33m"
#define BGRN "\033[1;32m"
#define BBLU "\033[1;34m"
#define MAG "\033[0;35m"
#define CRESET "\033[0m"

#ifdef DBG_CONTAINERS
#include <tuple> // required for std::get
#endif

namespace logh {

template <size_t... Ids> struct GroupList {};

template <size_t Id, typename Lst> struct isInfoIdActive;

template <size_t Id> struct isInfoIdActive<Id, GroupList<>> {
  static constexpr bool value = false;
};

template <size_t Id, size_t Head, size_t... Ids>
struct isInfoIdActive<Id, GroupList<Head, Ids...>> {
  static constexpr bool value =
      Id == Head || isInfoIdActive<Id, GroupList<Ids...>>::value;
};

#ifdef DBG_CONTAINERS

template <typename T> struct clear {
  using type = std::remove_const_t<std::remove_reference_t<T>>;
};

template <typename T> using clear_t = clear<T>::type;

template <typename T>
concept Iterable = requires(clear_t<T> t) {
  t.cbegin();
  t.cend();
  typename T::const_iterator;
} && !std::is_same_v<clear_t<T>, std::string>;

template <typename T>
concept TupleLike = requires(T obj) {
  std::get<0>(obj);
  typename std::tuple_element_t<0, T>;
  std::tuple_size_v<T>;
};

template <Iterable Container>
std::ostream &operator<<(std::ostream &os, Container const &container);

template <typename Tuple, size_t... Idx>
std::ostream &printTuple(std::ostream &os, Tuple const &tuple,
                         std::index_sequence<Idx...>) {
  return ((os << (Idx > 0 ? ", " : "") << std::get<Idx>(tuple)), ...);
}

template <template <typename...> class Tuple, typename... Types>
  requires TupleLike<Tuple<Types...>>
std::ostream &operator<<(std::ostream &os, Tuple<Types...> const &tuple) {
  os << "<";
  printTuple(os, tuple, std::make_index_sequence<(sizeof...(Types))>());
  return os << ">";
}

template <Iterable Container>
std::ostream &operator<<(std::ostream &os, Container const &container) {
  auto it = container.cbegin();

  os << "[" << *it++;
  for (; it != container.cend(); it++) {
    os << ", " << *it;
  }
  return os << "]";
}

#endif // DBG_CONTAINERS

} // namespace logh

#endif // defined(LOG)

// INFO is displayed all the time when LOG_INFO is defined
// INFO_GRP takes the id of a group, only the groups that are in INFO_GRPS are
// displayed
#if defined(LOG) && defined(LOG_INFO)
#define INFO_GRP(msg, id)                                                      \
  if constexpr (logh::isInfoIdActive<id, logh::GroupList<INFO_GRPS>>::value) { \
    std::cout << BBLU "INFO[" #id "]: " CRESET << msg << std::endl;            \
  }
#define INFO(msg) INFO_GRP(msg, 0)
#else
#define INFO(msg)
#define INFO_GRP(msg, id)
#endif

// WARN
#if defined(LOG) && defined(LOG_WARN)
#define WARN(msg) std::cout << BYEL "WARN: " CRESET << msg << std::endl;
#else
#define WARN(msg)
#endif

// ERROR
#if defined(LOG) && defined(LOG_ERR)
#define ERROR(msg) std::cout << BRED "ERROR: " CRESET << msg << std::endl;
#else
#define ERROR(msg)
#endif

// TODO
#if defined(LOG) && defined(LOG_TODO)
#define TODO(msg) std::cout << BGRN "TODO: " CRESET << msg << std::endl;
#else
#define TODO(msg)
#endif

// DBG
#if defined(LOG) && defined(LOG_DBG)
#ifdef DBG_CONTAINERS
#define DBG(var)                                                               \
  if constexpr (!std::is_same_v<const char(&)[sizeof(var)], decltype(var)>) {  \
    using logh::operator<<;                                                    \
    std::cout << MAG "DBG: " CRESET #var " = " << var << std::endl;            \
  } else { /* if not variable */                                               \
    using logh::operator<<;                                                    \
    std::cout << MAG "DBG: " CRESET << var << std::endl;                       \
  }
#else // DBG_CONTAINERS
#define DBG(var)                                                               \
  if constexpr (!std::is_same_v<const char(&)[sizeof(var)], decltype(var)>) {  \
    std::cout << MAG "DBG: " CRESET #var " = " << var << std::endl;            \
  } else { /* if not variable */                                               \
    std::cout << MAG "DBG: " CRESET << var << std::endl;                       \
  }
#endif // DBG_CONTAINERS
#else
#define DBG(var)
#endif

#endif
