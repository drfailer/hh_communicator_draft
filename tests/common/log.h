/*
 * log.h is a minimalist library that provides various log functions for people
 * who like debug prints. This header file is meat to be directly included into
 * a project and changed as needed (add info groups, print functions, ...).
 *
 * AUTHOR: drfailer
 */

#ifndef LOG_H
#define LOG_H
#include <iostream>
#include <sstream>
#include <tuple>
#include <type_traits>

/* config *********************************************************************/

// use preprocessor constants to toggle features

// log levels (comment to deactivate)
#define LOG stdout
#define LOG_INFO stdout
#define LOG_WARN stderr
#define LOG_ERR stderr
#define LOG_TODO stderr
#define LOG_DBG stderr

namespace logh {

enum class IG : int {
    Default = 0,
    Extra = 1,
    Inactive = 2,
};
#define ACTIVE_INFO_GROUPS IG::Default, IG::Extra

} // namespace logh

/******************************************************************************/

// colors and metafunctions
#define BRED "\033[1;31m"
#define BYEL "\033[1;33m"
#define BGRN "\033[1;32m"
#define BBLU "\033[1;34m"
#define MAG "\033[0;35m"
#define CRESET "\033[0m"

namespace logh {

template <typename... Types>
constexpr bool is_info_group_active(IG group, Types... active_groups) {
    return ((group == active_groups) || ...);
}

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

template <typename T> std::string stringify(T arg);

template <typename Tuple, size_t... Idx>
std::ostream &stringify_tuple_elements(std::ostream &os, Tuple const &tuple,
                                       std::index_sequence<Idx...>) {
    return ((os << (Idx > 0 ? ", " : "") << stringify(std::get<Idx>(tuple))),
            ...);
}

template <template <typename...> class Tuple, typename... Types>
std::string stringify_tuple(Tuple<Types...> const &tuple) {
    std::ostringstream oss;
    oss << "<";
    stringify_tuple_elements(oss, tuple,
                             std::make_index_sequence<(sizeof...(Types))>());
    oss << ">";
    return oss.str();
}

template <typename Iterable>
std::string stringify_iterable(Iterable const &iterable) {
    std::stringstream os;
    auto it = iterable.cbegin();

    if (it == iterable.cend()) {
        return "[]";
    }

    os << "[" << stringify(*it++);
    for (; it != iterable.cend(); it++) {
        os << ", " << stringify(*it);
    }
    os << "]";
    return os.str();
}

template <typename T> std::string stringify(T arg) {
    std::ostringstream oss;

    if constexpr (requires { oss << arg; }) {
        oss << arg;
    } else if constexpr (Iterable<T>) {
        return stringify_iterable(arg);
    } else if constexpr (TupleLike<T>) {
        return stringify_tuple(arg);
    } else {
        oss << "<" << typeid(arg).name() << ">";
    }
    return oss.str();
}

template <typename... Types>
void log(FILE *fd, char const *start, Types... msg) {
#if defined(LOG)
    std::ostringstream oss;
    oss << start;
    (oss << ... << stringify(msg));
    fprintf(fd, "%s\n", oss.str().c_str());
#endif
}

template <typename... Types>
void infog(IG group, std::string const &group_name, Types... msg) {
#if defined(LOG_INFO)
    if (is_info_group_active(group, ACTIVE_INFO_GROUPS)) {
        log(LOG_INFO, BBLU "INFO[", group_name, "]: " CRESET, msg...);
    }
#endif
}
#define INFOG(group, ...) logh::infog(logh::IG::group, #group, __VA_ARGS__)

template <typename... Types> void info(Types... msg) {
#if defined(LOG_INFO)
    if (is_info_group_active(IG::Default, ACTIVE_INFO_GROUPS)) {
        log(LOG_INFO, BBLU "INFO: " CRESET, msg...);
    }
#endif
}
#define INFO(...) logh::info(__VA_ARGS__)

template <typename... Types> void warn(Types... msg) {
#if defined(LOG_WARN)
    log(LOG_WARN, BYEL "WARN: " CRESET, msg...);
#endif
}
#define WARN(...) logh::warn(__VA_ARGS__)

template <typename... Types> void error(Types... msg) {
#if defined(LOG_ERR)
    log(LOG_ERR, BRED "ERROR: " CRESET, msg...);
#endif
}
#define ERROR(...) logh::error(__VA_ARGS__)

template <typename... Types> void todo(Types... msg) {
#if defined(LOG_TODO)
    log(LOG_TODO, BGRN "TODO: " CRESET, msg...);
#endif
}
#define TODO(...) logh::todo(__VA_ARGS__)

template <typename T>
void dbg([[maybe_unused]] std::string const &variable_name, T variable) {
#if defined(LOG_DBG)
    if constexpr (std::is_same_v<const char *, T>) {
        log(LOG_DBG, MAG "DBG: " CRESET, variable);
    } else {
        log(LOG_DBG, MAG "DBG: " CRESET, variable_name, " = ", variable);
    }
#endif
}
#define DBG(var) logh::dbg(#var, var)

} // namespace logh

#endif
