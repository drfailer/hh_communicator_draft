#ifndef COMMUNICATOR_TOOL_LOG
#define COMMUNICATOR_TOOL_LOG
#include <iostream>
#include <cstdio>
#include <sstream>
#include <source_location>

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {
/// @brief Tools namespace
namespace log {

/// @brief Print a log message (info level).
/// Requires defining HH_COMM_LOG_INFO.
/// @param args Elements to print.
void info([[maybe_unused]] auto ...args) {
#ifdef HH_COMM_LOG_INFO
    std::ostringstream oss;
    oss << "HH  INFO   ";
    (oss << ... << args);
    printf("%s\n", oss.str().c_str());
#endif
}

/// @brief Print a log message (warning level).
/// Requires defining HH_COMM_LOG_WARN.
/// @param args Elements to print.
void warn([[maybe_unused]] auto ...args) {
#ifdef HH_COMM_LOG_WARN
    std::ostringstream oss;
    oss << "HH  WARN   ";
    (oss << ... << args);
    fprintf(stderr, "%s\n", oss.str().c_str());
#endif
}

/// @brief Print an error log.
/// Always printed.
/// @param args Elements to print.
void error(auto ...args) {
    std::ostringstream oss;
    oss << "HH  ERROR  ";
    (oss << ... << args);
    fprintf(stderr, "%s\n", oss.str().c_str());
}

} // end namespace log

} // end namespace comm

} // end namespace hh

#endif
