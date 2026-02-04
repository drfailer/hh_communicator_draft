#ifndef COMMUNICATOR_TOOL_LOG
#define COMMUNICATOR_TOOL_LOG
#include <iostream>
#include <cstdio>
#include <sstream>
#include <source_location>

namespace hh {

namespace comm {

namespace log {

void info(auto ...args) {
    std::ostringstream oss;
    oss << "HH  INFO  ";
    (oss << ... << args);
    printf("%s\n", oss.str().c_str());
}

void warn(auto ...args) {
    std::ostringstream oss;
    oss << "HH  WARN  ";
    (oss << ... << args);
    fprintf(stderr, "%s\n", oss.str().c_str());
}

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
