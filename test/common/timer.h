#ifndef CPP_UTILS_TIMER_H
#define CPP_UTILS_TIMER_H
#include <chrono>

#define DEFAULT_TIMER_PRECISION std::chrono::nanoseconds
#define timer_start(ID)                                                        \
    auto _timer_start_##ID = std::chrono::high_resolution_clock::now()
#define timer_end(ID)                                                          \
    auto _timer_end_##ID = std::chrono::high_resolution_clock::now()
#define timer_count(ID)                                                        \
    std::chrono::duration_cast<DEFAULT_TIMER_PRECISION>(_timer_end_##ID        \
                                                        - _timer_start_##ID)   \
        .count()
#define timer_count_prec(ID, PREC)                                             \
    std::chrono::duration_cast<std::chrono::PREC>(_timer_end_##ID              \
                                                  - _timer_start_##ID)         \
        .count()
#define timer_report(ID)                                                       \
    std::cout << "timer " #ID ": " << timer_count(ID) << std::endl
#define timer_report_prec(ID, PREC)                                            \
    std::cout << "timer " #ID ": " << timer_count_prec(ID, PREC) << " " #PREC  \
              << std::endl

#endif
