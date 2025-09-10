#ifndef CPP_UTILS_UTEST_H
#define CPP_UTILS_UTEST_H
#include <iostream>
#include <sstream>
#include <string>

#define utest_start()                                                          \
    int __utest_nb_test_failed__ = 0;                                          \
    int __utest_nb_test__ = 0;
#define utest_end()                                                            \
    if (__utest_nb_test_failed__ > 0) {                                        \
        std::cerr << __utest_nb_test_failed__ << " / " << __utest_nb_test__    \
                  << " failed." << std::endl;                                  \
    } else {                                                                   \
        std::cerr << __utest_nb_test__ << " passed." << std::endl;             \
    }

#define UTEST_STATUS __utest_nb_test_failed__

#define UTest(name, ...)                                                       \
    void test_function_##name(                                                 \
        [[maybe_unused]] utest::test_status_t &__test_status__, __VA_ARGS__)

#define urun_test(name, ...)                                                   \
    ++__utest_nb_test__;                                                       \
    if (utest::run(test_function_##name, #name, __VA_ARGS__)) {                \
        ++__utest_nb_test_failed__;                                            \
    }

#define uassert(expr)                                                          \
    if (utest::assert_("ASSERT", expr, #expr, __FILE__, __LINE__) != 0) {      \
        ++__test_status__.nb_assert_failed;                                    \
    }
#define uassert_equal(found, expect)                                           \
    if (utest::assert_equal_("ASSERT", found, expect, #found, #expect,         \
                             __FILE__, __LINE__)                               \
        != 0) {                                                                \
        ++__test_status__.nb_assert_failed;                                    \
    }
#define uassert_float_equal(found, expect, prec)                               \
    if (utest::assert_float_equal_("ASSERT", found, expect, prec, #found,      \
                                   #expect, __FILE__, __LINE__)                \
        != 0) {                                                                \
        ++__test_status__.nb_assert_failed;                                    \
    }
#define urequire(expr)                                                         \
    if (utest::assert_("REQUIRE", expr, #expr, __FILE__, __LINE__) != 0) {     \
        __test_status__.require_failed = true;                                 \
        return;                                                                \
    }

namespace utest {

struct test_status_t {
    bool   require_failed = false;
    size_t nb_assert_failed = 0;
};

inline void report(std::string const &test_name, test_status_t const &status) {
    if (!status.require_failed && status.nb_assert_failed == 0) {
        std::cerr << "\033[0;32m[TEST PASSED] " << test_name << "\033[0m"
                  << std::endl;
    } else {
        std::cerr << "\033[1;31m[TEST FAILED] " << test_name << ":\033[0m ";

        if (status.require_failed) {
            std::cerr << "failed on require." << std::endl;
        } else {
            std::cerr << status.nb_assert_failed << " assertion failed."
                      << std::endl;
        }
    }
}

inline void error(std::string const &group, std::string const filename,
                  size_t line, std::string const &msg) {
    std::cerr << "\033[1;31m[" << group << " ERROR]:\033[0m " << filename << ":"
              << line << ": " << msg << std::endl;
}

inline bool run(auto test_function, std::string const test_name, auto &&...args) {
    utest::test_status_t status{false, 0};
    test_function(status, std::forward<decltype(args)>(args)...);
    utest::report(test_name, status);
    return status.nb_assert_failed > 0 || status.require_failed;
}

inline int assert_(std::string const &group, bool expr,
                   std::string const &expr_str, std::string const &filename,
                   size_t line) {
    if (!expr) {
        error(group, filename, line, "`" + expr_str + "` evaluated to false.");
        return 1;
    }
    return 0;
}

inline int assert_equal_(std::string const &group, auto const &found,
                         auto const &expect, std::string const &lhs_str,
                         std::string const &rhs_str,
                         std::string const &filename, size_t line) {
    if (found != expect) {
        std::ostringstream oss;
        oss << lhs_str << " != " << rhs_str << " -> " << found
            << " != " << expect << ".";
        error(group, filename, line, oss.str());
        return 1;
    }
    return 0;
}

inline int assert_float_equal_(std::string const &group, float found,
                               float expect, float prec,
                               std::string const &lhs_str,
                               std::string const &rhs_str,
                               std::string const &filename, size_t line) {
    if (!(expect - prec <= found && found <= expect + prec)) {
        std::ostringstream oss;
        oss << lhs_str << " != " << rhs_str << " -> " << found
            << " != " << expect << ".";
        error(group, filename, line, oss.str());
        return 1;
    }
    return 0;
}

} // namespace utest

#endif
