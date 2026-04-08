#ifndef CPP_UTILS_DEFER_H
#define CPP_UTILS_DEFER_H

template <typename T>
struct Defer {
    T run;
    Defer(T run)
        : run(run) {}
    ~Defer() {
        run();
    }
};

#define defer_impl(ops, suffix) Defer defer_##suffix([&]() { ops; });
#define defer_expand_line(ops, line) defer_impl(ops, line)
#define defer(ops) defer_expand_line(ops, __LINE__)

#endif
