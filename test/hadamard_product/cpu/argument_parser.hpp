#ifndef ARGUMENT_PARSER
#define ARGUMENT_PARSER
#include <cassert>
#include <functional>
#include <iostream>
#include <map>
#include <string>

namespace ap {

struct ArgumentHandler {
    std::function<void(std::string const &)> cb;
    bool                                     expectsArg;
};

struct ArgumentParser {
    int                                    argc;
    char                                 **argv;
    std::map<std::string, ArgumentHandler> args;
};

inline ArgumentParser argument_parser_create(int argc, char **argv) {
    return ArgumentParser{
        .argc = argc,
        .argv = argv,
        .args = {},
    };
}

inline void add_arg(ArgumentParser *ap, std::string const &arg, auto cb, bool expects_value = true) {
    ap->args.insert({arg, ArgumentHandler{cb, expects_value}});
}

#define AP_ADD_TYPE_ARG(type_, default_value_, arg_value_)                                                    \
    inline void add_##type_##_arg(ArgumentParser *ap, std::string const &arg, type_ *variable,                \
                                  type_ default_value = default_value_) {                                     \
        assert(variable != NULL);                                                                             \
        *variable = default_value;                                                                            \
        ap->args.insert({arg, ArgumentHandler{                                                                \
                                  .cb = [variable](std::string const &arg_value) { *variable = arg_value_; }, \
                                  .expectsArg = true,                                                         \
                              }});                                                                            \
    }
AP_ADD_TYPE_ARG(int, 0, std::stoi(arg_value))
AP_ADD_TYPE_ARG(size_t, 0, std::stoul(arg_value))

inline void add_string_arg(ArgumentParser *ap, std::string const &arg, std::string *variable,
                           std::string default_value = "") {
    assert(variable != NULL);
    *variable = default_value;
    ap->args.insert({arg, ArgumentHandler{
                              .cb = [variable](std::string const &arg_value) { *variable = arg_value; },
                              .expectsArg = true,
                          }});
}

inline void add_bool_arg(ArgumentParser *ap, std::string const &arg, bool *variable) {
    assert(variable != NULL);
    *variable = false;
    ap->args.insert({arg, ArgumentHandler{
                              .cb = [variable](std::string const &) { *variable = true; },
                              .expectsArg = false,
                          }});
}

struct ParsePrefixResult {
    bool        ok;
    std::string rest;
};
inline ParsePrefixResult parse_prefix_(std::string const &str, std::string const &prefix) {
    if (str.size() < prefix.size()) {
        return ParsePrefixResult{false, str};
    }

    for (size_t i = 0; i < prefix.size(); ++i) {
        if (str[i] != prefix[i]) {
            return ParsePrefixResult{false, str};
        }
    }
    return ParsePrefixResult{true, str.substr(prefix.size())};
}

inline bool argument_parser_run(ArgumentParser *ap) {
    int arg_idx = 0;

    while (arg_idx < ap->argc) {
        std::string curArg(ap->argv[arg_idx]);
        for (auto arg : ap->args) {
            auto result = parse_prefix_(curArg, arg.first);

            if (result.ok) {
                if (arg.second.expectsArg) {
                    if (result.rest.starts_with('=') || result.rest.size() > 1) {
                        arg.second.cb(result.rest.substr(1));
                    } else {
                        arg_idx += 1;
                        if (arg_idx > ap->argc) {
                            std::cerr << "error: expected value for argument " << arg.first << ".\n";
                            return false;
                        }
                        std::string value(ap->argv[arg_idx]);
                        arg.second.cb(value);
                    }
                } else {
                    arg.second.cb("");
                }
            }
        }
        arg_idx += 1;
    }
    return true;
}
} // namespace ap

#endif
