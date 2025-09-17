#ifndef AP_H
#define AP_H
#include <cassert>
#include <cstdio>
#include <functional>
#include <map>
#include <string>
#include <vector>

namespace ap {

using AHFunction = std::function<void(std::string const &)>;

struct ArgumentHandler {
    AHFunction  cb;
    bool        expectsArg;
    std::string doc;
};

struct ArgumentParser {
    int                                    argc;
    char                                 **argv;
    std::map<std::string, ArgumentHandler> dash_args;
    std::vector<ArgumentHandler>           positional_args;
    std::string                            doc;
};

inline ArgumentParser argument_parser_create(int argc, char **argv,
                                             std::string const &doc = "") {
    return ArgumentParser{
        .argc = argc,
        .argv = argv,
        .dash_args = {},
        .positional_args = {},
        .doc = doc.empty() ? std::string(argv[0]) : doc,
    };
}

inline void add_arg(ArgumentParser *ap, std::string const &arg, AHFunction cb,
                    bool expects_value = true, std::string const &doc = "") {
    assert(arg[0] == '-');
    ap->dash_args.insert({arg, ArgumentHandler{cb, expects_value, doc}});
}

inline void add_arg(ArgumentParser *ap, AHFunction cb,
                    std::string const &doc = "") {
    ap->positional_args.push_back(ArgumentHandler{cb, false, doc});
}

#define AP_ADD_TYPE_DASH_ARG(type_, default_value_, arg_value_)                \
    inline void add_##type_##_arg(                                             \
        ArgumentParser *ap, std::string const &arg, type_ *variable,           \
        type_ default_value = default_value_, std::string const &doc = "") {   \
        assert(variable != NULL);                                              \
        *variable = default_value;                                             \
        ap->dash_args.insert(                                                  \
            {arg, ArgumentHandler{                                             \
                      .cb =                                                    \
                          [variable](std::string const &arg_value) {           \
                              *variable = arg_value_;                          \
                          },                                                   \
                      .expectsArg = true,                                      \
                      .doc = doc,                                              \
                  }});                                                         \
    }
AP_ADD_TYPE_DASH_ARG(int, 0, std::stoi(arg_value))
AP_ADD_TYPE_DASH_ARG(size_t, 0, std::stoul(arg_value))

inline void add_string_arg(ArgumentParser *ap, std::string const &arg,
                           std::string       *variable,
                           std::string        default_value = "",
                           std::string const &doc = "") {
    assert(variable != NULL);
    *variable = default_value;
    ap->dash_args.insert(
        {arg,
         ArgumentHandler{
             .cb = [variable](
                       std::string const &arg_value) { *variable = arg_value; },
             .expectsArg = true,
             .doc = doc,
         }});
}

inline void add_bool_arg(ArgumentParser *ap, std::string const &arg,
                         bool *variable, bool default_value = false,
                         std::string const &doc = "") {
    assert(variable != NULL);
    *variable = default_value;
    ap->dash_args.insert(
        {arg,
         ArgumentHandler{
             .cb = [variable, default_value](
                       std::string const &) { *variable = !default_value; },
             .expectsArg = false,
             .doc = doc,
         }});
}

inline void print_help(ArgumentParser *ap) {
    printf("%s:\n", ap->doc.c_str());
    for (auto arg : ap->dash_args) {
        printf("\t%s :: %s\n", arg.first.c_str(), arg.second.doc.c_str());
    }
}

struct ParsePrefixResult {
    bool        ok;
    std::string rest;
};
inline ParsePrefixResult parse_prefix_(std::string const &str,
                                       std::string const &prefix) {
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

inline int parse_dash_arg_(ArgumentParser *ap, std::string const &cur_arg,
                           int *arg_idx) {
    for (auto da : ap->dash_args) {
        auto result = parse_prefix_(cur_arg, da.first);

        if (result.ok) {
            if (!da.second.expectsArg) {
                da.second.cb("");
                return 1;
            }
            if (result.rest[0] == '=' || result.rest.size() > 1) {
                da.second.cb(result.rest.substr(1));
            } else {
                *arg_idx += 1;
                if (*arg_idx >= ap->argc) {
                    fprintf(stderr, "error: expected value for argument %s.\n",
                            da.first.c_str());
                    return -1;
                }
                da.second.cb(std::string(ap->argv[*arg_idx]));
            }
            return 1;
        }
    }
    return 0;
}

enum class ArgumentParserStatus {
    Ok = 0,
    Help = 1,
    Error = -1,
};
inline ArgumentParserStatus argument_parser_run(ArgumentParser *ap) {
    int    arg_idx = 1;
    size_t positional_arg_idx = 0;

    while (arg_idx < ap->argc) {
        std::string arg(ap->argv[arg_idx]);

        if (arg[0] != '-') {
            if (positional_arg_idx < ap->positional_args.size()) {
                ap->positional_args[positional_arg_idx++].cb(arg);
            } else {
                fprintf(stderr, "error: unexpected argument %s.\n",
                        arg.c_str());
                return ArgumentParserStatus::Error;
            }
        } else {
            auto pdar = parse_dash_arg_(ap, arg, &arg_idx);

            if (pdar < 0) {
                return ArgumentParserStatus::Error;
            } else if (pdar == 0) {
                if (arg == "-h" || arg == "--help") {
                    print_help(ap);
                    return ArgumentParserStatus::Help;
                } else {
                    fprintf(stderr, "error: unknown argument %s\n",
                            arg.c_str());
                    return ArgumentParserStatus::Error;
                }
            }
        }
        arg_idx += 1;
    }
    if (positional_arg_idx < ap->positional_args.size()) {
        fprintf(stderr, "error: not all positional arguments were found.\n");
        return ArgumentParserStatus::Error;
    }
    return ArgumentParserStatus::Ok;
}

} // namespace ap

#endif
