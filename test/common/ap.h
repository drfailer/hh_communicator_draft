#ifndef AP_H
#define AP_H
#include <cassert>
#include <cstdio>
#include <functional>
#include <map>
#include <string>

namespace ap {

struct ArgumentHandler {
    std::function<void(std::string const &)> cb;
    bool                                     expectsArg;
    std::string                              doc;
};

struct ArgumentParser {
    int                                    argc;
    char                                 **argv;
    std::map<std::string, ArgumentHandler> args;
    std::string                            doc;
};

inline ArgumentParser argument_parser_create(int argc, char **argv,
                                             std::string const &doc = "") {
    if (doc.empty()) {
    }
    return ArgumentParser{
        .argc = argc,
        .argv = argv,
        .args = {},
        .doc = doc,
    };
}

inline void add_arg(ArgumentParser *ap, std::string const &arg, auto cb,
                    bool expects_value = true, std::string const &doc = "") {
    ap->args.insert({arg, ArgumentHandler{cb, expects_value, doc}});
}

#define AP_ADD_TYPE_ARG(type_, default_value_, arg_value_)                     \
    inline void add_##type_##_arg(                                             \
        ArgumentParser *ap, std::string const &arg, type_ *variable,           \
        type_ default_value = default_value_, std::string const &doc = "") {   \
        assert(variable != NULL);                                              \
        *variable = default_value;                                             \
        ap->args.insert(                                                       \
            {arg, ArgumentHandler{                                             \
                      .cb =                                                    \
                          [variable](std::string const &arg_value) {           \
                              *variable = arg_value_;                          \
                          },                                                   \
                      .expectsArg = true,                                      \
                      .doc = doc,                                              \
                  }});                                                         \
    }
AP_ADD_TYPE_ARG(int, 0, std::stoi(arg_value))
AP_ADD_TYPE_ARG(size_t, 0, std::stoul(arg_value))

inline void add_string_arg(ArgumentParser *ap, std::string const &arg,
                           std::string       *variable,
                           std::string        default_value = "",
                           std::string const &doc = "") {
    assert(variable != NULL);
    *variable = default_value;
    ap->args.insert({arg, ArgumentHandler{
                              .cb =
                                  [variable](std::string const &arg_value) {
                                      *variable = arg_value;
                                  },
                              .expectsArg = true,
                              .doc = doc,
                          }});
}

inline void add_bool_arg(ArgumentParser *ap, std::string const &arg,
                         bool *variable, bool default_value = false,
                         std::string const &doc = "") {
    assert(variable != NULL);
    *variable = default_value;
    ap->args.insert(
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
    for (auto arg : ap->args) {
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

enum class ArgumentParserStatus {
    Ok = 0,
    Help = 1,
    UnknownArgumentError = -1,
    InvalidValueError = -2,
};

inline ArgumentParserStatus argument_parser_run(ArgumentParser *ap) {
    int  arg_idx = 1;
    bool is_arg_valid;

    while (arg_idx < ap->argc) {
        std::string cur_arg(ap->argv[arg_idx]);

        is_arg_valid = false;
        for (auto arg : ap->args) {
            auto result = parse_prefix_(cur_arg, arg.first);

            if (result.ok) {
                if (arg.second.expectsArg) {
                    if (result.rest.starts_with('=')
                        || result.rest.size() > 1) {
                        arg.second.cb(result.rest.substr(1));
                    } else {
                        arg_idx += 1;
                        if (arg_idx > ap->argc) {
                            fprintf(stderr,
                                    "error: expected value for argument %s\n",
                                    arg.first.c_str());
                            return ArgumentParserStatus::InvalidValueError;
                        }
                        std::string value(ap->argv[arg_idx]);
                        arg.second.cb(value);
                    }
                } else {
                    arg.second.cb("");
                }
                is_arg_valid = true;
                break;
            }
        }
        if (!is_arg_valid) {
            if (cur_arg == "-h" || cur_arg == "--help") {
                print_help(ap);
                return ArgumentParserStatus::Help;
            } else {
                fprintf(stderr, "error: expected value for argument %s\n",
                        cur_arg.c_str());
                return ArgumentParserStatus::UnknownArgumentError;
            }
        }
        arg_idx += 1;
    }
    return ArgumentParserStatus::Ok;
}

} // namespace ap

#endif
