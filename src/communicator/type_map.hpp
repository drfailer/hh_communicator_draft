#ifndef TYPE_MAP
#define TYPE_MAP
#define TESTING
#include <stdexcept>
#include <type_traits>
#include <variant>
#include "protocol.hpp"

// The `TypeMap` is used to relate compile-time type to run-time type id. It's
// an object that stores a list of types. The position of a type in the list is
// used as the run-time type id (obtainable using idOf<Type>()). The run-time
// type id can be used with the `apply` function to execute a template lambda
// function parametrized with the corresponding compile-time type.
//
// This data structure allows writting a fully generic communicator task that
// supports multiple data types, since the types can be preserved, even when a
// piece of data goes through the network (MPI).

namespace hh {

namespace comm {

// we use type_id_t defined in `protocol.hpp`
using TypeMapIdType = type_id_t;

/// @brief Map that relates run-time type id to compile-time type.
/// @pparams list of supported types.
template <typename T, typename... Ts>
struct TypeMap {
  using id_t = TypeMapIdType;
  static constexpr size_t size = 1 + sizeof...(Ts);

  /// @brief Returns the id of the given type.
  /// @pparam Target Type for which we want the id.
  /// @return Run-time id of the given type.
  template <typename Target>
  static constexpr TypeMapIdType idOf() {
    if constexpr (std::is_same_v<Target, T>) {
      return 0;
    } else if constexpr (sizeof...(Ts) > 0) {
      return 1 + TypeMap<Ts...>::template idOf<Target>();
    }
    return 0;
  }


  /// @brief Runs the given tempalte lambda function and set its template
  ///        parameter to the type that correspond to the given id.
  /// @pparam Function Template lambda function to apply to the type.
  /// @param id Type id.
  template <typename Function>
  static constexpr void apply(id_t id, Function function) {
    if (id == 0) {
      function.template operator()<T>();
    } else {
      if constexpr (sizeof...(Ts) > 0) {
        TypeMap<Ts...>::apply((id_t)(id - 1), function);
      } else {
        throw std::logic_error("error: tried to apply a function on an invalid type map id.");
      }
    }
  }
};

#ifdef TESTING
static_assert(TypeMap<int, long, float, double>::template idOf<int>() == 0);
static_assert(TypeMap<int, long, float, double>::template idOf<long>() == 1);
static_assert(TypeMap<int, long, float, double>::template idOf<float>() == 2);
static_assert(TypeMap<int, long, float, double>::template idOf<double>() == 3);
static_assert(TypeMap<int, long, float, double>::size == 4);
#endif

template <typename TM>
struct variant_type;

template <typename... Types>
struct variant_type<TypeMap<Types...>> {
  using type = std::variant<std::shared_ptr<Types>...>;
};

/// @brief Deduces the variant of all the types contained in a given type map
/// @pparam Type map
template <typename TM>
using variant_type_t = typename variant_type<TM>::type;

} // end namespace comm

} // end namespace hh

#endif
