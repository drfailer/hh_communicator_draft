#ifndef TYPE_MAP
#define TYPE_MAP
#define TESTING
#include <stdexcept>
#include <type_traits>

namespace type_map {

template <typename IdType, typename... Types>
struct TypeMap {
  using id_type = IdType;
  static constexpr size_t size = sizeof...(Types);
};

template <typename T>
using clean_t = typename std::remove_const_t<std::remove_reference_t<T>>;

template <typename Target, typename IdType, typename T, typename... Ts>
constexpr IdType get_id(TypeMap<IdType, T, Ts...>) {
  if constexpr (std::is_same_v<Target, T>) {
    return 0;
  } else if constexpr (sizeof...(Ts) > 0) {
    return 1 + get_id<Target, IdType, Ts...>(TypeMap<IdType, Ts...>());
  }
  return 0;
}

#ifdef TESTING
static_assert(get_id<int>(TypeMap<int, int, long, float, double>()) == 0);
static_assert(get_id<long>(TypeMap<int, int, long, float, double>()) == 1);
static_assert(get_id<float>(TypeMap<int, int, long, float, double>()) == 2);
static_assert(get_id<double>(TypeMap<int, int, long, float, double>()) == 3);
#endif

template <typename Target, typename TM>
constexpr bool contains_v = get_id<Target, TM> < TM::size;

template <typename IdType, typename... Types>
constexpr bool id_valid(size_t id, TypeMap<IdType, Types...>) {
  return id < TypeMap<Types...>::size;
}

template <typename Function, typename IdType, typename T, typename... Ts>
constexpr void apply(TypeMap<IdType, T, Ts...>, IdType id, Function function) {
  if (id == 0) {
    function.template operator()<T>();
  } else {
    if constexpr (sizeof...(Ts)) {
      apply(TypeMap<IdType, Ts...>(), IdType(id - 1), function);
    } else {
      throw std::logic_error("error: id not found");
    }
  }
}

} // end namespace type_map

#endif
