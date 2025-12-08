#ifndef TYPE_MAP
#define TYPE_MAP
#define TESTING
#include <stdexcept>
#include <type_traits>
#include <variant>

namespace hh {

namespace comm {

template <typename T>
using clean_t = typename std::remove_const_t<std::remove_reference_t<T>>;

using TypeMapIdType = unsigned char;

template <typename T, typename... Ts>
struct TypeMap {
  using id_t = TypeMapIdType;
  static constexpr size_t size = 1 + sizeof...(Ts);

  template <typename Target>
  static constexpr TypeMapIdType idOf() {
    if constexpr (std::is_same_v<Target, T>) {
      return 0;
    } else if constexpr (sizeof...(Ts) > 0) {
      return 1 + TypeMap<Ts...>::template idOf<Target>();
    }
    return 0;
  }

  static constexpr bool isIdValid(id_t id) {
    return id < size;
  }

  template <typename Function>
  static constexpr void apply(id_t id, Function function) {
    if (id == 0) {
      function.template operator()<T>();
    } else {
      if constexpr (sizeof...(Ts) > 0) {
        TypeMap<Ts...>::apply((id_t)(id - 1), function);
      } else {
        std::ostringstream oss;
        oss << "error: tried to apply a function on an invalid type map id.";
        throw std::logic_error(oss.str());
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

template <typename Target, typename TM>
constexpr bool contains_v = TM::template getId<Target>() < TM::size;

template <typename TM>
struct variant_type;

template <typename... Types>
struct variant_type<TypeMap<Types...>> {
  using type = std::variant<std::shared_ptr<Types>...>;
};

template <typename TM>
using variant_type_t = typename variant_type<TM>::type;

} // end namespace comm

} // end namespace hh

#endif
