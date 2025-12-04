#ifndef COMMUNICATOR_PROTOCOL
#define COMMUNICATOR_PROTOCOL
#include "hedgehog/src/tools/meta_functions.h"
#include "type_map.hpp"
#include <memory>
#include <variant>

namespace hh {

namespace comm {

template <typename... Types>
using TypeTable = type_map::TypeMap<unsigned char, Types...>;

template <typename TM>
struct variant_type;

template <typename... Types>
struct variant_type<TypeTable<Types...>> {
  using type = std::variant<std::shared_ptr<Types>...>;
};

template <typename TM>
using variant_type_t = typename variant_type<TM>::type;

using Request = void*;

struct Buffer {
    char *mem;
    size_t len;
};

// Signal //////////////////////////////////////////////////////////////////////

enum class Signal : std::uint8_t {
  None,
  Data,
  Disconnect,
};

// Header //////////////////////////////////////////////////////////////////////

struct Header {
  std::uint32_t source : 32;
  std::uint8_t  signal : 2; // 0 -> data | 1 -> signal
  std::uint8_t  typeId : 6; // 64 types (number of types managed by one task)
  std::uint8_t  channel : 8; // 256 channels (number of CommTasks)
  std::uint16_t packageId : 14; // 16384 packages
  std::uint8_t  bufferId : 2; // 4 buffers per package

  Header(std::uint32_t source, std::uint8_t signal, std::uint8_t typeId, std::uint8_t channel, std::uint16_t packageId,
         std::uint8_t bufferId)
      : source(source),
        signal(signal),
        typeId(typeId),
        channel(channel),
        packageId(packageId),
        bufferId(bufferId) {}

  Header(std::uint64_t tag) {
    this->fromTag(tag);
  }

  std::uint64_t toTag() const {
    std::uint64_t tag = 0;
    tag |= (std::uint64_t)this->source << FIELDS[SOURCE].offset;
    tag |= (std::uint64_t)this->signal << FIELDS[SIGNAL].offset;
    tag |= (std::uint64_t)this->typeId << FIELDS[TYPE_ID].offset;
    tag |= (std::uint64_t)this->channel << FIELDS[CHANNEL].offset;
    tag |= (std::uint64_t)this->packageId << FIELDS[PACKAGE_ID].offset;
    tag |= (std::uint64_t)this->bufferId << FIELDS[BUFFER_ID].offset;
    return tag;
  }

  void fromTag(std::uint64_t tag) {
    this->source = (tag & FIELDS[SOURCE].mask) >> FIELDS[SOURCE].offset;
    this->signal = (tag & FIELDS[SIGNAL].mask) >> FIELDS[SIGNAL].offset;
    this->typeId = (tag & FIELDS[TYPE_ID].mask) >> FIELDS[TYPE_ID].offset;
    this->channel = (tag & FIELDS[CHANNEL].mask) >> FIELDS[CHANNEL].offset;
    this->packageId = (tag & FIELDS[PACKAGE_ID].mask) >> FIELDS[PACKAGE_ID].offset;
    this->bufferId = (tag & FIELDS[BUFFER_ID].mask) >> FIELDS[BUFFER_ID].offset;
  }

  enum Fields {
    SOURCE = 0,
    SIGNAL = 1,
    TYPE_ID = 2,
    CHANNEL = 3,
    PACKAGE_ID = 4,
    BUFFER_ID = 5,
  };

  struct FieldInfo {
    size_t        offset;
    std::uint64_t mask;
  };

  static constexpr FieldInfo FIELDS[]{
      {.offset = 32, .mask = 0b1111111111111111111111111111111100000000000000000000000000000000},
      {.offset = 30, .mask = 0b0000000000000000000000000000000011000000000000000000000000000000},
      {.offset = 24, .mask = 0b0000000000000000000000000000000000111111000000000000000000000000},
      {.offset = 16, .mask = 0b0000000000000000000000000000000000000000111111110000000000000000},
      {.offset = 2, .mask = 0b0000000000000000000000000000000000000000000000001111111111111100},
      {.offset = 0, .mask = 0b0000000000000000000000000000000000000000000000000000000000000011},
  };
};
static_assert(sizeof(Header) <= sizeof(std::uint64_t));

} // end namespace comm

} // end namespace hh

#endif
