#ifndef COMMUNICATOR_PROTOCOL
#define COMMUNICATOR_PROTOCOL
#include <hedgehog.h>
#include <memory>
#if __cplusplus >= 202002L
#include <span>
#endif

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

/******************************************************************************/
/*                                   Buffer                                   */
/******************************************************************************/

/// @brief Buffer structure.
#if __cplusplus < 202002L
  struct Buffer {
    char  *mem; ///< Pointer to the data.
    size_t len; ///< Length of the data.

    [[nodiscard]] auto data() const { return mem; }
    [[nodiscard]] auto size() const { return len; }
  };
#else
  using Buffer = std::span<char>;
#endif

/******************************************************************************/
/*                                   Header                                   */
/******************************************************************************/

// The following aliases are used to determin the types of the header
// components. They also make the code more readable.

/// @brief Channel id type.
using channel_t = std::uint64_t;
/// @brief Rank type.
using rank_t = std::uint64_t;
/// @brief Type of the type id.
using type_id_t = std::uint64_t;
/// @brief Type of the buffer id.
using buffer_id_t = std::uint64_t;

/// @brief Header type used in the requests.
struct Header {
  channel_t    channel;   ///< Channel id (which communicator task).
  rank_t       source;    ///< Source that sent the requests.
  type_id_t    typeId;    ///< Type id of the data contained in the request (if data).
  buffer_id_t  bufferId;  ///< Buffer id of the data.

  /// @brief constructor from all the elements.
  /// @param source    Value of the source.
  /// @param typeId    Value of the typeId.
  /// @param channel   Value of the channel.
  /// @param bufferId  Value of the bufferId.
  Header(rank_t source = 0, type_id_t typeId = 0, channel_t channel = 0, buffer_id_t bufferId = 0)
      : channel(channel),
        source(source),
        typeId(typeId),
        bufferId(bufferId) {}

  /// @brief enum that model the fileds of the header.
  enum Fields {
    CHANNEL,
    SOURCE,
    TYPE_ID,
    BUFFER_ID,
  };

  /// @brief Structure that can be used to encode/decode a header into a tag.
  struct FieldInfo {
    size_t        offset; ///< Offset of the field.
    std::uint64_t mask;   ///< Mask to get the filed value.
  };

  /// @brief Allow to compare headers.
  /// @param other Other header to compare with.
  /// @return true if `this` is inferior to `other`.
  bool operator<(Header const &other) const {
    std::uint64_t thisVals[]
        = {this->source, this->typeId, this->channel, this->bufferId};
    std::uint64_t otherVals[]
        = {other.source, other.typeId, other.channel, other.bufferId};

    for (size_t i = 0; i < sizeof(thisVals) / sizeof(this->source); ++i) {
      if (thisVals[i] != otherVals[i]) {
        return thisVals[i] < otherVals[i];
      }
    }
    return false;
  }
};

} // end namespace comm

} // end namespace hh

#endif
