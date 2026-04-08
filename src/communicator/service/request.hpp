#ifndef COMMUNICATOR_SERVICE_REQUEST
#define COMMUNICATOR_SERVICE_REQUEST
#include "../tool/log.hpp"
#include <hedgehog.h>
#include <source_location>
#include <thread>
#include <type_traits>
#include <vector>

/*
 * In order to be able to use multiple services that each have their own
 * request type without adding an extra template parameter, we use an opaque
 * type for the request. The other advantage of this is that we can easily
 * track the memory.
 *
 * Why non releasing the memory is considered an error here?
 * In this case, the pool is only used internally to allocate requests, which
 * are temporary elements that can be owned by multiple components of the
 * library. Since the pool is used not only to free the memory automatically,
 * but also to reduce the number of allocations, we consider that an unused
 * request should always return to the pool to be reused.
 */

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

/// @brief Generic request type exposed to the other components of the library.
using Request = void*;

/// @brief Memory pool used for allocating the requests in the communicator
///        services (this is a standard memory pool implementation).
template <typename T>
    requires std::is_default_constructible_v<T>
class RequestPool {
  struct Node;

public:
  /// @brief Constructor of the request pool.
  /// @param defaultCapacity Number of pre-allocated requests (can be 0 since
  ///                        the pool grows dynamically).
  RequestPool(size_t defaultCapacity = 0) {
    for (size_t i = 0; i < defaultCapacity; ++i) {
      Node *node = new Node();
      node->next = this->free_nodes_;
      this->free_nodes_ = node;
    }
  }

  /// @brief Request pool destructor: delete all the allocated requests and
  ///        print error messages for requests that were not released (giving
  ///        the source location where the request was allocated and the total
  ///        number of non released requests).
  ~RequestPool() {
    Node  *cur = nullptr;
    size_t ttlNodeCount = 0, nonReleasedNodeCount = 0;

    // delete the free list
    for (cur = this->free_nodes_; cur != nullptr;) {
      Node *next = cur->next;
      delete cur;
      cur = next;
      ttlNodeCount += 1;
    }

    // delete the use list + debug
    for (cur = this->used_nodes_; cur != nullptr;) {
      log::error("non released " + hh::tool::typeToStr<T>() + " allocated at (",
                 cur->loc.file_name(), ":", cur->loc.line(), ").");
      Node *next = cur->next;
      delete cur;
      cur = next;
      ttlNodeCount += 1;
      nonReleasedNodeCount += 1;
    }
    if (nonReleasedNodeCount > 0) {
      log::error(nonReleasedNodeCount, " / ", ttlNodeCount, " released.");
    }
  }

  /// @brief Allocate a new requests: if the free list is not empty, the
  ///        result requests will be taken out of it, otherwise, a new request
  ///        will be dynamically allocated.
  /// @param loc Source location of the allocation.
  /// @return Pointer to the allocated data.
  T *allocate(std::source_location loc = std::source_location::current()) {
    std::lock_guard<std::mutex> lock(this->mutex_);

    if (this->free_nodes_ == nullptr) {
      this->free_nodes_ = new Node();
    }
    Node *node = this->free_nodes_;
    this->free_nodes_ = node->next;
    node->loc = loc;
    node->next = this->used_nodes_;
    if (node->next) {
      node->next->prev = node;
    }
    memset(&node->data, 0, sizeof(node->data)); // clear the memory
    return (T *)node;
  }

  /// @brief Return the given request to the pool.
  ///        WARN: this function is unsafe and the released request must have
  ///        been allocated using the pool (the request is casted to a pool
  ///        node).
  void release(T *data) {
    std::lock_guard<std::mutex> lock(this->mutex_);
    Node                       *node = (Node *)data;

    if (node->next) {
      node->next->prev = node->prev;
    }
    if (node->prev) {
      node->prev->next = node->next;
    } else {
      this->used_nodes_ = node->next;
    }
    node->next = this->free_nodes_;
    this->free_nodes_ = node;
  }

private:
  /// @brief internal memory pool node.
  struct Node {
    T                    data;           ///< user data
    std::source_location loc = {};       ///< allocation location (for debugging).
    Node                *prev = nullptr; ///< next node
    Node                *next = nullptr; ///< previous node
  };
  Node      *free_nodes_ = nullptr; ///< List of free nodes.
  Node      *used_nodes_ = nullptr; ///< List of used nodes.
  std::mutex mutex_; ///< Mutex used to make the pool thread-safe.
};

} // namespace comm

} // end namespace hh

#endif
