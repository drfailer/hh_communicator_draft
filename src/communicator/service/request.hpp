#ifndef COMMUNICATOR_SERVICE_REQUEST
#define COMMUNICATOR_SERVICE_REQUEST
#include "../tool/log.hpp"
#include <hedgehog/hedgehog.h>
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

namespace hh {

namespace comm {

using Request = void*;

template <typename T>
requires std::is_default_constructible_v<T>
class RequestPool {
  struct Node;

public:
  RequestPool(size_t defaultCapacity = 0) {
    for (size_t i = 0; i < defaultCapacity; ++i) {
      Node *node = new Node();
      node->next = this->free_nodes_;
      this->free_nodes_ = node;
    }
  }

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
  struct Node {
    T                    data;
    std::source_location loc = {}; // use for debugging
    Node                *prev = nullptr;
    Node                *next = nullptr;
  };
  Node      *free_nodes_ = nullptr;
  Node      *used_nodes_ = nullptr;
  std::mutex mutex_;
};

} // namespace comm

} // end namespace hh

#endif
