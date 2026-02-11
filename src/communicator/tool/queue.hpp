#ifndef COMMUNICATOR_TOOL_QUEUE
#define COMMUNICATOR_TOOL_QUEUE
#include <cstddef>
#include <vector>
#include <cassert>

#define DEFAULT_QUEUE_CAPACITY 32

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

/// @brief Cache friendly queue that supports fast insert/delete.
/// @tparam T Type of the element stored in the queue.
template <typename T>
class Queue {
public:
  // @brief Constructor from size.
  // @param default_capacity Default capacity of the queue.
  Queue(size_t default_capacity = DEFAULT_QUEUE_CAPACITY)
      : memory_(default_capacity + 1) {
    for (size_t i = 1; i < this->memory_.size() - 1; ++i) {
      this->memory_[i].next = i + 1;
    }
    this->memory_.back().next = 0;
    this->freeList_ = this->memory_.size() > 1 ? 1 : 0;
  }

  /// @brief Default move constructor.
  Queue(Queue<T> &&) = default;

  /// @brief Default move assignment operator.
  Queue<T> &operator=(Queue<T> &&) = default;

  /// @brief No copy constructor to prevent any silly mistake (use the copy method).
  Queue(Queue<T> const &) = delete;

  /// @brief No copy operator to prevent any silly mistake (use the copy method).
  Queue<T> const &operator=(Queue<T> const &) = delete;

  /// @brief Creates a copy of the queue.
  /// @return Copy of the queue.
  Queue<T> copy() const {
    Queue<T> queue;
    queue.freeList_ = this->freeList_;
    queue.usedList_ = this->usedList_;
    queue.size_ = this->size_;
    queue.memory_ = this->memory_;
    return queue;
  }

  /// @brief Iterator type.
  /// @tparam Q Queue type.
  /// @tparam V Value type.
  template <typename Q, typename V>
  struct iterator_type {
    size_t idx; ///< Index of the current element in the queue.
    Q     *queue; ///< Pointer to the queue.

    /// @brief Advance the iterator.
    /// @return Iterator to the next position.
    iterator_type<Q, V> const &operator++() {
      this->idx = queue->memory_[this->idx].next;
      return *this;
    }

    /// @brief Advance the iterator.
    /// @return Iterator to the last position.
    iterator_type<Q, V> operator++(int) {
      iterator it = *this;
      this->idx = queue->memory_[this->idx].next;
      return it;
    }

    /// @brief Access the element pointed by the iterator.
    /// @return reference to the element value.
    V &operator*() { return queue->memory_[this->idx].data; }

    /// @brief Member access to the element pointed by the iterator.
    /// @return Pointer to the element value.
    V *operator->() { return &queue->memory_[this->idx].data; }

    /// @brief != operator for the iterator.
    /// @param it Other iterator to compare with.
    bool operator!=(iterator_type<Q, V> const &it) const { return this->idx != it.idx; }
  };

  /// @brief Iterator type.
  using iterator = iterator_type<Queue<T>, T>;

  /// @brief Const iterator type.
  using const_iterator = iterator_type<const Queue<T>, T const>;

  /// @brief Value type.
  using value_type = T;

  /// @brief Creates an iterator to the beginning of the queue.
  /// @return Iterator to the beginning of the queue.
  iterator begin() { return iterator{this->usedList_, this}; }

  /// @brief Creates an iterator to the end of the queue (the end is an invalid element).
  /// @return Iterator to the end of the queue.
  iterator end() { return iterator{0, this}; }

  /// @brief Creates a const iterator to the beginning of the queue.
  /// @return Const iterator to the beginning of the queue.
  const_iterator begin() const { return const_iterator{this->usedList_, this}; }

  /// @brief Creates a const iterator to the end of the queue (the end is an invalid element).
  /// @return Const iterator to the end of the queue.
  const_iterator end() const { return const_iterator{0, this}; }

  /// @brief Add an element to the queue (append).
  /// @param data Data to add to the queue.
  /// @return Index of the element in the queue memory (address).
  size_t add(T const &data) {
    size_t newIdx = this->freeList_;

    if (newIdx == 0) {
      newIdx = this->memory_.size();
      this->memory_.push_back(Node{});
    }

    this->freeList_ = this->memory_[newIdx].next;
    if (this->usedList_ != 0) {
      size_t lastIdx = this->memory_[this->usedList_].prev;
      this->memory_[lastIdx].next = newIdx;
      this->memory_[newIdx].prev = lastIdx;
      this->memory_[this->usedList_].prev = newIdx;
    } else {
      this->usedList_ = newIdx;
      this->memory_[newIdx].prev = newIdx;
    }
    this->memory_[newIdx].next = 0;
    this->memory_[newIdx].data = data;
    this->size_ += 1;
    return newIdx;
  }

  /// @brief Remove the element based on the index.
  ///
  /// Note: this is not the index of the element within the queue, this is the
  /// address of the element withing the queue memory. This index is returned
  /// by `add`.
  ///
  /// @param idx Index of the element to remove.
  /// @return Iterator to the next element.
  iterator remove(size_t idx) {
    if (idx == 0) {
      return iterator{idx, this};
    }
    Node    &node = this->memory_[idx];
    iterator next{node.next, this};

    if (node.next != 0) {
      this->memory_[node.next].prev = node.prev;
    } else {
      this->memory_[this->usedList_].prev = node.prev;
    }

    if (idx != this->usedList_) {
      assert(node.prev != 0);
      this->memory_[node.prev].next = node.next;
    } else {
      this->usedList_ = node.next;
    }

    node.next = this->freeList_;
    node.prev = 0;
    this->freeList_ = idx;
    this->size_ -= 1;
    return next;
  }

  /// @brief Remove the element pointed by the iterator.
  ///
  /// Note: if the given element is 0, nothing happens.
  ///
  /// @param it Iterator to remove.
  /// @return Iterator to the next element.
  iterator remove(iterator it) {
    return remove(it.idx);
  }

  /// @brief Access a element based on its index.
  /// @param idx Index of the element (index returned by `add`).
  /// @return Reference to the element.
  T &at(size_t idx) { return this->memory_[idx].data; }

  /// @brief Access a element based on its index.
  /// @param idx Index of the element (index returned by `add`).
  /// @return Const reference to the element.
  T const &at(size_t idx) const { return this->memory_[idx].data; }

  /// @brief Get the size of the queue.
  /// @return Size of the queue.
  size_t size() const { return this->size_; }

  /// @brief Tells if the queue is empty.
  /// @return True if the queue is empty, false otherwise.
  bool empty() const { return this->size_ == 0; }

  /// @brief Clear the queue.
  void clear() {
    this->freeList_ = this->memory_.size() > 1 ? 1 : 0;
    this->usedList_ = 0;
    this->size_ = 0;
    for (size_t i = 1; i < this->memory_.size(); ++i) {
      this->memory_[i].next = i + 1;
    }
    this->memory_.back().next = 0;
  }

private:
  /// @brief Internal queue node type.
  struct Node {
    T      data = {}; ///< Data to store in the queue.
    size_t next = 0;  ///< Index of the next element.
    size_t prev = 0;  ///< Index of the previous element.
  };
  size_t            freeList_ = 0; ///< Index of the free list head.
  size_t            usedList_ = 0; ///< Index of the used list head.
  size_t            size_ = 0;     ///< Size of the queue.
  std::vector<Node> memory_ = {};  ///< Memory of the queue.
};

} // end namespace comm

} // end namespace hh

#endif
