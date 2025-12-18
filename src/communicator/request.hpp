#ifndef COMMUNICATOR_REQUEST
#define COMMUNICATOR_REQUEST
#include "../log.hpp"
#include <thread>
#include <type_traits>
#include <vector>
#include <source_location>

/*
 * In order to be able to use multiple services that each have their own
 * request type without adding an extra template parameter, we use an opaque
 * type for the request. Only the underlying service knows the real reaquest
 * type and can access the data using the opaque request (in this case, it is
 * an index in a pool array). The other advantage of this is that we can easily
 * track the memory.
 *
 */

namespace hh {

namespace comm {

using Request = size_t;

template <typename T>
requires std::is_default_constructible_v<T>
class RequestPool {
public:
  struct DebugInfo {
    std::source_location loc;
    bool        free;
  };

public:
  RequestPool(size_t defaultCapacity = 0)
      : requests_(defaultCapacity, T{}),
        freeIndexes_(defaultCapacity, 0),
        debugInfos_(defaultCapacity, DebugInfo{{}, true}) {
    for (size_t i = 0; i < requests_.size(); ++i) {
      freeIndexes_[i] = i;
    }
  }

  ~RequestPool() {
    if (freeIndexes_.size() < requests_.size()) {
      size_t nbNonFree = requests_.size() - freeIndexes_.size();
      size_t ttl = requests_.size();
      logh::warn(nbNonFree, "/", ttl, " requests where not released.");
      for (auto info : debugInfos_) {
        if (!info.free) {
          logh::warn("request allocated at (", info.loc.file_name(), ":", info.loc.line(), ") was not released.");
        }
      }
    }
  }

  Request allocate(T value = T{}, std::source_location loc = std::source_location::current()) {
    std::lock_guard<std::mutex> lock(mutex_);
    Request                     request;

    if (freeIndexes_.size() == 0) {
      freeIndexes_.push_back(requests_.size());
      requests_.emplace_back(T{});
      debugInfos_.emplace_back(loc, true);
    }
    request = freeIndexes_.back();
    freeIndexes_.pop_back();
    requests_[request] = value;
    debugInfos_[request] = DebugInfo{loc, false};
    return request;
  }

  void release(Request request) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (debugInfos_[request].free) {
      logh::error("request allocated at (", debugInfos_[request].loc.file_name(),
                  ":", debugInfos_[request].loc.line(), ") was released multiple times.");
    }
    debugInfos_[request].free = true;
    freeIndexes_.push_back(request);
  }

  T getDataAndRelease(Request request) {
    T data = requests_[request];
    release(request);
    return data; // returned by copy so it is safe
  }

  T getData(Request request) {
    std::lock_guard<std::mutex> lock(mutex_);
    return requests_[request];
  }

  void setData(Request request, T const &data) {
    std::lock_guard<std::mutex> lock(mutex_);
    requests_[request] = data;
  }

  // WARN: requires manual locking
  T &dataRef(Request request) {
    return requests_[request];
  }

  void lock() {
    mutex_.lock();
  }

  void unlock() {
    mutex_.unlock();
  }

private:
  std::vector<T>         requests_;
  std::vector<Request>   freeIndexes_;
  std::vector<DebugInfo> debugInfos_;
  std::mutex             mutex_;
};

} // end namespace comm

} // end namespace hh

#endif
