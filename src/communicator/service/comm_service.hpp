#ifndef COMMUNICATOR_COMM_SERVICE
#define COMMUNICATOR_COMM_SERVICE
#include "../protocol.hpp"
#include <cassert>
#include <thread>

namespace hh {

namespace comm {

class CommService {
public:
  CommService(bool collectStats) : collectStats_(collectStats) {}
  virtual ~CommService() = default;

public:
  virtual void send(Header const header, std::uint32_t dest, Buffer const &buffer) = 0;
  virtual Request sendAsync(Header const header, std::uint32_t dest, Buffer const &buffer) = 0;

  virtual void recv(Request probeRequest, Buffer const &buffer) = 0;
  virtual Request recvAsync(Request probeRequest, Buffer const &buffer) = 0;

  virtual Request probe(std::uint8_t channel) = 0;
  virtual Request probe(std::uint8_t channel, std::uint32_t source) = 0;

  virtual bool requestCompleted(Request request) = 0;
  virtual void requestRelease(Request) {}
  virtual void requestCancel(Request request) = 0;
  virtual size_t bufferSize(Request request) = 0;
  virtual Header requestHeader(Request request) = 0;
  virtual bool probeSuccess(Request request) = 0;

  virtual void barrier() = 0;

  virtual std::uint32_t rank() const = 0;
  virtual std::uint32_t nbProcesses() const = 0;

  bool collectStats() const {
    return collectStats_;
  }

  virtual std::uint64_t newChannel() = 0;

  std::mutex &mutex() {
      return mutex_;
  }

private:
  bool          collectStats_ = false;
  std::mutex    mutex_;
};

} // end namespace comm

} // end namespace hh

#endif
