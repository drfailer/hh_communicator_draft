#ifndef COMMUNICATOR_COMM_SERVICE
#define COMMUNICATOR_COMM_SERVICE
#include "../protocol.hpp"

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

  virtual bool request_completed(Request request) const = 0;
  virtual void request_release(Request) const {}
  virtual void request_cancel(Request request) const = 0;
  virtual size_t buffer_len(Request request) const = 0;
  virtual std::uint64_t sender_tag(Request request) const = 0;
  virtual std::uint32_t sender_rank(Request request) const = 0;

  virtual void barrier() = 0;

  virtual std::uint32_t rank() const = 0;
  virtual std::uint32_t nbProcesses() const = 0;

  bool collectStats() const {
    return collectStats_;
  }

  std::uint8_t generateId() {
    assert(idGenerator_ < 255);
    return ++idGenerator_;
  }

private:
  std::uint8_t  idGenerator_ = 0;
  bool          collectStats_ = false;
};

} // end namespace comm

} // end namespace hh

#endif
