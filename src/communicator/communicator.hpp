#ifndef COMMUNICATOR_COMMUNICATOR
#define COMMUNICATOR_COMMUNICATOR
#include "../log.hpp"
#include <clh/buffer.h>
#include <clh/clh.h>
#include "protocol.hpp"
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <stdexcept>
#include <thread>

namespace hh {

namespace comm {

class Communicator {
public:
  Communicator(bool collectStats = false)
      : collectStats_(collectStats) {}

  // TODO: remove
  CLH_Handle clh_todoRemove() const { return clh_; }

public: // init and finalize ///////////////////////////////////////////////////
  void init(int *, char ***) {
    clh_init(&this->clh_);
    this->rank_ = clh_node_id(this->clh_);
    this->nbProcesses_ = clh_nb_nodes(this->clh_);
  }

  inline void finalize() {
    clh_finalize(this->clh_);
  }

public: // send ////////////////////////////////////////////////////////////////
  void send(Header const header, std::uint32_t dest, Buffer const &buffer) {
    std::uint64_t tag = headerToTag(header);
    Request       request = clh_send(this->clh_, dest, tag, buffer);
    checkCLH(clh_wait(this->clh_, request));
    clh_request_release(this->clh_, request);
  }

  Request sendAsync(Header const header, std::uint32_t dest, Buffer const &buffer) {
    std::uint64_t tag = headerToTag(header);
    CLH_Request  *request = clh_send(this->clh_, dest, tag, buffer);
    return request;
  }

public: // recv ////////////////////////////////////////////////////////////////
  // TODO: recv without request???
  void recv(Request probeRequest, Buffer const &buffer) {
    CLH_Request *request = clh_request_recv(this->clh_, probeRequest, buffer);
    checkCLH(clh_wait(this->clh_, request));
    clh_request_release(this->clh_, request);
  }

  Request recvAsync(Request probeRequest, Buffer const &buffer) {
    Request request = clh_request_recv(this->clh_, probeRequest, buffer);
    return request;
  }

public: // probe ///////////////////////////////////////////////////////////////
  Request probe(std::uint64_t tag, std::uint64_t tagMask) {
    return clh_probe(this->clh_, tag, tagMask, true);
  }

public: // synchronization /////////////////////////////////////////////////////
  void barrier() {
    clh_barrier(this->clh_);
  }

public:
  std::uint32_t rank() const {
    return rank_;
  }
  std::uint32_t nbProcesses() const {
    return nbProcesses_;
  }
  bool collectStats() const {
    return collectStats_;
  }

  std::uint8_t generateId() {
    return ++idGenerator_;
  }

private:
  bool checkCLH(CLH_Status code) {
    if (code == CLH_STATUS_SUCCESS) {
      return true;
    }
    logh::error("clh error: ", clh_status_string(code));
    return false;
  }

private:
  std::uint32_t rank_ = -1;
  std::uint32_t nbProcesses_ = -1;
  std::uint8_t  idGenerator_ = 0;
  bool          collectStats_ = false;
  CLH_Handle    clh_ = nullptr;
};

} // end namespace comm

} // end namespace hh

#endif
