#ifndef COMMUNICATOR_COMM_SERVICE
#define COMMUNICATOR_COMM_SERVICE
#include "../log.hpp"
#include "protocol.hpp"
#include <cassert>
#include <chrono>
#include <clh/buffer.h>
#include <clh/clh.h>
#include <cstddef>
#include <cstdlib>
#include <stdexcept>
#include <thread>

namespace hh {

namespace comm {

class CommService {
public:
  CommService(bool collectStats = false)
      : collectStats_(collectStats) {
    clh_init(&this->clh_);
    this->rank_ = clh_node_id(this->clh_);
    this->nbProcesses_ = clh_nb_nodes(this->clh_);
  }

  ~CommService() {
    clh_finalize(this->clh_);
  }

public: // send ////////////////////////////////////////////////////////////////
  void send(Header const header, std::uint32_t dest, Buffer const &buffer) {
    std::uint64_t tag = headerToTag(header);
    Request       request = clh_send(this->clh_, dest, tag, buffer);
    assert(request != nullptr);
    checkCLH(clh_wait(this->clh_, request));
    clh_request_release(this->clh_, request);
  }

  Request sendAsync(Header const header, std::uint32_t dest, Buffer const &buffer) {
    std::uint64_t tag = headerToTag(header);
    CLH_Request  *request = clh_send(this->clh_, dest, tag, buffer);
    assert(request != nullptr);
    return request;
  }

public: // recv ////////////////////////////////////////////////////////////////
  // TODO: recv without request???
  void recv(Request probeRequest, Buffer const &buffer) {
    CLH_Request *request = clh_request_recv(this->clh_, probeRequest, buffer);
    assert(request != nullptr);
    checkCLH(clh_wait(this->clh_, request));
    clh_request_release(this->clh_, request);
  }

  Request recvAsync(Request probeRequest, Buffer const &buffer) {
    Request request = clh_request_recv(this->clh_, probeRequest, buffer);
    assert(request != nullptr);
    return request;
  }

public: // probe ///////////////////////////////////////////////////////////////
  Request probe(std::uint8_t channel) {
    std::uint64_t tag = (std::uint64_t)channel << HEADER_FIELDS[CHANNEL].offset;
    std::uint64_t mask = HEADER_FIELDS[CHANNEL].mask;
    return clh_probe(this->clh_, tag, mask, true);
  }

  Request probe(std::uint8_t channel, std::uint32_t source) {
    std::uint64_t tag = (std::uint64_t)channel << HEADER_FIELDS[CHANNEL].offset
                      | (std::uint64_t)source << HEADER_FIELDS[SOURCE].offset;
    std::uint64_t mask = HEADER_FIELDS[CHANNEL].mask | HEADER_FIELDS[SOURCE].mask;
    return clh_probe(this->clh_, tag, mask, true);
  }

public: // requests ////////////////////////////////////////////////////////////
  bool request_completed(Request request) const {
    return clh_request_completed(this->clh_, request);
  }

  void request_release(Request request) const {
    clh_request_release(this->clh_, request);
  }

  void request_cancel(Request request) const {
    clh_cancel(this->clh_, request);
  }

  size_t buffer_len(Request request) const {
    return clh_request_buffer_len(request);
  }

  std::uint64_t sender_tag(Request request) const {
    return clh_request_tag(request);
  }

  // std::uint32_t sender_rank(Request request) const {
  //     // TODO
  // }

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
    assert(idGenerator_ < 255);
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
