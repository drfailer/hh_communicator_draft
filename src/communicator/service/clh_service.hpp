#ifndef COMMUNICATOR_CLH_SERVICE
#define COMMUNICATOR_CLH_SERVICE
#include "../../log.hpp"
#include "../protocol.hpp"
#include "comm_service.hpp"
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

class CLHService : public CommService {
public:
  CLHService(bool collectStats = false)
      : CommService(collectStats) {
    clh_init(&this->clh_);
    this->rank_ = clh_node_id(this->clh_);
    this->nbProcesses_ = clh_nb_nodes(this->clh_);
  }

  ~CLHService() {
    clh_finalize(this->clh_);
  }

public: // send ////////////////////////////////////////////////////////////////
  void send(Header const header, std::uint32_t dest, Buffer const &buffer) override {
    std::uint64_t tag = header.toTag();
    Request       request = (Request)clh_send(this->clh_, dest, tag, CLH_Buffer{buffer.mem, buffer.len});
    assert(request != nullptr);
    checkCLH(clh_wait(this->clh_, (CLH_Request*)request));
    clh_request_release(this->clh_, (CLH_Request*)request);
  }

  Request sendAsync(Header const header, std::uint32_t dest, Buffer const &buffer) override {
    std::uint64_t tag = header.toTag();
    Request       request = (Request)clh_send(this->clh_, dest, tag, CLH_Buffer{buffer.mem, buffer.len});
    assert(request != nullptr);
    return request;
  }

public: // recv ////////////////////////////////////////////////////////////////
  // TODO: recv without request???
  void recv(Request probeRequest, Buffer const &buffer) override {
    CLH_Request *request
        = clh_request_recv(this->clh_, (CLH_Request *)probeRequest, CLH_Buffer{buffer.mem, buffer.len});
    assert(request != nullptr);
    checkCLH(clh_wait(this->clh_, request));
    clh_request_release(this->clh_, request);
  }

  Request recvAsync(Request probeRequest, Buffer const &buffer) override {
    Request request = clh_request_recv(this->clh_, (CLH_Request *)probeRequest, CLH_Buffer{buffer.mem, buffer.len});
    assert(request != nullptr);
    return request;
  }

public: // probe ///////////////////////////////////////////////////////////////
  Request probe(std::uint8_t channel) override {
    std::uint64_t tag = (std::uint64_t)channel << Header::FIELDS[Header::CHANNEL].offset;
    std::uint64_t mask = Header::FIELDS[Header::CHANNEL].mask;
    return (Request)clh_probe(this->clh_, tag, mask, true);
  }

  Request probe(std::uint8_t channel, std::uint32_t source) override {
    std::uint64_t tag = (std::uint64_t)channel << Header::FIELDS[Header::CHANNEL].offset
                        | (std::uint64_t)source << Header::FIELDS[Header::SOURCE].offset;
    std::uint64_t mask = Header::FIELDS[Header::CHANNEL].mask | Header::FIELDS[Header::SOURCE].mask;
    return (Request)clh_probe(this->clh_, tag, mask, true);
  }

public: // requests ////////////////////////////////////////////////////////////
  bool requestCompleted(Request request) const override {
    return clh_request_completed(this->clh_, (CLH_Request *)request);
  }

  void requestRelease(Request request) const override {
    clh_request_release(this->clh_, (CLH_Request *)request);
  }

  void requestCancel(Request request) const override {
    clh_cancel(this->clh_, (CLH_Request *)request);
  }

  size_t bufferSize(Request request) const override {
    return clh_request_buffer_len((CLH_Request *)request);
  }

  std::uint64_t senderTag(Request request) const override {
    return clh_request_tag((CLH_Request *)request);
  }

  std::uint32_t senderRank(Request request) const override {
    return (std::uint32_t)((clh_request_tag((CLH_Request *)request) | Header::FIELDS[Header::SOURCE].mask)
                           >> Header::FIELDS[Header::SOURCE].offset);
  }

  bool probeSuccess(Request request) const override {
      auto clhRequest = (CLH_Request*)request;
      return clhRequest->data.probe.result;
  }

public: // synchronization /////////////////////////////////////////////////////
  void barrier() override {
    clh_barrier(this->clh_);
  }

public:
  std::uint32_t rank() const override {
    return rank_;
  }
  std::uint32_t nbProcesses() const override {
    return nbProcesses_;
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
  CLH_Handle    clh_ = nullptr;
};

} // end namespace comm

} // end namespace hh

#endif
