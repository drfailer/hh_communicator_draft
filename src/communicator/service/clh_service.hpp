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

private:
// struct Header {
//   std::uint32_t source : 32;
//   std::uint8_t  signal : 2; // 0 -> data | 1 -> signal
//   std::uint8_t  typeId : 6; // 64 types (number of types managed by one task)
//   std::uint8_t  channel : 8; // 256 channels (number of CommTasks)
//   std::uint16_t packageId : 14; // 16384 packages
//   std::uint8_t  bufferId : 2; // 4 buffers per package
// }
  static constexpr Header::FieldInfo HEADER_FIELDS[]{
      {.offset = 32, .mask = 0b1111111111111111111111111111111100000000000000000000000000000000},
      {.offset = 30, .mask = 0b0000000000000000000000000000000011000000000000000000000000000000},
      {.offset = 24, .mask = 0b0000000000000000000000000000000000111111000000000000000000000000},
      {.offset = 16, .mask = 0b0000000000000000000000000000000000000000111111110000000000000000},
      {.offset = 2, .mask = 0b0000000000000000000000000000000000000000000000001111111111111100},
      {.offset = 0, .mask = 0b0000000000000000000000000000000000000000000000000000000000000011},
  };

  static std::uint64_t headerToTag(Header const &header) {
    std::uint64_t tag = 0;
    tag |= header.source << HEADER_FIELDS[Header::SOURCE].offset;
    tag |= header.signal << HEADER_FIELDS[Header::SIGNAL].offset;
    tag |= header.typeId << HEADER_FIELDS[Header::TYPE_ID].offset;
    tag |= header.channel << HEADER_FIELDS[Header::CHANNEL].offset;
    tag |= header.packageId << HEADER_FIELDS[Header::PACKAGE_ID].offset;
    tag |= header.bufferId << HEADER_FIELDS[Header::BUFFER_ID].offset;
    return tag;
  }

  static Header tagToHeader(std::uint64_t tag) {
    Header header;
    header.source = (tag & HEADER_FIELDS[Header::SOURCE].mask) >> HEADER_FIELDS[Header::SOURCE].offset;
    header.signal = (tag & HEADER_FIELDS[Header::SIGNAL].mask) >> HEADER_FIELDS[Header::SIGNAL].offset;
    header.typeId = (tag & HEADER_FIELDS[Header::TYPE_ID].mask) >> HEADER_FIELDS[Header::TYPE_ID].offset;
    header.channel = (tag & HEADER_FIELDS[Header::CHANNEL].mask) >> HEADER_FIELDS[Header::CHANNEL].offset;
    header.packageId = (tag & HEADER_FIELDS[Header::PACKAGE_ID].mask) >> HEADER_FIELDS[Header::PACKAGE_ID].offset;
    header.bufferId = (tag & HEADER_FIELDS[Header::BUFFER_ID].mask) >> HEADER_FIELDS[Header::BUFFER_ID].offset;
    return header;
  }

public: // send ////////////////////////////////////////////////////////////////
  void send(Header const header, std::uint32_t dest, Buffer const &buffer) override {
    std::uint64_t tag = headerToTag(header);
    Request       request = (Request)clh_send(this->clh_, dest, tag, CLH_Buffer{buffer.mem, buffer.len});
    assert(request != nullptr);
    checkCLH(clh_wait(this->clh_, (CLH_Request*)request));
    clh_request_release(this->clh_, (CLH_Request*)request);
  }

  Request sendAsync(Header const header, std::uint32_t dest, Buffer const &buffer) override {
    std::uint64_t tag = headerToTag(header);
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
    std::uint64_t tag = (std::uint64_t)channel << HEADER_FIELDS[Header::CHANNEL].offset;
    std::uint64_t mask = HEADER_FIELDS[Header::CHANNEL].mask;
    return (Request)clh_probe(this->clh_, tag, mask, true);
  }

  Request probe(std::uint8_t channel, std::uint32_t source) override {
    std::uint64_t tag = (std::uint64_t)channel << HEADER_FIELDS[Header::CHANNEL].offset
                        | (std::uint64_t)source << HEADER_FIELDS[Header::SOURCE].offset;
    std::uint64_t mask = HEADER_FIELDS[Header::CHANNEL].mask | HEADER_FIELDS[Header::SOURCE].mask;
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

  Header requestHeader(Request request) const override {
      return tagToHeader(clh_request_tag((CLH_Request *)request));
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
