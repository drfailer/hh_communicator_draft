#ifndef COMMUNICATOR_MPI_SERVICE
#define COMMUNICATOR_MPI_SERVICE
#include "../../log.hpp"
#include "../protocol.hpp"
#include "comm_service.hpp"
#include "request.hpp"
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <mpi.h>
#include <stdexcept>
#include <thread>

namespace hh {

namespace comm {

class MPIService : public CommService {
public:
  MPIService(int *argc, char ***argv, bool collectStats = false)
      : CommService(collectStats) {
    int32_t provided = 0;
    // MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_rank(MPI_COMM_WORLD, &this->rank_);
    MPI_Comm_size(MPI_COMM_WORLD, &this->nbProcesses_);
  }

  ~MPIService() {
    MPI_Finalize();
  }

private:
  static constexpr Header::FieldInfo HEADER_FIELDS[]{
      // MPI tags are at least 15 bits
      {.offset = 32, .mask = 0b1111111111111111111111111111111111111111111111111000000000000000}, // source
      {.offset = 32, .mask = 0b1111111111111111111111111111111111111111111111111000000000000000}, // channel
      // communicator data
      {.offset = 14, .mask = 0b0000000000000000000000000000000000000000000000000100000000000000}, // signal?
      {.offset = 11, .mask = 0b0000000000000000000000000000000000000000000000000011100000000000}, // typeid
      {.offset = 2, .mask = 0b0000000000000000000000000000000000000000000000000000011111111100}, // package id
      {.offset = 0, .mask = 0b0000000000000000000000000000000000000000000000000000000000000011}, // buffer id
  };

  static int headerToTag(Header const &header) {
    std::uint64_t tag = 0;
    tag |= header.signal << HEADER_FIELDS[Header::SIGNAL].offset;
    tag |= header.typeId << HEADER_FIELDS[Header::TYPE_ID].offset;
    tag |= header.packageId << HEADER_FIELDS[Header::PACKAGE_ID].offset;
    tag |= header.bufferId << HEADER_FIELDS[Header::BUFFER_ID].offset;
    assert((tag & HEADER_FIELDS[0].mask) == 0);
    return (int)tag;
  }

  static Header tagToHeader(int tag) {
    assert(tag >= 0);
    assert((tag & HEADER_FIELDS[0].mask) == 0);
    Header header;
    header.signal = (tag & HEADER_FIELDS[Header::SIGNAL].mask) >> HEADER_FIELDS[Header::SIGNAL].offset;
    header.typeId = (tag & HEADER_FIELDS[Header::TYPE_ID].mask) >> HEADER_FIELDS[Header::TYPE_ID].offset;
    header.packageId = (tag & HEADER_FIELDS[Header::PACKAGE_ID].mask) >> HEADER_FIELDS[Header::PACKAGE_ID].offset;
    header.bufferId = (tag & HEADER_FIELDS[Header::BUFFER_ID].mask) >> HEADER_FIELDS[Header::BUFFER_ID].offset;
    assert((header.signal & ~(HEADER_FIELDS[Header::SIGNAL].mask >> HEADER_FIELDS[Header::SIGNAL].offset)) == 0);
    assert((header.typeId & ~(HEADER_FIELDS[Header::TYPE_ID].mask >> HEADER_FIELDS[Header::TYPE_ID].offset)) == 0);
    assert((header.packageId & ~(HEADER_FIELDS[Header::PACKAGE_ID].mask >> HEADER_FIELDS[Header::PACKAGE_ID].offset))
           == 0);
    assert((header.bufferId & ~(HEADER_FIELDS[Header::BUFFER_ID].mask >> HEADER_FIELDS[Header::BUFFER_ID].offset))
           == 0);
    return header;
  }

private:
  struct MPIRequest {
    MPI_Request request;
    MPI_Status  status;
    MPI_Comm    comm;
    int         flag;
  };

public: // send ////////////////////////////////////////////////////////////////
  void send(Header const &header, rank_t dest, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    int                         tag = headerToTag(header);

    Header testHeader = tagToHeader(tag);
    assert(header.signal == testHeader.signal);
    assert(header.typeId == testHeader.typeId);
    assert(header.packageId == testHeader.packageId);
    assert(header.bufferId == testHeader.bufferId);

    checkMPI(MPI_Send(buffer.mem, (int)buffer.len, MPI_BYTE, (int)dest, tag, this->comms_[header.channel]));
  }

  Request sendAsync(Header const &header, rank_t dest, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = requestPool_.allocate();
    int                         tag = headerToTag(header);

    // vvv DEBUG vvv
    Header testHeader = tagToHeader(tag);
    assert(header.signal == testHeader.signal);
    assert(header.typeId == testHeader.typeId);
    assert(header.packageId == testHeader.packageId);
    assert(header.bufferId == testHeader.bufferId);
    // ^^^ DEBUG ^^^

    r->comm = this->comms_[header.channel];
    checkMPI(MPI_Isend(buffer.mem, (int)buffer.len, MPI_BYTE, (int)dest, tag, r->comm, &r->request));
    return r;
  }

public: // recv ////////////////////////////////////////////////////////////////
  void recv(Header const &header, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPI_Status                  status;
    int                         tag = headerToTag(header);
    checkMPI(MPI_Recv(buffer.mem, (int)buffer.len, MPI_BYTE, (int)header.source, tag, this->comms_[header.channel],
                      &status));
  }

  Request recvAsync(Header const &header, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    int                         tag = headerToTag(header);
    MPIRequest                 *r = requestPool_.allocate();
    r->comm = this->comms_[header.channel];
    checkMPI(MPI_Irecv(buffer.mem, (int)buffer.len, MPI_BYTE, (int)header.source, tag, r->comm, &r->request));
    return r;
  }

  void recv(Request probeRequest, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = (MPIRequest *)probeRequest;
    checkMPI(MPI_Recv(buffer.mem, (int)buffer.len, MPI_BYTE, r->status.MPI_SOURCE, r->status.MPI_TAG, r->comm, &r->status));
    requestPool_.release(r);
  }

  Request recvAsync(Request probeRequest, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = (MPIRequest *)probeRequest;
    checkMPI(
        MPI_Irecv(buffer.mem, (int)buffer.len, MPI_BYTE, r->status.MPI_SOURCE, r->status.MPI_TAG, r->comm, &r->request));
    return probeRequest;
  }

public: // probe ///////////////////////////////////////////////////////////////
  Request probe(channel_t channel) override {
    return probe(channel, (rank_t)MPI_ANY_SOURCE);
  }

  Request probeAsync(channel_t channel) override {
    return probeAsync(channel, (rank_t)MPI_ANY_SOURCE);
  }

  Request probe(channel_t channel, rank_t source) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = requestPool_.allocate();
    r->comm = this->comms_[channel];
    checkMPI(MPI_Probe((int)source, MPI_ANY_TAG, r->comm, &r->status));
    return r;
  }

  Request probeAsync(channel_t channel, rank_t source) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = requestPool_.allocate();
    r->comm = this->comms_[channel];
    checkMPI(MPI_Iprobe((int)source, MPI_ANY_TAG, r->comm, &r->flag, &r->status));
    return r;
  }

public: // requests ////////////////////////////////////////////////////////////
  bool requestCompleted(Request request) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = (MPIRequest *)request;
    r->flag = 0;
    checkMPI(MPI_Test(&r->request, &r->flag, &r->status));
    return r->flag != 0;
  }

  void requestRelease(Request request) override {
    // MPI_Request_free(&r->request);
    requestPool_.release((MPIRequest *)request);
  }

  void requestCancel(Request request) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = (MPIRequest *)request;
    checkMPI(MPI_Cancel(&r->request));
    requestPool_.release((MPIRequest *)request);
  }

  size_t bufferSize(Request request) override {
    int         count = -1;
    MPIRequest *r = (MPIRequest *)request;

    MPI_Get_count(&r->status, MPI_BYTE, &count);
    assert(count > 0);
    return (size_t)count;
  }

  Header requestHeader(Request request) override {
    MPIRequest *r = (MPIRequest *)request;
    Header      header;

    header = tagToHeader(r->status.MPI_TAG);
    header.channel = (channel_t)r->comm;
    header.source = r->status.MPI_SOURCE;
    return header;
  }

  bool probeSuccess(Request request) override {
    MPIRequest *r = (MPIRequest *)request;
    return r->flag != 0;
  }

public: // synchronization /////////////////////////////////////////////////////
  void barrier() override {
    checkMPI(MPI_Barrier(MPI_COMM_WORLD));
  }

public:
  rank_t rank() const override {
    return (rank_t)rank_;
  }
  std::uint32_t nbProcesses() const override {
    return (std::uint32_t)nbProcesses_;
  }

public:
  channel_t newChannel() override {
    std::lock_guard<std::mutex> mpiLock(mutex());
    channel_t                   channel = comms_.size();
    packageCounters_.push_back(0);
    comms_.push_back(MPI_Comm{});
    checkMPI(MPI_Comm_split(MPI_COMM_WORLD, (int)channel, (int)rank_, &comms_.back()));
    return channel;
  }

  package_id_t newPackageId(channel_t channel) override {
    package_id_t result = packageCounters_[channel];
    // update the id and make sure it stays on 9 bits
    packageCounters_[channel] = (packageCounters_[channel] + 1) % 512;
    return result;
  }

private:
  void checkMPI(int code) {
    if (code == 0) {
      return;
    }
    char error[100] = {0};
    int  len = 0;
    MPI_Error_string(code, error, &len);
    logh::error("mpi error: ", std::string(error, error + len));
  }

private:
  int                     rank_ = -1;
  int                     nbProcesses_ = -1;
  RequestPool<MPIRequest> requestPool_ = {};
  std::vector<MPI_Comm>   comms_ = {};
  std::vector<size_t>     packageCounters_ = {};
};

} // end namespace comm

} // end namespace hh

#endif
