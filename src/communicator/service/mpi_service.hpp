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

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

/// @brief Implementation of hte MPI service backend.
class MPIService : public CommService {
public:
  /// @brief MPI Service constructor: calls MPI_Init with the program arguments.
  /// @param argc Pointer to the argument counter.
  /// @param argv Pointer to the arguments values.
  /// @param profilingEnabled Profiling flag (true to enable profiling).
  MPIService(int *argc, char ***argv, bool profilingEnabled = false)
      : CommService(profilingEnabled) {
    int32_t provided = 0;
    int32_t isInitialized = false;
    if(checkMPI(MPI_Initialized(&isInitialized)); !isInitialized) {
      isInitializer_ = true;
      checkMPI(MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided));
    }
    else {
      checkMPI(MPI_Query_thread(&provided));
    }

    if(provided != MPI_THREAD_MULTIPLE and provided != MPI_THREAD_SERIALIZED) {
      std::unordered_map<int32_t, std::string> threadMap = {
        {MPI_THREAD_SINGLE, "MPI_THREAD_SINGLE"},
        {MPI_THREAD_FUNNELED, "MPI_THREAD_FUNNELED"},
        // {MPI_THREAD_SERIALIZED, "MPI_THREAD_SERIALIZED"},
        // {MPI_THREAD_MULTIPLE, "MPI_THREAD_MULTIPLE"},
      };
      log::error("MPIService requires [MPI_THREAD_MULTIPLE|MPI_THREAD_SERIALIZED] but [", threadMap[provided], "] was provided!");
      checkMPI(MPI_Abort(MPI_COMM_WORLD, 0));
    }

    checkMPI(MPI_Comm_rank(MPI_COMM_WORLD, &this->rank_));
    checkMPI(MPI_Comm_size(MPI_COMM_WORLD, &this->nbProcesses_));

    // set the 0 channel to be MPI_COMM_WORLD
    this->comms_.emplace_back(MPI_COMM_WORLD);
  }

  /// @brief MPI Service destructor: calls MPI_Finalize.
  ~MPIService() override {
    if(!isInitializer_) return;
    MPI_Finalize();
  }

private:
  /// @brief header fields infos and offset: used to encode/decode MPI tags to header.
  static constexpr Header::FieldInfo HEADER_FIELDS[]{
      // MPI tags are at least 15 bits
      {.offset = 32, .mask = 0b1111111111111111111111111111111111111111111111111000000000000000}, // source
      {.offset = 32, .mask = 0b1111111111111111111111111111111111111111111111111000000000000000}, // channel
      // communicator data
      {.offset = 14, .mask = 0b0000000000000000000000000000000000000000000000000100000000000000}, // signal?
      {.offset = 11, .mask = 0b0000000000000000000000000000000000000000000000000011100000000000}, // typeid
      {.offset = 0, .mask = 0b0000000000000000000000000000000000000000000000000000000000000011}, // buffer id
  };

  /// @brief Encode the given header to an MPI tag.
  /// @param header Header to encode.
  /// @return MPI tag.
  static int headerToTag(Header const &header) {
    std::uint64_t tag = 0;
    tag |= header.signal << HEADER_FIELDS[Header::SIGNAL].offset;
    tag |= header.typeId << HEADER_FIELDS[Header::TYPE_ID].offset;
    tag |= header.bufferId << HEADER_FIELDS[Header::BUFFER_ID].offset;
    assert((tag & HEADER_FIELDS[0].mask) == 0);
    return (int)tag;
  }

  /// @brief Dencode the given MPI tag to a header.
  /// @param tag MPI tag to decode.
  /// @return Header.
  static Header tagToHeader(int tag) {
    assert(tag >= 0);
    assert((tag & HEADER_FIELDS[0].mask) == 0);
    Header header;
    header.signal = (tag & HEADER_FIELDS[Header::SIGNAL].mask) >> HEADER_FIELDS[Header::SIGNAL].offset;
    header.typeId = (tag & HEADER_FIELDS[Header::TYPE_ID].mask) >> HEADER_FIELDS[Header::TYPE_ID].offset;
    header.bufferId = (tag & HEADER_FIELDS[Header::BUFFER_ID].mask) >> HEADER_FIELDS[Header::BUFFER_ID].offset;
    assert((header.signal & ~(HEADER_FIELDS[Header::SIGNAL].mask >> HEADER_FIELDS[Header::SIGNAL].offset)) == 0);
    assert((header.typeId & ~(HEADER_FIELDS[Header::TYPE_ID].mask >> HEADER_FIELDS[Header::TYPE_ID].offset)) == 0);
    assert((header.bufferId & ~(HEADER_FIELDS[Header::BUFFER_ID].mask >> HEADER_FIELDS[Header::BUFFER_ID].offset))
           == 0);
    return header;
  }

private:
  /// @brief Internal service request.
  struct MPIRequest {
    MPI_Request request; ///< MPI request.
    MPI_Status  status;  ///< MPI status.
    MPI_Comm    comm;    ///< MPI communicator (correspond to the channel id).
    int         flag;    ///< Flag used with some MPI functions such as `probe`.
  };

public: // send ////////////////////////////////////////////////////////////////

  /// @brief Send a buffer to a given destination synchronously (MPI_Send).
  /// @param header Header of the request.
  /// @param dest   Rank of the destination process.
  /// @param buffer Buffer to send.
  void send(Header const &header, rank_t dest, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    int                         tag = headerToTag(header);

    Header testHeader = tagToHeader(tag);
    assert(header.signal == testHeader.signal);
    assert(header.typeId == testHeader.typeId);
    assert(header.bufferId == testHeader.bufferId);

    checkMPI(MPI_Send(buffer.data(), (int)buffer.size(), MPI_BYTE, (int)dest, tag, this->comms_[header.channel]));
  }

  /// @brief Send a buffer to a given destination asynchronously (MPI_Isend).
  /// @param header Header of the request.
  /// @param dest   Rank of the destination process.
  /// @param buffer Buffer to send.
  /// @return Send request.
  Request sendAsync(Header const &header, rank_t dest, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = requestPool_.allocate();
    int                         tag = headerToTag(header);

    // vvv DEBUG vvv
    Header testHeader = tagToHeader(tag);
    assert(header.signal == testHeader.signal);
    assert(header.typeId == testHeader.typeId);
    assert(header.bufferId == testHeader.bufferId);
    // ^^^ DEBUG ^^^

    r->comm = this->comms_[header.channel];
    checkMPI(MPI_Isend(buffer.data(), (int)buffer.size(), MPI_BYTE, (int)dest, tag, r->comm, &r->request));
    return r;
  }

public: // recv ////////////////////////////////////////////////////////////////

  /// @brief Receive a request header and buffer synchronously (MPI_Recv).
  /// @param header Header of the request.
  /// @param buffer Buffer to receive the data.
  void recv(Header const &header, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPI_Status                  status;
    int                         tag = headerToTag(header);
    checkMPI(MPI_Recv(buffer.data(), (int)buffer.size(), MPI_BYTE, (int)header.source, tag, this->comms_[header.channel],
                      &status));
  }

  /// @brief Receive a request header and buffer asynchronously (MPI_Irecv).
  /// @param header Header of the request.
  /// @param buffer Buffer to receive the data.
  /// @return Receive request.
  Request recvAsync(Header const &header, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    int                         tag = headerToTag(header);
    MPIRequest                 *r = requestPool_.allocate();
    r->comm = this->comms_[header.channel];
    checkMPI(MPI_Irecv(buffer.data(), (int)buffer.size(), MPI_BYTE, (int)header.source, tag, r->comm, &r->request));
    return r;
  }

  /// @brief Receive a request after a successful probe synchronously (MPI_Recv).
  /// @param probeRequest Request returned by the probe.
  /// @param buffer       Buffer to receive the data.
  void recv(Request probeRequest, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = (MPIRequest *)probeRequest;
    checkMPI(MPI_Recv(buffer.data(), (int)buffer.size(), MPI_BYTE, r->status.MPI_SOURCE, r->status.MPI_TAG, r->comm, &r->status));
    requestPool_.release(r);
  }

  /// @brief Receive a request after a successful probe asynchronously (MPI_Irecv).
  /// @param probeRequest Request returned by the probe.
  /// @param buffer       Buffer to receive the data.
  /// @return Receive request.
  Request recvAsync(Request probeRequest, Buffer const &buffer) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = (MPIRequest *)probeRequest;
    checkMPI(
        MPI_Irecv(buffer.data(), (int)buffer.size(), MPI_BYTE, r->status.MPI_SOURCE, r->status.MPI_TAG, r->comm, &r->request));
    return probeRequest;
  }

public: // probe ///////////////////////////////////////////////////////////////

  /// @brief Probe a given channel for incoming requests synchronously
  ///        (MPI_Probe to MPI_ANY_SOURCE).
  /// @param channel Channel to probe (each communicator task use a different
  ///                channel, the channel is create with `newChannel`).
  /// @return Probe request.
  Request probe(channel_t channel) override {
    return probe(channel, (rank_t)MPI_ANY_SOURCE);
  }

  /// @brief Probe a given channel for incoming requests asynchronously
  ///        (MPI_Iprobe to MPI_ANY_SOURCE).
  /// @param channel Channel to probe (each communicator task use a different
  ///                channel, the channel is create with `newChannel`).
  /// @return Probe request.
  Request probeAsync(channel_t channel) override {
    return probeAsync(channel, (rank_t)MPI_ANY_SOURCE);
  }

  /// @brief Probe a given channel for incoming requests sent by the given
  ///        source synchronously (MPI_Probe).
  /// @param channel Channel to probe (each communicator task use a different
  ///                channel, the channel is create with `newChannel`).
  /// @param source  Sender rank.
  /// @return Probe request.
  Request probe(channel_t channel, rank_t source) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = requestPool_.allocate();
    r->comm = this->comms_[channel];
    checkMPI(MPI_Probe((int)source, MPI_ANY_TAG, r->comm, &r->status));
    return r;
  }

  /// @brief Probe a given channel for incoming requests sent by the given
  ///        source asynchronously (MPI_Iprobe).
  /// @param channel Channel to probe (each communicator task use a different
  ///                channel, the channel is create with `newChannel`).
  /// @param source  Sender rank.
  /// @return Probe request.
  Request probeAsync(channel_t channel, rank_t source) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = requestPool_.allocate();
    r->comm = this->comms_[channel];
    checkMPI(MPI_Iprobe((int)source, MPI_ANY_TAG, r->comm, &r->flag, &r->status));
    return r;
  }

public: // requests ////////////////////////////////////////////////////////////

  /// @brief Tells if the given request is completed (MPI_Test).
  /// @param request Request for which the completion requires to be tested.
  /// @return True if the request is completed, false otherwise.
  bool requestCompleted(Request request) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = (MPIRequest *)request;
    r->flag = 0;
    checkMPI(MPI_Test(&r->request, &r->flag, &r->status));
    return r->flag != 0;
  }

  /// @brief Release the given request.
  /// @param request Request to release.
  void requestRelease(Request request) override {
    // MPI_Request_free(&r->request);
    requestPool_.release((MPIRequest *)request);
  }

  /// @brief Cancel the given request (MPI_Cancel).
  /// @param request Request to cancel.
  void requestCancel(Request request) override {
    std::lock_guard<std::mutex> mpiLock(this->mutex());
    MPIRequest                 *r = (MPIRequest *)request;
    checkMPI(MPI_Cancel(&r->request));
    requestPool_.release((MPIRequest *)request);
  }

  /// @brief Get the buffer size of an incoming request (MPI_Get_count).
  /// @param request Request that contains the buffer size information.
  /// @return Buffer size.
  size_t bufferSize(Request request) override {
    int         count = -1;
    MPIRequest *r = (MPIRequest *)request;

    MPI_Get_count(&r->status, MPI_BYTE, &count);
    assert(count > 0);
    return (size_t)count;
  }

  /// @brief Get the header of a request (tagToHeader(MPI_Status::MPI_TAG)).
  /// @param request Request that contains the header information.
  /// @return Header of the request.
  Header requestHeader(Request request) override {
    MPIRequest *r = (MPIRequest *)request;
    Header      header;

    header = tagToHeader(r->status.MPI_TAG);
    header.channel = (channel_t)r->comm;
    header.source = r->status.MPI_SOURCE;
    return header;
  }

  /// @brief Used to know if a probe has found a matching request (used with
  ///        async probe) (check the value of the flag in/out argument of
  ///        MPI_Test).
  /// @param request Request returned by `probeAsync`.
  /// @return True if the probe found a messages, false otherwise.
  bool probeSuccess(Request request) override {
    MPIRequest *r = (MPIRequest *)request;
    return r->flag != 0;
  }

public: // synchronization /////////////////////////////////////////////////////

  /// @brief Synchronize all processes up to a certain point (MPI_Barrier).
  void barrier(channel_t channel = 0) override {
    checkMPI(MPI_Barrier(this->comms_[channel]));
  }

public:
  /// @brief Rank accessor.
  /// @return Rank of the current process.
  rank_t rank() const override { return (rank_t)rank_; }

  /// @brief Number of processes accessor.
  /// @return Number of processes.
  std::uint32_t nbProcesses() const override { return (std::uint32_t)nbProcesses_; }

public:
  /// @brief Create a new channel (creates a new MPI_Comm with MPI_Comm_Split).
  /// @return Id of the new channel.
  channel_t newChannel() override {
    std::lock_guard<std::mutex> mpiLock(mutex());
    channel_t                   channel = comms_.size();
    comms_.push_back(MPI_Comm{});
    checkMPI(MPI_Comm_split(MPI_COMM_WORLD, (int)channel, (int)rank_, &comms_.back()));
    return channel;
  }

private:
  /// @brief Internal mpi check function.
  /// @brief MPI status code.
  void checkMPI(int code) {
    if (code == 0) {
      return;
    }
    char error[100] = {0};
    int  len = 0;
    MPI_Error_string(code, error, &len);
    log::error("mpi error: ", std::string(error, error + len));
  }

private:
  bool                    isInitializer_ = false;///< MPI initialized by MPIService?
  int                     rank_ = -1;            ///< MPI rank.
  int                     nbProcesses_ = -1;     ///< MPI comm size.
  RequestPool<MPIRequest> requestPool_ = {};     ///< Request memory pool to optimize request allocations.
  std::vector<MPI_Comm>   comms_ = {};           ///< MPI Communicators (comms[channel_id] -> channel communicator)
};

} // end namespace comm

} // end namespace hh

#endif
