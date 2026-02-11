#ifndef COMMUNICATOR_COMM_SERVICE
#define COMMUNICATOR_COMM_SERVICE
#include "../protocol.hpp"
#include "request.hpp"
#include "../stats.hpp"
#include <cassert>
#include <thread>

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

/// @brief Communicator service used to interface different communication
///        backends.
class CommService {
public:
  /// @brief Communicator service constructor
  /// @param profilingEnabled Profiling flag.
  CommService(bool profilingEnabled)
      : profilingEnabled_(profilingEnabled), startTime_(std::chrono::system_clock::now()) {}

  /// @brief Communicator service destructor
  virtual ~CommService() = default;

public:
  /// @brief Send a buffer to a given destination synchronously.
  /// @param header Header of the request (see Header structure).
  /// @param dest   Rank of the destination process.
  /// @param buffer Buffer to send.
  virtual void send(Header const &header, rank_t dest, Buffer const &buffer) = 0;

  /// @brief Send a buffer to a given destination asynchronously.
  /// @param header Header of the request (see Header structure).
  /// @param dest   Rank of the destination process.
  /// @param buffer Buffer to send.
  /// @return Send request.
  virtual Request sendAsync(Header const &header, rank_t dest, Buffer const &buffer) = 0;

  /// @brief Receive a request header and buffer synchronously.
  /// @param header Header of the request (see Header structure).
  /// @param buffer Buffer to receive the data.
  virtual void recv(Header const &header, Buffer const &buffer) = 0;

  /// @brief Receive a request header and buffer asynchronously.
  /// @param header Header of the request (see Header structure).
  /// @param buffer Buffer to receive the data.
  /// @return Receive request.
  virtual Request recvAsync(Header const &header, Buffer const &buffer) = 0;

  /// @brief Receive a request after a successful probe synchronously.
  /// @param probeRequest Request returned by the probe.
  /// @param buffer       Buffer to receive the data.
  virtual void recv(Request probeRequest, Buffer const &buffer) = 0;

  /// @brief Receive a request after a successful probe asynchronously.
  /// @param probeRequest Request returned by the probe.
  /// @param buffer       Buffer to receive the data.
  /// @return Receive request.
  virtual Request recvAsync(Request probeRequest, Buffer const &buffer) = 0;

  /// @brief Probe a given channel for incoming requests synchronously.
  /// @param channel Channel to probe (each communicator task use a different
  ///                channel, the channel is create with `newChannel`).
  /// @return Probe request.
  virtual Request probe(channel_t channel) = 0;

  /// @brief Probe a given channel for incoming requests asynchronously.
  /// @param channel Channel to probe (each communicator task use a different
  ///                channel, the channel is create with `newChannel`).
  /// @return Probe request.
  virtual Request probeAsync(channel_t channel) = 0;

  /// @brief Probe a given channel for incoming requests sent by the given source synchronously.
  /// @param channel Channel to probe (each communicator task use a different
  ///                channel, the channel is create with `newChannel`).
  /// @param source  Sender rank.
  /// @return Probe request.
  virtual Request probe(channel_t channel, rank_t source) = 0;

  /// @brief Probe a given channel for incoming requests sent by the given source asynchronously.
  /// @param channel Channel to probe (each communicator task use a different
  ///                channel, the channel is create with `newChannel`).
  /// @param source  Sender rank.
  /// @return Probe request.
  virtual Request probeAsync(channel_t channel, rank_t source) = 0;

  /// @brief Tells if the given request is completed (asynchronous requests).
  /// @param request Request for which the completion requires to be tested.
  /// @return True if the request is completed, false otherwise.
  virtual bool requestCompleted(Request request) = 0;

  /// @brief Release the given request.
  /// @param request Request to release.
  virtual void requestRelease(Request request) = 0;

  /// @brief Cancel the given request.
  /// @param request Request to cancel.
  virtual void requestCancel(Request request) = 0;

  /// @brief Get the buffer size of an incoming request (use with probe).
  /// @param request Request that contains the buffer size information.
  /// @return Buffer size.
  virtual size_t bufferSize(Request request) = 0;

  /// @brief Get the header of a request.
  /// @param request Request that contains the header information.
  /// @return Header of the request.
  virtual Header requestHeader(Request request) = 0;

  /// @brief Used to know if a probe has found a matching request (used with async probe).
  /// @param request Request returned by `probeAsync`.
  /// @return True if the probe found a messages, false otherwise.
  virtual bool probeSuccess(Request request) = 0;

  /// @brief Synchronize all processes up to a certain point.
  virtual void barrier() = 0;

  /// @brief Rank accessor.
  /// @return Rank of the current process.
  virtual rank_t rank() const = 0;

  /// @brief Number of processes accessor.
  /// @return Number of processes.
  virtual std::uint32_t nbProcesses() const = 0;

  /// @brief Tells if profiling is turned on.
  /// @return True if profiling is on, false otherwise.
  bool profilingEnabled() const { return profilingEnabled_; }

  /// @brief Return the time point when the program was started (used for profiling).
  /// @return Start time point.
  time_t startTime() const { return startTime_; }

  /// @brief Create a new channel (used by communicator tasks).
  /// @return Id of the new channel.
  virtual channel_t newChannel() = 0;

  /// @brief Give access to the mutex to the implementation.
  /// @return mutex.
  std::mutex &mutex() { return mutex_; }
private:
  bool       profilingEnabled_ = false; ///< profiling flag.
  std::mutex mutex_;                    ///< mutex for synchronization.
  time_t     startTime_;                ///< Program start time point (profiling).
};

} // end namespace comm

} // end namespace hh

#endif
