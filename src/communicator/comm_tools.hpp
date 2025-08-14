#ifndef COMMUNICATOR_COMM_TOOLS
#define COMMUNICATOR_COMM_TOOLS
#include "serializer/serialize.hpp"
#include <cstddef>
#include <mpi.h>
#include <serializer/serializer.hpp>

/*
 * Hedgehog communicator backend implementation with serializer-cpp and MPI
 */

namespace hh {

namespace comm {

// TypeTable (serialize implementation) ////////////////////////////////////////

template <typename... Types> using TypeTable = serializer::tools::TypeTable<Types...>;

// Buffer //////////////////////////////////////////////////////////////////////

struct Buffer {
  serializer::Bytes data;
  size_t pos;
};

inline Buffer bufferCreate(auto &&...args) {
  return Buffer{
      .data = serializer::Bytes(args...),
      .pos = 0,
  };
}

// Comm ////////////////////////////////////////////////////////////////////////

enum class CommSignal {
  Data,
  Terminate,
  Disconnect,
  None,
};

struct CommHandle {
  int rank;
  int nb_processes;
};

inline CommHandle commCreate() {
  CommHandle handle = {
      .rank = -1,
      .nb_processes = -1,
  };
  MPI_Comm_rank(MPI_COMM_WORLD, &handle.rank);
  MPI_Comm_size(MPI_COMM_WORLD, &handle.nb_processes);
  return handle;
}

inline void commInit(int argc, char **argv) { MPI_Init(&argc, &argv); }

inline void commFinalize() { MPI_Finalize(); }

inline void sendSignal(std::vector<int> const &dests, int graphId, int taskId, CommSignal signal) {
  namespace ser = serializer;
  ser::Bytes buffer(1024);
  ser::serialize<ser::Serializer<ser::Bytes>>(buffer, 0, graphId, taskId, signal);
  for (int dest : dests) {
    MPI_Send(buffer.data(), buffer.size(), MPI_BYTE, dest, 0, MPI_COMM_WORLD);
  }
}

template <typename TypeTable, typename T>
void sendData(std::vector<int> const &dests, int graphId, int taskId, std::shared_ptr<T> data) {
  namespace ser = serializer;
  ser::Bytes buf(1024);
  size_t pos = ser::serialize<ser::Serializer<ser::Bytes>>(buf, 0, graphId, taskId, CommSignal::Data);
  pos = ser::serializeWithId<ser::Serializer<ser::Bytes, TypeTable>, T>(buf, pos, data);
  for (int dest : dests) {
    MPI_Send(buf.data(), buf.size(), MPI_BYTE, dest, 0, MPI_COMM_WORLD);
  }
}

inline void recvSignal(Buffer &buf, int &senderRank, int &graphId, int &taskId, CommSignal &signal) {
  namespace ser = serializer;
  MPI_Status status;
  MPI_Recv(buf.data.data(), buf.data.size(), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
  senderRank = status.MPI_SOURCE;
  buf.pos = ser::deserialize<ser::Serializer<ser::Bytes>>(buf.data, 0, graphId, taskId, signal);
}

template <typename TypeTable> void unpackData(Buffer &buf, auto cb) {
  namespace ser = serializer;
  auto typeId = ser::tools::getId<TypeTable>(buf.data, buf.pos);
  serializer::tools::applyId(typeId, TypeTable(), [&]<typename T>() {
    std::shared_ptr<T> data = nullptr;
    buf.pos = ser::deserializeWithId<ser::Serializer<ser::Bytes, TypeTable>, T>(buf.data, buf.pos, data);
    cb.template operator()<T>(data);
  });
}

} // end namespace comm

} // end namespace hh

#endif
