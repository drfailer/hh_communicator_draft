#ifndef MPI
#define MPI
#include <mpi.h>
#include <serializer/serializer.hpp>

inline int getMPIRank() {
  int rank = -1;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  return rank;
}

inline int getMPINbProcesses() {
  int nb_processes = -1;
  MPI_Comm_size(MPI_COMM_WORLD, &nb_processes);
  return nb_processes;
}


inline void initMPI(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  // int provided;
  // MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  // if (provided != MPI_THREAD_MULTIPLE) {
  //     throw std::runtime_error("MPI_THREAD_MULTIPLE not supported");
  // }
}

enum class MPISignal {
    Data,
    Terminate,
    Disconnect,
    None,
};

inline void sendSignal(std::vector<int> const &dests, int graphId, int taskId, MPISignal signal) {
  namespace ser = serializer;
  ser::Bytes buffer(1024);
  ser::serialize<ser::Serializer<ser::Bytes>>(buffer, 0, graphId, taskId, signal);
  for (int dest : dests) {
      MPI_Send(buffer.data(), buffer.size(), MPI_BYTE, dest, 0, MPI_COMM_WORLD);
  }
}

template <typename Ser, typename T>
void sendData(std::vector<int> const &dests, int graphId, int taskId, std::shared_ptr<T> data) {
  namespace ser = serializer;
  ser::Bytes buffer(1024);
  size_t pos = ser::serialize<ser::Serializer<ser::Bytes>>(buffer, 0, graphId, taskId, MPISignal::Data);
  pos = ser::serializeWithId<Ser, T>(buffer, pos, data);
  for (int dest : dests) {
      MPI_Send(buffer.data(), buffer.size(), MPI_BYTE, dest, 0, MPI_COMM_WORLD);
  }
}


#endif
