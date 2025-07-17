#ifndef MPI
#define MPI
#include <mpi.h>

inline int mpi_rank() {
  int rank = -1;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  return rank;
}

inline int mpi_nb_processes() {
  int nb_processes = -1;
  MPI_Comm_size(MPI_COMM_WORLD, &nb_processes);
  return nb_processes;
}


inline void init_mpi(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  // int provided;
  // MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  // if (provided != MPI_THREAD_MULTIPLE) {
  //     throw std::runtime_error("MPI_THREAD_MULTIPLE not supported");
  // }
}

#endif
