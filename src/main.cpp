#include "mpi.hpp"
#include "mpi_bridge.hpp"
#include <hedgehog/hedgehog.h>
#include <iostream>
#include <mpi.h>

struct TestGraph1 : hh::Graph<1, int, int> {
  TestGraph1() : hh::Graph<1, int, int>("TestGraph1") {
    auto in = std::make_shared<hh::LambdaTask<1, int, int>>("input", 1);
    auto b01 = std::make_shared<hh::MPIBridge<int>>(std::vector<int>({1}));
    auto frgn = std::make_shared<hh::LambdaTask<1, int, int>>("foreign task", 1);
    auto b10 = std::make_shared<hh::MPIBridge<int>>(std::vector<int>({0}));
    auto out = std::make_shared<hh::LambdaTask<1, int, int>>("output", 1);

    in->setLambda<int>([](std::shared_ptr<int> data, auto self) {
      std::cout << "[TASK] in -> " << mpi_rank() << std::endl;
      *data += 1;
      DBG(*data);
      self.addResult(data);
    });
    frgn->setLambda<int>([](std::shared_ptr<int> data, auto self) {
      std::cout << "[TASK] frng -> " << mpi_rank() << std::endl;
      *data += 1;
      DBG(*data);
      self.addResult(data);
    });
    out->setLambda<int>([](std::shared_ptr<int> data, auto self) {
      std::cout << "[TASK] out -> " << mpi_rank() << std::endl;
      *data += 1;
      DBG(*data);
      self.addResult(data);
    });

    this->inputs(in);
    this->edges(in, b01);
    this->edges(b01, frgn);
    this->edges(frgn, b10);
    this->edges(b10, out);
    this->outputs(out);
  }

  // TODO: graphs should communicate with a specific tag
  void sendBridgesTerminationSignal(int dest) {
    namespace ser = serializer;
    ser::Bytes buffer(1024);
    ser::serialize<ser::Serializer<ser::Bytes>>(buffer, 0, 0);

    MPI_Send(buffer.data(), buffer.size(), MPI_BYTE, dest, 0, MPI_COMM_WORLD);
  }

  void terminate() {
    int rank = mpi_rank();

    this->finishPushingData();
    if (rank == 0) {
      sendBridgesTerminationSignal(rank);
    }
    this->waitForTermination();

    // send termination signal to the other processes
    if (rank == 0) {
      size_t nb_processes = (size_t)mpi_nb_processes();
      for (size_t i = 1; i < nb_processes; ++i) {
        sendBridgesTerminationSignal(i);
      }
    }
  }
};

int main(int argc, char **argv) {
  auto data = std::make_shared<int>(4);
  TestGraph1 graph;

  init_mpi(argc, argv);

  std::cout << "rank = " << mpi_rank() << std::endl;

  graph.executeGraph(true);

  if (mpi_rank() == 0) {
    graph.pushData(data);
    graph.finishPushingData();
    auto result = graph.getBlockingResult();
    std::cout << "result = " << *std::get<std::shared_ptr<int>>(*result) << std::endl;
  }

  graph.terminate();

  MPI_Finalize();
  return 0;
}
