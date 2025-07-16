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
    graph.waitForTermination();
  }

  MPI_Finalize();
  return 0;
}
