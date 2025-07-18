#include "mpi.hpp"
#include "mpi_bridge.hpp"
#include <hedgehog/hedgehog.h>
#include <iostream>
#include <mpi.h>
#include <mutex>

std::mutex stdout_mutex;

struct TestGraph1 : hh::Graph<1, int, int> {
  TestGraph1() : hh::Graph<1, int, int>("TestGraph1") {
    auto in = std::make_shared<hh::LambdaTask<1, int, int>>("input", 1);
    auto b01 = std::make_shared<hh::MPIBridge<int>>(std::vector<int>({1}));
    DBG(b01->taskId());
    auto b02 = std::make_shared<hh::MPIBridge<int>>(std::vector<int>({2}));
    DBG(b02->taskId());
    auto frgn1 = std::make_shared<hh::LambdaTask<1, int, int>>("foreign task", 1);
    auto frgn2 = std::make_shared<hh::LambdaTask<1, int, int>>("foreign task", 1);
    auto bn0 = std::make_shared<hh::MPIBridge<int>>(std::vector<int>({0}));
    DBG(bn0->taskId());
    auto out = std::make_shared<hh::LambdaTask<1, int, int>>("output", 1);

    in->setLambda<int>([](std::shared_ptr<int> data, auto self) {
      std::cout << "[TASK] in -> " << getMPIRank() << std::endl;
      *data += 1;
      DBG(*data);
      self.addResult(data);
    });
    frgn1->setLambda<int>([](std::shared_ptr<int> data, auto self) {
      std::cout << "[TASK] frng1 -> " << getMPIRank() << std::endl;
      *data += 1;
      DBG(*data);
      self.addResult(data);
    });
    frgn2->setLambda<int>([](std::shared_ptr<int> data, auto self) {
      std::cout << "[TASK] frng2 -> " << getMPIRank() << std::endl;
      *data *= 2;
      DBG(*data);
      self.addResult(data);
    });
    out->setLambda<int>([](std::shared_ptr<int> data, auto self) {
      std::cout << "[TASK] out -> " << getMPIRank() << std::endl;
      *data += 1;
      DBG(*data);
      self.addResult(data);
    });

    this->inputs(in);
    this->edges(in, b01);
    this->edges(in, b02);
    this->edges(b01, frgn1);
    this->edges(b02, frgn2);
    this->edges(frgn1, bn0);
    this->edges(frgn2, bn0);
    this->edges(bn0, out);
    this->outputs(out);
  }
};

int main(int argc, char **argv) {
  auto data = std::make_shared<int>(4);
  TestGraph1 graph;
  std::vector<int> results;

  initMPI(argc, argv);

  std::cout << "rank = " << getMPIRank() << std::endl;

  graph.executeGraph(true);

  if (getMPIRank() == 0) {
    graph.pushData(data);
    graph.finishPushingData();
    while (auto result = graph.getBlockingResult()) {
      results.push_back(*std::get<std::shared_ptr<int>>(*result));
    }
  }

  graph.finishPushingData(); // we have to make sure that all the ranks are done
  graph.waitForTermination();

  if (getMPIRank() == 0) {
    graph.createDotFile("test.dot");
    DBG(results);
  }

  MPI_Finalize();
  return 0;
}
