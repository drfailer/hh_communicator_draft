#include "communicator/comm_tools.hpp"
#include "communicator/communicator_task.hpp"
#include "log.hpp"
#include <hedgehog/hedgehog.h>
#include <iostream>
#include <mpi.h>
#include <mutex>

std::mutex stdout_mutex;

struct TestGraph1 : hh::Graph<1, int, int> {
  hh::comm::CommHandle *commHandle_;

  TestGraph1(hh::comm::CommHandle *commHandle) : hh::Graph<1, int, int>("TestGraph1"), commHandle_(commHandle) {
    assert(commHandle->nbProcesses == 3);
    auto in = std::make_shared<hh::LambdaTask<1, int, int>>("input", 1);
    auto b01 = std::make_shared<hh::CommunicatorTask<int>>(commHandle, std::vector<int>({1}));
    auto b02 = std::make_shared<hh::CommunicatorTask<int>>(commHandle, std::vector<int>({2}));
    auto frgn1 = std::make_shared<hh::LambdaTask<1, int, int>>("foreign task", 1);
    auto frgn2 = std::make_shared<hh::LambdaTask<1, int, int>>("foreign task", 1);
    auto bn0 = std::make_shared<hh::CommunicatorTask<int>>(commHandle, std::vector<int>({0}));
    auto out = std::make_shared<hh::LambdaTask<1, int, int>>("output", 1);

    in->setLambda<int>([commHandle](std::shared_ptr<int> data, auto self) {
      std::cout << "[TASK] in -> " << commHandle->rank << std::endl;
      *data += 1;
      DBG(*data);
      self.addResult(data);
    });
    frgn1->setLambda<int>([commHandle](std::shared_ptr<int> data, auto self) {
      std::cout << "[TASK] frng1 -> " << commHandle->rank << std::endl;
      *data += 1;
      DBG(*data);
      self.addResult(data);
    });
    frgn2->setLambda<int>([commHandle](std::shared_ptr<int> data, auto self) {
      std::cout << "[TASK] frng2 -> " << commHandle->rank << std::endl;
      *data *= 2;
      DBG(*data);
      self.addResult(data);
    });
    out->setLambda<int>([commHandle](std::shared_ptr<int> data, auto self) {
      std::cout << "[TASK] out -> " << commHandle->rank << std::endl;
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

  // helper function of a builtin "DistributedGraph" ?
  template <typename T> void pushData(std::shared_ptr<T> data) {
    if (commHandle_->rank == 0) {
      hh::Graph<1, int, int>::template pushData<T>(data);
    }
  }
};

int main(int argc, char **argv) {
  hh::comm::commInit(argc, argv);
  hh::comm::CommHandle *ch = hh::comm::commCreate();
  auto data = std::make_shared<int>(4);
  TestGraph1 graph(ch);
  std::vector<int> results;

  std::cout << "rank = " << ch->rank << std::endl;

  graph.executeGraph(true);
  graph.pushData(data);
  if (ch->rank == 0)
    graph.finishPushingData();

  if (ch->rank == 0) {
    // while (auto result = graph.getBlockingResult()) {
    //   results.push_back(*std::get<std::shared_ptr<int>>(*result));
    // }
    results.push_back(*std::get<std::shared_ptr<int>>(*graph.getBlockingResult()));
    results.push_back(*std::get<std::shared_ptr<int>>(*graph.getBlockingResult()));
  }

  hh::comm::commBarrier();
  graph.finishPushingData();
  graph.waitForTermination();

  if (ch->rank == 0) {
    graph.createDotFile("test.dot");
    DBG(results);
  }

  hh::comm::commDestroy(ch);
  hh::comm::commFinalize();
  return 0;
}
