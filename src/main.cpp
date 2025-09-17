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
    auto b01 = std::make_shared<hh::CommunicatorTask<int>>(commHandle, std::vector<int>({1}), hh::CommunicatorTaskOpt{});
    auto b02 = std::make_shared<hh::CommunicatorTask<int>>(commHandle, std::vector<int>({2}));
    auto frgn1 = std::make_shared<hh::LambdaTask<1, int, int>>("foreign task", 1);
    auto frgn2 = std::make_shared<hh::LambdaTask<1, int, int>>("foreign task", 1);
    auto bn0 = std::make_shared<hh::CommunicatorTask<int>>(commHandle, std::vector<int>({0}));
    auto out = std::make_shared<hh::LambdaTask<1, int, int>>("output", 1);

    auto mm = std::make_shared<hh::tool::MemoryPool<int>>();
    mm->template fill<int>(5);

    b01->setMemoryManager(mm);
    b02->setMemoryManager(mm);
    bn0->setMemoryManager(mm);

    in->setLambda<int>([commHandle](std::shared_ptr<int> data, auto self) {
      hh::logh::log(stdout, "[TASK] ", "in -> ", commHandle->rank);
      *data += 1;
      HH_DBG(*data);
      self.addResult(data);
    });
    frgn1->setLambda<int>([commHandle](std::shared_ptr<int> data, auto self) {
      hh::logh::log(stdout, "[TASK] ", "frng1 -> ", commHandle->rank);
      *data += 1;
      HH_DBG(*data);
      self.addResult(data);
    });
    frgn2->setLambda<int>([commHandle](std::shared_ptr<int> data, auto self) {
      hh::logh::log(stdout, "[TASK] ", "frng2 -> ", commHandle->rank);
      *data *= 2;
      HH_DBG(*data);
      self.addResult(data);
    });
    out->setLambda<int>([commHandle](std::shared_ptr<int> data, auto self) {
      hh::logh::log(stdout, "[TASK] ", "out -> ", commHandle->rank);
      *data += 1;
      HH_DBG(*data);
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
  hh::comm::CommHandle *ch = hh::comm::commCreate(true);
  auto data = std::make_shared<int>(4);
  TestGraph1 graph(ch);
  std::vector<int> results;

  std::cout << "rank = " << ch->rank << std::endl;

  graph.executeGraph(true);
  graph.pushData(data);
  hh::comm::commBarrier();
  graph.finishPushingData();

  if (ch->rank == 0) {
    while (auto result = graph.getBlockingResult()) {
      results.push_back(*std::get<std::shared_ptr<int>>(*result));
    }
  }

  graph.waitForTermination();

  graph.createDotFile(std::to_string(ch->rank) + "test.dot");
  hh::comm::commBarrier();
  if (ch->rank == 0) {
    HH_DBG(results);
  }

  hh::comm::commDestroy(ch);
  hh::comm::commFinalize();
  return 0;
}
