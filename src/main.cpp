#include "communicator/service/clh_service.hpp"
#include "communicator/service/mpi_service.hpp"
#include "communicator/communicator_task.hpp"
#include "communicator/send_strategies.hpp"
#include "communicator/tool/memory_pool.hpp"
#include "log.hpp"
#include <hedgehog/hedgehog.h>
#include <iostream>
#include <mutex>

std::mutex stdout_mutex;

struct TestGraph1 : hh::Graph<1, int, int> {
  hh::comm::CommService *communicator_;

  /*
   * Running this program should return a memory error. In this graph, the input
   * task gives the data to two communicator task and each will try to return
   * the momory to the pool (mm). This specific use case would normally require
   * creating a custom type that implements some of the memory manager related
   * functions to avoid the double free.
   */

  TestGraph1(hh::comm::CommService *service, std::shared_ptr<hh::comm::tool::MemoryPool<int>> mm)
      : hh::Graph<1, int, int>("TestGraph1"), communicator_(service) {
    assert(service->nbProcesses() == 3);
    auto in = std::make_shared<hh::LambdaTask<1, int, int>>("input", 1);
    auto b01 = std::make_shared<hh::CommunicatorTask<int>>(service);
    auto b02 = std::make_shared<hh::CommunicatorTask<int>>(service);
    auto frgn1 = std::make_shared<hh::LambdaTask<1, int, int>>("foreign task", 1);
    auto frgn2 = std::make_shared<hh::LambdaTask<1, int, int>>("foreign task", 1);
    auto bn0 = std::make_shared<hh::CommunicatorTask<int>>(service);
    auto out = std::make_shared<hh::LambdaTask<1, int, int>>("output", 1);

    b01->template strategy<int>(hh::comm::strategy::SendTo<int>(1));
    b02->template strategy<int>(hh::comm::strategy::SendTo<int>(2));
    bn0->template strategy<int>(hh::comm::strategy::SendTo<int>(0));

    // FIXME: the data returns to the memory pool too early
    b01->setMemoryManager(mm->memoryManager());
    b02->setMemoryManager(mm->memoryManager());
    bn0->setMemoryManager(mm->memoryManager());

    in->setLambda<int>([service](std::shared_ptr<int> data, auto self) {
      int output = *data + 1;
      hh::logh::log(stdout, "[", service->rank(), "][in]: input = ", *data, ", output = ", output);
      *data += 1;
      self.addResult(data);
    });
    frgn1->setLambda<int>([service](std::shared_ptr<int> data, auto self) {
      int output = *data + 1;
      hh::logh::log(stdout, "[", service->rank(), "][frng1]: input = ", *data, ", output = ", output);
      *data += 1;
      self.addResult(data);
    });
    frgn2->setLambda<int>([service](std::shared_ptr<int> data, auto self) {
      int output = *data * 2;
      hh::logh::log(stdout, "[", service->rank(), "][frng2]: input = ", *data, ", output = ", output);
      *data *= 2;
      self.addResult(data);
    });
    out->setLambda<int>([service](std::shared_ptr<int> data, auto self) {
      int output = *data + 1;
      hh::logh::log(stdout, "[", service->rank(), "][out]: input = ", *data, ", output = ", output);
      *data += 1;
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
    if (communicator_->rank() == 0) {
      hh::Graph<1, int, int>::template pushData<T>(data);
    }
  }
};

int main(int argc, char **argv) {
  // hh::comm::CommService *service = new hh::comm::CLHService(true);
  hh::comm::CommService *service = new hh::comm::MPIService(&argc, &argv, true);

  auto mm = std::make_shared<hh::comm::tool::MemoryPool<int>>();
  mm->template fill<int>(5);

  // auto data = std::make_shared<int>(4);
  TestGraph1 graph(service, mm);
  std::vector<std::uint32_t> results;

  std::cout << "rank = " << service->rank() << std::endl;

  graph.executeGraph(true);
  if (service->rank() == 0) {
      auto data = mm->template allocate<int>();
      *data = 4;
      graph.pushData(data);
  }
  service->barrier();
  graph.finishPushingData();

  if (service->rank() == 0) {
    while (auto result = graph.getBlockingResult()) {
      auto resultPtr = std::get<std::shared_ptr<int>>(*result);
      results.push_back(*resultPtr);
      mm->template release<int>(std::move(resultPtr));
    }
  }

  graph.waitForTermination();

  graph.createDotFile("graph_" + std::to_string(service->rank()) + ".dot");
  service->barrier();
  if (service->rank() == 0) {
    HH_DBG(results);
  }
  delete service;
  return 0;
}
