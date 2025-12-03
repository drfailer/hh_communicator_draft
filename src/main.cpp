#include "communicator/communicator.hpp"
#include "communicator/communicator_task.hpp"
#include "log.hpp"
#include <hedgehog/hedgehog.h>
#include <iostream>
#include <mutex>

std::mutex stdout_mutex;

struct TestGraph1 : hh::Graph<1, int, int> {
  hh::comm::Communicator *communicator_;

  TestGraph1(hh::comm::Communicator *communicator) : hh::Graph<1, int, int>("TestGraph1"), communicator_(communicator) {
    assert(communicator->nbProcesses() == 3);
    auto in = std::make_shared<hh::LambdaTask<1, int, int>>("input", 1);
    auto b01 = std::make_shared<hh::CommunicatorTask<int>>(communicator, std::vector<std::uint32_t>({1}), hh::CommunicatorTaskOpt{});
    auto b02 = std::make_shared<hh::CommunicatorTask<int>>(communicator, std::vector<std::uint32_t>({2}));
    auto frgn1 = std::make_shared<hh::LambdaTask<1, int, int>>("foreign task", 1);
    auto frgn2 = std::make_shared<hh::LambdaTask<1, int, int>>("foreign task", 1);
    auto bn0 = std::make_shared<hh::CommunicatorTask<int>>(communicator, std::vector<std::uint32_t>({0}));
    auto out = std::make_shared<hh::LambdaTask<1, int, int>>("output", 1);

    auto mm = std::make_shared<hh::tool::MemoryPool<int>>();
    mm->template fill<int>(5);

    // FIXME: the data returns to the memory pool too early
    // b01->setMemoryManager(mm);
    // b02->setMemoryManager(mm);
    // bn0->setMemoryManager(mm);

    in->setLambda<int>([communicator](std::shared_ptr<int> data, auto self) {
      auto output = std::make_shared<int>(*data + 1);
      hh::logh::log(stdout, "[", communicator->rank(), "][in]: input = ", *data, ", output = ", *output);
      self.addResult(output);
    });
    frgn1->setLambda<int>([communicator](std::shared_ptr<int> data, auto self) {
      auto output = std::make_shared<int>(*data + 1);
      hh::logh::log(stdout, "[", communicator->rank(), "][frng1]: input = ", *data, ", output = ", *output);
      self.addResult(output);
    });
    frgn2->setLambda<int>([communicator](std::shared_ptr<int> data, auto self) {
      auto output = std::make_shared<int>(*data * 2);
      hh::logh::log(stdout, "[", communicator->rank(), "][frng2]: input = ", *data, ", output = ", *output);
      self.addResult(output);
    });
    out->setLambda<int>([communicator](std::shared_ptr<int> data, auto self) {
      auto output = std::make_shared<int>(*data + 1);
      hh::logh::log(stdout, "[", communicator->rank(), "][out]: input = ", *data, ", output = ", *output);
      self.addResult(output);
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
  hh::comm::Communicator communicator(true);

  communicator.init(&argc, &argv);
  auto data = std::make_shared<int>(4);
  TestGraph1 graph(&communicator);
  std::vector<std::uint32_t> results;

  std::cout << "rank = " << communicator.rank() << std::endl;

  graph.executeGraph(true);
  graph.pushData(data);
  communicator.barrier();
  graph.finishPushingData();

  if (communicator.rank() == 0) {
    while (auto result = graph.getBlockingResult()) {
      results.push_back(*std::get<std::shared_ptr<int>>(*result));
    }
  }

  graph.waitForTermination();

  graph.createDotFile("graph_" + std::to_string(communicator.rank()) + ".dot");
  communicator.barrier();
  if (communicator.rank() == 0) {
    HH_DBG(results);
  }

  communicator.finalize();
  return 0;
}
