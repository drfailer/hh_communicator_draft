# Hedgehog Communicator Task

## Requirements

- Hedgehog
- MPI

## Example

```cpp
// create the service
bool enabledProfiling = true;
hh::comm::MPIService service(&argc, &argv, enabledProfiling);

// create and setup the communicator
auto ct = std::make_shared<hh::comm::CommunicatorTask<Data1, Data2>>(&service, "example");

// create a memory manager (use the memory pool)
auto mm = std::make_shared<hh::comm::tool::MemoryPool<Data1, Data2>>();
mm->template fill<Data1>(100);
mm->template fill<Data2>(100);
ct->setMemoryManager(mm); // set the memory manager

// set the send strategies for each type

// the send strategy is a function that returns a list of destination
// ranks, we can use a lambda to compute the destination depending on the
// input data
gatherTask->strategy<Data1>([&](auto data) {
    hh::comm::rank_t rank = service.rank();
    size_t           nbProcesses = service.nbProcesses();
    return std::vector<hh::comm::rank_t>({(rank + 1) % nbProcesses});
});
// or use some of the generic strategies provided by the library (send to rank 1 and 2)
gatherTask->strategy<Data2>(hh::comm::strategy::SendTo(1, 2));

// hints (optional)

// the communicator will pre-comminit 10 receive requests for the Data1 and the
// rank 0 (source)
ct->template addHint<Data1>(hh::comm::hint::RecvCountFrom(0, 10));

// the communicator will make sure that there are always 2 pre-committed
// receive requests in the queue for the Data2 and the rank 1 (source).
ct->template addHint<Data2>(hh::comm::hint::continuousRecvFrom(1, 2));

// send threshold (optional): do not send more that 100 packages at a time.
ct->sendThreshold(100);

// use the communicator like any other tasks:
graph.edges(otherTask, ct);
// ...
```

## Run the test

```sh
mpirun -n 3 ./mpi_bridge
```
