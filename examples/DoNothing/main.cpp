//
// Created by Jixian Li on 2019-07-27.
//

#include <mpi.h>
#include <vector>
#include <map>

#include <mpi/Controller.h>
#include <ModuloMap.h>
#include <PreProcessInputTaskGraph.hpp>
#include <sstream>
#include "DoNothingTaskGraph.h"

using namespace std;
using namespace BabelFlow;
using namespace BabelFlow::mpi;

int print_data(vector<Payload> &inputs, vector<Payload> &outputs, TaskId task) {
  int32_t size = inputs[0].size();
  int *data = new int[size / sizeof(int)];
  memcpy(data, inputs[0].buffer(), size);

  stringstream ss;
  for (int i = 0; i < size / sizeof(int); ++i) {
    ss << data[i] << " ";
  }
  printf("%s\n", ss.str().c_str());

  delete[] inputs[0].buffer();
  delete[] data;
  return 0;
}

int main(int argc, char **argv) {
  MPI_Init(nullptr, nullptr);


  int mpi_size, mpi_rank;
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
  vector<int> data(8, mpi_rank);
  DoNothingTaskGraph graph(mpi_size);
  ModuloMap taskMap(mpi_size, graph.size());

  PreProcessInputTaskGraph<DoNothingTaskGraph> meowTaskGraph(mpi_size, &graph, &taskMap);
  std::map<TaskId, uint64_t> m = meowTaskGraph.new_gids;
  if (mpi_rank == 0) {
    for (auto iter = m.begin(); iter != m.end(); ++iter) {
      printf("%u %llu\n", iter->first, iter->second);
    }
    printf("new callback id %d\n",meowTaskGraph.newCallBackId);
  }
  // TODO we need to update TaskMap here. But TaskMap is statically defined


  Controller master;
  master.initialize(graph, &taskMap, MPI_COMM_WORLD);
  master.registerCallback(1, print_data);
  Payload payload;
  char *buffer = new char[data.size() * sizeof(int)];
  memcpy(buffer, data.data(), data.size() * sizeof(int));
  payload.initialize(data.size() * sizeof(int), buffer);

  std::map<TaskId, Payload> inputs;
  inputs[mpi_rank] = payload;

  master.run(inputs);


  MPI_Finalize();
  return 0;
}