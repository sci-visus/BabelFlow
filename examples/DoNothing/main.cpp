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
#include <ModTaskMap.hpp>
#include "DoNothingTaskGraph.h"

using namespace std;
using namespace BabelFlow;
using namespace BabelFlow::mpi;

int print_data(vector<Payload> &inputs, vector<Payload> &outputs, TaskId task)
{
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

int preprocess_data(vector<Payload> &inputs, vector<Payload> &outputs, TaskId task)
{
  int32_t size = inputs[0].size();
  int *data = new int[size/sizeof(int)];
  memcpy(data, inputs[0].buffer(), size);
  for (int i = 0; i < size/sizeof(int); ++i){
    data[i] = data[i] + i;
  }
  Payload msg(size, (char *)data);
  outputs.resize(1);
  outputs[0] = msg;
  delete inputs[0].buffer();
  return 0;
}

int main(int argc, char **argv)
{
  MPI_Init(nullptr, nullptr);


  int mpi_size, mpi_rank;
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
  vector<int> data(8, mpi_rank);
  DoNothingTaskGraph graph(mpi_size);
  ModuloMap taskMap(mpi_size, graph.size());

  PreProcessInputTaskGraph modGraph(mpi_size, &graph, &taskMap);
  ModTaskMap modMap(&taskMap);
  modMap.update(modGraph);

  if (mpi_rank == 0) {
    FILE *meow = fopen("meow.dot", "w");
    modGraph.output_graph(mpi_size, &modMap, meow);
    fclose(meow);
  }

  Controller master;
  master.initialize(modGraph, &modMap, MPI_COMM_WORLD);
  master.registerCallback(1, print_data);
  master.registerCallback(modGraph.newCallBackId, preprocess_data);

  Payload payload;
  char *buffer = new char[data.size() * sizeof(int)];
  memcpy(buffer, data.data(), data.size() * sizeof(int));
  payload.initialize(data.size() * sizeof(int), buffer);

  std::map<TaskId, Payload> inputs;
  inputs[modGraph.new_tids[mpi_rank]] = payload;

  master.run(inputs);


  MPI_Finalize();
  return 0;
}
