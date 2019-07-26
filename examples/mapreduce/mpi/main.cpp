/*
 * Copyright (c) statement needed.
 */
#include <iostream>
#include <sstream>
#include <vector>
#include <mpi.h>

#include <mpi/Controller.h>
#include <cmath>
#include <ModuloMap.h>
#include "Definitions.h"
#include "TaskGraph.h"
#include "Task.h"

#include "MapReduceGraph.h"
#include "MapReduceCallback.h"

using namespace BabelFlow;
using namespace BabelFlow::mpi;
using namespace std;

int main(int argc, char *argv[])
{
  auto err = MPI_Init(&argc, &argv);
  assert(err == MPI_SUCCESS);

  int mpi_size, mpi_rank;

  err = MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
  assert(err == MPI_SUCCESS);
  err = MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
  assert(err == MPI_SUCCESS);

  int n_workers = 8;
  size_t arr_len = 50;

  MapReduceGraph graph(n_workers);
  ModuloMap task_map(mpi_size, graph.size());
  Controller master;
  master.initialize(graph, &task_map, MPI_COMM_WORLD);
  master.registerCallback(SPLIT_LOAD, split_load);
  master.registerCallback(MAP_FUNC, map_func);
  master.registerCallback(RED_FUNC, reduce_func);
  master.registerCallback(PRINT_FUNC, print_func);

  if (mpi_rank == 0) {
    stringstream ss;
    ss << "graph.dot";
    FILE *output = fopen(ss.str().c_str(), "w");
    graph.output_graph(mpi_size, &task_map, output);
    fclose(output);
  }

  // Build input
  std::map<TaskId, Payload> inputs;
  int buffer_size = sizeof(int) + sizeof(size_t) + sizeof(int) * arr_len;
  char *buffer = new char[buffer_size];
  auto *header_ptr = reinterpret_cast<size_t *>(buffer + sizeof(int));
  int *data_ptr = reinterpret_cast<int *>(buffer + sizeof(int) + sizeof(size_t));

  // write n_workers arr_en to the buffer
  reinterpret_cast<int *>(buffer)[0] = n_workers;
  header_ptr[0] = arr_len;
  for (size_t i = 0; i < arr_len; ++i) {
    data_ptr[i] = static_cast<int>(i+1);
  }

  Payload data(buffer_size, buffer);
  inputs[0] = data;

  master.run(inputs);


  MPI_Finalize();
  return 0;
}
