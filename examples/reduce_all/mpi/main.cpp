/*
 * Copyright (c) 2017 University of Utah 
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <cstdio>
#include <unistd.h>
#include <cstdlib>
#include <sstream>
#include <iostream>
#include <mpi.h>

#include "ReduceAllGraph.h"
#include "ReduceAllCallbacks.h"
#include "ModuloMap.h"
#include "RelayTask.h"
#include "mpi/Controller.h"


using namespace BabelFlow;
using namespace BabelFlow::mpi;

#define OUTPUT_GRAPH 1

int main(int argc, char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <nr-of-leafs> <fan-in> \n", argv[0]);
    return 0;
  }

  srand(100);

  MPI_Init(&argc, &argv);

  // Find out how many controllers we need
  int mpi_width;
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_width);

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == 0)
    fprintf(stderr, "Using %d ranks\n", mpi_width);

  uint32_t leafs = atoi(argv[1]);
  uint32_t valence = atoi(argv[2]);

  clock_t start, finish;

  start = clock();
  ReduceAllGraph graph(leafs, valence);
  TaskGraph::registerCallback( graph.type(), ReduceAllGraph::LOCAL_COMPUTE_TASK, add_int);
  TaskGraph::registerCallback( graph.type(), ReduceAllGraph::REDUCTION_TASK, add_int);
  TaskGraph::registerCallback( graph.type(), ReduceAllGraph::COMPLETE_REDUCTION_TASK, add_int_broadcast);
  TaskGraph::registerCallback( graph.type(), ReduceAllGraph::RESULT_REPORT_TASK, print_result);
  TaskGraph::registerCallback( graph.type(), ReduceAllGraph::RESULT_BROADCAST_TASK, relay_message );

  if (rank == 0)
    printf("Graph size %d reduction size %d leaf tasks %d\n", graph.size(), graph.reductionSize(), graph.leafCount());

  ModuloMap task_map(mpi_width, graph.size());

  Controller master;

#if OUTPUT_GRAPH
  if (rank == 0) 
  {
    std::stringstream graph_name;
    graph_name << "task_graph_" << rank << ".dot";
    graph.outputGraph( mpi_width, &task_map, graph_name.str()) ;
  }
#endif

  master.initialize(graph,&task_map);

  std::map<TaskId,Payload> inputs;

  uint32_t count=1;
  uint32_t sum = 0;
  for (TaskId i = graph.reductionSize() - graph.leafCount(); i < graph.reductionSize(); i++) {

    int32_t size = sizeof(uint32_t);
    char* buffer = (char*)(new uint32_t[1]);
    *((uint32_t*)buffer) = count;

    Payload data(size,buffer);

    inputs[i] = data;

    sum += count++;
  }

  master.run(inputs);

  finish = clock();

  if (rank == 0)
    fprintf(stderr,"The result was supposed to be %d\n",sum);

  double max_proc_time;
  double proc_time = (double(finish) - double(start)) / CLOCKS_PER_SEC;
  MPI_Reduce(&proc_time, &max_proc_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

  if (rank == 0) {
    std::cout << "max proc time without finalize= " << max_proc_time << std::endl;
  }
  start = clock();

  MPI_Finalize();

  finish = clock();
  max_proc_time += (double(finish) - double(start)) / CLOCKS_PER_SEC;
  if (rank == 0) {
    std::cout << "max proc time = " << max_proc_time << std::endl;
  }

  return 0;
}


