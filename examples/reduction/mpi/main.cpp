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

#include "mpi.h"

#include "../Reduction.h"
#include "ModuloMap.h"
#include "mpi/Controller.h"

using namespace DataFlow;
using namespace DataFlow::mpi;

int add_int(std::vector<Payload>& inputs, std::vector<Payload>& output, TaskId task)
{
  int32_t size = sizeof(int32_t);
  char* buffer = (char*)(new uint32_t[1]);

  uint32_t* result = (uint32_t*)buffer;

  *result = 0;
  for (uint32_t i=0;i<inputs.size();i++) {
    *result += *((uint32_t *)inputs[i].buffer());
  }

  output[0].initialize(size,buffer);

  int r = rand() % 100000;
  usleep(r);

  return 1;
}

int report_sum(std::vector<Payload>& inputs, std::vector<Payload>& output, TaskId task)
{
  uint32_t result = 0;

  for (uint32_t i=0;i<inputs.size();i++)
    result += *((uint32_t *)inputs[i].buffer());

  fprintf(stderr,"Total sum is %d\n",result);

  int r = rand() % 100000;
  usleep(r);

  return 1;
}



int main(int argc, char* argv[])
{
  if (argc < 3) {
    fprintf(stderr,"Usage: %s <nr-of-leafs> <fan-in> \n", argv[0]);
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
    fprintf(stderr,"Using %d ranks\n",mpi_width);

  uint32_t leafs = atoi(argv[1]);
  uint32_t valence = atoi(argv[2]);

  Reduction graph(leafs,valence);
  ModuloMap task_map(mpi_width,graph.size());

  Controller master;

  FILE* output = fopen("task_graph.dot","w");
  graph.output_graph(mpi_width,&task_map,output);
  fclose(output);

  master.initialize(graph,&task_map);
  master.registerCallback(1,add_int);
  master.registerCallback(2,report_sum);

  std::map<TaskId,Payload> inputs;


  uint32_t count=1;
  uint32_t sum = 0;
  for (TaskId i=graph.size()-graph.leafCount();i<graph.size();i++) {

    int32_t size = sizeof(uint32_t);
    char* buffer = (char*)(new uint32_t[1]);
    *((uint32_t*)buffer) = count;


    Payload data(size,buffer);

    inputs[i] = data;

    sum += count++;
  }

  master.run(inputs);

  if (rank == 0)
    fprintf(stderr,"The result was supposed to be %d\n",sum);

  MPI_Finalize();
  return 0;
}


