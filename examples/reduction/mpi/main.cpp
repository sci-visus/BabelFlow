/*
 * main.cpp
 *
 *  Created on: Dec 14, 2014
 *      Author: bremer5
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

  fprintf(stderr,"After MPI_Init\n");
  // FInd out how many controllers we need
  int mpi_width;
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_width);

  fprintf(stderr,"Using %d processes\n",mpi_width);

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == 0)
    fprintf(stderr,"My rank %d\n",rank);


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

  fprintf(stderr,"The result was supposed to be %d\n",sum);
  MPI_Finalize();
  return 0;
}


