/*
 * main.cpp
 *
 *  Created on: Dec 14, 2014
 *      Author: bremer5
 */

#include <cstdio>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <cstdlib>

#include "mpi.h"

#include "Broadcast.h"
#include "ModuloMap.h"
#include "Controller.h"

uint32_t gCount = 0;

int print_message(std::vector<DataBlock>& inputs, std::vector<DataBlock>& output, TaskId task)
{
  char* str = (char*)inputs[0].buffer;

  int r = rand() % 3000000;
  usleep(r);
  printf("Got message \"%s\"  %d\n",str,task);

  return 1;
}


int main(int argc, char* argv[])
{
  if (argc < 4) {
    fprintf(stderr,"Usage: %s <nr-of-leafs> <fanout> <message>\n", argv[0]);
    return 0;
  }

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

  Broadcast graph(leafs,valence);
  std::cout << "graph size " << graph.size() << "\n";
  ModuloMap task_map(mpi_width,graph.size());

  Controller master;

  FILE* output = fopen("task_graph.dot","w");
  graph.output_graph(mpi_width,&task_map,output);
  fclose(output);

  master.initialize(graph,&task_map);
  master.registerCallback(1,print_message);

  
  std::map<TaskId,DataBlock> inputs;

  if (rank ==0 ) {
    DataBlock data;
    data.size = strlen(argv[3]) + 1;
    data.buffer = new char[data.size];
    memcpy(data.buffer,argv[3],data.size);

    inputs[0] = data;
  }

  master.run(inputs);
  

  fprintf(stderr,"Done\n");
  MPI_Finalize();
  return 0;
}


