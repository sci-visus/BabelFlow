/*
 * main.cpp
 *
 *  Created on: Dec 14, 2014
 *      Author: petruzza, bremer5
 */

#include <cstdio>
#include <unistd.h>
#include <cstdlib>

#include "mpi.h"
#include "ModuloMap.h"
#include "mpi/Controller.h"

//#include "HierarchicalReduction.h"
#include "Reduction.h"
#include "Broadcast.h"
#include "HierarchicalTaskGraph.h"

using namespace DataFlow;

int add_int(std::vector<DataBlock>& inputs, std::vector<DataBlock>& output, TaskId task)
{
  output[0].size = sizeof(uint32_t);
  output[0].buffer = (char*)(new uint32_t[1]);
  uint32_t* result = (uint32_t*)output[0].buffer;

  *result = 0;

  for (uint32_t i=0;i<inputs.size();i++) {
    *result += *((uint32_t *)inputs[i].buffer);
  }

  int r = rand() % 100000;
  usleep(r);

  return 1;
}

int report_sum(std::vector<DataBlock>& inputs, std::vector<DataBlock>& output, TaskId task)
{
  uint32_t result = 0;

  for (uint32_t i=0;i<inputs.size();i++)
    result += *((uint32_t *)inputs[i].buffer);

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

//  HierarchicalReduction graph(leafs,valence);
  
//  Task task;
//  std::vector<TaskId> incoming(leafs);
//  std::vector<std::vector<TaskId> > outgoing(1);
//  outgoing[0].resize(1);
//  outgoing[0][0] = 0;
//  task.id(1);
//  
//  for(TaskId i=0;i<leafs;i++){
//    incoming[i] = i+1;
//  }
//  task.incoming() = incoming;
//  task.outputs() = outgoing;
  
//  graph.computeHierarchicalGraph();

  //Reduction graph(leafs,valence);
  Broadcast graph(leafs, valence);
  ModuloMap task_map(1, graph.size());

  FILE* output = fopen("task_graph.dot","w");
//  graph.output_hierarchical_graph(output);
  graph.output_graph(1, &task_map, output);
  fclose(output);

  FILE* houtput = fopen("htask_graph.dot","w");
//  HierarchicalTaskGraph htg(graph.getAllTasks(), 2, 1);
  HierarchicalTaskGraph htg(graph.localGraph(0, &task_map), 2, 1);
  
  printf("------REDUCE-----\n");
  htg.reduce();
  printf("------REDUCE-----\n");
  htg.reduce();
  printf("------REDUCE-----\n");
  htg.reduce();
//  htg.reduceAll();
//  printf("------EXPAND-----\n");
//  htg.expand();
//  printf("------EXPAND-----\n");
//  htg.expand();
//  printf("------EXPAND-----\n");
//  htg.expand();
  htg.expandAll();
  htg.output_hierarchical_graph(houtput);
  fclose(houtput);
  
  // for all the new leaves check for external connections, expose all,
  // follow (startingo from the new leaves) and correct directed connections to the new leaves (not all the nodes in the tree)
  // mapping at the task level
  
  
//  
//  
//  ModuloMap task_map(mpi_width,graph.size());
//  
//  Controller master;
//  master.initialize(graph,&task_map);
//  master.registerCallback(1,add_int);
//  master.registerCallback(2,report_sum);
//
//  std::vector<TaskId> inputsIds = graph.getInputsIds();
//  std::map<TaskId,DataBlock> inputs;
//
//  uint32_t count=1;
//  uint32_t sum = 0;
//
//  for (uint32_t i=0;i<inputsIds.size();i++) {
//    DataBlock data;
//    data.size = sizeof(uint32_t);
//    data.buffer = (char*)(new uint32_t[1]);
//
//    *((uint32_t*)data.buffer) = count;
//    inputs[inputsIds[i]] = data;
//
//    sum += count++;
//  }
//
//  master.run(inputs);
//
//  fprintf(stderr,"The result was supposed to be %d\n",sum);
  MPI_Finalize();
  return 0;
}


