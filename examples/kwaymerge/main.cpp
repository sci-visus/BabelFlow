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
#include "KWayTaskMap.h"
#include "Payload.h"
#include "mpi/Controller.h"


#include "KWayMerge.h"

using namespace DataFlow;
using namespace DataFlow::mpi;


int add_int(std::vector<Payload>& inputs, std::vector<Payload>& output, TaskId task)
{
  output[0] = Payload(sizeof(uint32_t),(char*)(new uint32_t[1]));

  //output[0].size = sizeof(uint32_t);
  //output[0].buffer = (char*)(new uint32_t[1]);
  uint32_t* result = (uint32_t*)output[0].buffer();

  *result = 0;

  for (uint32_t i=0;i<inputs.size();i++) {
    *result += *((uint32_t *)inputs[i].buffer());
  }

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


int main(int argc, char* argv[]){
//  if (argc < 3) {
//    fprintf(stderr,"Usage: %s <nr-of-leafs> <fan-in> \n", argv[0]);
//    return 0;
//  }
//
//  srand(100);
//
//  MPI_Init(&argc, &argv);
//
//  fprintf(stderr,"After MPI_Init\n");
//  // FInd out how many controllers we need
//  int mpi_width;
//  MPI_Comm_size(MPI_COMM_WORLD, &mpi_width);
//
//  fprintf(stderr,"Using %d processes\n",mpi_width);
//
//  int rank;
//  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
//
//  if (rank == 0)
//    fprintf(stderr,"My rank %d\n",rank);

//  uint32_t leafs = atoi(argv[1]);
//  uint32_t valence = atoi(argv[2]);

  uint32_t leafs[3] = {2,2,2};
  KWayMerge graph(leafs,2);
  KWayTaskMap tmap(1, &graph);
  
  
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


  FILE* output = fopen("task_graph.dot","w");
  graph.output_graph(1, &tmap, output);
  fclose(output);
  
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
//  MPI_Finalize();
  return 0;
}


