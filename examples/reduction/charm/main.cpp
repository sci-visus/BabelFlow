/*
 * main.cpp
 *
 *  Created on: Dec 14, 2014
 *      Author: bremer5
 */

#include <cstdio>
#include <unistd.h>
#include <cstdlib>
#include <sstream>

#include "../Reduction.h"
#include "ModuloMap.h"
#include "RelayTask.h"
#include "charm/CharmTask.h"
#include "charm/Controller.h"


#include "reduction.decl.h"

/* readonly */ CProxy_Main mainProxy;

using namespace DataFlow;

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

  mainProxy.done();
  return 1;
}


class ReductionCallbacks
{
public:
  static Callback callback(CallbackId id)
  {
    switch (id) {
      case 0:
        return DataFlow::relay_message;
      case 1:
        return add_int;
      case 2:
        return report_sum;
      default:
        assert(false);
        break;
    }
    return NULL;
  }
};

template<typename dummy1>
class TestTask : public CBase_TestTask<dummy1>
{
public:

  TestTask() {
    fprintf(stderr,"Task %d\n",this->thisIndex);
    if (this->thisIndex == 9) {
      usleep(1000);
      mainProxy.done();
    }
  }

  TestTask(CkMigrateMessage *m) {}

};

class TestTask2 : public CBase_TestTask2
{
public:

  TestTask2() {
    fprintf(stderr,"Task %d\n",this->thisIndex);
    if (this->thisIndex == 9) {
      usleep(1000);
      mainProxy.done();
    }
  }

  TestTask2(CkMigrateMessage *m) {}

};

class Main : public CBase_Main
{
public:

  //! The main constructor that constructs *and* starts the dataflow
  Main(CkArgMsg* m)
  {

    if (m->argc < 3) {
      fprintf(stderr,"Usage: %s <nr-of-leafs> <fan-in> \n", m->argv[0]);
    }
    else
      fprintf(stderr,"Starting program with %d leafs and fanin %d\n",atoi(m->argv[1]),atoi(m->argv[2]));

    mainProxy = thisProxy;

    uint32_t leafs = atoi(m->argv[1]);
    uint32_t valence = atoi(m->argv[2]);

    // Pass the first two command line arguments to the graph
    std::stringstream config;
    config << m->argv[1] << " " << m->argv[2];

    Reduction graph(config.str());

    DataFlow::charm::Controller<Reduction,ReductionCallbacks> controller;

    /*
    // This should be the correct true code
    DataFlow::charm::Controller<Reduction,ReductionCallbacks>::ProxyType proxy;
    proxy = controller.initialize(config.str());
     */

    // Various Test code

    // This will seg fault
    //CProxy_TestTask<int> proxy = CProxy_TestTask<int>::ckNew(10);

    // While this succeeds
    CProxy_TestTask2 proxy = CProxy_TestTask2::ckNew(10);

    return;


    /*// Original code
    uint32_t count=1;
    uint32_t sum = 0;
    for (TaskId i=graph.size()-graph.leafCount();i<graph.size();i++) {

      std::vector<char> buffer(sizeof(uint32_t));

      buffer.assign(&count,&count + sizeof(uint32_t));

      proxy[graph.gId(i)].addInput(TNULL,buffer);
      sum += count++;
    }
     */
  }

  Main(CkMigrateMessage *m) {}

  void done() {CkExit();}
};

#define CK_TEMPLATES_ONLY
#include "reduction.def.h"
#undef CK_TEMPLATES_ONLY


#include "reduction.def.h"
#include "charm_dataflow.def.h"

/*
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
*/

