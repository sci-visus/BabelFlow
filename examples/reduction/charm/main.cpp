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
#include "charm/CharmTaskGraph.h"

#include "reduction.decl.h"

/* readonly */ CProxy_Main mainProxy;

using namespace DataFlow;
using namespace charm;

inline void operator|(PUP::er &p, Reduction& graph) {p|(DataFlow::TaskGraph&)graph;}


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


class Main : public CBase_Main
{
public:

  uint32_t mSum;

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

    Reduction graph(leafs,valence);


    // Output the graph for testing
    ModuloMap task_map(1,graph.size());
    FILE* output=fopen("output.dot","w");
    graph.output_graph(1,&task_map,output);
    fclose(output);

    DataFlow::charm::Controller<Reduction,ReductionCallbacks> controller;
    
    DataFlow::charm::Controller<Reduction,ReductionCallbacks>::ProxyType proxy;
    proxy = controller.initialize(graph);
    
    uint32_t count=1;
    mSum = 0;
    for (TaskId i=graph.size()-graph.leafCount();i<graph.size();i++) {

      std::vector<char> buffer(sizeof(uint32_t));

      buffer.assign((char*)&count,((char *)&count) + sizeof(uint32_t));

      proxy[graph.gId(i)].addInput(TNULL,buffer);
      mSum += count++;
    }
  }

  Main(CkMigrateMessage *m) {}

  void done() {fprintf(stderr,"Correct total sum is %d\n",mSum);CkExit();}
};

#define CK_TEMPLATES_ONLY
#include "reduction.def.h"
#undef CK_TEMPLATES_ONLY


#include "reduction.def.h"
#include "charm_dataflow.def.h"

