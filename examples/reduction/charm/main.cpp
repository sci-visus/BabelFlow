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

#include "../Reduction.h"
#include "ModuloMap.h"
#include "RelayTask.h"
#include "charm/CharmTask.h"
#include "charm/Controller.h"
//#include "charm/CharmTaskGraph.h"

#include "reduction.decl.h"

/* readonly */ CProxy_Main mainProxy;

using namespace DataFlow;
using namespace charm;


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


DataFlow::Callback registered_callback(DataFlow::CallbackId id)
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

DataFlow::TaskGraph* make_task_graph(DataFlow::Payload buffer)
{
  return DataFlow::charm::make_task_graph_template<Reduction>(buffer);
}


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

    DataFlow::charm::Controller controller;
    
    DataFlow::charm::Controller::ProxyType proxy;
    proxy = controller.initialize(graph.serialize(),graph.size());
    
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


#include "reduction.def.h"

