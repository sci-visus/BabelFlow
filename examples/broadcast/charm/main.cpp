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

#include "BroadcastGraph.h"
#include "BroadcastCallbacks.h"
#include "ModuloMap.h"
#include "RelayTask.h"
#include "charm/CharmTask.h"
#include "charm/Controller.h"

#include "broadcast.decl.h"

/* readonly */ CProxy_Main mainProxy;

using namespace BabelFlow;
using namespace charm;

BabelFlow::Callback registered_callback(BabelFlow::CallbackId id)
{
  switch (id) {
    case 0:
      return BabelFlow::relay_message;
    case 1:
      return print_message;
    default:
      assert(false);
      break;
  }
  return NULL;
}

BabelFlow::TaskGraph* make_task_graph(BabelFlow::Payload buffer)
{
  return BabelFlow::charm::make_task_graph_template<BroadcastGraph>(buffer);
}

class Main : public CBase_Main
{
public:

  uint32_t mSum;

  //! The main constructor that constructs *and* starts the dataflow
  Main(CkArgMsg* m)
  {

    if (m->argc < 3) {
      fprintf(stderr,"Usage: %s <nr-of-leafs> <fanout> \n", m->argv[0]);
      return;
    }

    mainProxy = thisProxy;

    uint32_t leafs = atoi(m->argv[1]);
    uint32_t valence = atoi(m->argv[2]);

    BroadcastGraph graph(leafs,valence);

    // Output the graph for testing
    ModuloMap task_map(1,graph.size());
    FILE* output=fopen("broadcast_task_graph.dot","w");
    graph.output_graph(1,&task_map,output);
    fclose(output);

    Controller controller;

    BabelFlow::charm::Controller::ProxyType proxy;
    proxy = controller.initialize(graph.serialize(),graph.size());
    
    std::map<TaskId,Payload> inputs;
    
    int32_t size = arr_length*sizeof(int);
    char* buffer = (char*)(new int[size]);

    int *arr = (int*)buffer;
    // Initialize the array with ints
    for (int i=0; i<arr_length; i++) {
      arr[i] = i;
    }
    // Collect the sum in the first element
    for (int i=1; i<arr_length; i++) 
      arr[0] += arr[i];

    std::vector<char> vbuffer(buffer, buffer+size);

    proxy[graph.gId(0)].addInput(TNULL,vbuffer);

    printf("Sum: %d\n", arr[0]);
    
  }

  Main(CkMigrateMessage *m) {}

  void done() {fprintf(stderr,"Done\n");CkExit();}
};


#include "broadcast.def.h"

