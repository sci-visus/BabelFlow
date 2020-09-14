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

#include "legion.h"

#include "ReductionGraph.h"
#include "ReductionCallbacks.h"
#include "ModuloMap.h"
#include "legion/Controller.h"

#ifdef USE_GASNET
#include <gasnet.h>
#endif

using namespace LegionRuntime::HighLevel;
using namespace LegionRuntime::Accessor;

using namespace BabelFlow;
using namespace BabelFlow::legion;

// Task for data loading
LogicalRegion load_task(const Legion::Task *task,
                                    const std::vector<PhysicalRegion> &regions,
                                    Context ctx, HighLevelRuntime *runtime){
  //printf("entering arglen %d\n", task->local_arglen);
  Domain dom_incall = runtime->get_index_space_domain(ctx, task->regions[0].region.get_index_space());
  Rect<1> elem_rect_in = dom_incall.get_rect<1>();

  //printf("from task %lld volume %d!\n", task->index_point.point_data[0], elem_rect_in.volume());
  assert(task->local_arglen == sizeof(DomainSelection));

  DomainSelection box = *(DomainSelection*)task->local_args;//args;
  /*if(elem_rect_in.volume() == 0){
    fprintf(stdout,"invalid rect %llu %llu\n", elem_rect_in.lo,elem_rect_in.hi);
    fprintf(stdout,"invalid data %d %d %d - %d %d %d \n", box.low[0],box.low[1],box.low[2],box.high[0],box.high[1],box.high[2]);
    assert(false);
    }*/

  // FunctionType threshold = (FunctionType)(-1)*FLT_MAX;
  // char* dataset = NULL;
  // //fprintf(stderr, "input initialization started\n");

  // for (int i = 1; i < this_argc; i++){
  //   if (!strcmp(this_argv[i],"-t"))
  //     threshold = atof(this_argv[++i]);
  //   if (!strcmp(this_argv[i],"-f"))
  //     dataset = this_argv[++i];
  // }

  fprintf(stdout,"Load data %d %d %d - %d %d %d \n", box.low[0],box.low[1],box.low[2],box.high[0],box.high[1],box.high[2]);

  // char* data_block = read_block(dataset,box.low,box.high);

// #ifdef READ_DUMP_TEST
//   char filename[128];
//    sprintf(filename,"dump_%d_%d_%d-%d_%d_%d.raw", box.low[0],box.low[1],box.low[2], box.high[0],box.high[1],box.high[2]);
//    std::ofstream outfile (filename,std::ofstream::binary);

//    int32_t datasize = (box.high[0]-box.low[0]+1)*(box.high[1]-box.low[1]+1)*(box.high[2]-box.low[2]+1)*sizeof(FunctionType);
//    outfile.write(pay.buffer(),datasize);

//    outfile.close();
// #endif

  //Payload pay = make_local_block((FunctionType*)(data_block), box.low, box.high, threshold);

  Payload pay(sizeof(long long), (char*)&(task->index_point.point_data[0]));

  // char filename[128];
  // sprintf(filename, "outblock_%d.raw", task->index_point.point_data[0]);
  // std::ofstream outfile (filename,std::ofstream::binary);
  // outfile.write(pay.buffer(), pay.size());
  // outfile.close();

  // printf("volume in %d size %d\n", task->index_point.point_data[0], pay.size());
  assert(elem_rect_in.volume() >= pay.size()/BYTES_PER_POINT);

  Controller::bufferToRegion(pay.buffer(), pay.size()/BYTES_PER_POINT, elem_rect_in, regions[0]);//&ctx, runtime);
  //runtime->unmap_region(ctx,regions[0]);

  // char offname[128];
  //   sprintf(offname,"read_%d.raw", RegionsIndexType(elem_rect_in.lo));
  //   std::ofstream outresfile(offname,std::ofstream::binary);

  //    outresfile.write(reinterpret_cast<char*>(pay.buffer()),pay.size());

  //    outresfile.close();

  //delete [] pay.buffer();

  return task->regions[0].region;
}

int main(int argc, char* argv[])
{
  if (argc < 3) {
    fprintf(stderr,"Usage: %s <nr-of-leafs> <fan-in> \n", argv[0]);
    return 0;
  }

  srand(100);

  uint32_t leafs = atoi(argv[1]);
  uint32_t valence = atoi(argv[2]);

  ReductionGraph graph(leafs,valence);
  graph.registerCallback( ReductionGraph::RED_TASK_CB, add_int );
  graph.registerCallback( ReductionGraph::ROOT_TASK_CB, report_sum );
  ModuloMap task_map(leafs ,graph.size());

  Controller* master = Controller::getInstance();

  graph.outputGraph( leafs, &task_map, "task_graph.dot" );

  master->initialize(graph,&task_map, argc, argv);

  HighLevelRuntime::register_legion_task<LogicalRegion, load_task>(LOAD_TASK_ID,
        Processor::LOC_PROC, true/*single*/, false/*index*/,
        AUTO_GENERATE_ID, TaskConfigOptions(true/*leaf*/), "load_task");

  std::map<TaskId,Payload> inputs;


  FunctionType count=1;
  FunctionType sum = 0;

  //std::vector<DomainSelection> blocks(graph.leafCount());

  for (TaskId i=graph.size()-graph.leafCount();i<graph.size();i++) {

/*
    int32_t size = sizeof(uint32_t);
    char* buffer = (char*)(new uint32_t[1]);
    *((uint32_t*)buffer) = count;


    Payload data(size,buffer);

    inputs[i] = data;
*/

    //*((FunctionType)blocks[i].data) = count;
    DomainSelection b;
    b.low[0]=static_cast<GlobalIndexType>(count);
    b.high[0]=static_cast<GlobalIndexType>(count+1);

    size_t size = sizeof(DomainSelection);
    char* input = (char*)malloc(size);

    memcpy(input, &b, size);
    Payload pay(size, input);

    inputs[i] = pay;

    sum += count++;
  }
  
  master->run(inputs);

  return 0;
}


