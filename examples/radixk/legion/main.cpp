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
#include <fstream>
#include <iostream>

#include "CompositingUtils.h"
#include "ModuloMap.h"
#include "RelayTask.h"
#include "legion/Controller.h"

#ifdef USE_GASNET
#include <gasnet.h>
#endif

using namespace LegionRuntime::HighLevel;
using namespace LegionRuntime::Accessor;

using namespace BabelFlow;
using namespace BabelFlow::legion;

std::string img_prefix;

void register_callbacks()
{
  comp_utils::register_callbacks();
  comp_utils::BabelGraphWrapper::sIMAGE_NAME = "radixk_img";
}


// Task for data loading
LogicalRegion load_task(const Legion::Task *task,
                                    const std::vector<PhysicalRegion> &regions,
                                    Context ctx, HighLevelRuntime *runtime){
  //printf("entering arglen %d\n", task->local_arglen);
  Domain dom_incall = runtime->get_index_space_domain(ctx, task->regions[0].region.get_index_space());
  Rect<1> elem_rect_in = dom_incall.get_rect<1>();

  //printf("from task %lld volume %d!\n", task->index_point.point_data[0], elem_rect_in.volume());
  assert(task->local_arglen == sizeof(DomainSelection));

  DomainSelection box = *(DomainSelection*)task->local_args;

//  fprintf(stdout,"Load data %d %d %d - %d %d %d \n", box.low[0],box.low[1],box.low[2],box.high[0],box.high[1],box.high[2]);

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

  FunctionType* buffer = new FunctionType[1];
  *(buffer) = (FunctionType)box.low[0];

  std::stringstream img_name;
  img_name << img_prefix << task->index_point.point_data[0] << ".bin";
  std::ifstream ifs(img_name.str());
  if( !ifs.good() )
  {
    std::cout << "Couldn't open " << img_name.str() << std::endl;
    exit(-1);
  }
  ifs.seekg (0, ifs.end);
  int length = ifs.tellg();
  ifs.seekg (0, ifs.beg);
  char* buff = new char[length];
  ifs.read( buff, length );
  ifs.close();

  BabelFlow::Payload pay(length, buff);

  //printf("volume in %d size %d\n", task->index_point.point_data[0], pay.size());
  assert(elem_rect_in.volume() >= pay.size()/BYTES_PER_POINT);

  Controller::bufferToRegion(pay.buffer(), pay.size()/BYTES_PER_POINT, elem_rect_in, regions[0]);//&ctx, runtime);

  delete [] pay.buffer();
  
  return task->regions[0].region;
}

class BabelCompRadixK_Legion : public comp_utils::BabelCompRadixK
{
public:
  BabelCompRadixK_Legion(int32_t rank_id,
                      int32_t n_blocks,
                      int32_t fanin,
                      const std::vector<uint32_t>& radix_v)
  : comp_utils::BabelCompRadixK(rank_id, n_blocks, fanin, radix_v) {}

  virtual void Initialize(std::map<BabelFlow::TaskId, BabelFlow::Payload>& inputs, int argc,  char** argv) 
  {
    m_controller = BabelFlow::legion::Controller::getInstance();
    BabelCompRadixK::Initialize(inputs);

    m_controller->initialize( m_radGatherGraph, &m_radGatherTaskMap, argc, argv );
  }
  
  virtual void Execute(std::map<BabelFlow::TaskId, BabelFlow::Payload>& inputs) override
  {
    m_controller->run(inputs);
  }

protected:
  BabelFlow::legion::Controller* m_controller;
  BabelFlow::ControllerMap m_contMap;
};

int main(int argc, char* argv[])
{
    if (argc < 5)
    {
      std::cout << "Usage: " << argv[0] << " <nr of processes> <fan-in> <img_prefix> <radices>" << std::endl;
      exit(-1);
    }
    
    int32_t num_procs = atoi(argv[1]);
    int32_t fanin = atoi(argv[2]);
    img_prefix = argv[3];   // e.g.,  "raw_img_<rank>.bin"
    std::vector<uint32_t> radix_v;

    for( uint32_t i = 4; i < 6; ++i )
      radix_v.push_back(atoi(argv[i]));

    std::cout << "---" << std::endl;
    std::cout << "Starting program with " << num_procs << " procs, fanin: " << fanin << std::endl;
    std::cout << "Image prefix:" << img_prefix << std::endl;
    std::cout << "Radices:" << std::endl;
    for( uint32_t i = 0; i < radix_v.size(); ++i )
      std::cout << radix_v[i] << " " << std::endl;
    std::cout << "---" << std::endl;

    // BabelCompRadixK_Charm radixk_charm_wrapper(0, num_procs, fanin, radix_v);
    // radixk_charm_wrapper.Initialize(inputs);
    // radixk_charm_wrapper.Execute(inputs);

    // Function to fill up the input data
    HighLevelRuntime::register_legion_task<LogicalRegion, load_task>(LOAD_TASK_ID,
          Processor::LOC_PROC, true/*single*/, false/*index*/,
          AUTO_GENERATE_ID, TaskConfigOptions(true/*leaf*/), "load_task");

    std::map<TaskId,Payload> inputs;

    FunctionType count=0;

    comp_utils::BabelCompRadixK wgraph(0, num_procs, fanin, radix_v);
    
    //std::vector<DomainSelection> blocks(graph.leafCount());

    for (int i=0;i<num_procs;i++) {

      // Here we are just setting up the domain decomposition,
      // the input data are filled up by the load_task function
      DomainSelection b;
      b.low[0]=static_cast<GlobalIndexType>(count);

      size_t size = sizeof(DomainSelection);
      char* input = (char*)malloc(size);

      std::stringstream img_name;
      img_name << img_prefix << i << ".bin";
      std::ifstream ifs(img_name.str());
      if( !ifs.good() )
      {
        std::cout << "Couldn't open " << img_name.str() << std::endl;
        exit(-1);
      }
      ifs.seekg (0, ifs.end);
      int length = ifs.tellg();

      count += length/sizeof(FunctionType);

      b.high[0]=static_cast<GlobalIndexType>(count);

      memcpy(input, &b, size);
      Payload pay(size, input);

      inputs[i] = pay;

    }

    comp_utils::register_callbacks();
    comp_utils::BabelGraphWrapper::sIMAGE_NAME = "radixk_img";

    BabelCompRadixK_Legion radixk_legion_wrapper(0, num_procs, fanin, radix_v);
    radixk_legion_wrapper.Initialize(inputs, argc, argv);
    radixk_legion_wrapper.Execute(inputs);

    printf("Done\n");
 
}

