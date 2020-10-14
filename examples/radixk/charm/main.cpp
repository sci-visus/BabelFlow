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
#include "charm/CharmTask.h"
#include "charm/Controller.h"

#include "radixk.decl.h"


using namespace BabelFlow;
using namespace charm;


//-----------------------------------------------------------------------------

class BabelCompRadixK_Charm : public comp_utils::BabelCompRadixK
{
public:
  BabelCompRadixK_Charm(int32_t rank_id,
                        int32_t n_blocks,
                        int32_t fanin,
                        const std::vector<uint32_t>& radix_v)
  : comp_utils::BabelCompRadixK(rank_id, n_blocks, fanin, radix_v) {}

  virtual void Initialize(std::map<BabelFlow::TaskId, BabelFlow::Payload>& inputs) override
  {
    BabelCompRadixK::Initialize(inputs);

    m_proxy = m_controller.initialize(m_radGatherGraph.serialize(), m_radGatherGraph.size());
  }
  
  virtual void Execute(std::map<BabelFlow::TaskId, BabelFlow::Payload>& inputs) override
  {
    for( int32_t i = 0; i < m_nRanks; ++i )
    {
      BabelFlow::Payload& payl = inputs[i];
      std::vector<char> buffer(payl.size());
      buffer.assign(payl.buffer(), payl.buffer() + payl.size());

      // convert i to global_id ?
      m_proxy[i].addInput(BabelFlow::TNULL, buffer);
    }
  }

protected:
  BabelFlow::charm::Controller m_controller;  
  BabelFlow::charm::Controller::ProxyType m_proxy;
};



//-----------------------------------------------------------------------------

/* readonly */ CProxy_Main mainProxy;


BabelFlow::TaskGraph* make_task_graph(BabelFlow::Payload buffer)
{
  return BabelFlow::charm::make_task_graph_template<BabelFlow::ComposableTaskGraph>(buffer);
}


class Main : public CBase_Main
{
public:

  // uint32_t mSum;

  //! The main constructor that constructs *and* starts the dataflow
  Main(CkArgMsg* m)
  {

    if (m->argc < 5)
    {
      std::cout << "Usage: " << m->argv[0] << " <nr of processes> <fan-in> <img_prefix> <radices>" << std::endl;
      exit(-1);
    }
    
    int32_t num_procs = atoi(m->argv[1]);
    int32_t fanin = atoi(m->argv[2]);
    std::string img_prefix(m->argv[3]);   // e.g.,  "raw_img_<rank>.bin"
    std::vector<uint32_t> radix_v;

    for( uint32_t i = 4; i < m->argc; ++i )
      radix_v.push_back(atoi(m->argv[i]));

    std::cout << "Starting program with " << num_procs << " procs, fanin " << fanin << std::endl;

    mainProxy = thisProxy;

    std::map<BabelFlow::TaskId, BabelFlow::Payload> inputs;
    // read all input images
    for( uint32_t i = 0; i < num_procs; ++i )
    {
      std::stringstream img_name;
      img_name << img_prefix << i << ".bin";
      std::ifstream ifs(img_name.str());
      ifs.seekg (0, ifs.end);
      int length = ifs.tellg();
      ifs.seekg (0, ifs.beg);
      char* buff = new char[length];
      ifs.read( buff, length );
      ifs.close();

      BabelFlow::Payload payl(length, buff);
      inputs[i] = payl;
    }

    BabelCompRadixK_Charm radixk_charm_wrapper(0, num_procs, fanin, radix_v);
    radixk_charm_wrapper.Initialize(inputs);
    radixk_charm_wrapper.Execute(inputs);

    // uint32_t leafs = atoi(m->argv[1]);
    // uint32_t valence = atoi(m->argv[2]);

    // ReductionGraph graph(leafs,valence);
    // graph.registerCallback( ReductionGraph::RED_TASK_CB, add_int );
    // graph.registerCallback( ReductionGraph::ROOT_TASK_CB, report_sum );

    // Output the graph for testing
    // ModuloMap task_map(1,graph.size());
    // graph.outputGraph( 1, &task_map, "output.dot" );
    
    // BabelFlow::charm::Controller controller;
    
    // BabelFlow::charm::Controller::ProxyType proxy;
    // proxy = controller.initialize(graph.serialize(),graph.size());
    
    // uint32_t count=1;
    // mSum = 0;
    // for (TaskId i=graph.size()-graph.leafCount();i<graph.size();i++) {

    //   std::vector<char> buffer(sizeof(uint32_t));

    //   buffer.assign((char*)&count,((char *)&count) + sizeof(uint32_t));

    //   proxy[graph.gId(i)].addInput(TNULL,buffer);
    //   mSum += count++;
    // }
  }

  Main(CkMigrateMessage *m) {}

  void done() 
  {
    std::cout << "Finished executing..." << std::endl;
    CkExit();
  }
};


#include "radixk.def.h"

