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

#ifndef CHARM_CHARMTASK_H_
#define CHARM_CHARMTASK_H_

#include "charm++.h"
#include "pup_stl.h"

#include "../Definitions.h"
#include "../TaskGraph.h"
#include "../Payload.h"

namespace BabelFlow {
namespace charm {

class CharmPayload : public Payload
{
public:
  CharmPayload() {}
  CharmPayload(Payload& payl) : Payload(payl.size(), payl.buffer()) {}

  void pup(PUP::er &p);
};

class CharmTaskId : public TaskId
{
public:
  CharmTaskId() {}
  CharmTaskId(const TaskId& tid) : TaskId(tid) {}

  void pup(PUP::er &p);
};

}
}


#include "charm_babelflow.decl.h"


//! Global function to register callbacks
//! Must be implemented by the calling application code
extern void register_callbacks();

//! Global function to create a task graph
//! Must be implemented by the calling application code
extern BabelFlow::TaskGraph* make_task_graph(BabelFlow::Payload buffer);


namespace BabelFlow {
namespace charm {


//! Make defining the global task graph function easy
template<class TaskGraphClass>
TaskGraph *make_task_graph_template(Payload payl)
{
  TaskGraph* graph =  new TaskGraphClass();
  graph->deserialize(payl);

  return graph;
}


class StatusMgr : public Chare
{
public:
  StatusMgr(unsigned int total_tasks)
  {
    m_totalTasks = total_tasks;
    m_startTime = CkWallTimer();
  }

  void done()
  {
    static uint32_t checkin_count = 0;

    checkin_count++;
    if( m_totalTasks == checkin_count )
    {
      std::cout << "Finished executing, runtime (sec): " <<  CkWallTimer() - m_startTime << std::endl;
      CkExit();
    }
  }

private:
  uint32_t m_totalTasks;
  uint32_t m_startTime;
};


//! Default message for charm
typedef std::vector<char> Buffer;

class CharmTask : public CBase_CharmTask
{
public:

  //! Constructor which sets the callback and decodes destinations
  CharmTask(CharmPayload buffer);

  //! Default
  CharmTask(CkMigrateMessage *m) {}

  ~CharmTask() {}


  //! The main compute call for the task
  void exec();

  //! Call to add new input data
  void addInput(CharmTaskId source, Buffer buffer);

  static void initStatusMgr(uint32_t total_tasks);

private:

  //! The corresponding base task
  Task mTask;

  //! The array of inputs
  std::vector<Payload> mInputs;

  //! The global charm-ids of all outputs
  std::vector<std::vector<uint64_t> > mOutputs;

  static CProxy_StatusMgr mainStatusMgr;
};



}
}






#endif /* CHARM_CHARMTASK_H_ */
