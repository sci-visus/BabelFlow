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

#include <mutex>
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


class StatusMsg : public CMessage_StatusMsg 
{
public:
  StatusMsg(int mid, int mval = 0) : m_msgId(mid), m_msgVal(mval) {}

	int m_msgId;
  int m_msgVal;
};


class StatusMgr : public CBase_StatusMgr
{
public:
  enum StatusCode {NCYCLE = 0, START = 1, DONE = 2};
  
  StatusMgr(unsigned int total_tasks)
  {
    newCycle( total_tasks );
  }

  void status(StatusMsg* msg)
  {
    // CkPrintf("StatusMgr::status msg =  %d\n", msg ? msg->m_msgId : -1);

    if( !msg )
      return;

    switch(msg->m_msgId)
    {
      case StatusCode::NCYCLE:
        newCycle(msg->m_msgVal);
        break;
      case StatusCode::START:
        start();
        break;
      case StatusCode::DONE:
        done(msg->m_msgVal);
        break;
    }

    delete msg;
  }

  void newCycle(int total_tasks)
  {
    m_totalTasks = total_tasks;
    m_startCnt = m_doneCnt = 0;

    // CkPrintf("Starting CharmTask new cycle: %d\n", m_totalTasks);
  }

  void start()
  {
    m_startCnt++;
    if( m_startCnt == 1 )
      m_startTime = CkWallTimer();

    // CkPrintf("Starting CharmTask\n");
  }

  void done(int arr_id)
  {
    m_doneCnt++;
    if( m_totalTasks == m_doneCnt )
    {
      double duration = CkWallTimer() - m_startTime;
      CkPrintf("Finished executing CharmTask's, runtime(sec): %f\n", float(duration));

      // All the CharmTask's finished runnning, so we can destroy them
      CkArrayID aid = CkGroupID{arr_id};
      CProxy_CharmTask(aid).ckDestroy();
    }
  }

  void pup(PUP::er &p)
  {
    p|m_startCnt;
    p|m_doneCnt;
    p|m_totalTasks;
    p|m_startTime;
  }

private:
  uint32_t m_startCnt;
  uint32_t m_doneCnt;
  uint32_t m_totalTasks;
  double m_startTime;
};


//! Default message for charm
typedef std::vector<char> Buffer;

class CharmTask : public CBase_CharmTask
{
public:

  //! Constructor which sets the callback and decodes destinations
  CharmTask(CharmPayload buffer, int status_ep, int status_id);

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

  int mStatEp;

  int mStatId;

  static CProxy_StatusMgr mainStatusMgr;
};



}
}






#endif /* CHARM_CHARMTASK_H_ */
