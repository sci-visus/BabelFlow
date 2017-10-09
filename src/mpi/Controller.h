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

#ifndef CONTROLLER_H
#define CONTROLLER_H

#include <vector>
#include <deque>
#include <map>
#include <set>
#include <thread>
#include <mutex>
#include <queue>

#include "../Definitions.h"
#include "../TaskGraph.h"
#include "../Task.h"
#include "../Payload.h"
#include "mpi.h"

namespace DataFlow {

namespace mpi {

/*! A controller handles the communication as well as thread
 *  management for the tasks assigned to it
 */
class Controller
{
public:

  //! Default controller
  Controller();

  //! Default  destructor
  ~Controller() {}

  //! Initialize the controller
  int initialize(const DataFlow::TaskGraph& graph, const DataFlow::TaskMap* task_map, MPI_Comm comm = MPI_COMM_WORLD,
                 const DataFlow::ControllerMap* controller_map = new DataFlow::ControllerMap());

  //! Register a callback for the given id
  int registerCallback(CallbackId id, Callback func);

  //! Start the computation
  int run(std::map<TaskId,Payload>& initial_inputs);

//private:

  //! The wrapper used to start a task
  class TaskWrapper
  {
  public:

    //! Default constructor
    TaskWrapper(const DataFlow::Task& t);

    //! Copy constructor
    TaskWrapper(const TaskWrapper& t);

    //! Default destructor
    ~TaskWrapper() {}

    TaskWrapper& operator=(const TaskWrapper& t);

    //! Return the corresponding task
    const DataFlow::Task& task() const {return mTask;}

    //! Add an input
    bool addInput(TaskId source, Payload data);

    //! Return whether this task is ready to be executed
    bool ready() const;

    //! The task
    DataFlow::Task mTask;

    //! Mutex to check if task is ready. We use this in the addInput routine as
    //! both the master and the worker thready can check if a task is ready and
    //! could potentially start the same task if this mutex is not used.
    std::mutex mTaskReadyMutex;

    //! The input buffers
    std::vector<Payload> mInputs;

    //! The output buffers
    std::vector<Payload> mOutputs;
  };

  //! A list of registered callbacks
  std::vector<Callback> mCallbacks;
   
  //! Post a send of the given data to all destinations
  int initiateSend(TaskId source,const std::vector<TaskId>& destinations, 
                   Payload data);

private:

  //! The id of this controller
  ShardId mId;

  //! The MPI rank used by the controller. If no MPI rank its TNULL
  int mRank;

  //! The MPI Comm used by the controller. If no MPI rank its TNULL
  MPI_Comm mComm;

  //! A map from TaskId to TaskWrapper for all tasks assigned to this 
  //! controller
  std::map<TaskId,TaskWrapper> mTasks;

  //! The active task map
  const DataFlow::TaskMap* mTaskMap;

  //! The active controller map
  const DataFlow::ControllerMap* mControllerMap;

  //! The map from rank-id to the number of expected messages
  std::map<int, uint32_t> mMessageLog;

  //! A message holds both the destination and the fully serialized
  //! payload
  struct Message {
    uint32_t destination; //! MPI destination rank
    uint32_t size; //! Number of bytes

    //! <uint32_t> <TaskId> .... <TaskId> <paylod>
    //! nr-of-tasks <sourceTask>  <t0> ... <tn> <payload>
    char* buffer;
  };

  //! The set of threads we have created
  std::vector<std::thread*> mThreads;

  //! The set of staged tasks
  std::deque<TaskId> mTaskQueue;

  //! The mutex protecting task queue
  std::mutex mQueueMutex;
  
  //! The mutex to check if all incoming data is received
  std::mutex mReadyMutex;
  
  //! Stage task indicating that this task is ready to run
  int stageTask(TaskId t);

  //! Start a thread for the given task in a thread
  int startTask(TaskWrapper& task);

  //! Start all queued up tasks
  int processQueuedTasks();
 
  // MPI related members
  //! Buffer size for received messages
  const uint32_t mRecvBufferSize;

  //! A set of outgoing messages with destination-id -> Data
  std::vector<char*> mOutgoing;

  //! The mutex controlling access to the outgoing messages
  std::mutex mOutgoingMutex;

  //! A list of message buffers for sending and receiveing
  std::vector<char*> mMessages;

  //! A queue to maintain the free slots in the mMessageBuffers
  std::queue<uint32_t> mFreeMessagesQ;
  
  //! A list of MPI request handles for sends and recvs
  std::vector<MPI_Request> mMPIreq;

  //! Send all outstanding messages
  char* packMessage(std::map<uint32_t,std::vector<TaskId> >::iterator pIt,
                    TaskId source, Payload data) ;

  TaskId* unPackMessage(char* messsage, Payload* data_block,
                        TaskId* source_task, uint32_t* num_tasks_msg);

  //! Test for MPI events to guarantee progress and handle receives
  int testMPI();

  //! Post MPI receives for incomming messages
  int postRecv(int32_t source_rank);
  
};

//! Execute the given task and send the outputs
int execute(Controller* c, Controller::TaskWrapper task);

}
}
#endif /* CONTROLLER_H_ */
