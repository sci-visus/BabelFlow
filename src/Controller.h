/*
 * Controller.h
 *
 *  Created on: Dec 12, 2014
 *      Author: bremer5
 */

#ifndef CONTROLLER_H
#define CONTROLLER_H

#include <vector>
#include <deque>
#include <map>
#include <set>
#include <thread>
#include <mutex>


#include "Definitions.h"
#include "TaskGraph.h"
#include "Task.h"

//! A DataBlock abstracts a chunk of memory
class DataBlock
{
public:

  DataBlock(void *b=NULL, uint32_t s=0) : buffer(b), size(s) {}

  void* buffer;
  uint32_t size;
};

//! The typedef for the accepted callbacks
/*! A Callback is the only accepted function signature for a task.
 *  It takes n>=0  DataBlocks as input and produces m>=0 DataBlocks
 *  as output. The callback will assume ownership of the input buffers
 *  as is responsible for deleting any associated memory. Similarly,
 *  the callback will give up ownership of all output buffers to the
 *  caller
 *
 * @param inputs A set of DataBlocks that are the inputs
 * @param outputs A set of DataBlocks storing the outputs
 * @return 1 if successful and 0 otherwise
 */
typedef int (*Callback)(std::vector<DataBlock>& inputs, std::vector<DataBlock>& outputs);

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
  int initialize(const TaskGraph& graph, const TaskMap* task_map,
                 const ControllerMap* controller_map = new ControllerMap());

  //! Register a callback for the given id
  int registerCallback(CallbackId id, Callback func);

  //! Start the computation
  int run(std::map<TaskId,DataBlock>& initial_inputs);

//private:

  //! The wrapper used to start a task
  class TaskWrapper
  {
  public:

    //! Default constructor
    TaskWrapper(const Task& t);

    //! Copy constructor
    TaskWrapper(const TaskWrapper& t);

    //! Default destructor
    ~TaskWrapper() {}

    TaskWrapper& operator=(const TaskWrapper& t);

    //! Return the corresponding task
    const Task& task() const {return mTask;}

    //! Add an input
    int addInput(TaskId source, DataBlock data);

    //! Return whether this task is ready to be executed
    bool ready() const;

    //! The task
    Task mTask;

    //! The input buffers
    std::vector<DataBlock> mInputs;

    //! The output buffers
    std::vector<DataBlock> mOutputs;
  };

  //! The id of this controller
  ControllerId mId;

  //! A map from TaskId to TaskWrapper for all tasks assigned to this controller
  std::map<TaskId,TaskWrapper> mTasks;

  //! The active task map
  const TaskMap* mTaskMap;

  //! The active controller map
  const ControllerMap* mControllerMap;

  //! A list of registered callbacks
  std::vector<Callback> mCallbacks;

  //! The map from rank-id to the number of expected messages
  std::map<uint32_t, uint32_t> mMessageLog;

  //! A message holds both the destination and the fully serialized
  //! payload
  struct Message {
    uint32_t destination; //! MPI destination rank
    uint32_t size; //! Number of bytes

    //! <uint32_t> <TaskId> .... <TaskId> <paylod>
    //! nr-of-tasks <t0> ... <tn> <payload>
    char* buffer;
  };

  //! A set of outgoing messages with destination-id -> Data
  std::vector<Message> mOutgoing;

  //! The mutex controlling access to the outgoing messages
  std::mutex mOutgoingMutex;

  //! The set of threads we have created
  std::vector<std::thread*> mThreads;

  //! The set of staged tasks
  std::deque<TaskId> mTaskQueue;

  //! The mutex protecting task queue
  std::mutex mQueueMutex;

  //! Test for MPI events to guarantee progress and handle receives
  int testIncoming();

  //! Send all outstanding messages
  int sendOutgoing();

  //! Stage task indicating that this task is ready to run
  int stageTask(TaskId t);

  //! Start a thread for the given task in a thread
  int startTask(TaskWrapper& task);

   //! Post a send of the given data to all destinations
  int initiateSend(TaskId source,const std::vector<TaskId>& destinations, DataBlock data);

  //! Start all queued up tasks
  int processQueuedTasks();

};

//! Execute the given task and send the outputs
int execute(Controller* c, Controller::TaskWrapper task);


#endif /* CONTROLLER_H_ */
