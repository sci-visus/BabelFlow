/*
 * Controller.cpp
 *
 *  Created on: Dec 12, 2014
 *      Author: bremer5
 */

#include <unistd.h>
#include "mpi.h"

#include "Controller.h"
#include "relayTask.h"

Controller::TaskWrapper::TaskWrapper(const Task& t) : mTask(t)
{
  mInputs.resize(t.fanin());
  mOutputs.resize(t.fanout());
}

Controller::TaskWrapper::TaskWrapper(const TaskWrapper& t) : mTask(t.mTask),
    mInputs(t.mInputs), mOutputs(t.mOutputs)
{
}

Controller::TaskWrapper& Controller::TaskWrapper::operator=(const TaskWrapper& t)
{
  mTask = t.mTask;
  mInputs = t.mInputs;
  mOutputs = t.mOutputs;

  return *this;
}


int Controller::TaskWrapper::addInput(TaskId source, DataBlock data)
{
  TaskId i;

  for (i=0;i<mTask.fanin();i++) {
    if (mTask.incoming()[i] == source) {
      mInputs[i] = data;
      break;
    }
  }

  if (i == mTask.fanin()) {
    fprintf(stderr,"Unknown sender %d in TaskWrapper::addInput for task %d\n",source,mTask.id());
    assert (false);
  }

  return 1;
}

bool Controller::TaskWrapper::ready() const
{
  TaskId i;

  for (i=0;i<mTask.fanin();i++) {
    if (mInputs[i].buffer == NULL)
      return false;
  }

  return true;
}

Controller::Controller()
{
  mId = CNULL;
  mTaskMap = NULL;
  mControllerMap = NULL;

  mCallbacks.push_back(&relay_message);
}

int Controller::initialize(const TaskGraph& graph, const TaskMap* task_map,
                           const ControllerMap* controller_map)
{
  mTaskMap = task_map;
  mControllerMap = controller_map;


  //! First lets find our id
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  mId = mControllerMap->controller(rank);

  // Get all tasks assigned to this controller
  std::vector<Task> tasks = graph.localGraph(mId,task_map);
  std::vector<Task>::const_iterator tIt;
  std::vector<TaskId>::const_iterator it;
  std::map<uint32_t,uint32_t>::iterator mIt;
  ControllerId cId;

  // Now collect the message log
  // For all tasks
  for (tIt=tasks.begin();tIt!=tasks.end();tIt++) {
    // Loop through all incoming messages
    for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {

      // If this is an input that will come from the dataflow
      if (*it != TNULL) {
        cId = mTaskMap->controller(*it);

        // And one that comes from the outside
        if (cId != mId) {

          rank = mControllerMap->rank(cId);
          mIt = mMessageLog.find(rank);

          // If we have not seen a message from this rank before
          if (mIt == mMessageLog.end())
            mMessageLog[*it] = 1; // Create an entry
          else // Otherwise
            mIt->second = mIt->second+1; // just increase the count
        }
      }
    } //end-for all incoming messages

    // Finally, create the tasks wrappers
    mTasks.insert(std::pair<TaskId,TaskWrapper>(tIt->id(),TaskWrapper(*tIt)));
  } // end-for all local tasks

  return 1;
}

int Controller::registerCallback(CallbackId id, Callback func)
{
  assert (id > 0); // Callback 0 is reserved for relays

  if (mCallbacks.size() <= id)
    mCallbacks.resize(id+1);

  mCallbacks[id] = func;

  return 1;
}

//! Start the computation
int Controller::run(std::map<TaskId,DataBlock>& initial_inputs)
{
  std::map<uint32_t,uint32_t>::iterator mIt;
  std::map<TaskId,DataBlock>::iterator tIt;
  std::map<TaskId,TaskWrapper>::iterator wIt;

  // First we post receives for all ranks
  for (mIt=mMessageLog.begin();mIt!=mMessageLog.end();mIt++) {
    // Post receive
  }

  // For all of the initial inputs assign the corresponding input
  // and start the task
  for (tIt=initial_inputs.begin();tIt!=initial_inputs.end();tIt++) {
    // First make sure that we are even responsible for this task
    wIt = mTasks.find(tIt->first);
    assert (wIt != mTasks.end());

    // Pass on the input using TNULL as source indicating an outside input
    wIt->second.addInput(TNULL,tIt->second);

    // Start the task (note that for now we assume that all leaf tasks
    // have only a single input)
    startTask(wIt->second);
  }

  // Finally, start the while loop that tests for MPI events and
  // asynchronously starts inputs
  while (true) {

    // Test for MPI stuff happening
    //testIncoming();

    // Send all outstanding messages
    sendOutgoing();

    // Start all recently ready tasks
    processQueuedTasks();

    // If there are no more incoming messages
    if (mMessageLog.empty()) {
      // We wait for all threads we have created to finish
      while (true) {

        uint32_t i = 0;
        while (i < mThreads.size()) {
          if (mThreads[i]->joinable()) {// If we have found a thread that is finished
            mThreads[i]->join(); // Join it to the main thread

            std::swap(mThreads[i],mThreads.back());

            delete mThreads.back();
            mThreads.pop_back();
          }
          else {
            i++;
          }
        } // end-while

        // Test whether there are new threads we need to start
        processQueuedTasks();

        if (!mThreads.empty()) { // If there are still some threads going
          //fprintf(stderr,"Waiting for %d tasks\n",mThreads.size());

          usleep(100); // Give them time to finish
        }
        else
          break; // Otherwise, we are done
      } // end-while(true)

      break; // Break out of the main message loop
    } // end-if !mMessageLog.empty()
    else { // we are waiting for more messages
      usleep(100); // Given them time to arrive
    }
  }

  return 1;
}

int Controller::stageTask(TaskId t)
{
  mQueueMutex.lock();
  mTaskQueue.push_back(t);
  mQueueMutex.unlock();

  return 1;
}


int Controller::startTask(TaskWrapper& task)
{
  assert (task.ready());

  std::thread* t = new std::thread(&execute,this,task);

  mThreads.push_back(t);

  return 1;
}

int Controller::initiateSend(TaskId source, const std::vector<TaskId>& destinations, DataBlock data)
{
  std::vector<TaskId>::const_iterator it;
  std::map<TaskId,TaskWrapper>::iterator tIt;
  std::map<uint32_t,std::vector<TaskId> > packets;
  std::map<uint32_t,std::vector<TaskId> >::iterator pIt;
  uint32_t rank;

  for (it=destinations.begin();it!=destinations.end();it++) {

    // First, we check whether the destination is a local task
    tIt = mTasks.find(*it);
    if (tIt != mTasks.end()) {// If it is a local task
      tIt->second.addInput(source,data); // Pass on the data
      if (tIt->second.ready()) { // If this task is now ready to execute start it
        stageTask(*it); // Note that we can't start the task as that would require locks on the threads
      }
    }
    else { // If this is a remote task

      // Figure out where it needs to go
      rank = mControllerMap->rank(mTaskMap->controller(*it));

      // See whether somebody else already send to the same MPI rank
      pIt = packets.find(rank);
      if (pIt==packets.end()) {
        packets[rank] = std::vector<TaskId>(1,*it);
      }
      else {
        pIt->second.push_back(*it);
      }
    }
  }

  // Assemble the messages
  for (pIt=packets.begin();pIt!=packets.end();pIt++) {
    Message msg;

    msg.destination = pIt->first;
    msg.size = sizeof(uint32_t) + pIt->second.size()*sizeof(TaskId) + data.size;
    msg.buffer = new char[msg.size];

    *((uint32_t*)msg.buffer) = static_cast<uint32_t>(pIt->second.size());

    TaskId* t = (TaskId*)(msg.buffer + sizeof(uint32_t));

    for (uint32_t i=0;i<pIt->second.size();i++)
      t[i] = pIt->second[i];

    // This memcpy is annoying but appears necessary if we want to encode multiple
    // destination task ids. Might look at this later
    memcpy(msg.buffer + sizeof(uint32_t) + pIt->second.size()*sizeof(TaskId),
           data.buffer,data.size);

    // Now we need to post this for sending
    // First, get the lock
    mOutgoingMutex.lock();
    mOutgoing.push_back(msg);
    mOutgoingMutex.unlock();
  }

  return 1;
}

int Controller::testIncoming()
{
  // Not implemented yet
  assert (false);
}

int Controller::sendOutgoing()
{
  uint32_t i=0;

  mOutgoingMutex.lock();

  while (i < mOutgoing.size()) {
    // Sending not implemented yet
    assert (false);
    i++;
  }
  mOutgoingMutex.unlock();

  return 1;
}

int Controller::processQueuedTasks()
{
  std::map<TaskId,TaskWrapper>::iterator mIt;

  mQueueMutex.lock();
  while (!mTaskQueue.empty()) {
    mIt = mTasks.find(mTaskQueue.front());
    startTask(mIt->second);
    mTaskQueue.pop_front();
  }
  mQueueMutex.unlock();

  return 1;
}


int execute(Controller *c,Controller::TaskWrapper task)
{
  assert (task.task().callback() < c->mCallbacks.size());

  // Call the appropriate callback
  c->mCallbacks[task.task().callback()](task.mInputs,task.mOutputs);

  // Now we "own" all the BlockData for the outputs and we must send
  // them onward. So for all outputs we start a send
  for (uint32_t i=0;i<task.task().fanout();i++) {
    c->initiateSend(task.task().id(),task.task().outgoing(i),task.mOutputs[i]);
  }

  return 1;
}




