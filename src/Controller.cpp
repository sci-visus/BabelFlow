/*
 * Controller.cpp
 *
 *  Created on: Dec 12, 2014
 *      Author: bremer5
 */

#include <unistd.h>
#include <assert.h>
#include <cstring>
#include "mpi.h"
#include <iostream>

#include "Controller.h"
#include "RelayTask.h"

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

Controller::Controller():mRecvBufferSize(1024*1024)
{
  mId = CNULL;
  mTaskMap = NULL;
  mControllerMap = NULL;
  mCallbacks.push_back(&relay_message);
  mRank = TNULL;
}

int Controller::initialize(const TaskGraph& graph, const TaskMap* task_map,
                           const ControllerMap* controller_map)
{
  mTaskMap = task_map;
  mControllerMap = controller_map;


  //! First lets find our id
  MPI_Comm_rank(MPI_COMM_WORLD, &mRank);
  mId = mControllerMap->controller(mRank);
    std::cout << "rank : " << mRank << " mId :: " << mId << "\n";

  // Get all tasks assigned to this controller
  std::vector<Task> tasks = graph.localGraph(mId,task_map);
  std::vector<Task>::const_iterator tIt;
  std::vector<TaskId>::const_iterator it;
  std::map<int,uint32_t>::iterator mIt;
  ControllerId cId;
  int c_rank;

  // Now collect the message log
  // For all tasks
  for (tIt=tasks.begin();tIt!=tasks.end();tIt++) {
    //std::cout << "initialize mRank : " << mRank << " task id :: " << tIt->id() << "\n";
    // Loop through all incoming tasks
    for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {

      //std::cout << "Incoming task: " << *it << "\n";
      // If this is an input that will come from the dataflow
      if (*it != TNULL) {
        cId = mTaskMap->controller(*it);

        // And one that comes from the outside
        if (cId != mId) {

          c_rank = mControllerMap->rank(cId);
          mIt = mMessageLog.find(c_rank);

          // If we have not seen a message from this rank before
          if (mIt == mMessageLog.end())
            mMessageLog[*it] = 1; // Create an entry
          else // Otherwise
            mIt->second = mIt->second+1; // just increase the count

          //std::cout << "Dest mID:: " << mId << " source  cID: " <<cId <<  " Incoming from :: " << *it <<  " To " << tIt->id() << "\n";
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
  std::map<int,uint32_t>::iterator mIt;
  std::map<TaskId,DataBlock>::iterator tIt;
  std::map<TaskId,TaskWrapper>::iterator wIt;

  int num_processes;
  MPI_Comm_size(MPI_COMM_WORLD, &num_processes);

  if (num_processes > 1) {
    // First we post receives for all ranks
    for (mIt=mMessageLog.begin();mIt!=mMessageLog.end();mIt++) {
      // Post receive
      postRecv(mIt->first);
    }
  }

  //if (!initial_inputs.empty()) {//std::cout << "Input EMPTY for rank :: " << mRank << "\n";
  // For all of the initial inputs assign the corresponding input
  // and start the task
  for (tIt=initial_inputs.begin();tIt!=initial_inputs.end();tIt++) {
    // First make sure that we are even responsible for this task
    //for (wIt = mTasks.begin(); wIt != mTasks.end(); wIt++) {
    //  std::cout << wIt->first << " " << mRank << " Searching for " << tIt->first << "\n";
   // }

    wIt = mTasks.find(tIt->first);
    assert (wIt != mTasks.end());

    // Pass on the input using TNULL as source indicating an outside input
    wIt->second.addInput(TNULL,tIt->second);

    // Start the task (note that for now we assume that all leaf tasks
    // have only a single input)
    std::cout << "tIt:: " << tIt->first << "\n";
    std::cout << "Start task:: " << wIt->second.task().id() << "\n";
    startTask(wIt->second);
  }
 // } 
 // else {
 //   wIt = mTasks.begin();
 //   wIt->second.addInput(TNULL, NULL);
 //   std::cout << "Start task:: " << wIt->second.task().id() << "\n";

 //   startTask(wIt->second);
 // }

  // Finally, start the while loop that tests for MPI events and
  // asynchronously starts inputs
  while (true) {

    // Test for MPI stuff happening
    //testIncoming();
    if (num_processes > 1) {
      testMPI();
    }

    // Send all outstanding messages
    //sendOutgoing();

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

  std::cout << "Task Started:: " << task.task().id() << "\n";

  mThreads.push_back(t);

  return 1;
}

int Controller::initiateSend(TaskId source, 
                             const std::vector<TaskId>& destinations, 
                             DataBlock data)
{
  std::vector<TaskId>::const_iterator it;
  std::map<TaskId,TaskWrapper>::iterator tIt;
  std::map<uint32_t,std::vector<TaskId> > packets;
  std::map<uint32_t,std::vector<TaskId> >::iterator pIt;
  int rank;

  for (it=destinations.begin();it!=destinations.end();it++) {

    // First, we check whether the destination is a local task
    tIt = mTasks.find(*it);
    if (tIt != mTasks.end()) {// If it is a local task
      tIt->second.addInput(source,data); // Pass on the data
      if (tIt->second.ready()) { // If this task is now ready to execute stage it
        stageTask(*it); // Note that we can't start the task as that would 
        // require locks on the threads
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
    //Message* msg = new Message;
    uint32_t size = 3*sizeof(uint32_t) // dest, size, number of dest tasks
                    + sizeof(TaskId)   // source taskId
                    + pIt->second.size()*sizeof(TaskId) // the destination tasks
                    + data.size; // payload
    char* msg = new char[size];
    *(uint32_t*)msg = pIt->first;
    *(uint32_t*)(msg + sizeof(uint32_t)) = size;
    char* ptr = msg + sizeof(uint32_t)*2;
    //msg->destination = pIt->first;
    //msg->size = sizeof(uint32_t) + pIt->second.size()*sizeof(TaskId) + data.size;
    //msg->buffer = new char[msg->size];

    //*((uint32_t*)msg->buffer) = static_cast<uint32_t>(pIt->second.size());
    *((uint32_t*)ptr) = static_cast<uint32_t>(pIt->second.size());

    TaskId* source_task = (TaskId*)(ptr + sizeof(uint32_t));
    *source_task = source; // Store the source taskId first

    // Now store the dest taskIds
    TaskId* t = source_task + 1; 
    for (uint32_t i=0;i<pIt->second.size();i++)
      t[i] = pIt->second[i];

    // This memcpy is annoying but appears necessary if we want to encode multiple
    // destination task ids. Might look at this later
    memcpy(ptr + sizeof(uint32_t) + sizeof(TaskId) + 
           pIt->second.size()*sizeof(TaskId), data.buffer, data.size);

    // Now we need to post this for sending
    // First, get the lock
    mOutgoingMutex.lock();
    mOutgoing.push_back(msg);
    mOutgoingMutex.unlock();
  }

  return 1;
}

int Controller::postRecv(int32_t source_rank) {
  MPI_Request req;
  char* buffer = new char[mRecvBufferSize];

  std::cout << "Posting recv:: " << source_rank << "\n";
  MPI_Irecv((void*)buffer, mRecvBufferSize, MPI_BYTE, source_rank, 0,
            MPI_COMM_WORLD, &req);

  //mMessagesMutex.lock();
  if (!mFreeMessagesQ.empty()) {
    mMessages[mFreeMessagesQ.front()] = buffer;
    mMPIreq[mFreeMessagesQ.front()] = req;
    mFreeMessagesQ.pop();
  }
  else {
    mMessages.push_back(buffer);
    mMPIreq.push_back(req);
  }
  //mMessagesMutex.unlock();

  return 1;
}

int Controller::testMPI()
{
  int index = -1;
  MPI_Status mpi_status;
  int status;
  int req_complete_flag = 0;

  //need to post receives first so do a testany first
  //then do the Isends for the new messages
  status = MPI_Testany(mMPIreq.size(), &mMPIreq[0], &index, &req_complete_flag,
                       &mpi_status);

  if (status != MPI_SUCCESS) {
    fprintf(stderr, "Error in Test any!!\n");
    assert(status != MPI_SUCCESS);
  }

  if (req_complete_flag) {
    assert(index >= 0);

    char* message = mMessages[index];
    uint32_t dest = (*(uint32_t*)message);
    
    if (dest == mRank) { // This is a received message

      // Read message header
      uint32_t message_size = *(uint32_t*)(message + sizeof(uint32_t));
      uint32_t num_tasks_msg = *(uint32_t*)(message + 2*sizeof(uint32_t));
      TaskId source_task = *(TaskId*)(message + 3*sizeof(uint32_t));
      TaskId* task_ids= (TaskId*)(message + 3*sizeof(uint32_t) + sizeof(TaskId));
      char* data_ptr = (char*)(task_ids + num_tasks_msg);

      //printf("%s\n", data_ptr);
      // Create DataBlock from message
      DataBlock data_block;
      data_block.size = (int)message_size - 
                    (sizeof(uint32_t)*3 + sizeof(TaskId)*(1 + num_tasks_msg));
      data_block.buffer = new char[data_block.size];
      memcpy(data_block.buffer, data_ptr, data_block.size);

      //if (mRank == 1) {
      //std::cout << "Data Size :: " << data_block.size << "\n";
      //std::cout << "Msg Size :: " << message_size << "\n";
      //std::cout << "Source task :: " << source_task << "\n";
      //std::cout << "Num task :: " << num_tasks_msg << "\n";
      //std::cout << "task :: " << task_ids[0] << "\n";
      //}
      // Add DataBlock to TaskWrapper
      std::map<TaskId,TaskWrapper>::iterator wIt;
      for (int i=0; i< num_tasks_msg; i++) {
        wIt = mTasks.find(task_ids[i]);
        wIt->second.addInput(source_task, data_block);
        if (wIt->second.ready()) {
          stageTask(wIt->first);
        }
      std::cout << "****Revced:: " << mRank << "\n";
      }

      // Delete message
      //delete[] mMessages[index];
      //mFreeMessagesQ.push(index);
      
      // Update the message log and check for more messages from this rank
      std::map<int,uint32_t>::iterator mIt;
      mIt = mMessageLog.find(mpi_status.MPI_SOURCE);
      mIt->second = mIt->second - 1; // Update the number of messages 
      // to be received from this rank

      // If more messages to be received from this rank we will post recv
      if (mIt->second > 0)
        postRecv(mIt->first);
    }
    else { // This is a completed send message

      // Delete the completed send message
      delete[] mMessages[index];
      mFreeMessagesQ.push(index);
    }
  }
  // Now post the Isends
  mOutgoingMutex.lock();
  for (int i=0; i < mOutgoing.size(); i++) {

    MPI_Request req;
    // Get the dest rank and size from the message header
    if (!mFreeMessagesQ.empty()) {
      mMessages[mFreeMessagesQ.front()] = (char*)mOutgoing[i];
      mMPIreq[mFreeMessagesQ.front()] = req;
      mFreeMessagesQ.pop();
    }
    else {
      mMessages.push_back((char*)mOutgoing[i]);
      mMPIreq.push_back(req);
    }

    uint32_t destination = *(uint32_t*)mOutgoing[i];
    uint32_t size = *(uint32_t*)(mOutgoing[i] + sizeof(uint32_t));
    std::cout << "Sending from: " << mRank << " to " << destination << "\n";
    MPI_Isend((void*)mOutgoing[i], size, MPI_BYTE, destination, 0, 
              MPI_COMM_WORLD, &req);
  }
  mOutgoing.clear();
  mOutgoingMutex.unlock();

  return 1;
}

//int Controller::sendOutgoing()
//{
//  uint32_t i=0;
//
//  mOutgoingMutex.lock();
//
//  while (i < mOutgoing.size()) {
//    // Sending not implemented yet
//    assert (false);
//    i++;
//  }
//  mOutgoingMutex.unlock();
//
//  return 1;
//}

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



