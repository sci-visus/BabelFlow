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

#ifndef TASK_H
#define TASK_H

#include <vector>

#include "Definitions.h"
#include "Payload.h"

namespace BabelFlow
{

class TaskGraph;

/*! A task is the abstract description of a piece of idempotent
 *  computation in a dataflow. Each task is identified by a unique
 *  id and stores how many inputs from whom, it needs to run and
 *  how many outputs it will produce and to which task(s) these must
 *  be send.
 */
class Task
{
public:

  //! Default constructor
  Task(TaskId id = TNULL) : mId(id), mCallbackId(0), mCallbackFunc(nullptr) {}

  //! Copy constructor
  Task(const Task& t);

  //! Default destructor
  ~Task() {}

  //! Assignment operator
  Task& operator=(const Task& t);

  //! Return the id
  TaskId id() const {return mId;}

  //! Set the task id
  void id(TaskId i) {mId = i;}

  //! Return the callback index
  CallbackId callbackId() const { return mCallbackId; }

  //! Return the callback func pointer
  Callback callbackFunc() const { return mCallbackFunc; }

  //! Set the callback
  void callback( CallbackId cbid, Callback cb ) { mCallbackId = cbid; mCallbackFunc = cb; }

  //! Return the number of incoming messages
  uint32_t fanin() const {return mIncoming.size();}

  //! Return the number of outgoing messages
  uint32_t fanout() const {return mOutgoing.size();}

  //! Return the list of tasks expected to produce an input
  const std::vector<TaskId>& incoming() const {return mIncoming;}

  //! Return a reference to the incoming tasks
  std::vector<TaskId>& incoming() {return mIncoming;}

  //! Set the incoming tasks
  void incoming(std::vector<TaskId>& in) {mIncoming = in;}

  //! Return a reference to the outgoing tasks
  const std::vector<std::vector<TaskId> >& outputs() const {return mOutgoing;}

  //! Return a reference to the outgoing tasks
  std::vector<std::vector<TaskId> >& outputs() {return mOutgoing;}

  //! Set the outgoing tasks
  void outputs(const std::vector<std::vector<TaskId> >& out) {mOutgoing = out;}

  //! Return a list of tasks for the given output
  const std::vector<TaskId>& outgoing(uint32_t i) const {return mOutgoing[i];}

  //! Return a reference to the outgoing task for the given output
  std::vector<TaskId>& outgoing(uint32_t i) {return mOutgoing[i];}

private:

  //! The globally unique id of this task
  TaskId mId;

  //! The index of the callback associate with this task
  CallbackId mCallbackId;

  //! The callback func pointer associated with this task
  Callback mCallbackFunc;

  //! The set of tasks which will produce one of the inputs
  std::vector<TaskId> mIncoming;

  //! A list of outputs and the tasks to which they must be send
  std::vector<std::vector<TaskId> > mOutgoing;
};
  
}

#endif /* TASK_H_ */
