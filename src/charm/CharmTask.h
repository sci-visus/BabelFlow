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

#include "charm_babelflow.decl.h"

//! Global function returning the callbacks
//! Must be implemented by the calling application code
extern BabelFlow::Callback registered_callback(BabelFlow::CallbackId id);

//! Global function to create a task graph
//! Must be implemented by the calling application code
extern BabelFlow::TaskGraph* make_task_graph(BabelFlow::Payload buffer);


namespace BabelFlow {
namespace charm {

//! Allow us to send Payload buffers using charm++
inline void operator|(PUP::er &p, BabelFlow::Payload& buffer) {

  p|buffer.mSize;
  if (p.isUnpacking())
    buffer.mBuffer = new char[buffer.size()];
  PUParray(p, buffer.buffer(), buffer.size());


  // If charm will delete the object make sure that we release the
  // memory buffer
  if (p.isDeleting())
    delete[] buffer.buffer();
}

//! Make defining the global task graph function easy
template<class TaskGraphClass>
TaskGraph *make_task_graph_template(Payload buffer)
{
  TaskGraph* graph =  new TaskGraphClass();
  graph->deserialize(buffer);

  return graph;
}


//! Default message for charm
typedef std::vector<char> Buffer;

class CharmTask : public CBase_CharmTask
{
public:

  //! Constructor which sets the callback and decodes destinations
  CharmTask(Payload buffer);

  //! Default
  CharmTask(CkMigrateMessage *m) {}

  ~CharmTask() {}


  //! The main compute call for the task
  void exec();

  //! Call to add new input data
  void addInput(TaskId source, Buffer buffer);

private:

  //! The corresponding base task
  Task mTask;

  //! The array of inputs
  std::vector<Payload> mInputs;

  //! The global charm-ids of all outputs
  std::vector<std::vector<uint64_t> > mOutputs;
};



}
}






#endif /* CHARM_CHARMTASK_H_ */
