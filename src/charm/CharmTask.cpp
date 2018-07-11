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

#include "CharmTask.h"

namespace BabelFlow {
namespace charm {


CharmTask::CharmTask(Payload buffer)
{
  //fprintf(stderr,"Starting Tasks %d\n",this->thisIndex);

  TaskGraph* graph = make_task_graph(buffer);
  mTask = graph->task(this->thisIndex);

  mInputs.resize(mTask.fanin());

  //! Now we pre-compute the global ids of the outgoing tasks so we
  //! later do not need the task graph again
  mOutputs.resize(mTask.fanout());
  for (uint32_t i=0;i<mTask.fanout();i++) {

    mOutputs[i].resize(mTask.outgoing(i).size());
    for (uint32_t j=0;j<mTask.outgoing(i).size();j++) {

      if (mTask.outgoing(i)[j] != TNULL)
        mOutputs[i][j] = graph->gId(mTask.outgoing(i)[j]);
      else
        mOutputs[i][j] = (uint64_t)-1;
    }
  }
}

void CharmTask::exec()
{
  //fprintf(stderr,"CharmTask<TaskGraphClass, CallbackClass>::exec() %d  fanout %d\n",mTask.id(),mTask.fanout());
  std::vector<Payload> outputs(mTask.fanout());

  //mCallback(mInputs,outputs,mTask.id());

  Callback cb = registered_callback(mTask.callback());
  cb(mInputs,outputs,mTask.id());

  std::vector<char> buffer;

  for(int i=0;i<outputs.size();i++)
  {
    buffer.assign(outputs[i].buffer(), outputs[i].buffer()+outputs[i].size());

    // For all tasks that need this output as input
    for(int j=0;j<mOutputs[i].size();j++)
    {
      //fprintf(stderr,"\nProcessing output from %d to %d\n",mTask.id(),mOutputs[i][j]);
      if (mOutputs[i][j] != (uint64_t)-1)
        this->thisProxy[mOutputs[i][j]].addInput(mTask.id(),buffer);
    }
  }

  // Clean up

  // The actual compute tasks have assumed responsibility for the
  // input payloads
  mInputs.clear();

  // But we should delete the output payloads
  for(int i=0;i<outputs.size();i++)
    outputs[i].reset();

  outputs.clear();
}

void CharmTask::addInput(TaskId source, Buffer buffer)
{
  TaskId i;
  bool is_ready = true;
  bool input_added = false;

  //fprintf(stderr,"CharmTask<TaskGraphClass, CallbackClass>::addInput id %d source %d \n", mTask.id(),source);

  for (i=0;i<mTask.fanin();i++) {
    //fprintf(stderr,"\t %d incoming %d\n",mTask.id(),mTask.incoming()[i]);
    if (mTask.incoming()[i] == source) {
      assert(mInputs[i].buffer() == NULL);

      // Not clear whether we need to copy the data here
      char *data = new char[buffer.size()];
      memcpy(data, &buffer[0], buffer.size());

      mInputs[i].initialize(buffer.size(),data);
      input_added = true;
    }
    if (mInputs[i].buffer() == NULL)
      is_ready = false;
  }

  if (!input_added) {
    fprintf(stderr,"Unknown sender %d in CharmTask::addInput for task %d\n",source,mTask.id());
    assert (false);
  }
  else{
    //printf("\tinput added is_ready = %d\n",is_ready);
  }

  if (is_ready)
    exec();
}



}
}

#include "charm_babelflow.def.h"


