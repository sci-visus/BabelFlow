/*
 * CharmTask.cpp
 *
 *  Created on: Dec 14, 2016
 *      Author: bremer5
 */

#include "CharmTask.h"

namespace DataFlow {
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

#include "charm_dataflow.def.h"


