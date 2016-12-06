/*
 * CharmTask.cpp
 *
 *  Created on: Dec 2, 2016
 *      Author: bremer5
 */


#include "charm/CharmTask.h"

namespace DataFlow {
namespace charm {

CharmTask::CharmTask(CharmTaskGraphBase cgraph, std::vector<Callback> callbacks) : mCallback(NULL)
{
  TaskGraph* graph = cgraph.buildGraph();

  mTask = graph->task(thisIndex);
  mCallback = callbacks[mTask.callback()];

  mInputs.resize(mTask.fanin());

  //! Now we pre-compute the global ids of the outgoing tasks so we
  //! later do not need the task graph again
  mOutputs.resize(mTask.fanout());
  for (uint32_t i;i<mTask.fanout();i++) {
    mOutputs[i].resize(mTask.outgoing(i).size());
    for (uint32_t j=0;j<mTask.outgoing(i).size();j++) {

      if (mTask.outgoing(i)[j] != TNULL)
        mOutputs[i][j] = graph->gId(mTask.outgoing(i)[j]);
      else
        mOutputs[i][j] = (uint64_t)-1;
    }
  }

  delete graph;
}

void CharmTask::exec()
{
  assert (mCallback != NULL);

  std::vector<Payload> outputs(mTask.fanout());

  mCallback(mInputs,outputs,thisIndex);

  std::vector<char> buffer;

  for(int i=0;i<outputs.size();i++)
  {
    buffer.assign(outputs[i].buffer(), outputs[i].buffer()+outputs[i].size());

    // For all tasks that need this output as input
    for(int j=0;j<mOutputs[i].size();j++)
    {
      if (mOutputs[i][j] != (uint64_t)-1)
        thisProxy[mOutputs[i][j]].addInput(mTask.id(),buffer);
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

  for (i=0;i<mTask.fanin();i++) {
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
    fprintf(stderr,"Unknown sender %d in TaskWrapper::addInput for task %d\n",source,mTask.id());
    assert (false);
  }

  if (is_ready)
    exec();
}

}
}




