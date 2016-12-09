/*
 * CharmTask.h
 *
 *  Created on: Dec 2, 2016
 *      Author: bremer5
 */

#ifndef CHARM_CHARMTASK_H_
#define CHARM_CHARMTASK_H_

#include "charm++.h"
#include "pup_stl.h"

#include "Definitions.h"
#include "TaskGraph.h"
#include "Payload.h"

#include "charm_dataflow.decl.h"

namespace DataFlow {
namespace charm {


//! Default message for charm
typedef std::vector<char> Buffer;

template<class TaskGraphClass, class CallbackClass>
class CharmTask : public CBase_CharmTask<TaskGraphClass, CallbackClass>
{
public:

  //! Constructor which sets the callback and decodes destinations
  CharmTask(TaskGraphClass& graph);

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

  //! The actual callback to execute once all inputs are assembled
  Callback mCallback;

  //! The array of inputs
  std::vector<Payload> mInputs;

  //! The global charm-ids of all outputs
  std::vector<std::vector<uint64_t> > mOutputs;
};


template<class TaskGraphClass, class CallbackClass>
CharmTask<TaskGraphClass, CallbackClass>::CharmTask(TaskGraphClass& graph) : mCallback(NULL)
{
  fprintf(stderr,"Starting Tasks %d\n",this->thisIndex);

  mTask = graph.task(this->thisIndex);

  mCallback = CallbackClass::callback(mTask.callback());

  mInputs.resize(mTask.fanin());

  //! Now we pre-compute the global ids of the outgoing tasks so we
  //! later do not need the task graph again
  mOutputs.resize(mTask.fanout());
  for (uint32_t i=0;i<mTask.fanout();i++) {

    mOutputs[i].resize(mTask.outgoing(i).size());
    for (uint32_t j=0;j<mTask.outgoing(i).size();j++) {

      if (mTask.outgoing(i)[j] != TNULL)
        mOutputs[i][j] = graph.gId(mTask.outgoing(i)[j]);
      else
        mOutputs[i][j] = (uint64_t)-1;
    }
  }
}

template<class TaskGraphClass, class CallbackClass>
void CharmTask<TaskGraphClass, CallbackClass>::exec()
{
  assert (mCallback != NULL);

  //fprintf(stderr,"CharmTask<TaskGraphClass, CallbackClass>::exec() %d  fanout %d\n",mTask.id(),mTask.fanout());
  std::vector<Payload> outputs(mTask.fanout());

  mCallback(mInputs,outputs,mTask.id());

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

template<class TaskGraphClass, class CallbackClass>
void CharmTask<TaskGraphClass, CallbackClass>::addInput(TaskId source, Buffer buffer)
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
#define CK_TEMPLATES_ONLY
#include "charm_dataflow.def.h"
#undef CK_TEMPLATES_ONLY






#endif /* CHARM_CHARMTASK_H_ */
