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

#include "../Definitions.h"
#include "../TaskGraph.h"
#include "../Payload.h"

#include "charm_dataflow.decl.h"

//! Global function returning the callbacks
//! Must be implemented by the calling application code
extern DataFlow::Callback registered_callback(DataFlow::CallbackId id);

//! Global function to create a task graph
//! Must be implemented by the calling application code
extern DataFlow::TaskGraph* make_task_graph(DataFlow::Payload buffer);


namespace DataFlow {
namespace charm {

//! Allow us to send Payload buffers using charm++
inline void operator|(PUP::er &p, DataFlow::Payload& buffer) {

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
