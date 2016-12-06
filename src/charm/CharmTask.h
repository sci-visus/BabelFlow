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
#include "CharmTaskGraph.h"

#include "charm_dataflow.decl.h"

namespace DataFlow {
namespace charm {

//! Default message for charm
typedef std::vector<char> Buffer;


class CharmTask : public CBase_CharmTask
{
public:

  //! Constructor which sets the callback and decodes destinations
  CharmTask(CharmTaskGraphBase cgraph, std::vector<Callback> callbacks);

  //! Default
  CharmTask(CkMigrateMessage *m) {}

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

}
}




#endif /* CHARM_CHARMTASK_H_ */
