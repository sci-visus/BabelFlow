/*
 * SingleTaskGraph.h
 *
 *  Created on: Jul 16, 2020
 *      Author: sshudler
 */

#ifndef BFLOW_RED_SINGLE_TASK_GRAPH_H_
#define BFLOW_RED_SINGLE_TASK_GRAPH_H_

#include <vector>

#include "../TaskGraph.h"



namespace BabelFlow
{

class SingleTaskGraph : public TaskGraph
{
public:

  SingleTaskGraph() {}

  //! Destructor
  virtual ~SingleTaskGraph() {}

  //! Compute the fully specified tasks for the given controller
  virtual std::vector<Task> localGraph(ShardId id, const TaskMap* task_map) const override;

  //! Return the task for the given global task id
  virtual Task task(uint64_t gId) const override;

  //! Return the global id of the given task id
  virtual uint64_t gId(TaskId tId) const override { return tId; }

  //! Return the total number of tasks (or some reasonable upper bound)
  virtual uint32_t size() const override { return 1; }

  //! Serialize a task graph
  virtual Payload serialize() const override;

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize(Payload buffer) override;

};  // end class SingleTaskGraph

}   // end namespace BabelFlow


#endif    // #ifndef BFLOW_RED_SINGLE_TASK_GRAPH_H_
