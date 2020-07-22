/*
 * ComposableTaskGraph.h
 *
 *  Created on: Jul 7, 2020
 *      Author: sshudler
 */
 
#ifndef COMPOSABLE_TASK_GRAPH_H__
#define COMPOSABLE_TASK_GRAPH_H__

#include <vector>
#include "TaskGraph.h"
#include "TaskGraphConnector.h"


namespace BabelFlow
{


class ComposableTaskGraph : public TaskGraph
{
public:
  ComposableTaskGraph( std::vector<TaskGraph*>& gr_vec, std::vector<TaskGraphConnector*>& gr_connectors );
  
  virtual ~ComposableTaskGraph() {}
  
  void init();
  
  //! Compute the fully specified tasks for the given controller
  virtual std::vector<Task> localGraph(ShardId id, const TaskMap* task_map) const override;

  //! Return the task for the given global task id
  virtual Task task(uint64_t gId) const override { assert(false); return Task(); }

  //! Return the global id of the given task id
  virtual uint64_t gId(TaskId tId) const override { return tId; }

  //! Return the total number of tasks (or some reasonable upper bound)
  virtual uint32_t size() const override;

  //! Serialize a task graph
  virtual Payload serialize() const override;

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize(Payload buffer) override;

private:
  Task task(const TaskId& tid) const;
  
  std::vector<TaskGraph*>           m_graphs;
  std::vector<TaskGraphConnector*>  m_connectors;

};  // class ComposableTaskGraph


}   // namespace BabelFlow


#endif    // #ifndef COMPOSABLE_TASK_GRAPH_H__
