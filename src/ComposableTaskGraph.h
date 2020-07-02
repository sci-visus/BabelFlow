#ifndef COMPOSABLE_TASK_GRAPH_H__
#define COMPOSABLE_TASK_GRAPH_H__

#include "TaskGraph.h"


namespace BabelFlow
{


class ComposableTaskGraph : public TaskGraph
{
public:
  ComposableTaskGraph( TaskGraph* graph1, TaskGraph* graph2 );
  
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
  
  TaskGraph* m_graph1;
  TaskGraph* m_graph2;

};  // class ComposableTaskGraph


}   // namespace BabelFlow


#endif    // #ifndef COMPOSABLE_TASK_GRAPH_H__
