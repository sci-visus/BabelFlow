#ifndef COMPOSABLE_TASK_MAP_H__
#define COMPOSABLE_TASK_MAP_H__

#include "TaskGraph.h"


namespace BabelFlow
{

class ComposableTaskMap : public TaskMap
{
public:

  ComposableTaskMap( TaskMap* map1, TaskMap* map2 );
  
  virtual ~ComposableTaskMap() {}
  
  //! Return which controller is assigned to the given task
  virtual ShardId shard(TaskId id) const;

  //! Return the set of task assigned to the given controller
  virtual std::vector<TaskId> tasks(ShardId id) const;

private:
  TaskMap* m_TMap1;
  TaskMap* m_TMap2;
  
};      // class ComposableTaskMap

}       // namespace BabelFlow

#endif  // #ifndef COMPOSABLE_TASK_MAP_H__
