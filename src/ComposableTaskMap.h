/*
 * ComposableTaskMap.h
 *
 *  Created on: Jul 7, 2020
 *      Author: sshudler
 */

#ifndef COMPOSABLE_TASK_MAP_H__
#define COMPOSABLE_TASK_MAP_H__

#include <vector>
#include "TaskGraph.h"


namespace BabelFlow
{

class ComposableTaskMap : public TaskMap
{
public:
  //! Default constructor
  ComposableTaskMap() = default; // added to accommodate ascent interface

  ComposableTaskMap( const std::vector<TaskMap*>& task_maps );
  
  virtual ~ComposableTaskMap() {}
  
  //! Return which controller is assigned to the given task
  virtual ShardId shard(TaskId id) const;

  //! Return the set of task assigned to the given controller
  virtual std::vector<TaskId> tasks(ShardId id) const;

private:

  std::vector<TaskMap*> m_taskMaps;
  
};      // class ComposableTaskMap

}       // namespace BabelFlow

#endif  // #ifndef COMPOSABLE_TASK_MAP_H__
