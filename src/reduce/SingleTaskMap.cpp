/*
 * SingleTaskMap.cpp
 *
 *  Created on: Jul 16, 2020
 *      Author: sshudler
 */

#include "SingleTaskMap.h"


namespace BabelFlow
{

//-----------------------------------------------------------------------------

SingleTaskMap::SingleTaskMap(ShardId controller_count, const SingleTaskGraph* task_graph)
 : mControllerCount(controller_count), mTaskGraph(task_graph)
{
}

//-----------------------------------------------------------------------------

ShardId SingleTaskMap::shard(TaskId id) const
{
  return 0;   // The single task in this graph should be executed only by the root rank
}

//-----------------------------------------------------------------------------

std::vector<TaskId> SingleTaskMap::tasks(ShardId id) const
{
  std::vector<TaskId> tasks;

  tasks.push_back( TaskId( 1 ) );    // Only one task
  
  return tasks;
}

//-----------------------------------------------------------------------------

}   // end namespace BabelFlow
