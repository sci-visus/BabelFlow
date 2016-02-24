/*
 * ModuloMap.cpp
 *
 *  Created on: Dec 14, 2014
 *      Author: bremer5
 */

#include "ModuloMap.h"

ModuloMap::ModuloMap(ShardId shard_count, TaskId task_count) : TaskMap(),
mShardCount(shard_count), mTaskCount(task_count)
{
}

ShardId ModuloMap::shard(TaskId id) const
{
  return (id % mShardCount);
}

std::vector<TaskId> ModuloMap::tasks(ShardId id) const
{
  std::vector<TaskId> back;

  TaskId t = id;

  while (t < mTaskCount) {
    back.push_back(t);

    t += mShardCount;
  }

  return back;
}


