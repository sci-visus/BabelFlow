/*
 * BinarySwapTaskMap.cpp
 *
 *  Created on: Sep 3, 2016
 *      Author: spetruzza
 */

#include <cstdio>
#include "BinarySwapTaskMap.h"


using namespace BabelFlow;

BinarySwapTaskMap::BinarySwapTaskMap(ShardId controller_count,const BinarySwap* task_graph) :
mControllerCount(controller_count), mTaskGraph(task_graph)
{
}

ShardId BinarySwapTaskMap::shard(TaskId id) const
{
  ShardId cId = id % mControllerCount;
  //printf("base id: %d con count : %d cid : %d\n", base_id, mControllerCount, cId);
  return cId;
}

std::vector<TaskId> BinarySwapTaskMap::tasks(ShardId id) const
{
  std::vector<TaskId> tasks;
  uint8_t k;

  TaskId leaf_count = mTaskGraph->lvlOffset()[1];

  // For all leafs assigned to this controller
  for (TaskId leaf=id;leaf<leaf_count;leaf+=mControllerCount) {
    tasks.push_back(leaf); // Take the leaf

    for (k=1;k<=mTaskGraph->rounds();k++) {
      tasks.push_back(mTaskGraph->roundId(leaf,k));
    }

  } // end-for all leafs

  // printf("Controller id: %d Tasks: ", id);
  // for (int i=0; i<tasks.size(); i++)
  //   printf(" %d ", tasks[i]);
  // printf("\n");
  
  return tasks;
}
