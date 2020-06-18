
#include <cstdio>
#include "RadixKExchangeTaskMap.h"


using namespace BabelFlow;

RadixKExchangeTaskMap::RadixKExchangeTaskMap(ShardId controller_count, const RadixKExchange* task_graph) 
 : mControllerCount(controller_count), mTaskGraph(task_graph)
{
}

ShardId RadixKExchangeTaskMap::shard(TaskId id) const
{
  TaskId base_id = mTaskGraph->baseId(id);
  ShardId cId = base_id % mControllerCount;
  //printf("base id: %d con count : %d cid : %d\n", base_id, mControllerCount, cId);
  return cId;
}

std::vector<TaskId> RadixKExchangeTaskMap::tasks(ShardId id) const
{
  std::vector<TaskId> tasks;
  TaskId leaf_count = mTaskGraph->lvlOffset()[1];

  // For all leafs assigned to this controller
  for (TaskId leaf = id; leaf < leaf_count; leaf += mControllerCount) 
  {
    tasks.push_back(leaf); // Take the leaf

    for (uint32_t k = 1; k <= mTaskGraph->totalLevels(); ++k)
      tasks.push_back(mTaskGraph->idAtLevel(leaf,k));
  } // end-for all leafs

  // printf("Controller id: %d Tasks: ", id);
  // for (int i=0; i<tasks.size(); i++)
  //   printf(" %d ", tasks[i]);
  // printf("\n");
  
  return tasks;
}
