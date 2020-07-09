//
// Created by Jixian Li on 2019-07-27.
//

#ifndef BABELFLOW_MODTASKMAP_H
#define BABELFLOW_MODTASKMAP_H

#include "TaskGraph.h"

namespace BabelFlow
{
  class ModTaskMap : public TaskMap
  {

  public:
    ModTaskMap() = default;

    explicit ModTaskMap(TaskMap* m) : mBaseMap(m) {}
    
    void update(const PreProcessInputTaskGraph& modGraph)
    {
      // TODO make the input more generic than PreprocTaskGraph
      for (auto iter = modGraph.new_tids.begin(); iter != modGraph.new_tids.end(); ++iter) {
        auto new_tid = iter->second;
        auto new_shard = modGraph.new_sids.at(new_tid);
        mShards[new_tid] = new_shard;
        mTasks[new_shard].push_back(new_tid);
      }
    }

    ShardId shard(TaskId id) const override
    {
      auto iter = mShards.find(id);
      if (iter != mShards.end()) return iter->second;
      else return mBaseMap->shard(id);
    }

    std::vector<TaskId> tasks(ShardId id) const override
    {
      auto iter = mTasks.find(id);
      std::vector<TaskId> modedTasks;
      if (iter != mTasks.end()) {
        modedTasks = iter->second;
      }
      std::vector<TaskId> oldTasks = mBaseMap->tasks(id);
      oldTasks.insert(oldTasks.end(), modedTasks.begin(), modedTasks.end());
      return oldTasks;
    }

  private:
    // TODO: Create appropriate getter / setter methods
    TaskMap* mBaseMap;

    std::map<TaskId, ShardId> mShards;
    std::map<ShardId, std::vector<TaskId>> mTasks;
  };
}

#endif //BABELFLOW_MODTASKMAP_H
