//
// Created by Jixian Li on 2019-07-27.
//

#ifndef BABELFLOW_MODTASKMAP_H
#define BABELFLOW_MODTASKMAP_H

#include <TaskGraph.h>

namespace BabelFlow {
  template<class BaseTaskMap>
  class ModTaskMap : public TaskMap {
    BaseTaskMap *baseMap;
    std::map<TaskId, ShardId> mShards;
    std::map<ShardId, std::vector<TaskId>> mTasks;
  public:
    explicit ModTaskMap(BaseTaskMap *m) {
      baseMap = m;
    }


    ShardId shard(TaskId id) const override {
      auto iter = mShards.find(id);
      if (iter != mShards.end()) return iter->second;
      else return baseMap->shard(id);
    }

    std::vector<TaskId> tasks(ShardId id) const override {
      auto iter = mTasks.find(id);
      std::vector<TaskId> modedTasks;
      if (iter != mTasks.end()) {
        modedTasks = iter->second;
      }
      std::vector<TaskId> oldTasks = baseMap->tasks(id);
      oldTasks.insert(oldTasks.end(), modedTasks.begin(), modedTasks.end());
      return oldTasks;
    }
  };
}

#endif //BABELFLOW_MODTASKMAP_H
