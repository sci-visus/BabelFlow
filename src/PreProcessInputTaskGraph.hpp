#include <utility>

//
// Created by Li, Jixian on 2019-07-25.
//

#ifndef BABELFLOW_DOWNSIZEGHOSTTASKGRAPH_HPP
#define BABELFLOW_DOWNSIZEGHOSTTASKGRAPH_HPP

#include "TaskGraph.h"
#include <map>

namespace BabelFlow
{


class PreProcessInputTaskGraph : public TaskGraph
{
public:
  enum TaskCB { PRE_PROC_TASK_CB = 9 };

  PreProcessInputTaskGraph() = default;

  PreProcessInputTaskGraph(ShardId count, TaskGraph* g, TaskMap* m);

  std::vector<Task> localGraph(ShardId id, const TaskMap* task_map) const override;

  Task task(uint64_t gId) const override;

  uint64_t gId(TaskId tId) const override 
  { return ((tId <= maxTid) ? mGraph->gId(tId) : maxGid + (tId - maxTid)); }

  uint32_t size() const override { return mGraph->size() + new_tids.size(); }

  Payload serialize() const override;

  void deserialize(Payload payload) override;
    
  TaskId gid2otid(const uint64_t &gid) const;
    
  template<class K, class V>
  Payload serializeMap(const std::map<K, V> &m) const;

  template<class K, class V>
  std::map<K, V> deserializeMap(char *buffer);
    
  // TODO: create proper getter / setter methods
  ShardId n_controllers;
  TaskGraph* mGraph;
  std::map<TaskId, ShardId> new_sids;
  std::map<TaskId, TaskId> new_tids;
  std::map<TaskId, uint64_t> new_gids;
  std::map<uint64_t, TaskId> old_g2t;
  std::map<TaskId, Task> old_tasks;
  std::vector<std::vector<TaskId> > data_tasks;
  uint64_t maxGid = 0;
  TaskId maxTid = 0;
};


}


#endif //BABELFLOW_DOWNSIZEGHOSTTASKGRAPH_HPP
