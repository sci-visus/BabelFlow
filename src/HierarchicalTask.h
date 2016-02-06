/*
 * TaskGraph.h
 *
 *  Created on: Jan 17, 2015
 *      Author: petruzza, bremer5
 */

#ifndef HIERARCHICAL_TASK_H
#define HIERARCHICAL_TASK_H

#include <vector>
#include <cstdio>
#include <map>

#include "Task.h"

namespace DataFlow {
 
static TaskId ht_global_id = 1000000;

class HierarchicalTask : public Task{

private: HierarchicalTask(const Task& task) : Task(task){};
  
public:
  
  HierarchicalTask(){id(ht_global_id++);};
   
  void reduce(int32_t hfactor, int32_t vfactor);
  void expand(int32_t hfactor, int32_t vfactor);
  
  // Recursive subtask search
  bool isInternalTask(TaskId tid, bool recursive = true);

//  void checkUnresolvedReduce(std::vector<HierarchicalTask*>& new_leaves, HierarchicalTask* supertask);
  void resolveEdgesReduce(HierarchicalTask* supertask);
  void resolveEdgesExpand(HierarchicalTask* supertask);

  HierarchicalTask* getParentTask(TaskId tid, bool recursive = true);
  HierarchicalTask* getTask(TaskId tid, bool recursive = true);
  bool addSubTask(HierarchicalTask task, bool recursive = true);
  void addSubTask(Task task){
    addSubTask(HierarchicalTask(task));
  };

  bool isLeafTask();
  
  std::vector<HierarchicalTask> mSubtasks;
  
  std::map<TaskId,TaskId> incoming_map;
  std::map<TaskId,TaskId> outgoing_map;

};

}
#endif /* HIERARCHICAL_TASK_H */
