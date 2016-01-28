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

static TaskId ht_global_id = 1000;

class HierarchicalTask : public Task{

private: HierarchicalTask(const Task& task) : Task(task){};
  
public:
  
  HierarchicalTask(){id(ht_global_id++);};
   
  void addSubTask(Task task){
    HierarchicalTask newt(task);
    newt.id(ht_global_id++);
    
    HierarchicalTask subt(task);
    newt.addSubTask(subt);
    addSubTask(newt);
  };
  
  HierarchicalTask reduce(int32_t hfactor, int32_t vfactor);
  HierarchicalTask expand(int32_t hfactor, int32_t vfactor);
  
    // Recursive subtask search
  TaskId isSubTask(TaskId tid);
  
  void updateMapping();

  void checkUnresolvedReduce(HierarchicalTask* supertask);
  void checkUnresolvedExpand(HierarchicalTask* supertask);

  void addSubTask(HierarchicalTask task);

  bool isLeafTask();
  
  std::vector<HierarchicalTask> mSubtasks;
  
  std::map<TaskId,TaskId> incoming_map;
  std::map<TaskId,TaskId> outgoing_map;

};


#endif /* HIERARCHICAL_TASK_H */
