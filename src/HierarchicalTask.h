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
 /*   HierarchicalTask newt;//(task);
    newt.id(ht_global_id++);
    // check callback
    
    HierarchicalTask subt(task);
    newt.addSubTask(subt);
    addSubTask(newt);
  */
    
    addSubTask(HierarchicalTask(task));
  };
  
  void reduce(int32_t hfactor, int32_t vfactor);
  void expand(int32_t hfactor, int32_t vfactor);
  
    // Recursive subtask search
  TaskId isSubTask(TaskId tid, bool recursive = true);
  bool isInternalTask(TaskId tid, bool recursive = true);
  
//  void updateMapping();

  void checkUnresolvedReduce(std::vector<HierarchicalTask*>& new_leaves, HierarchicalTask* supertask);
  void checkUnresolvedReduce(HierarchicalTask* supertask);
  void checkUnresolvedExpand(HierarchicalTask* supertask);

  HierarchicalTask* getParentTask(TaskId tid, bool recursive = true);
  HierarchicalTask* getTask(TaskId tid, bool recursive = true);
  bool addSubTask(HierarchicalTask task, bool recursive = true);

  bool isLeafTask();
  
  std::vector<HierarchicalTask> mSubtasks;
  
  std::map<TaskId,TaskId> incoming_map;
  std::map<TaskId,TaskId> outgoing_map;

};


#endif /* HIERARCHICAL_TASK_H */
