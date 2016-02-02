/*
 * TaskGraph.h
 *
 *  Created on: Jan 17, 2015
 *      Author: petruzza, bremer5
 */

#ifndef HIERARCHICAL_TASKGRAPH_H
#define HIERARCHICAL_TASKGRAPH_H

#include <vector>
#include <cstdio>

#include "TaskGraph.h"
#include "HierarchicalTask.h"

class HierarchicalTaskGraph : public TaskGraph{
public:
  HierarchicalTaskGraph(std::vector<Task> tasks, int32_t hfactor, int32_t vfactor){
    
    mHfactor = hfactor;
    mVfactor = vfactor;
    reduction_level = 0;
    
    for(uint32_t i=0; i < tasks.size(); i++){
      supertask.addSubTask(tasks[i]);
    }
    
    printf("%lu tasks inserted\n", supertask.mSubtasks.size());

    for(uint32_t i=0; i < supertask.mSubtasks.size(); i++){
      supertask.mSubtasks[i].resolveEdgesReduce(&supertask);
    }
  };
  
  std::vector<Task> localGraph(ControllerId id, const TaskMap* task_map) const{
    return std::vector<Task>(); // TODO adapt or ignore
  }
  
  void reduceAll(){
    while(supertask.mSubtasks.size() != mHfactor+mVfactor)
      reduce();
  }
  
  void reduce(){
    supertask.reduce(mHfactor, mVfactor);
    reduction_level++;
  }
  
  void expand(){
    supertask.expand(mHfactor, mVfactor);
    reduction_level--;
  }
  
  void expandAll(){
    while(reduction_level > 0)
      expand();
  }
  
  const std::vector<HierarchicalTask>& getTasks(){
    return supertask.mSubtasks;
  }
  
  ~HierarchicalTaskGraph(){}; // TODO

  int output_hierarchical_graph(FILE* output) const
  {
    const std::vector<HierarchicalTask>& mSubtasks = supertask.mSubtasks;
    
    fprintf(output,"digraph G {\n");
    
    std::vector<HierarchicalTask>::const_iterator tIt;
    std::vector<TaskId>::const_iterator it;
    
    for (tIt=mSubtasks.begin();tIt!=mSubtasks.end();tIt++) {
      fprintf(output,"%d [label=\"%d,%d\"]\n",tIt->id(),tIt->id(),tIt->callback());
      for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {
        if (*it != TNULL)
          fprintf(output,"%d -> %d\n",*it,tIt->id());
      }
    }
    
    fprintf(output,"}\n");
    return 1;
  }
  
  int output_hierarchical_graph(const HierarchicalTask& task, FILE* output) const
  {
    
    std::vector<HierarchicalTask>::const_iterator tIt;
    std::vector<TaskId>::const_iterator it;
    
    for (tIt=task.mSubtasks.begin();tIt!=task.mSubtasks.end();tIt++) {
      fprintf(output,"%d [label=\"%d,%d\"]\n",tIt->id(),tIt->id(),tIt->callback());
      for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {
        if (*it != TNULL)
          fprintf(output,"%d -> %d\n",*it,tIt->id());
      }
    
    }
    
    return 1;
  }

private:
  HierarchicalTask supertask;
  int32_t mHfactor;
  int32_t mVfactor;
  int32_t reduction_level;
  
//  std::vector<std::map<TaskId,TaskId> > incoming_map;
//  std::vector<std::map<TaskId,TaskId> > outgoing_map;
  
};

#endif /* HIERARCHICAL_TASKGRAPH_H */
