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

namespace DataFlow{

class HierarchicalTaskGraph : public TaskGraph{
public:
  HierarchicalTaskGraph(std::vector<Task> tasks, int32_t hfactor, int32_t vfactor);
  
  std::vector<Task> localGraph(ShardId id, const TaskMap* task_map) const{
    return std::vector<Task>(); // TODO adapt or ignore
  }
  
  // TODO this condition not always holds (i.e. for broadcast)
  void reduceAll(){
    while(supertask.mSubtasks.size() != mHfactor+mVfactor)
      reduce();
    
    supertask.resolveEdgesReduce(&supertask);
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
  
  const HierarchicalTask& getSuperTask(){
    return supertask;
  }
  
  ~HierarchicalTaskGraph(){}; // TODO

  int output_hierarchical_graph(FILE* output) const;

private:
  HierarchicalTask supertask;
  int32_t mHfactor;
  int32_t mVfactor;
  int32_t reduction_level;
  
};
  
}

#endif /* HIERARCHICAL_TASKGRAPH_H */
