/*
 * HierarchicalTaskGraph.cpp
 *
 *  Created on: Jan 27, 2016
 *      Author: petruzza
 */

#include "HierarchicalTaskGraph.h"

using namespace DataFlow;

HierarchicalTaskGraph::HierarchicalTaskGraph(std::vector<Task> tasks, int32_t hfactor, int32_t vfactor){
  
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
}

int HierarchicalTaskGraph::output_hierarchical_graph(FILE* output) const{
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
