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
    
    for(uint32_t i=0; i < tasks.size(); i++){
      supertask.addSubTask(tasks[i]);
    }
    
//    for(uint32_t i=0; i < supertask.mSubtasks.size(); i++){
//      printf("ins task %d\n", supertask.mSubtasks[i].id());
//    }
    
    printf("%lu tasks inserted\n", supertask.mSubtasks.size());
    supertask.checkUnresolvedReduce(&supertask);
    
  };
  
  std::vector<Task> localGraph(ControllerId id, const TaskMap* task_map) const{
    return std::vector<Task>();
  }
  
  void reduce(){
    supertask = supertask.reduce(mHfactor, mVfactor);
  }
  
  void expand(){
    supertask = supertask.expand(mHfactor, mVfactor);
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
    
//    fprintf(output,"%d [label=\"%d,%d\"]\n",supertask.id(),supertask.id(),supertask.callback());
//    for (it=supertask.incoming().begin();it!=supertask.incoming().end();it++) {
//      if (*it != TNULL)
//        fprintf(output,"%d -> %d\n",*it,supertask.id());
//    }
//    
//    for (tIt=mSubtasks.begin();tIt!=mSubtasks.end();tIt++) {
//      fprintf(output,"%d [label=\"%d,%d\"]\n",tIt->id(),tIt->id(),tIt->callback());
//      for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {
//        if (*it != TNULL)
//          fprintf(output,"%d -> %d\n",*it,tIt->id());
//      }
//      
//      HierarchicalTask t = *tIt;
//      output_hierarchical_graph(t, output);
//    }
    
    
    fprintf(output,"}\n");
    return 1;
  }
  
  int output_hierarchical_graph(HierarchicalTask& task, FILE* output) const
  {
    
    std::vector<HierarchicalTask>::const_iterator tIt;
    std::vector<TaskId>::const_iterator it;
    
//    fprintf(output,"%d [label=\"%d,%d\"]\n",task.id(),task.id(),task.callback());
//    for (it=task.incoming().begin();it!=task.incoming().end();it++) {
//      if (*it != TNULL)
//        fprintf(output,"%d -> %d\n",*it,task.id());
//    }
    
    for (tIt=task.mSubtasks.begin();tIt!=task.mSubtasks.end();tIt++) {
      fprintf(output,"%d [label=\"%d,%d\"]\n",tIt->id(),tIt->id(),tIt->callback());
      for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {
        if (*it != TNULL)
          fprintf(output,"%d -> %d\n",*it,tIt->id());
      }
      
//      HierarchicalTask t = *tIt;
//      output_hierarchical_graph(t, output);
    }
    
    return 1;
  }

private:
  HierarchicalTask supertask;
  int32_t mHfactor;
  int32_t mVfactor;
  
};

#endif /* HIERARCHICAL_TASKGRAPH_H */
