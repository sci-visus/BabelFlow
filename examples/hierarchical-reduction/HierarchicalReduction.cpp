/*
 * Reduction.cpp
 *
 *  Created on: Dec 15, 2014
 *      Author: bremer5
 */

#include <cmath>

#include "HierarchicalReduction.h"

using namespace DataFlow;

HierarchicalReduction::HierarchicalReduction(uint32_t leafs, uint32_t valence) : TaskGraph(), mValence(valence)
{
  // Find the number of leafs that is a power of valence
  mLeafs = 1;
  mLevels = 0;
  
  while (mLeafs < leafs) {
    mLeafs *= mValence;
    mLevels++;
  }
  
  curr_id = mLeafs + 1;
  
}

void HierarchicalReduction::computeLevel0(){
  for(uint32_t i=0; i<leafCount(); i++){
    Task t;
    t.id(i);
    std::vector<TaskId> inleaf(1);
    inleaf[0] = TNULL;
    t.incoming() = inleaf;
    
    std::vector<std::vector<TaskId> > outgoing(1);
    outgoing[0].resize(1);
    outgoing[0][0] = TNULL;
    t.outputs() = outgoing;
    
    level0.push_back(t);
    inputs.push_back(t.id());
  }
}


void HierarchicalReduction::computeHierarchicalGraphReversed(Task& super_task){
  Task red_task;
  std::vector<TaskId> red_incoming(1);
  red_incoming.push_back(1);
  red_task.id(0);
  red_task.incoming() = red_incoming;
  
  computeHierarchicalGraphReversed(super_task, red_task, 0, 0);
  
}


void HierarchicalReduction::computeHierarchicalGraphReversed(Task& super_task, Task& out_red_task, uint32_t level, uint32_t offset)
{
  if(level == mLevels)
    return;
  
  std::vector<TaskId>& super_incoming = super_task.incoming();
  
  std::vector<Task> tasks(mValence);
  
  uint32_t n_split = super_incoming.size() / mValence;
  
  std::vector<std::vector<TaskId> > incoming(mValence);
  
  for(uint32_t j=0;j<super_incoming.size();j++){
    uint32_t task_n = j/n_split;
    uint32_t in_n = j%((task_n+1)*n_split);

    incoming[task_n].push_back(super_incoming[in_n]);
  }
  
  Task reduceTask;
  std::vector<TaskId> red_incoming(mValence);
  
  uint32_t red_id = generateId();
  
  for(uint32_t i=0;i<mValence;i++){
    tasks[i].id(generateId());
    
    std::vector<std::vector<TaskId> > outgoing(1);
    outgoing[0].resize(1);
    outgoing[0][0] = red_id;
    tasks[i].outputs() = outgoing;
    
    red_incoming[i] = tasks[i].id();
    
    if(level != mLevels-1)
      tasks[i].incoming() = incoming[i];
    else{
      std::vector<TaskId> inleaf(1);
      inleaf[0] = TNULL;
      tasks[i].incoming() = inleaf;
      inputs.push_back(tasks[i].id());
      alltasks.push_back(tasks[i]);
    }
 
  }
  
  reduceTask.id(red_id);
  reduceTask.incoming() = red_incoming;
  std::vector<std::vector<TaskId> > outgoing(1); // and one output
  outgoing[0].resize(1); // With one target
  outgoing[0][0] = out_red_task.id();//super_task.outputs()[0][0];
  reduceTask.outputs() = outgoing;
  out_red_task.incoming()[offset] = red_id;

//  printf("Level %d, tasks %lu, n_split %d\n", level, tasks.size(), n_split);
//  for(uint32_t i=0;i<mValence;i++){
//    printf("task %d in ", tasks[i].id());
//    for(uint32_t j=0;j<n_split;j++){
//      printf("%d ", incoming[i][j]);
//    }
//    printf("\n");
//    printf("task out %d\n", tasks[i].outputs()[0][0]);
//  }
//  
//  printf("reduce in ");
//  for(uint32_t j=0;j<mValence;j++){
//    printf("%d ", red_incoming[j]);
//  }
//  printf("\nreduce out %d\n", reduceTask.outputs()[0][0]);
  
  for(uint32_t i=0;i<mValence;i++){
    computeHierarchicalGraphReversed(tasks[i], reduceTask, level+1, i);
  }
  
  // Then we assign the outputs
  if (level != 0) {// If we are not the root
    reduceTask.callback(1);
  }
  else {  //If we are the root
    reduceTask.callback(2);
    std::vector<std::vector<TaskId> > root;
    reduceTask.outputs() = root;
    
  }
  
  alltasks.push_back(reduceTask);
  
}


void HierarchicalReduction::computeHierarchicalGraph(){
  if(level0.size() == 0)
    computeLevel0();
  
  computeHierarchicalGraph(level0, 0);
}

/**
 Legion: could take as input callback and the input region from the super level
 **/
void HierarchicalReduction::computeHierarchicalGraph(std::vector<Task>& level_tasks, uint32_t level)
{
  if(level == mLevels)
    return;
  
  // Create tasks for next level
  uint32_t n_tasks = level_tasks.size()/mValence;
  std::vector<Task> tasks(n_tasks);
  
  std::vector<std::vector<TaskId> > incoming(n_tasks);
  
  for(uint32_t i=0;i<n_tasks;i++){
    tasks[i].id(generateId());
    
    for(uint32_t j=0;j<mValence;j++){
      // Update super level outputs with new level tasks
      level_tasks[j+i*mValence].outputs()[0][0] = tasks[i].id();
      
      // Set inputs from super level
      incoming[i].push_back(level_tasks[j+i*mValence].id());
      
      // Add task to the graph
      alltasks.push_back(level_tasks[j+i*mValence]);
    }
    
    //
    // Legion: prepare new region for the OUTPUT of current layer
    //
    
    // Set default output TNULL current level's tasks
    std::vector<std::vector<TaskId> > outgoing(1);
    outgoing[0].resize(1);
    outgoing[0][0] = TNULL;
    tasks[i].outputs() = outgoing;

    // Set inputs for current level's tasks
    tasks[i].incoming() = incoming[i];
    
    if(level == mLevels-1){ // We are at the root level
      tasks[i].callback(2);
      std::vector<std::vector<TaskId> > root;
      tasks[i].outputs() = root;
      alltasks.push_back(tasks[i]);
    }
    else
      tasks[i].callback(1);
      
  }
  
//  printf("Level %d, tasks %lu\n", level, level_tasks.size());
//  for(uint32_t i=0;i<n_tasks;i++){
//    printf("task %d in ", level_tasks[i].id());
//    for(uint32_t j=0;j<mValence;j++){
//      if (level > 0)
//        printf("%d ", level_tasks[i].incoming()[j]);
//    }
//    printf("\n");
//    printf("task out %d\n", level_tasks[i].outputs()[0][0]);
//  }
  
  //
  // Legion: launch the tasks of the current layer using the INPUT region and the OUTPUT
  //
  
  // Compute the next level of tasks starting from the current
  if(level < mLevels)
    computeHierarchicalGraph(tasks, level+1);
  
}

int HierarchicalReduction::output_hierarchical_graph(FILE* output) const
{
  fprintf(output,"digraph G {\n");
  
  std::vector<Task>::const_iterator tIt;
  std::vector<TaskId>::const_iterator it;
  
  for (tIt=alltasks.begin();tIt!=alltasks.end();tIt++) {
    fprintf(output,"%d [label=\"%d,%d\"]\n",tIt->id(),tIt->id(),tIt->callback());
    for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {
      if (*it != TNULL)
        fprintf(output,"%d -> %d\n",*it,tIt->id());
    }
  }

  
  fprintf(output,"}\n");
  return 1;
}
