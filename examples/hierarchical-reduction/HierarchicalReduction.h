/*
 * Reduction.h
 *
 *  Created on: Jan 16, 2016
 *      Author: petruzza, bremer5
 */

#ifndef HIERARCHICAL_REDUCTION_H_
#define HIERARCHICAL_REDUCTION_H_

#include <stdint.h>
#include <cmath>

#include "Definitions.h"
#include "TaskGraph.h"
#include "HierarchicalTaskGraph.h"
#include "Reduction.h"

class HierarchicalReduction : public TaskGraph{
public:
  HierarchicalReduction(uint32_t leafs, uint32_t valence);
  
  // These functions were
  void computeHierarchicalGraphReversed(Task& super_task);
  void computeHierarchicalGraphReversed(Task& super_task, Task& red_task, uint32_t level, uint32_t offset = 0);
  
  void computeHierarchicalGraph();
  void computeHierarchicalGraph(std::vector<Task>& level_tasks, uint32_t level);
  
  // this function doesn't make sense here, in this implementation we have only 1 graph builder
  std::vector<Task> localGraph(ControllerId id, const TaskMap* task_map) const{
    return alltasks;
  }
  
  int output_hierarchical_graph(FILE* output) const;
  
  int output_graph(ControllerId count, const TaskMap* task_map, FILE* output){
    return output_hierarchical_graph(output);
  }
  
  //! Return the total number of tasks
  TaskId size() const {return (pow(mValence,mLevels+1) - 1) / (mValence-1);}
  
  //! Return the number of leafs
  TaskId leafCount() const {return pow(mValence,mLevels);}
  
  uint32_t generateId(){
    return curr_id++;
  }
  
  std::vector<TaskId>& getInputsIds(){
    return inputs;
  };
  
  ~HierarchicalReduction(){};
  
protected:
  
  std::vector<Task> alltasks;
  std::vector<Task> level0;
  std::vector<TaskId> inputs;
  
  //! The number of leafs in the reduction
  uint32_t mLeafs;
  
  //! The fanout
  uint32_t mValence;
  
  //! The number of levels in the tree
  uint32_t mLevels;

private:
  void computeLevel0();
  
  uint32_t curr_id;

};


#endif /* HIERARCHICAL_REDUCTION_H_ */
