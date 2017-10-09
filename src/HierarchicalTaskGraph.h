/*
 * Copyright (c) 2017 University of Utah 
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
