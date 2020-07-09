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

#include "HierarchicalTaskGraph.h"

using namespace BabelFlow;

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
    TaskId::InnerTaskId tid = tIt->id();
    fprintf(output, "%d [label=\"%d,%d\"]\n", tid, tid, tIt->callback());
    for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {
      if (*it != TNULL)
        fprintf(output, "%d -> %d\n", TaskId::InnerTaskId(*it), tid);
    }
  }
  
  fprintf(output,"}\n");
  return 1;
}
