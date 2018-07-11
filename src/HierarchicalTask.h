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
#ifndef HIERARCHICAL_TASK_H
#define HIERARCHICAL_TASK_H

#include <vector>
#include <cstdio>
#include <map>

#include "Task.h"

namespace BabelFlow {
 
static TaskId ht_global_id = 1000000;

class HierarchicalTask : public Task{

private: HierarchicalTask(const Task& task) : Task(task){};
  
public:
  
  HierarchicalTask(){id(ht_global_id++);};
   
  void reduce(int32_t hfactor, int32_t vfactor);
  void expand(int32_t hfactor, int32_t vfactor);
  
  // Recursive subtask search
  bool isInternalTask(TaskId tid, bool recursive = true);

//  void checkUnresolvedReduce(std::vector<HierarchicalTask*>& new_leaves, HierarchicalTask* supertask);
  void resolveEdgesReduce(HierarchicalTask* supertask);
  void resolveEdgesExpand(HierarchicalTask* supertask);

  HierarchicalTask* getParentTask(TaskId tid, bool recursive = true);
  HierarchicalTask* getTask(TaskId tid, bool recursive = true);
  bool addSubTask(HierarchicalTask task, bool recursive = true);
  void addSubTask(Task task){
    addSubTask(HierarchicalTask(task));
  };

  bool isLeafTask();
  
  std::vector<HierarchicalTask> mSubtasks;
  
  std::map<TaskId,TaskId> incoming_map;
  std::map<TaskId,TaskId> outgoing_map;

};

}
#endif /* HIERARCHICAL_TASK_H */
