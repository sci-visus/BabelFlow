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

#include "HierarchicalTask.h"

using namespace DataFlow;

bool HierarchicalTask::isInternalTask(TaskId tid, bool recursive){
  for(uint32_t i=0; i < mSubtasks.size(); i++){
    if(mSubtasks[i].id() == tid){
      return true;
    }
    else if(recursive){
      bool internal = mSubtasks[i].isInternalTask(tid);
      if(internal)
        return true;
    }
  }
  
  return false;
}

HierarchicalTask* HierarchicalTask::getParentTask(TaskId tid, bool recursive){
  for(uint32_t i=0; i < mSubtasks.size(); i++){
    if(mSubtasks[i].id() == tid){
      return this;
    }
    else if(recursive){
      HierarchicalTask* parent = mSubtasks[i].getParentTask(tid);
      if(parent != NULL)
        return parent;
    }
  }
  
  return NULL;
}

HierarchicalTask* HierarchicalTask::getTask(TaskId tid, bool recursive){
  for(uint32_t i=0; i < mSubtasks.size(); i++){
    if(mSubtasks[i].id() == tid){
      return &mSubtasks[i];
    }
    else if(recursive){
      HierarchicalTask* parent = mSubtasks[i].getParentTask(tid);
      if(parent != NULL)
        return parent;
    }
  }
  
  return NULL;
}

void HierarchicalTask::resolveEdgesReduce(HierarchicalTask* supertask){
  for(uint32_t sb=0; sb < mSubtasks.size(); sb++){
    
    const std::vector<TaskId>& task_incoming = mSubtasks[sb].incoming();
    const std::vector<std::vector<TaskId> >& task_outgoing = mSubtasks[sb].outputs();
    
    for(uint32_t i=0; i < task_incoming.size(); i++){
      
      if(task_incoming[i] == TNULL ){
        incoming().push_back(TNULL);
        continue;
      }
      
      // If parent not found or internal continue
      if(isInternalTask(task_incoming[i]))
        continue;
      
      HierarchicalTask* parent = supertask->getParentTask(task_incoming[i]);

//      printf("%d mapping in %d to %d\n", id(), mSubtasks[sb].incoming()[i], parent->id());
    
      // The incoming edge comes from outside, we need to map to the new parent
      TaskId swap_id = mSubtasks[sb].incoming()[i];
      incoming_map[parent->id()] = swap_id;
    
      // This supernode add the new external incoming edge from A
      incoming().push_back(parent->id());
    
      // We need now to change the corresponding outgoing edge from A to point to this new node
      HierarchicalTask* task = supertask->getTask(swap_id);
      
      for(uint32_t k=0; k < task->outputs().size(); k++){
        for(uint32_t z=0; z < task->outputs()[k].size(); z++){
          if(isInternalTask(task->outputs()[k][z])){
            task->outputs()[k][z] = id();
          }
        }
      }
      
//      printf("%d: add in %d\n",mSubtasks[sb].id(), parent->id());
      
    }

    for(uint32_t i=0; i < task_outgoing.size(); i++){
      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
     
        // If parent not found or internal continue
        if(isInternalTask(task_outgoing[i][j]))
          continue;
        
        HierarchicalTask* parent = supertask->getTask(task_outgoing[i][j]);
      
//        printf("%d mapping out %d to %d\n", id(), mSubtasks[sb].outputs()[i][j], parent->id());
      
        // The output is external we need to map it
        TaskId swap_id = mSubtasks[sb].outputs()[i][j];
        outgoing_map[parent->id()] = swap_id;
      
        // Add the external output to this supertask
        std::vector<TaskId> new_out(1);
        new_out.resize(1);
        new_out[0] = parent->id();
        outputs().push_back(new_out);
      
        // Need to map back the incoming task
        HierarchicalTask* task = supertask->getTask(swap_id);
        for(uint32_t k=0; k < task->incoming().size(); k++){
          if(isInternalTask(task->incoming()[k]))
            task->incoming()[k] = id();
        }
      
      }
      
    }
  }
  
}

void HierarchicalTask::resolveEdgesExpand(HierarchicalTask* supertask){

  for(uint32_t sb=0; sb < mSubtasks.size(); sb++){
    
    const std::vector<TaskId>& task_incoming = mSubtasks[sb].incoming();
    const std::vector<std::vector<TaskId> >& task_outgoing = mSubtasks[sb].outputs();
    
    for(uint32_t i=0; i < task_incoming.size(); i++){
      if(incoming_map.find(task_incoming[i]) != incoming_map.end()){
//        printf("in mapping back %d to %d\n", mSubtasks[sb].incoming()[i], incoming_map[task_incoming[i]]);
        TaskId swap_id = task_incoming[i];
        mSubtasks[sb].incoming()[i] = incoming_map[swap_id];
        
        // We need now to change the corresponding outgoing edge from A to point to this new node
        HierarchicalTask* task = supertask->getTask(swap_id);
        
        for(uint32_t k=0; k < task->outputs().size(); k++){
          for(uint32_t z=0; z < task->outputs()[k].size(); z++){
            if(task->outputs()[k][z] == id()){
              task->outputs()[k][z] = mSubtasks[sb].id();
            }
          }
        }
        
      }
    }
    
    for(uint32_t i=0; i < task_outgoing.size(); i++){
      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
        TaskId swap_id = mSubtasks[sb].outputs()[i][j];
        
        if(outgoing_map.find(swap_id) != outgoing_map.end()){
//          printf("out mapping back %d to %d\n", swap_id, mSubtasks[sb].outgoing_map[mSubtasks[sb].outputs()[i][j]]);
          mSubtasks[sb].outputs()[i][j] = outgoing_map[mSubtasks[sb].outputs()[i][j]];
          
          // Need to map back the incoming task
          HierarchicalTask* task = supertask->getTask(swap_id);
          for(uint32_t k=0; k < task->incoming().size(); k++){
            if(task->incoming()[k] == id())
              task->incoming()[k] = mSubtasks[sb].id();
          }
        }

      }
      
    }
  }
  
}


bool HierarchicalTask::addSubTask(HierarchicalTask task, bool recursive){
  if(!isInternalTask(task.id(), recursive)){ //isSubTask(task.id(), recursive) == TNULL){
     mSubtasks.push_back(task);
    return true;
//    printf("%d: insert %d\n\n", id(), task.id());
  }
  else return false;
  
}

void HierarchicalTask::reduce(int32_t hfactor, int32_t vfactor){
  
//  HierarchicalTask supertask;
  
  // Find the leaves
  
  int32_t new_nodes = 0;
    
  std::vector<HierarchicalTask> leaves;
  std::vector<HierarchicalTask> therest;
  
  std::vector<TaskId> toRemove;
  
  // Find the leaves
  for(uint32_t i=0; i < mSubtasks.size(); i++){
    if(mSubtasks[i].isLeafTask()){
      leaves.push_back(mSubtasks[i]);
      
      // the leaves has to be removed
      toRemove.push_back(mSubtasks[i].id());
    }
    else{
      // the rest of the nodes, to check connections
      therest.push_back(mSubtasks[i]);
    }
  }
  
  printf("Found %lu leaves \n", leaves.size());
  
  // collapse horizontally
  for(uint32_t i=0; i<leaves.size(); i+=hfactor){
    HierarchicalTask leaft;
    //TODO min
    for(uint32_t hf=0; hf < hfactor && hf < leaves.size(); hf++){
      uint32_t curr_t = i+hf;
      
      HierarchicalTask& sht = leaves[curr_t];
      
      HierarchicalTask* vert_child = &sht;
//      int32_t vf = 0;
//      while (vert_child != NULL && vf < vfactor){
        for(uint32_t iot=0; iot < vert_child->outputs().size(); iot++){
        for(uint32_t ot=0; ot < vert_child->outputs()[iot].size(); ot++){
          HierarchicalTask* child = getTask(vert_child->outputs()[iot][ot]);
          if(child != NULL){
            leaft.addSubTask(*child);
            toRemove.push_back(child->id());
          }
//          vert_child = child;
//          vf++;
        }
        }

        
//      }
      
      toRemove.push_back(sht.id());
      
      leaft.addSubTask(sht);
    } // end collapse horizontally

//    printf("grouped %lu nodes\n", leaft.mSubtasks.size());
    
    if(addSubTask(leaft))
      new_nodes++;
  }
  
  // Remove all the collapsed tasks
  
  for(int32_t st=0; st < toRemove.size(); st++){
    for(int32_t t = 0; t < mSubtasks.size(); t++){
    
      if(mSubtasks[t].id() == toRemove[st]){
//        printf("removed %u\n", mSubtasks[t].id());
        mSubtasks.erase(mSubtasks.begin()+t); t--;
        
        break;
      }
    }
  }
  
  printf("New graph size %lu \n", mSubtasks.size());
  
  // Resolve edges
  for(uint32_t i=0; i < new_nodes; i++){
    HierarchicalTask& nt = mSubtasks[mSubtasks.size()-i-1];
    nt.resolveEdgesReduce(this);
  }
  
}

void HierarchicalTask::expand(int32_t hfactor, int32_t vfactor){
  
  // Find the leaves
  std::vector<HierarchicalTask> leaves;

  for(uint32_t i=0; i < mSubtasks.size(); i++){
    if(mSubtasks[i].isLeafTask()){
      leaves.push_back(mSubtasks[i]);
      
      mSubtasks.erase(mSubtasks.begin()+i); i--;
    }
  }
  
  printf("Found %lu leaves \n", leaves.size());
  
  int32_t new_nodes = 0;
  // Add subtasks to the graph
  for(uint32_t i=0; i<leaves.size(); i++){
      
    HierarchicalTask& sht = leaves[i];

    for(uint32_t vf=0; vf < sht.mSubtasks.size(); vf++){
      
      if(this->addSubTask(sht.mSubtasks[vf], false))
        new_nodes++;
    }
  }
  
  for(uint32_t i=0; i < leaves.size(); i++){
    leaves[i].resolveEdgesExpand(this);
  }
  
  printf("New graph size %lu \n", this->mSubtasks.size());

}

bool HierarchicalTask::isLeafTask(){
  for(uint32_t j=0; j < incoming().size(); j++){
    if(incoming()[j] == TNULL)
      return true;
  }
  
  for(uint32_t i=0; i < mSubtasks.size(); i++){
    for(uint32_t j=0; j < mSubtasks[i].incoming().size(); j++){
      return mSubtasks[i].isLeafTask();
    }
  }
  
  return false;
}