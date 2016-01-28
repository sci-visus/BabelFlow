/*
 * HierarchicalTaskGraph.cpp
 *
 *  Created on: Jan 27, 2016
 *      Author: petruzza
 */

#include "HierarchicalTask.h"

// Recursive subtask search
TaskId HierarchicalTask::isSubTask(TaskId tid){
  for(uint32_t i=0; i < mSubtasks.size(); i++){
    if(mSubtasks[i].id() == tid){
      return id();
    }
    else{
      TaskId parent = mSubtasks[i].isSubTask(tid);
      if(parent != TNULL)
        return parent;
    }
  }
  
  return TNULL;
}

void HierarchicalTask::updateMapping(){
  for(uint32_t sb=0; sb < mSubtasks.size(); sb++){
    const std::vector<TaskId>& task_incoming = mSubtasks[sb].incoming();
    const std::vector<std::vector<TaskId> >& task_outgoing = mSubtasks[sb].outputs();
    
    for(uint32_t i=0; i < task_incoming.size(); i++){
      incoming_map[incoming_map.size()] = task_incoming[i];
    }
    
    for(uint32_t i=0; i < task_outgoing.size(); i++){
      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
        outgoing_map[outgoing_map.size()] = task_outgoing[i][j];
      }
    }
    
  }
}

void HierarchicalTask::checkUnresolvedReduce(HierarchicalTask* supertask){
  for(uint32_t sb=0; sb < mSubtasks.size(); sb++){
    
    const std::vector<TaskId>& task_incoming = mSubtasks[sb].incoming();
    const std::vector<std::vector<TaskId> >& task_outgoing = mSubtasks[sb].outputs();
    
    for(uint32_t i=0; i < task_incoming.size(); i++){
      TaskId parent = supertask->isSubTask(task_incoming[i]);
      printf("%d in:look for %d found parent %d\n", mSubtasks[sb].id(), task_incoming[i], parent );
      if(parent == id())
        continue;
      
      if(parent != TNULL || task_incoming[i] == TNULL){ // has a parent task or is a leaf
//        mSubtasks[sb].incoming().push_back(parent);
        printf("%d mapping in %d to %d\n", id(), mSubtasks[sb].incoming()[i], parent);
        incoming_map[parent] = mSubtasks[sb].incoming()[i];
        mSubtasks[sb].incoming()[i] = parent;
        printf("%d: add in %d\n",mSubtasks[sb].id(), parent);
        
      }
    
//      if(isSubTask(task_incoming[i]) == TNULL){
//        incoming().push_back(task_incoming[i]);
//      }

    }
    
    for(uint32_t i=0; i < task_outgoing.size(); i++){
      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
        
        TaskId parent = supertask->isSubTask(task_outgoing[i][j]);
        printf("%d out:look for %d found parent %d\n", mSubtasks[sb].id(), task_outgoing[i][j], parent );
        if(parent == id())
          continue;
        
        if(parent != TNULL){
          printf("%d mapping out %d to %d\n", id(), mSubtasks[sb].outputs()[i][j], parent);
          outgoing_map[parent] = mSubtasks[sb].outputs()[i][j];
          mSubtasks[sb].outputs()[i][j] = parent;
        }
        
//        if(isSubTask(task_outgoing[i][j]) == TNULL){
//          std::vector<TaskId> new_out(1);
//          new_out.resize(1);
//          new_out[0] = task_outgoing[i][j];
//          outputs().push_back(new_out);
//        }
        
      }
      
    }
  }


//  for(uint32_t i=0; i < mSubtasks.size(); i++)
//    mSubtasks[i].checkUnresolved();

}

void HierarchicalTask::checkUnresolvedExpand(HierarchicalTask* supertask){
  for(uint32_t sb=0; sb < mSubtasks.size(); sb++){
    
    const std::vector<TaskId>& task_incoming = mSubtasks[sb].incoming();
    const std::vector<std::vector<TaskId> >& task_outgoing = mSubtasks[sb].outputs();
    
    for(uint32_t i=0; i < task_incoming.size(); i++){
      if(supertask->incoming_map.find(task_incoming[i]) != supertask->incoming_map.end()){
        printf("in mapping back %d to %d\n", mSubtasks[sb].incoming()[i], supertask->incoming_map[task_incoming[i]]);
        mSubtasks[sb].incoming()[i] = supertask->incoming_map[task_incoming[i]];
      }
    }
    
    for(uint32_t i=0; i < task_outgoing.size(); i++){
      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
        if(supertask->outgoing_map.find(mSubtasks[sb].outputs()[i][j]) != supertask->outgoing_map.end()){
          printf("out mapping back %d to %d\n", mSubtasks[sb].outputs()[i][j], supertask->outgoing_map[mSubtasks[sb].outputs()[i][j]]);
          mSubtasks[sb].outputs()[i][j] = supertask->outgoing_map[mSubtasks[sb].outputs()[i][j]];
        }

      }
      
    }
  }
  
  
  //  for(uint32_t i=0; i < mSubtasks.size(); i++)
  //    mSubtasks[i].checkUnresolved();
  
}


void HierarchicalTask::addSubTask(HierarchicalTask task){
  if(isSubTask(task.id()) == TNULL){
     mSubtasks.push_back(task);
//    printf("%d: insert %d\n\n", id(), task.id());
  }
  
     
  //    subtask_map[task->id()] = id();
  
  //    const std::vector<TaskId>& task_incoming = task.incoming();
  //    const std::vector<std::vector<TaskId> >& task_outgoing = task.outputs();
  //
  //    for(uint32_t i=0; i < task_incoming.size(); i++){
  //      TaskId parent = isSubTask(task_incoming[i]);
  //      if(parent == TNULL)
  //        unresolved_in.push_back(task_incoming[i]);
  //      else
  //        incoming().push_back(task_incoming[i]);
  //    }
  //
  //    for(uint32_t i=0; i < task_outgoing.size(); i++){
  //      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
  //
  //        TaskId parent = isSubTask(task_outgoing[0][i]);
  //        if(parent == TNULL)
  //          unresolved_out.push_back(task_outgoing[i][j]);
  //        else{
  //          //assume 1
  //          std::vector<TaskId> new_out(1);
  //          new_out.resize(1);
  //          new_out[0] = task_outgoing[i][j];
  //          outputs().push_back(new_out);
  //        }
  //      }
  //    }
  
  
}

HierarchicalTask HierarchicalTask::reduce(int32_t hfactor, int32_t vfactor){
  
  HierarchicalTask supertask;
  
  // Find the leaves
  std::vector<HierarchicalTask> leaves;
  std::vector<HierarchicalTask> therest;
  
  for(uint32_t i=0; i < mSubtasks.size(); i++){
    if(mSubtasks[i].isLeafTask()){
      leaves.push_back(mSubtasks[i]);
    }
    else
      therest.push_back(mSubtasks[i]);
  }
  
  printf("Found %lu leaves \n", leaves.size());
  
  // collapse horizontally
  for(uint32_t i=0; i<leaves.size(); i+=hfactor){
    HierarchicalTask leaft;
    
    for(int32_t hf=0; hf < hfactor; hf++){
      uint32_t curr_t = i+hf;
      
      HierarchicalTask& sht = leaves[curr_t];
      
      // leaf hierarchical task
//      sht.incoming().push_back(TNULL);
      
      // collapse vertically
      for(uint32_t vf=0; vf < vfactor; vf++){
//        if(leaves[curr_t].outputs().size()>0){
        if(leaves[curr_t].outputs()[0].size() > vf){
          //            bool found = false;
          for(uint32_t st=0; st < therest.size(); st++){
            if(therest[st].id() == sht.outputs()[0][vf]){
              leaft.addSubTask(therest[st]);
              
              leaft.outputs().push_back(therest[st].outputs()[0]);
              
              therest.erase(therest.begin()+st); st--; // remove from therest
              break;
            }
            
//          }
        }
          
        } // end if any output
      } // end collapse vertically
      
      leaft.addSubTask(sht);
    } // end collapse horizontally

    printf("grouped %lu nodes\n", leaft.mSubtasks.size());
//    leaft.checkUnresolved(this);
    leaft.incoming().push_back(TNULL);
//    leaft.outputs().push_back(std::vector<TaskId>());
    supertask.addSubTask(leaft);
    
  }
  
  printf("created task with %lu leaves\n", supertask.mSubtasks.size());
  
  for(uint32_t st=0; st < therest.size(); st++){
    supertask.addSubTask(therest[st]);
  }
  
  printf("new task size %lu \n", supertask.mSubtasks.size());
  
//  for(uint32_t st=0; st < supertask.mSubtasks.size(); st++){
//    supertask.mSubtasks[st].checkUnresolved(&supertask);
//  }
  supertask.checkUnresolvedReduce(&supertask);
  return supertask;
  
}

HierarchicalTask HierarchicalTask::expand(int32_t hfactor, int32_t vfactor){
  
  HierarchicalTask supertask;
  supertask.incoming_map = incoming_map;
  supertask.outgoing_map = outgoing_map;
  
  // Find the leaves
  std::vector<HierarchicalTask> leaves;
  std::vector<HierarchicalTask> therest;
  
  for(uint32_t i=0; i < mSubtasks.size(); i++){
    if(mSubtasks[i].isLeafTask()){
      leaves.push_back(mSubtasks[i]);
    }
    else
      therest.push_back(mSubtasks[i]);
  }
  
  printf("Found %lu leaves \n", leaves.size());
  
  // collapse horizontally
  for(uint32_t i=0; i<leaves.size(); i+=hfactor){
    HierarchicalTask leaft;
    
    for(int32_t hf=0; hf < hfactor; hf++){
      uint32_t curr_t = i+hf;
      
      HierarchicalTask& sht = leaves[curr_t];
    
      // expand vertically
      for(uint32_t vf=0; vf < sht.mSubtasks.size(); vf++){
        //leaft.addSubTask(sht.mSubtasks[vf]);
        supertask.addSubTask(sht.mSubtasks[vf]);
      
      } // end collapse vertically
    } // end collapse horizontally
    
  }
  
  printf("created task with %lu leaves\n", supertask.mSubtasks.size());
  
  for(uint32_t st=0; st < therest.size(); st++){
    supertask.addSubTask(therest[st]);
  }
  
  printf("new task size %lu \n", supertask.mSubtasks.size());

  supertask.checkUnresolvedExpand(&supertask);
  return supertask;
  
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