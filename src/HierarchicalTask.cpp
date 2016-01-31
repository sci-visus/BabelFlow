/*
 * HierarchicalTaskGraph.cpp
 *
 *  Created on: Jan 27, 2016
 *      Author: petruzza
 */

#include "HierarchicalTask.h"

// Recursive subtask search
TaskId HierarchicalTask::isSubTask(TaskId tid, bool recursive){
  for(uint32_t i=0; i < mSubtasks.size(); i++){
    if(mSubtasks[i].id() == tid){
      return id();
    }
    else if(recursive){
      TaskId parent = mSubtasks[i].isSubTask(tid);
      if(parent != TNULL)
        return parent;
    }
  }
  
  return TNULL;
}

//void HierarchicalTask::updateMapping(){
//  for(uint32_t sb=0; sb < mSubtasks.size(); sb++){
//    const std::vector<TaskId>& task_incoming = mSubtasks[sb].incoming();
//    const std::vector<std::vector<TaskId> >& task_outgoing = mSubtasks[sb].outputs();
//    
//    for(uint32_t i=0; i < task_incoming.size(); i++){
//      incoming_map[incoming_map.size()] = task_incoming[i];
//    }
//    
//    for(uint32_t i=0; i < task_outgoing.size(); i++){
//      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
//        outgoing_map[outgoing_map.size()] = task_outgoing[i][j];
//      }
//    }
//    
//  }
//}

//void HierarchicalTask::checkUnresolvedReduce(HierarchicalTask* supertask){
//  for(uint32_t sb=0; sb < mSubtasks.size(); sb++){
//    
//    const std::vector<TaskId>& task_incoming = mSubtasks[sb].incoming();
//    const std::vector<std::vector<TaskId> >& task_outgoing = mSubtasks[sb].outputs();
//    
//    for(uint32_t i=0; i < task_incoming.size(); i++){
//      TaskId parent = supertask->isSubTask(task_incoming[i]);
//      printf("%d in:look for %d found parent %d\n", mSubtasks[sb].id(), task_incoming[i], parent );
//      if(parent == id())
//        continue;
//      
//      if(parent != TNULL || task_incoming[i] == TNULL){ // has a parent task or is a leaf
////        mSubtasks[sb].incoming().push_back(parent);
//        printf("%d mapping in %d to %d\n", id(), mSubtasks[sb].incoming()[i], parent);
////        mSubtasks[sb].incoming_map[parent] = mSubtasks[sb].incoming()[i];
//        mSubtasks[sb].incoming()[i] = parent;
//        incoming_map[parent] = mSubtasks[sb].incoming()[i];
////        incoming().push_back(parent);
//        printf("%d: add in %d\n",mSubtasks[sb].id(), parent);
//        
//      }
//    
////      if(isSubTask(task_incoming[i]) == TNULL){
////        incoming().push_back(task_incoming[i]);
////      }
//
//    }
//    
//    for(uint32_t i=0; i < task_outgoing.size(); i++){
//      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
//        
//        TaskId parent = supertask->isSubTask(task_outgoing[i][j]);
//        printf("%d out:look for %d found parent %d\n", mSubtasks[sb].id(), task_outgoing[i][j], parent );
//        if(parent == id())
//          continue;
//        
//        if(parent != TNULL){
//          printf("%d mapping out %d to %d\n", id(), mSubtasks[sb].outputs()[i][j], parent);
//          outgoing_map[parent] = mSubtasks[sb].outputs()[i][j];
////          std::vector<TaskId> new_out(1);
////          new_out.resize(1);
////          new_out[0] = parent;
////          outputs().push_back(new_out);
////          mSubtasks[sb].outgoing_map[parent] = mSubtasks[sb].outputs()[i][j];
//          mSubtasks[sb].outputs()[i][j] = parent;
//        }
//        
////        if(isSubTask(task_outgoing[i][j]) == TNULL){
////          std::vector<TaskId> new_out(1);
////          new_out.resize(1);
////          new_out[0] = task_outgoing[i][j];
////          outputs().push_back(new_out);
////        }
//        
//      }
//      
//    }
//  }
//
//
////  for(uint32_t i=0; i < mSubtasks.size(); i++)
////    mSubtasks[i].checkUnresolved();
//
//}
//
//void HierarchicalTask::checkUnresolvedExpand(HierarchicalTask* supertask){
//
//  for(uint32_t sb=0; sb < mSubtasks.size(); sb++){
//    
//    const std::vector<TaskId>& task_incoming = mSubtasks[sb].incoming();
//    const std::vector<std::vector<TaskId> >& task_outgoing = mSubtasks[sb].outputs();
//    
//    for(uint32_t i=0; i < task_incoming.size(); i++){
//      if(mSubtasks[sb].incoming_map.find(task_incoming[i]) != mSubtasks[sb].incoming_map.end()){
//        printf("in mapping back %d to %d\n", mSubtasks[sb].incoming()[i], mSubtasks[sb].incoming_map[task_incoming[i]]);
//        mSubtasks[sb].incoming()[i] = mSubtasks[sb].incoming_map[task_incoming[i]];
//      }
//    }
//    
//    for(uint32_t i=0; i < task_outgoing.size(); i++){
//      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
//        if(mSubtasks[sb].outgoing_map.find(mSubtasks[sb].outputs()[i][j]) != mSubtasks[sb].outgoing_map.end()){
//          printf("out mapping back %d to %d\n", mSubtasks[sb].outputs()[i][j], mSubtasks[sb].outgoing_map[mSubtasks[sb].outputs()[i][j]]);
//          mSubtasks[sb].outputs()[i][j] = mSubtasks[sb].outgoing_map[mSubtasks[sb].outputs()[i][j]];
//        }
//
//      }
//      
//    }
//  }
//  
//  
//  //  for(uint32_t i=0; i < mSubtasks.size(); i++)
//  //    mSubtasks[i].checkUnresolved();
//  
//}


bool HierarchicalTask::addSubTask(HierarchicalTask task, bool recursive){
  if(isSubTask(task.id(), recursive) == TNULL){
     mSubtasks.push_back(task);
    return true;
//    printf("%d: insert %d\n\n", id(), task.id());
  }
  else return false;
  
     
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

void HierarchicalTask::reduce(int32_t hfactor, int32_t vfactor){
  
//  HierarchicalTask supertask;
  
  // Find the leaves
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
  
//  int32_t new_nodes = 0;
  
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
        if(sht.outputs()[0].size() > vf){
          //            bool found = false;
          for(uint32_t st=0; st < therest.size(); st++){
            if(therest[st].id() == sht.outputs()[0][vf]){
              leaft.outputs().push_back(therest[st].outputs()[0]);
              leaft.addSubTask(therest[st]);
              toRemove.push_back(therest[st].id());
              
//              therest.erase(therest.begin()+st); st--; // remove from therest
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
   // leaft.checkUnresolvedReduce(&leaft); /// TODO CONTINUE do mapping per level of reduction in the graph
//    if(addSubTask(leaft))
//      new_nodes++;
    
    addSubTask(leaft);
  }
  
//  printf("checking %d new nodes\n", new_nodes);
//  for(int32_t i= 0; i<new_nodes; i++){
//    HierarchicalTask& nt = mSubtasks[mSubtasks.size()-i-1];
//    nt.checkUnresolvedReduce(this);
//  }
  
  printf("created task with %lu leaves\n", mSubtasks.size());
  
  for(int32_t st=0; st < toRemove.size(); st++){
    for(int32_t t = 0; t < mSubtasks.size(); t++){
    
      if(mSubtasks[t].id() == toRemove[st]){
        mSubtasks.erase(mSubtasks.begin()+t); t--;
      }
      
    }
  }
  
  printf("new task size %lu \n", mSubtasks.size());
  
//  for(uint32_t st=0; st < supertask.mSubtasks.size(); st++){
//    supertask.mSubtasks[st].checkUnresolved(&supertask);
//  }
//  checkUnresolvedReduce(this);
  
}

void HierarchicalTask::expand(int32_t hfactor, int32_t vfactor){
  
//  HierarchicalTask supertask;
//  supertask.incoming_map = incoming_map;
//  supertask.outgoing_map = outgoing_map;
  
  // Find the leaves
  std::vector<HierarchicalTask> leaves;
  std::vector<HierarchicalTask> therest;
//  
//  for(uint32_t i=0; i < mSubtasks.size(); i++){
//      supertask.addSubTask(mSubtasks[i]);
//  }

  for(uint32_t i=0; i < mSubtasks.size(); i++){
    if(mSubtasks[i].isLeafTask()){
//      mSubtasks[i].checkUnresolvedExpand(this);
      leaves.push_back(mSubtasks[i]);
      
      mSubtasks.erase(mSubtasks.begin()+i); i--;
    }
    else
      therest.push_back(mSubtasks[i]);
  }
  
  printf("Found %lu leaves \n", leaves.size());
  
  // expand horizontally
  for(uint32_t i=0; i<leaves.size(); i++){
      
    HierarchicalTask& sht = leaves[i];

    // expand vertically
    for(uint32_t vf=0; vf < sht.mSubtasks.size(); vf++){
      //sht.addSubTask(sht.mSubtasks[vf]);
      
      this->addSubTask(sht.mSubtasks[vf], false);
      
    } // end expand vertically
  
  }
  
  printf("created task with %lu leaves\n", this->mSubtasks.size());
  
//  for(uint32_t st=0; st < therest.size(); st++){
//    this->addSubTask(therest[st]);
//  }
  
  printf("new task size %lu \n", this->mSubtasks.size());

//  this->checkUnresolvedExpand(this);
  
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