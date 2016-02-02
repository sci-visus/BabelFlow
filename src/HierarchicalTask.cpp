/*
 * HierarchicalTask.cpp
 *
 *  Created on: Jan 27, 2016
 *      Author: petruzza
 */

#include "HierarchicalTask.h"

// Recursive subtask search
//TaskId HierarchicalTask::isSubTask(TaskId tid, bool recursive){
//  for(uint32_t i=0; i < mSubtasks.size(); i++){
//    if(mSubtasks[i].id() == tid){
//      return id();
//    }
//    else if(recursive){
//      TaskId parent = mSubtasks[i].isSubTask(tid);
//      if(parent != TNULL)
//        return parent;
//    }
//  }
//  
//  return TNULL;
//}

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
//      for(uint32_t j=0; j < mSubtasks[i].mSubtasks.size(); j++){
//        HierarchicalTask* parent = mSubtasks[i].mSubtasks[j].getParentTask(tid);
//        if(parent != NULL)
//          return &mSubtasks[i];
//      }
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

void HierarchicalTask::checkUnresolvedReduce(HierarchicalTask* supertask){
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

      printf("%d mapping in %d to %d\n", id(), mSubtasks[sb].incoming()[i], parent->id());
    
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
      
      printf("%d: add in %d\n",mSubtasks[sb].id(), parent->id());
      
    }

    for(uint32_t i=0; i < task_outgoing.size(); i++){
      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
     
        // If parent not found or internal continue
        if(isInternalTask(task_outgoing[i][j]))
          continue;
        
        HierarchicalTask* parent = supertask->getTask(task_outgoing[i][j]);
      
        printf("%d mapping out %d to %d\n", id(), mSubtasks[sb].outputs()[i][j], parent->id());
      
        // The output is external we need to map it
        TaskId swap_id = mSubtasks[sb].outputs()[i][j];
        outgoing_map[parent->id()] = swap_id;
      
        // Add the external output to this supertask
        std::vector<TaskId> new_out(1);
        new_out.resize(1);
        new_out[0] = parent->id();
        outputs().push_back(new_out);
        //mSubtasks[sb].outputs()[i][j] = parent->id();
      
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

void HierarchicalTask::checkUnresolvedExpand(HierarchicalTask* supertask){

  for(uint32_t sb=0; sb < mSubtasks.size(); sb++){
    
    const std::vector<TaskId>& task_incoming = mSubtasks[sb].incoming();
    const std::vector<std::vector<TaskId> >& task_outgoing = mSubtasks[sb].outputs();
    
    for(uint32_t i=0; i < task_incoming.size(); i++){
      if(incoming_map.find(task_incoming[i]) != incoming_map.end()){
        printf("in mapping back %d to %d\n", mSubtasks[sb].incoming()[i], incoming_map[task_incoming[i]]);
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
          printf("out mapping back %d to %d\n", swap_id, mSubtasks[sb].outgoing_map[mSubtasks[sb].outputs()[i][j]]);
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
  
  int32_t new_nodes = 0;
  
  // collapse horizontally
  for(uint32_t i=0; i<leaves.size(); i+=hfactor){
    HierarchicalTask leaft;
    
    for(uint32_t hf=0; hf < hfactor; hf++){
      uint32_t curr_t = i+hf;
      
      HierarchicalTask& sht = leaves[curr_t];
      
      // leaf hierarchical task
//      sht.incoming().push_back(TNULL);
      
      // collapse vertically
      for(uint32_t vf=0; vf < vfactor; vf++){
     //   if(sht->outputs().size()>0){
        if(sht.outputs()[0].size() > vf){
          //            bool found = false;
          for(uint32_t st=0; st < therest.size(); st++){
            if(therest[st].id() == sht.outputs()[0][vf]){
             // leaft.outputs().push_back(therest[st]->outputs()[0]);
              leaft.addSubTask(therest[st]);
              toRemove.push_back(therest[st].id());
              
//              therest.erase(therest.begin()+st); st--; // remove from therest
              break;
            }
            
          }
  //        }
          
        } // end if any output
      } // end collapse vertically
      
      toRemove.push_back(sht.id());
      
      leaft.addSubTask(sht);
    } // end collapse horizontally

    printf("grouped %lu nodes\n", leaft.mSubtasks.size());
//    leaft.checkUnresolved(this);
//    leaft.incoming().push_back(TNULL);
//    leaft.outputs().push_back(std::vector<TaskId>());
   // leaft.checkUnresolvedReduce(&leaft); /// TODO CONTINUE do mapping per level of reduction in the graph
//    if(addSubTask(leaft))
//      new_nodes++;
    
    if(addSubTask(leaft))
      new_nodes++;
//      new_leaves.push_back(&mSubtasks.back());
  }
  
//  printf("checking %d new nodes\n", new_nodes);
//  for(int32_t i= 0; i<new_nodes; i++){
//    HierarchicalTask& nt = mSubtasks[mSubtasks.size()-i-1];
//    nt.checkUnresolvedReduce(this);
//  }

  for(uint32_t i=0; i < new_nodes; i++){
    HierarchicalTask& nt = mSubtasks[mSubtasks.size()-i-1];
    nt.checkUnresolvedReduce(this);
  }
  
//  for(uint32_t st=0; st < new_leaves.size(); st++){
//    new_leaves[st]->checkUnresolvedReduce(this);
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
//  checkUnresolvedReduce(this);
  
}

void HierarchicalTask::expand(int32_t hfactor, int32_t vfactor){
  
//  HierarchicalTask supertask;
//  supertask.incoming_map = incoming_map;
//  supertask.outgoing_map = outgoing_map;
  
  // Find the leaves
  std::vector<HierarchicalTask> leaves;
//  std::vector<HierarchicalTask> therest;
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
//    else
//      therest.push_back(mSubtasks[i]);
  }
  
  printf("Found %lu leaves \n", leaves.size());
  
  int32_t new_nodes = 0;
  // expand horizontally
  for(uint32_t i=0; i<leaves.size(); i++){
      
    HierarchicalTask& sht = leaves[i];

    // expand vertically
    for(uint32_t vf=0; vf < sht.mSubtasks.size(); vf++){
      //sht.addSubTask(sht.mSubtasks[vf]);
      
      if(this->addSubTask(sht.mSubtasks[vf], false))
        new_nodes++;
    } // end expand vertically
  
  }
  
  printf("created task with %lu leaves\n", this->mSubtasks.size());
  
  for(uint32_t i=0; i < leaves.size(); i++){
//    HierarchicalTask& nt = mSubtasks[mSubtasks.size()-i-1];
    leaves[i].checkUnresolvedExpand(this);
  }
  
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