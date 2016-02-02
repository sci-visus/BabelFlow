/*
 * HierarchicalTaskGraph.cpp
 *
 *  Created on: Jan 27, 2016
 *      Author: petruzza
 */

#include "HierarchicalTaskGraph.h"


//void HierarchicalTaskGraph::checkUnresolvedReduce(){
//  std::vector<HierarchicalTask>& mSubtasks = supertask.mSubtasks;
//  
//  printf("Mapping reduce level %d\n", reduction_level);
//  
//  for(uint32_t sb=0; sb < mSubtasks.size(); sb++){
//    
//    const std::vector<TaskId>& task_incoming = mSubtasks[sb].incoming();
//    const std::vector<std::vector<TaskId> >& task_outgoing = mSubtasks[sb].outputs();
//    
//    for(uint32_t i=0; i < task_incoming.size(); i++){
//      TaskId parent = supertask.isSubTask(task_incoming[i]);
////      printf("%d in:look for %d found parent %d\n", mSubtasks[sb].id(), task_incoming[i], parent );
//      if(parent == supertask.id())
//        continue;
//      
//      if(parent != TNULL || task_incoming[i] == TNULL){ // has a parent task or is a leaf
//
////        printf("%d mapping in %d to %d\n", supertask.id(), mSubtasks[sb].incoming()[i], parent);
//        incoming_map[reduction_level][parent] = mSubtasks[sb].incoming()[i];
////        supertask.incoming().push_back(parent);
//        
//        mSubtasks[sb].incoming()[i] = parent;
//
////        printf("%d: add in %d\n",mSubtasks[sb].id(), parent);
//        
//      }
//    
//
//    }
//    
//    for(uint32_t i=0; i < task_outgoing.size(); i++){
//      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
//        
//        TaskId parent = supertask.isSubTask(task_outgoing[i][j]);
////        printf("%d out:look for %d found parent %d\n", mSubtasks[sb].id(), task_outgoing[i][j], parent );
//        if(parent == supertask.id())
//          continue;
//        
//        if(parent != TNULL){
////          printf("%d mapping out %d to %d\n", supertask.id(), mSubtasks[sb].outputs()[i][j], parent);
//          outgoing_map[reduction_level][parent] = mSubtasks[sb].outputs()[i][j];
//          
////          std::vector<TaskId> new_out(1);
////          new_out.resize(1);
////          new_out[0] = parent;
////          supertask.outputs().push_back(new_out);
//          
//          mSubtasks[sb].outputs()[i][j] = parent;
//        }
//        
//      }
//      
//    }
//  }
//
//}
//
//void HierarchicalTaskGraph::checkUnresolvedExpand(){
//
//  printf("Mapping expand level %d\n", reduction_level);
//  
//  std::vector<HierarchicalTask>& mSubtasks = supertask.mSubtasks;
//  
//  for(uint32_t sb=0; sb < mSubtasks.size(); sb++){
//    
//    const std::vector<TaskId>& task_incoming = mSubtasks[sb].incoming();
//    const std::vector<std::vector<TaskId> >& task_outgoing = mSubtasks[sb].outputs();
//    
//    for(uint32_t i=0; i < task_incoming.size(); i++){
//      if(incoming_map[reduction_level].find(task_incoming[i]) != incoming_map[reduction_level].end()){
////        printf("in mapping back %d to %d\n", mSubtasks[sb].incoming()[i], incoming_map[reduction_level][task_incoming[i]]);
//        mSubtasks[sb].incoming()[i] = incoming_map[reduction_level][task_incoming[i]];
//      }
//    }
//    
//    for(uint32_t i=0; i < task_outgoing.size(); i++){
//      for(uint32_t j=0; j < task_outgoing[i].size(); j++){
//        if(outgoing_map[reduction_level].find(mSubtasks[sb].outputs()[i][j]) != outgoing_map[reduction_level].end()){
////          printf("out mapping back %d to %d\n", mSubtasks[sb].outputs()[i][j], outgoing_map[reduction_level][mSubtasks[sb].outputs()[i][j]]);
//          mSubtasks[sb].outputs()[i][j] = outgoing_map[reduction_level][mSubtasks[sb].outputs()[i][j]];
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