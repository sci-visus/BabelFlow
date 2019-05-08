/*
 *  Utils.cc
 *
 *  Created on: Feb 1, 2016
 *      Author: spetruzza
 */

#ifndef __PMT_UTILS_H__
#define __PMT_UTILS_H__

#include <cmath>
#include "legion.h"
#include "Utils.h"
#include "datatypes.h"
//#include "TypeDefinitions.h"
#include "Controller.h"
#include "Definitions.h"
#include "TaskGraph.h"

//#define DEBUG_DATAFLOW

#ifdef DEBUG_DATAFLOW
# define DEBUG_PRINT(x) fprintf x
#else
# define DEBUG_PRINT(x) do {} while (0)
#endif

using namespace LegionRuntime::HighLevel;
using namespace LegionRuntime::Accessor;
using namespace LegionRuntime::Arrays;

using namespace BabelFlow;

namespace Utils {

  void printDomain(const LogicalRegion& region, Context ctx, HighLevelRuntime *runtime){
    Domain dom_in = runtime->get_index_space_domain(ctx,region.get_index_space());
    Rect<1> rect_in = dom_in.get_rect<1>();
    for (GenericPointInRectIterator<1> pir(rect_in); pir; pir++){
      std::cout << Point<1>(pir.p) << " ";
    }
    std::cout << std::endl;
  }
  
  void printDomain(const Domain& dom_in){
    Rect<1> rect_in = dom_in.get_rect<1>();
    for (GenericPointInRectIterator<1> pir(rect_in); pir; pir++){
      std::cout << Point<1>(pir.p) << " ";
    }
    std::cout << std::endl;
  }
  
  DomainColoring partitionInput(int partsize, const LogicalRegion& region, Context ctx, HighLevelRuntime *runtime){
    DomainColoring coloring_in;
    
    Domain dom_in = runtime->get_index_space_domain(ctx, region.get_index_space());
    Rect<1> rect_in = dom_in.get_rect<1>();
    
    int lo =rect_in.lo;
    int hi =rect_in.hi;
    
    int numpart = (hi-lo) / partsize;
    
    Point<1> p1(lo);
    // if(lo > 0) p1 = (int)p1 -1;
    Point<1> p2((int)p1 + partsize-1);
    int i =0;
    //  printf("lo %d hi %d\n", lo, hi);
    while((int)p2 <= hi){
      //    std::cout << "new input partition from " << p1 << " to " << p2 << std::endl;
      Rect<1> subrect(p1,p2);
      coloring_in[i++] = Domain::from_rect<1>(subrect);
      
      p1 = (int)p2 + 1;
      p2 = (int)p1 + partsize -1;
    }
    
    return coloring_in;
    
  }
  
  int fake_computation(){
    // fake computing time of the task
    float fakeres = rand()*10000;
    for(int j=0; j<10000000; j++){
    for(int i=0; i<10000000; i++){
      fakeres = std::sqrt(i) * std::pow(j,5);
      fakeres -= std::sqrt(i) * std::pow(j,5);
    }
    }
  }
  
  float sum_task(std::vector<float>& in, int id){
    float sum = 0;
    //  printf("ht %d summing ", id);
    for(int i=0; i < in.size(); i++){
      //    printf("%f ", in[i]);
      sum += in[i];
    }
    //  printf("\n");
    
    fake_computation();
    
    return sum;
  }

  void indipendent_groups(std::set<TaskId> nextEpochTasks, std::vector<std::set<TaskId> >& round_groups,
      std::map<TaskId,BabelFlow::Task>& taskmap, std::map<EdgeTaskId,VPartId>& vpart_map){

    std::set<TaskId>::iterator it;
    std::map<EdgeTaskId, std::set<TaskId> > new_groups;

    for (it = nextEpochTasks.begin(); it != nextEpochTasks.end(); ++it){
      const BabelFlow::Task& task = taskmap[*it];
      
      if(task.id() == TNULL){
        DEBUG_PRINT((stderr,"TASK ID NULL, no map\n"));
        break;
      }

      for(int ri=0; ri < task.incoming().size(); ri++){
        
        if(vpart_map.find(std::make_pair(task.incoming()[ri], task.id())) == vpart_map.end()){
          fprintf(stderr,"THIS SHOULD NOT HAPPEN: Mapping input region %d -> %d not found\n", task.incoming()[ri], task.id());
          assert(false);
        }
      
        VPartId vpid = vpart_map[std::make_pair(task.incoming()[ri], task.id())];

        EdgeTaskId edge_part = std::make_pair(vpid.round_id, vpid.part_id);
        new_groups[edge_part].insert(task.id());
      }

    }

    std::map<EdgeTaskId, std::set<TaskId> >::iterator itr;
    for(itr = new_groups.begin(); itr != new_groups.end(); itr++){
      round_groups.push_back(itr->second);
    }

  }

  uint32_t compute_virtual_partitions(uint32_t launch_id, const RegionsIndexType input_block_size, std::set<TaskId> nextEpochTasks, MyArgumentMap& arg_map,
                        std::vector<VirtualPartition>& vparts, std::map<TaskId,BabelFlow::Task>& taskmap, 
                        std::map<EdgeTaskId,VPartId>& vpart_map, std::vector<LaunchData>& launch_data){

     std::set<TaskId>::iterator it;
     uint32_t task_counter = 0;

     std::vector<VPartId> verification;
     const BabelFlow::Task& task0 = taskmap[*nextEpochTasks.begin()];
     int ver_size = task0.incoming().size() + task0.outputs().size();
     verification.resize(ver_size);
     for(int i=0; i< verification.size(); i++)
      verification[i].round_id = TNULL;

     for (it = nextEpochTasks.begin(); it != nextEpochTasks.end(); ++it){
      const BabelFlow::Task& task = taskmap[*it];
      
      if(task.id() == TNULL){
        DEBUG_PRINT((stderr,"TASK ID NULL, no map\n"));
        break;
      }

      DEBUG_PRINT((stderr,"MAPPING task %d callback %d\n", task.id(), task.callback()));
      
      //MetaBabelFlow metadata;
      //TaskInfo& ti = metadata.info;//argsvec[task_counter].info;
      TaskInfo ti;
      ti.id = task.id();
      ti.callbackID = task.callback();
      ti.lenInput = task.incoming().size();
      ti.lenOutput = task.outputs().size();
      // for(int to=0; to < task.outputs().size(); to++){
      //   ti.lenOutput += task.outputs()[to].size();
      // }
      //arg_map.set_point(DomainPoint::from_point<1>(Point<1>(task_counter)), TaskArgument(&ti,sizeof(TaskInfo)));//&metadata, sizeof(MetaBabelFlow)));//
      
      arg_map[DomainPoint::from_point<1>(Point<1>(task_counter))] = TaskArgument(&ti,sizeof(TaskInfo));

      if(vparts.size() < ti.lenInput+ti.lenOutput)      	
      	vparts.resize(ti.lenInput+ti.lenOutput);
      
      //printf("task %d PARTS size %d\n", ti.id, ti.lenInput+ti.lenOutput);

      RegionsIndexType total_input_size = 0;
      RegionsIndexType output_size = 1024*1024*1024;
    
      for(int ri=0; ri < ti.lenInput; ri++){
        
        if(vpart_map.find(std::make_pair(task.incoming()[ri], task.id())) == vpart_map.end()){
          
          fprintf(stderr,"THIS SHOULD NOT HAPPEN: Mapping input region %d -> %d not found\n", task.incoming()[ri], task.id());

          assert(false);
        }
      
        VPartId vpid = vpart_map[std::make_pair(task.incoming()[ri], task.id())];

        DomainColoring& colors = launch_data[vpid.round_id].vparts[vpid.part_id].coloring;

        Rect<1> lrect = colors[vpid.color].get_rect<1>();
        vparts[ri].local_index = lrect.lo[0];
        vparts[ri].coloring[task_counter] = colors[vpid.color];
        vparts[ri].source_coloring[task_counter] = vpid.color;
        vparts[ri].input = true;
        vparts[ri].id = vpid;

        if(verification[ri].round_id == TNULL)
          verification[ri] = vpid;

        if(verification[ri].round_id != vpid.round_id || verification[ri].part_id != vpid.part_id){
          DEBUG_PRINT((stderr,"ERROR: using different input regions for the same requirement\n"));
          assert(false);
        }
        

        DEBUG_PRINT((stderr,"\t mapping in (%d,%d) rect [%llu %llu] vpart_id %d %d (%d) size %d\n", task.incoming()[ri], task.id(), lrect.lo[0], lrect.hi[0],vpid.round_id,vpid.part_id, vpid.color, RegionsIndexType(lrect.hi-lrect.lo)+1));
        //printf("setting in %d index %d rect [%llu %llu]\n", ri, requirements[ri].local_index, lrect.lo[0], lrect.hi[0]);

        total_input_size += RegionsIndexType(lrect.hi-lrect.lo)+1;

      }

      std::map<EdgeTaskId,RegionsIndexType> next_edges;
      //fprintf(stderr,"input block size %d\n", input_block_size);
      int out_count = 0;
      //vparts[ti.lenInput].local_index = 0;
      for(int ro=0; ro < task.outputs().size(); ro++){

#if PMT_OUTPUT_SIZE
        if(task.callback() ==1){
          if(ro == 0)
            output_size = input_block_size/9;///9;
          else
            output_size = input_block_size+input_block_size/4;
        }
        else if(task.callback() == 2){
          output_size = input_block_size;//input_block_size+input_block_size/9;//10
        }
        else if(task.callback() == 3){
          output_size = input_block_size+input_block_size/4;
        }
        else
          output_size = input_block_size;//+input_block_size/10;
#else
        output_size = input_block_size*2;
#endif
        Rect<1> out_rect(Point<1>(vparts[ti.lenInput+ro].local_index),Point<1>(vparts[ti.lenInput+ro].local_index+output_size-1));
        
        vparts[ti.lenInput+ro].coloring[task_counter] = Domain::from_rect<1>(out_rect);
        vparts[ti.lenInput+ro].local_index += output_size;
        VPartId vpid;
        vpid.round_id = launch_id;
        vpid.part_id = ti.lenInput+ro;
        vpid.color = task_counter;
        vparts[ti.lenInput+ro].id = vpid;
        vparts[ti.lenInput+ro].input = false;
        //printf("setting out %d index %d rect [%llu %llu]\n", ti.lenInput+ro, requirements[ti.lenInput+ro].local_index, out_rect.lo[0], out_rect.hi[0]);

        for(int inro=0; inro < task.outputs()[ro].size(); inro++){

          if(task.outputs()[ro][inro] == TNULL) continue;

          EdgeTaskId out_edge = std::make_pair(task.id(), task.outputs()[ro][inro]);
          vpart_map[out_edge] = vpid;
          out_count++;

          DEBUG_PRINT((stderr,"\t mapping out (%d,%d) [%d %d] vpart_id %d %d (%d) size %d\n", task.id(), task.outputs()[ro][inro],out_rect.lo[0],out_rect.hi[0],vpid.round_id,vpid.part_id,vpid.color, output_size));

        }
    
      }

      task_counter++;
   
    }

    //printf("ROUND COUNT %d\n", task_counter);
    return task_counter;

  }

  bool findEdge(EdgeTaskId edge, std::set<EdgeTaskId> toResolveEdges){
    
    std::set<EdgeTaskId>::iterator edges;

    for(edges=toResolveEdges.begin(); edges != toResolveEdges.end(); edges++){
      EdgeTaskId ed = *edges;
      //printf("\t in %d %d\n", ed.first, ed.second);
      if(ed.first == edge.first && ed.second == edge.second){
        return true;
      }
    }        
    return false;
  }

  void compute_launch_data(const RegionsIndexType input_block_size, std::set<TaskId>& currEpochTasks, std::set<EdgeTaskId>& current_inputs, 
    std::set<EdgeTaskId>& current_outputs, std::map<TaskId,BabelFlow::Task>& taskmap, 
    std::vector<LaunchData>& launch_data, std::map<EdgeTaskId,VPartId>& vpart_map){

    std::set<EdgeTaskId> toResolveEdges;
    std::set<TaskId> unresolvedTasks;

    uint32_t round_id = 1;
   
    while(currEpochTasks.size() > 0){

      std::set<TaskId> nextEpochTasks;
      std::set<TaskId> round_tasks;
      
      currEpochTasks.insert(unresolvedTasks.begin(), unresolvedTasks.end());
      unresolvedTasks.clear();
      std::set<TaskId> tempCurrEpoch;

      std::set<TaskId>::iterator currEpIt;
      for(currEpIt=currEpochTasks.begin(); currEpIt != currEpochTasks.end(); currEpIt++){
        const BabelFlow::Task& lt = taskmap[*currEpIt];
        //printf("task %d\n",lt.id());

        int in_unresolved = lt.incoming().size();
        for(int in=0; in < lt.incoming().size(); in++){
          EdgeTaskId inedge = std::make_pair(lt.incoming()[in], lt.id());
          //printf("\tlooking for edge %d %d\n", lt.incoming()[in], lt.id());
          if(findEdge(inedge, current_inputs)){
            in_unresolved--; // one fail is enough
          }else
            break;
        }
        if(in_unresolved == 0){
          //printf("task %d resolved callback %d\n\n", lt.id(), lt.callback());
          nextEpochTasks.insert(lt.id());

          round_tasks.insert(lt.id());

          const std::vector<std::vector<TaskId> >& outputs = lt.outputs();
          for(int o=0; o < outputs.size(); o++){
          for(int inro=0; inro < outputs[o].size(); inro++){
            EdgeTaskId outedge = std::make_pair(lt.id(),outputs[o][inro]);
            
            if(in_unresolved == 0){
              current_outputs.insert(outedge);
            }
            tempCurrEpoch.insert(outputs[o][inro]);

            //printf("add to next input %d %d\n", lt.id(),outputs[o][inro]);
          }
        }
          
        }
        else{
          unresolvedTasks.insert(lt.id());
        }

      }

      // std::map<CallbackId, std::set<TaskId> >::iterator rit;
      std::vector<std::set<TaskId> > round_groups;

      // Give the current round tasks slipt into independent groups (to satisfy index launches)
      indipendent_groups(round_tasks, round_groups, taskmap, vpart_map);

      for(int r=0; r < round_groups.size(); r++){
      
        LaunchData launch;
        launch.callback = taskmap[*round_groups[r].begin()].callback();

        if(round_id > 0 && (*round_groups[r].begin())== TNULL) { // TODO check why this happens
          DEBUG_PRINT((stderr,"skipping invalid round with first task TNULL (r>0)\n"));
          continue;
        }

        //printf("ROUND START %d\n", round_id);
        launch.n_tasks = compute_virtual_partitions(round_id, input_block_size, round_groups[r], launch.arg_map, launch.vparts, taskmap, vpart_map, launch_data);
        //printf("PUTTING IN launch %d callback %d\n", round_id, launch.callback);
        launch_data.push_back(launch);
        round_id++;
      }

      current_inputs.insert(current_outputs.begin(), current_outputs.end());
      current_outputs.clear();

      currEpochTasks.clear();
      currEpochTasks.insert(tempCurrEpoch.begin(), tempCurrEpoch.end());  
      
      round_tasks.clear();
      round_groups.clear();

    }
   

  }

  struct classcomp {

    bool operator() (const TaskId& lhs, const TaskId& rhs) const

    {return lhs>rhs;}

  };

  void reorder_tasks(std::vector<BabelFlow::Task>& ordered_tasks, std::map<TaskId,BabelFlow::Task>& taskmap, std::set<TaskId>& currRoots){

    std::vector<TaskId> leaves;
    std::set<TaskId> order;
    std::map<TaskId,BabelFlow::Task>::iterator it;

    for(it = taskmap.begin(); it != taskmap.end(); it++){
      BabelFlow::Task& task = it->second;

      if(task.outputs().size() == 0){
        leaves.push_back(task.id());
        ordered_tasks.push_back(task);
      }

    }

    order.insert(leaves.begin(), leaves.end());

    std::set<TaskId>::iterator itl;

    int nroots = 0;
    // printf("roots in: ");
    // for(itl = currRoots.begin(); itl != currRoots.end(); itl++){
    //   printf("%d ", *itl);
    // }
    // printf("\n");
    // printf("ORIGINAL ORDER ");
    while(nroots < currRoots.size()){
      std::vector<TaskId> next_leaves;

      //for(itl = leaves.begin(); itl != leaves.end(); itl++){
      for(int l=0; l < leaves.size(); l++){
        BabelFlow::Task& task = taskmap[leaves[l]];//*itl];
        
        for(int i=0; i < task.incoming().size(); i++){
          TaskId& tid = task.incoming()[i];
          // printf("tid %d\n", tid);
          if(currRoots.find(tid) != currRoots.end()) {
            nroots++;
            // printf("found root %d\n", tid);
          }

          if(taskmap.find(tid) != taskmap.end()){
            if(order.find(tid) == order.end()){
              ordered_tasks.push_back(taskmap[tid]);
              order.insert(tid);
            }
            next_leaves.push_back(tid);
            // printf("%d ", tid);
            
          }
        }

      }

      leaves.clear();
      leaves.insert(leaves.end(), next_leaves.begin(), next_leaves.end());

    }
    
    // printf("\n");

    // std::set<TaskId>::iterator itr;

    // for(itr = order.begin(); itr != order.end(); itr++){
    //   BabelFlow::Task& task = taskmap[*itr];
    //   ordered_tasks.push_back(task);

    // }

    //printf("end compute_externals\n");

    std::reverse(std::begin(ordered_tasks), std::end(ordered_tasks));


  }

  void reorder_tasks(std::vector<BabelFlow::Task>& ordered_tasks, std::set<EdgeTaskId> externals, std::set<TaskId>& currEpochTasks, 
    std::map<TaskId,BabelFlow::Task>& taskmap, int shard){

#if 0
    for(std::map<TaskId,BabelFlow::Task, classcomp>::iterator it=taskmap.begin(); it !=taskmap.end(); it++){
      ordered_tasks.push_back(it->second);
    }

#else
    std::set<EdgeTaskId> current_inputs; 

    std::set<TaskId>::iterator currEpIt;

    for(currEpIt=currEpochTasks.begin(); currEpIt != currEpochTasks.end(); currEpIt++){
      EdgeTaskId edge;
      edge.first = TNULL; // This is not used
      edge.second = *currEpIt;
      current_inputs.insert(edge);

      printf("curr epoch %d %d\n", edge.first, edge.second);
    }

    //current_inputs.insert(externals.begin(), externals.end());

    std::set<EdgeTaskId> current_outputs;

    std::set<EdgeTaskId> toResolveEdges;
    std::set<TaskId> unresolvedTasks;

    uint32_t round_id = 1;
   
    while(currEpochTasks.size() > 0){

      std::set<TaskId> nextEpochTasks;
      
      currEpochTasks.insert(unresolvedTasks.begin(), unresolvedTasks.end());
      unresolvedTasks.clear();
      std::set<TaskId> tempCurrEpoch;

      //printf("-----%d: Round %d-----\n", shard, round_id);

      for(currEpIt=currEpochTasks.begin(); currEpIt != currEpochTasks.end(); currEpIt++){
        
        std::map<TaskId,BabelFlow::Task>::iterator it = taskmap.find(*currEpIt);

        if(it == taskmap.end()){
          //printf("(ext task %d)\n", *currEpIt);
          continue;
        }

        const BabelFlow::Task& lt = it->second;//taskmap[*currEpIt];

        //printf("task %d \n", lt.id());

        int in_unresolved = lt.incoming().size();
        for(int in=0; in < lt.incoming().size(); in++){
          EdgeTaskId inedge = std::make_pair(lt.incoming()[in], lt.id());
          //printf("\tlooking for edge %d %d\n", lt.incoming()[in], lt.id());
          if(findEdge(inedge, current_inputs) || (externals.find(inedge) != externals.end())){
            in_unresolved--; // one fail is enough
          }else
            break;
        }

        if(in_unresolved == 0){
          //printf("task %d resolved callback %d\n\n", lt.id(), lt.callback());
          nextEpochTasks.insert(lt.id());

          ordered_tasks.push_back(lt);

          const std::vector<std::vector<TaskId> >& outputs = lt.outputs();
          for(int o=0; o < outputs.size(); o++){
            for(int inro=0; inro < outputs[o].size(); inro++){
              EdgeTaskId outedge = std::make_pair(lt.id(),outputs[o][inro]);
              
              if(in_unresolved == 0){
                current_outputs.insert(outedge);
              }
              tempCurrEpoch.insert(outputs[o][inro]);

              //printf("add to next input %d %d\n", lt.id(),outputs[o][inro]);
            }
        }
          
        }
        else{
          unresolvedTasks.insert(lt.id());
        }

      }

      current_inputs.insert(current_outputs.begin(), current_outputs.end());
      current_outputs.clear();

      currEpochTasks.clear();
      currEpochTasks.insert(tempCurrEpoch.begin(), tempCurrEpoch.end());  

      round_id++;

    }
#endif
  }

}

#endif // __PMT_UTILS_H__

