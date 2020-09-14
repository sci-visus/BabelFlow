/*
 *  Utils.h
 *
 *  Created on: Feb 1, 2016
 *      Author: spetruzza
 */

#ifndef __PMT_UTILS_H__
#define __PMT_UTILS_H__

#include "datatypes.h"
#include "Controller.h"
#include "legion.h"
#include "../Definitions.h"
#include "../TaskGraph.h"

using namespace LegionRuntime::HighLevel;
using namespace LegionRuntime::Accessor;
using namespace LegionRuntime::Arrays;
using namespace BabelFlow::legion;

#ifdef DEBUG_DATAFLOW
# define DEBUG_PRINT(x) fprintf x
#else
# define DEBUG_PRINT(x) do {} while (0)
#endif

namespace Utils {

  DomainColoring partitionInput(int partsize, const LogicalRegion& region, Context ctx, HighLevelRuntime *runtime);
  
  float sum_task(std::vector<float>& in, int id);
  void printDomain(const Domain& dom_in);
  void printDomain(const LogicalRegion& region, Context ctx, HighLevelRuntime *runtime);

  //// Evaluate the necessary partitions for a given round
  uint32_t compute_virtual_partitions(uint32_t launch_id, const RegionsIndexType input_block_size, std::set<BabelFlow::TaskId> nextEpochTasks, ArgumentMap& arg_map,
                        std::vector<VirtualPartition>& vparts, std::map<BabelFlow::TaskId,BabelFlow::Task>& taskmap,
                        std::map<EdgeTaskId,VPartId>& vpart_map, std::vector<LaunchData>& launch_data);

  //// Evaluate all the launches, regions and partitions give the current input launch and tasks
  void compute_launch_data(const RegionsIndexType input_block_size, std::set<BabelFlow::TaskId>& currEpochTasks, std::set<EdgeTaskId>& current_inputs,
  	std::set<EdgeTaskId>& current_outputs, std::map<BabelFlow::TaskId,BabelFlow::Task>& taskmap,
    std::vector<LaunchData>& launch_data, std::map<EdgeTaskId,VPartId>& vpart_map);
  
  int fake_computation();

}

#endif // __PMT_UTILS_H__

