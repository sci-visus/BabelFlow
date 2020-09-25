/*
 * Controller.h
 *
 *  Created on: Mar 15, 2016
 *      Author: spetruzza
 */

#ifndef LEGION_CONTROLLER_H
#define LEGION_CONTROLLER_H

#include <vector>
#include <map>
#include <set>

// TODO when we move back the Legion controller to the BabelFlow project
// we need to update these includes

#include "../Definitions.h"
#include "../TaskGraph.h"
#include "../Payload.h"
#include "legion.h"

#include "datatypes.h"


using namespace LegionRuntime::HighLevel;
using namespace LegionRuntime::Accessor;

namespace BabelFlow {

namespace legion {

typedef std::pair<BabelFlow::TaskId,BabelFlow::TaskId> EdgeTaskId;
typedef LegionRuntime::Arrays::coord_t RegionsIndexType;

typedef uint32_t RegionPointType;
#define BYTES_PER_POINT sizeof(RegionPointType)

struct TaskInfo{
  BabelFlow::TaskId id;
  BabelFlow::CallbackId callbackID;
  BabelFlow::Callback callbackFunc;
  size_t lenInput;
  size_t lenOutput;
};

class DomainSelection{
 public:
  GlobalIndexType low[3];
  GlobalIndexType high[3];

  DomainSelection(){
    for(int d=0;d<3;d++){
      low[d]=0;
      high[d]=0;
    }
  }
};

class VPartId{
public:
  BabelFlow::TaskId round_id;
  BabelFlow::TaskId part_id;
  Color color;
};

class VirtualPartition{
public:
  VPartId id;
  RegionsIndexType local_index;
  DomainColoring coloring;
  std::map<Color,Color> source_coloring;
  bool input;

  VirtualPartition(){
    local_index = 0;
    input = false;
  }
};

class LaunchData{
public:
  BabelFlow::CallbackId callbackID;
  BabelFlow::Callback callbackFunc;
  ArgumentMap arg_map;
  std::vector<VirtualPartition> vparts;
  uint32_t n_tasks;

  LaunchData(){arg_map = ArgumentMap();}
};

// struct MetaRegions{
//   LogicalRegion in_regions[MAX_INPUTS];
//   LogicalRegion out_regions[MAX_OUTPUTS];
// };

struct MetaDataFlow{
  TaskInfo info;
  int prepare;
  // MetaRegions regions;
};

enum{
  PID_ALLOCED_DATA = 7,
};

enum {
  PFID_USE_DATA_TASK = 77,
};

/*! A controller handles the communication as well as thread
 *  management for the tasks assigned to it
 */
class Controller
{

  //! Default controller
  //  Controller(){ Controller(0, NULL); };
  
  //! Controller with parameters for Legion runtime
  //Controller(int argc, char **argv);
private:
  Controller();
  static Controller* instance;
  
  static std::map<BabelFlow::TaskId,BabelFlow::Payload> (*input_initialization)(int, char**);
  static BabelFlow::Payload (*distributed_input_initialization)(char* , uint32_t *, uint32_t* , int , int , float );
public:
  
  static Controller* getInstance(){
    if(!instance)
      instance = new Controller();
    return instance;
  }

  //! Default  destructor
  ~Controller() {}

  void registerInputInitialization(std::map<BabelFlow::TaskId,BabelFlow::Payload> (*fun)(int, char**)){
    input_initialization = fun;
  }

  void registerInputInitialization(BabelFlow::Payload (*fun)(char* , uint32_t *, uint32_t* , int , int , float )){
    distributed_input_initialization = fun;
  }

  static void compute_virtual_partitions(uint32_t launch_id, std::set<BabelFlow::TaskId> nextEpochTasks, ArgumentMap& arg_map,
                        std::vector<VirtualPartition>& vparts, std::map<BabelFlow::TaskId,BabelFlow::Task>& taskmap,
                        std::map<EdgeTaskId,VPartId>& vpart_map, std::vector<LaunchData>& launch_data);

  static void compute_launch_data(std::set<BabelFlow::TaskId>& currEpochTasks, std::set<EdgeTaskId>& current_inputs, std::set<EdgeTaskId>& current_outputs, std::map<BabelFlow::TaskId,BabelFlow::Task>& taskmap,
    std::vector<LaunchData>& launch_data);


  static int bufferToRegion(char* buffer, RegionsIndexType size, LegionRuntime::Arrays::Rect<1> rect, const PhysicalRegion& region);

  static void init();

  //! Initialize the controller
  int initialize(const BabelFlow::TaskGraph& graph, const BabelFlow::TaskMap* task_map, int argc = 0, char **argv = NULL);

  //! Start the computation
  int run(std::map<BabelFlow::TaskId,BabelFlow::Payload>& initial_inputs);

  static LogicalRegion load_task(const Legion::Task *task,
                   const std::vector<PhysicalRegion> &regions,
                   Context ctx, HighLevelRuntime *runtime);

  static int generic_task(const Legion::Task *task,
                  const std::vector<PhysicalRegion> &regions,
                  Context ctx, HighLevelRuntime *runtime);
  
  static void distributed_initialization(const Legion::Task *task,
                    const std::vector<PhysicalRegion> &phy_regions,
                    Context ctx, HighLevelRuntime *runtime);
  
  static void mapper_registration(Machine machine, HighLevelRuntime *rt,
                      const std::set<Processor> &local_procs);
  
  static void top_level_task(const Legion::Task *task,
                 const std::vector<PhysicalRegion> &regions,
                 Context ctx, HighLevelRuntime *runtime);
  
  static std::vector<BabelFlow::Task> alltasks;
  static std::map<BabelFlow::TaskId,BabelFlow::Payload> mInitial_inputs;

  static uint32_t data_size[3];
  
};

}
}
#endif /* LEGION_CONTROLLER_H */
