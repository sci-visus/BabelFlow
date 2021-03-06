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

//! The typedef for the accepted callbacks
/*! A Callback is the only accepted function signature for a task.
 *  It takes n>=0  DataBlocks as input and produces m>=0 DataBlocks
 *  as output. The callback will assume ownership of the input buffers
 *  as is responsible for deleting any associated memory. Similarly,
 *  the callback will give up ownership of all output buffers to the
 *  caller
 *
 * @param inputs A set of DataBlocks that are the inputs
 * @param outputs A set of DataBlocks storing the outputs
 * @return 1 if successful and 0 otherwise
 */
typedef int (*Callback)(std::vector<BabelFlow::Payload>& inputs, std::vector<BabelFlow::Payload>& outputs, BabelFlow::TaskId task);

typedef std::pair<BabelFlow::TaskId,BabelFlow::TaskId> EdgeTaskId;
typedef LegionRuntime::Arrays::coord_t RegionsIndexType;

typedef uint32_t RegionPointType;
#define BYTES_PER_POINT sizeof(RegionPointType)

struct TaskInfo{
  BabelFlow::TaskId id;
  BabelFlow::CallbackId callbackID;
  size_t lenInput;
  size_t lenOutput;
};

class DomainSelection{
 public:
  GlobalIndexType low[3];
  GlobalIndexType high[3];
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
  BabelFlow::CallbackId callback;
  ArgumentMap arg_map;
  std::vector<VirtualPartition> vparts;
  uint32_t n_tasks;
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

  //! Register a callback for the given id
  int registerCallback(BabelFlow::CallbackId id, Callback func);

  //! Start the computation
  int run(std::map<BabelFlow::TaskId,BabelFlow::Payload>& initial_inputs);

  //! A list of registered callbacks
  static std::vector<BabelFlow::Callback> mCallbacks;

  static LogicalRegion load_task(const Task *task,
                   const std::vector<PhysicalRegion> &regions,
                   Context ctx, HighLevelRuntime *runtime);

  static int generic_task(const Task *task,
                  const std::vector<PhysicalRegion> &regions,
                  Context ctx, HighLevelRuntime *runtime);
  
  static void distributed_initialization(const Task *task,
                    const std::vector<PhysicalRegion> &phy_regions,
                    Context ctx, HighLevelRuntime *runtime);
  
  static void mapper_registration(Machine machine, HighLevelRuntime *rt,
                      const std::set<Processor> &local_procs);
  
  static void top_level_task(const Task *task,
                 const std::vector<PhysicalRegion> &regions,
                 Context ctx, HighLevelRuntime *runtime);
  
  static std::vector<BabelFlow::Task> alltasks;
  static std::map<BabelFlow::TaskId,BabelFlow::Payload> mInitial_inputs;

  static uint32_t data_size[3];
  
};

#endif /* LEGION_CONTROLLER_H */
