/*
 *  Controller.cc
 *
 *  Created on: Mar 12, 2016
 *      Author: spetruzza
 */

#include <cstdio>
#include <cassert>
#include <cstdlib>
#include <cfloat>
#include <fstream>
#include <sys/time.h>
#include <stdint.h>
#include "legion.h"

#include "Utils.h"
#include "datatypes.h"
//#include "DataFlowMapper.h"
#include "Controller.h"
#include "default_mapper.h"

using namespace Legion;
using namespace Legion::Mapping;
using namespace LegionRuntime::HighLevel;
using namespace LegionRuntime::Accessor;

//#define DEBUG_DATAFLOW

#ifdef DEBUG_DATAFLOW
# define DEBUG_PRINT(x) fprintf x
#else
# define DEBUG_PRINT(x) do {} while (0)
#endif

#define USE_VIRTUAL_MAPPING 0

std::vector<LaunchData> launch_data;
std::map<BabelFlow::TaskId,BabelFlow::Task> taskmap;
std::map<EdgeTaskId,VPartId> vpart_map;

Controller* Controller::instance = NULL;
std::map<BabelFlow::TaskId,BabelFlow::Payload> (*Controller::input_initialization)(int, char**) = NULL;
std::vector<Callback> Controller::mCallbacks;

std::vector<BabelFlow::Task> Controller::alltasks;
std::map<BabelFlow::TaskId,BabelFlow::Payload> Controller::mInitial_inputs;
uint32_t Controller::data_size[3];

int legion_argc;
char **legion_argv;

void print_domain(Context ctx, HighLevelRuntime *runtime, LogicalRegion newlr){
  IndexSpace newis = newlr.get_index_space();
  Domain dom_new = runtime->get_index_space_domain(ctx, newis);
  auto rect_new = dom_new.get_rect<1>();
  printf("domain volume %zu lo %d hi %d\n", rect_new.volume(), (int)rect_new.lo, (int)rect_new.hi);

}

class UseDataProjectionFunctor : public ProjectionFunctor {
public:
    UseDataProjectionFunctor(void);

    virtual LogicalRegion project(Context ctx, Task *task,
                                  unsigned index,
                                  LogicalRegion upper_bound,
                                  const DomainPoint &point);

    virtual LogicalRegion project(Context ctx, Task *task,
                                  unsigned index,
                                  LogicalPartition upper_bound,
                                  const DomainPoint &point);

    virtual unsigned get_depth(void) const;
};

UseDataProjectionFunctor::UseDataProjectionFunctor(void) {}

// region -> region: UNUSED
LogicalRegion UseDataProjectionFunctor::project(Context ctx, Task *task,
                                                unsigned index,
                                                LogicalRegion upper_bound,
                                                const DomainPoint &point)
{
  assert(0);
  return LogicalRegion::NO_REGION;
}

// partition -> region path: [index].PID_ALLOCED_DATA.0
LogicalRegion UseDataProjectionFunctor::project(Context ctx, Task *task,
                                                unsigned index,
                                                LogicalPartition upper_bound,
                                                const DomainPoint &point)
{

  int nlaunch = *(int*)task->args;

  // IndexSpace lev_is = runtime->get_index_subspace(upper_bound, DomainPoint::from_point<1>(info.source_coloring[index]));

  // IndexPartition alloc_ip = runtime->get_index_partition(ctx, lev_is, PID_ALLOCED_DATA);
  // IndexSpace alloc_is = runtime->get_index_subspace(alloc_ip, DomainPoint::from_point<1>(0));

  // //Domain alloc_dom = runtime->get_index_space_domain(ctx, alloc_is);

  // LogicalRegion alloc_lr = runtime->get_logical_subregion(upper_bound, alloc_is);//alloc_lp, alloc_is);

  // return alloc_lr;

  // if our application did more than one index launch using this projection functor,
  // we could consider memoizing the result of this lookup to reduce overhead on subsequent
  // launches

  Color color = launch_data[nlaunch].vparts[index].source_coloring[point[0]];
  // printf("launch %d mapping %d to %d\n", nlaunch, index, color);
  // std::cout << upper_bound << std::endl;
  LogicalRegion lr1 = runtime->get_logical_subregion_by_color(ctx, upper_bound, DomainPoint::from_point<1>(color));
  LogicalPartition lp1 = runtime->get_logical_partition_by_color(ctx, lr1, PID_ALLOCED_DATA);
  LogicalRegion lr2 = runtime->get_logical_subregion_by_color(ctx, lp1, 0);
  return lr2;
}

unsigned UseDataProjectionFunctor::get_depth(void) const
{
  return 1;
}

// Tasks responsible to make copies of the first region into the others
int relay_task(const Task *task,
                             const std::vector<PhysicalRegion> &regions,
                             Context ctx, HighLevelRuntime *runtime){

  //RegionAccessor<AccessorType::Generic, RegionPointType> acc_in =
  //    regions[0].get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>();
  
  RegionAccessor<AccessorType::Affine<1>, RegionPointType> aff_in =
      regions[0].get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>().convert<AccessorType::Affine<1> >();

  //const FieldAccessor<READ_ONLY,RegionPointType,1> aff_in(regions[0], FID_PAYLOAD);

  IndexSpace is = task->regions[0].region.get_index_space();
  auto rect = runtime->get_index_space_domain(ctx, is).get_rect<1>();
    

  //printf("copying size %d in size %llu\n", size, (RegionsIndexType*)aff_in[rect.lo[0]]);

#if !USE_VIRTUAL_MAPPING
  RegionsIndexType size = 0;
  memcpy(&size, &aff_in[RegionsIndexType(rect.lo[0])], sizeof(RegionsIndexType));
#endif

  for(int i=0; i < regions.size()-1; i++){

#if USE_VIRTUAL_MAPPING

    RegionsIndexType size = rect.hi[0]-rect.lo[0]+1;

    LogicalRegion curr_lr = task->regions[i+1].region;

    IndexSpace curr_is = curr_lr.get_index_space();

    auto my_bounds = runtime->get_index_space_domain(curr_is).get_rect<1>();
    RegionsIndexType bounds_lo = my_bounds.lo[0];
   
    // remember rectangle bounds are inclusive on both sides
    LegionRuntime::Arrays::Rect<1> alloc_bounds(my_bounds.lo, bounds_lo + size - 1);

    DomainColoring dc;
    Domain color_space = Domain::from_rect<1>(LegionRuntime::Arrays::Rect<1>(0, 0));
    dc[0] = Domain::from_rect<1>(alloc_bounds);

    
    IndexPartition alloc_ip = runtime->create_index_partition(ctx,
                    curr_is,
                    color_space,
                    dc,
                    DISJOINT_KIND, // trivial
                    PID_ALLOCED_DATA
                    );

    LogicalPartition alloc_lp = runtime->get_logical_partition(curr_lr, alloc_ip);
    IndexSpace alloc_is = runtime->get_index_subspace(alloc_ip, DomainPoint::from_point<1>(0));
    LogicalRegion alloc_lr = runtime->get_logical_subregion(alloc_lp, alloc_is);

    // printf("RELAY inline mapping %lld size %lld\n",bounds_lo, size);
    // std::cout << "RELAY created subregion "<< alloc_lr.get_index_space().get_id() << " in [" << curr_lr.get_index_space().get_id() <<  "]" << std::endl; //<< alloc_lr << " region " << curr_lr <<
 
    InlineLauncher launcher(RegionRequirement(alloc_lr,
                WRITE_DISCARD,
                EXCLUSIVE,
                curr_lr,
                DefaultMapper::EXACT_REGION)
          .add_field(FID_PAYLOAD));
    PhysicalRegion pr = runtime->map_region(ctx, launcher);
    pr.wait_until_valid();

    //RegionAccessor<AccessorType::Generic, RegionPointType> acc_out =
    //  pr.get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>();
    RegionAccessor<AccessorType::Affine<1>, RegionPointType> aff_out =
    pr.get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>().convert<AccessorType::Affine<1> >();

    //const FieldAccessor<WRITE_DISCARD,RegionPointType,1> aff_out(pr, FID_PAYLOAD);

    auto curr_rect = runtime->get_index_space_domain(ctx, alloc_is).get_rect<1>();
   //RegionsIndexType curr_full_size = RegionsIndexType(curr_rect.hi[0]-curr_rect.lo[0]+1);

    //memset(&aff_out[RegionsIndexType(curr_rect.lo[0])],0,curr_full_size*BYTES_PER_POINT);
    memcpy(&aff_out[RegionsIndexType(curr_rect.lo[0])], &aff_in[RegionsIndexType(rect.lo[0])], size*BYTES_PER_POINT);

#else

    //RegionAccessor<AccessorType::Generic, RegionPointType> acc_out =
      //regions[i+1].get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>();
    RegionAccessor<AccessorType::Affine<1>, RegionPointType> aff_out =
      regions[i+1].get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>().convert<AccessorType::Affine<1> >();

    IndexSpace curr_is = task->regions[i+1].region.get_index_space();
    auto curr_rect = runtime->get_index_space_domain(ctx, curr_is).get_rect<1>();
    RegionsIndexType curr_full_size = RegionsIndexType(curr_rect.hi[0]-curr_rect.lo[0]+1);

    memset(&aff_out[RegionsIndexType(curr_rect.lo[0])],0,curr_full_size*BYTES_PER_POINT);
    memcpy(&aff_out[RegionsIndexType(curr_rect.lo[0])], &aff_in[RegionsIndexType(rect.lo[0])], size*BYTES_PER_POINT+sizeof(RegionsIndexType));

// #else // Using Copy Launcher as following does not work!
//     CopyLauncher copy_launcher;
//     copy_launcher.add_copy_requirements(
//         RegionRequirement(task->regions[0].region, READ_ONLY,
//                           EXCLUSIVE, task->regions[0].region),
//         RegionRequirement(task->regions[i+1].region, WRITE_DISCARD,
//                           EXCLUSIVE, task->regions[i+1].region));
//     copy_launcher.add_src_field(0, FID_PAYLOAD);
//     copy_launcher.add_dst_field(0, FID_PAYLOAD);
    
//     runtime->issue_copy_operation(ctx, copy_launcher);

#endif
  }

  return 0;
  
}

// Copy the content of a Region into an array 
int regionToBuffer(char*& buffer, RegionsIndexType& size, RegionsIndexType offset, 
      const PhysicalRegion& region){ 
  
  //RegionAccessor<AccessorType::Generic, RegionPointType> acc_in =
    //region.get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>();
  
  //  bool affine = acc_in.can_convert<AccessorType::Affine<1> >();
  //Domain dom_incall = runtime->get_index_space_domain(*ctx, region.get_logical_region().get_index_space());
  //Rect<1> dom_rect = dom_incall.get_rect<1>();

  //printf("RTB volume %d my vol %llu size %llu\n", dom_rect.volume() , RegionsIndexType(dom_rect.hi-dom_rect.lo)+1, size);
  //  assert(elem_rect_in.volume() >= size);

  //if(affine){ // Efficient specific accessor
    RegionAccessor<AccessorType::Affine<1>, RegionPointType> aff_in = 
      region.get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>().convert<AccessorType::Affine<1> >();

#if USE_VIRTUAL_MAPPING
    buffer = (char*)malloc(sizeof(RegionPointType)*size);
    memcpy(buffer, &aff_in[offset], size*BYTES_PER_POINT); //dom_rect.lo], size);
#else
    
    memcpy(&size, &aff_in[offset], sizeof(RegionsIndexType));

    //printf("deserializing off %lld found size %lld\n", offset, size);
    buffer = (char*)malloc(BYTES_PER_POINT*size);
    memcpy(buffer, &aff_in[(RegionsIndexType)(offset+sizeof(RegionsIndexType)/BYTES_PER_POINT)], size*BYTES_PER_POINT);

    // char offname[128];
    // sprintf(offname,"rtb_%d.raw", offset);
    // std::ofstream outresfile(offname,std::ofstream::binary);
   
    //  outresfile.write(reinterpret_cast<char*>(buffer),size*BYTES_PER_POINT);

    //  outresfile.close();
#endif
    /*}
  else{
    //Rect<1> elem_rect_in(Point<1>(0),Point<1>(size-1));
    Rect<1> elem_rect_in(Point<1>(dom_rect.lo),Point<1>(uint64_t(dom_rect.lo)+size-1));
    
    RegionAccessor<AccessorType::Generic, char> acc_in =
    region.get_field_accessor(FID_PAYLOAD).typeify<char>();
    
    uint32_t i=0;
    if(buffer == NULL)
      buffer = new char[size];
    
    for (GenericPointInRectIterator<1> pir(elem_rect_in); pir; pir++){
      buffer[i++] = acc_in.read(DomainPoint::from_point<1>(pir.p));
    }
  }*/
  
  return 1;
}

// Copy the content of a buffer into a Region
int Controller::bufferToRegion(char* buffer, RegionsIndexType size, LegionRuntime::Arrays::Rect<1> rect, const PhysicalRegion& region){ //, Context* ctx, Legion::HighLevelRuntime* runtime){
    
  //  RegionAccessor<AccessorType::Generic, RegionPointType> acc_in =
  //region.get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>();
  
  RegionsIndexType offset = RegionsIndexType(rect.lo);

  //bool affine = acc_in.can_convert<AccessorType::Affine<1> >();
  
  //Domain dom_incall = runtime->get_index_space_domain(*ctx, region.get_logical_region().get_index_space());
  //Rect<1> dom_rect = dom_incall.get_rect<1>();

  // printf("BTR volume %d my vol %d size %d\n", dom_rect.volume() ,  RegionsIndexType(dom_rect.hi- dom_rect.lo)+1, size);
  
  //assert(elem_rect_in.volume() >= size);

  //if(affine){ // Efficient specific accessor
    RegionAccessor<AccessorType::Affine<1>, RegionPointType> aff_in =
      region.get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>().convert<AccessorType::Affine<1> >();
  
#if USE_VIRTUAL_MAPPING
    memcpy(&aff_in[offset], buffer, size*BYTES_PER_POINT); //dom_rect.lo],buffer, size);
#else

    RegionsIndexType full_size = RegionsIndexType(rect.hi[0]-rect.lo[0]);
    
    memset(&aff_in[offset],0,full_size*BYTES_PER_POINT);

    //printf("serializing off %lld size %lld\n", offset, size);
    memcpy(&aff_in[offset], &size, sizeof(RegionsIndexType));
    memcpy(&aff_in[(RegionsIndexType)(offset+sizeof(RegionsIndexType)/BYTES_PER_POINT)], buffer, size*BYTES_PER_POINT);

    // char offname[128];
    // sprintf(offname,"btr_%d.raw", offset);
    // std::ofstream outresfile(offname,std::ofstream::binary);
   
    //  outresfile.write(reinterpret_cast<char*>(buffer),size*BYTES_PER_POINT);

    //  outresfile.close();
#endif

     /*}
  else{
    Rect<1> elem_rect_in(Point<1>(dom_rect.lo),Point<1>(RegionsIndexType(dom_rect.lo)+size-1));

    //Rect<1> elem_rect_in(Point<1>(0),Point<1>(size-1));
    RegionAccessor<AccessorType::Generic, char> acc_in =
    region.get_field_accessor(FID_PAYLOAD).typeify<char>();
    
    RegionsIndexType i=0;
    for (GenericPointInRectIterator<1> pir(elem_rect_in); pir; pir++){
      acc_in.write(DomainPoint::from_point<1>(pir.p), buffer[i++]);
    }
    
    }*/
  
  return 1;
}

void printColors(std::set<Color> colors){

  std::set<Color>::iterator it;
  printf("colors: {");
  for(it = colors.begin(); it != colors.end(); it++){
    printf(" %d,", *it); 
  }
  printf("}\n");
}


// Generic task. Accordingly with the task descriptor received takes a number input regions, 
// copy them into arrays, executes a callback, copy the output arrays into output regions
int Controller::generic_task(const Task *task,
                   const std::vector<PhysicalRegion> &regions,
                   Context ctx, HighLevelRuntime *runtime){

// #if USE_VIRTUAL_MAPPING
//   TaskInfo info = *(TaskInfo*)task->local_args;
// #else
//   TaskInfo info = *(TaskInfo*)task->local_args;
// #endif

  int nlaunch = *(int*)task->args;
  int subregion_index = task->index_point.get_point<1>();

  //printf("retrieve metadata launch %d sub %d size %d\n", nlaunch, subregion_index, launch_data[nlaunch].arg_map.get_point(DomainPoint::from_point<1>(Point<1>(coord_t(subregion_index)))).get_size());

  TaskInfo info = *(TaskInfo*)(launch_data[nlaunch].arg_map.get_point(DomainPoint::from_point<1>(LegionRuntime::Arrays::Point<1>(coord_t(subregion_index)))).get_ptr());
   
  DEBUG_PRINT((stderr,"Task %d: regions %lu (in %d + out %d), cb %d\n", info.id, task->regions.size(), info.lenInput, info.lenOutput, info.callbackID));

  if(info.callbackID == 0){ // Is a relay task
    DEBUG_PRINT((stderr, "<<<<< I'm a relay task %d\n", info.id));

    if(info.lenInput > 1){
      fprintf(stderr, "Relay task with more than one input!\n");
      assert(false);
    }
    return relay_task(task, regions, ctx, runtime);
  }
  
  std::vector<BabelFlow::Payload> inputs;

#ifdef DEBUG_DATAFLOW
  if(info.lenInput > task->regions.size()){
    fprintf(stderr, "wrong region size for task %u expected len %lu but %lu\n", info.id, info.lenInput+info.lenOutput, task->regions.size());
    exit(0);
  }
#endif

  // Copy the inputs from the regions into buffers
  for(int i=0; i < info.lenInput; i++){
    LogicalRegion lri = task->regions[i].region;
    LogicalRegion newlr = lri;

    //std::cout << "input region " << " [" << newlr.get_index_space().get_id() << "," << subregion_index << "]" << std::endl;

    std::set<Color> colors;
    runtime->get_index_space_partition_colors(ctx,lri.get_index_space(), colors);
      
    Domain dom_incall = runtime->get_index_space_domain(ctx, newlr.get_index_space());
    
    auto rect_incall = dom_incall.get_rect<1>();

    RegionsIndexType input_size = RegionsIndexType(rect_incall.hi[0]-rect_incall.lo[0])+1;//rect_incall.volume();

    DEBUG_PRINT((stderr,"in size before RTB %d lo %lld hi %lld\n", input_size, rect_incall.lo[0], rect_incall.hi[0]));
   
    char* buffer = NULL;
    regionToBuffer(buffer, input_size, RegionsIndexType(rect_incall.lo[0]), regions[i]);// &ctx, runtime);

    DEBUG_PRINT((stderr,"in size after RTB %d lo %lld hi %lld\n", input_size, rect_incall.lo[0], rect_incall.hi[0]));

    if(RegionsIndexType(rect_incall.lo[0])+input_size > RegionsIndexType(rect_incall.hi[0])+1){
      fprintf(stderr,"ERROR: trying to write buffer outside region: reg.lo %d size %d reg.hi %d \n", RegionsIndexType(rect_incall.lo[0]), input_size, RegionsIndexType(rect_incall.hi[0]));
      assert(false);
    }

    //runtime->unmap_region(ctx,regions[i]);
    BabelFlow::Payload pay(input_size*BYTES_PER_POINT, buffer);
    
    inputs.push_back(pay);
    
  }
  
  std::vector<BabelFlow::Payload> outputs;
  
  for(int i=0; i < info.lenOutput; i++){
    // The callback will allocate the buffer
    BabelFlow::Payload pay(0, NULL);
    outputs.push_back(pay);
  }
  
  // Execute the callbacks
  mCallbacks[info.callbackID](inputs,outputs,info.id);

  DEBUG_PRINT((stderr,"Task %d executed cb %d\n", info.id, info.callbackID));

  // Copy the outputs from buffers to Regions
  for(int i=0; i < outputs.size(); i++){
    BabelFlow::Payload& pay = outputs[i];
  

  LogicalRegion curr_lr = task->regions[i+info.lenInput].region;
  IndexSpace is = curr_lr.get_index_space();

  PhysicalRegion pr = regions[i+info.lenInput];

#if USE_VIRTUAL_MAPPING
  
  // bool was_virtual_mapped = !pr.is_mapped();
  // if(was_virtual_mapped){
    auto my_bounds = runtime->get_index_space_domain(is).get_rect<1>();
    RegionsIndexType bounds_lo = my_bounds.lo[0];
   
    // remember rectangle bounds are inclusive on both sides
    LegionRuntime::Arrays::Rect<1> alloc_bounds(my_bounds.lo, (const RegionsIndexType)(bounds_lo + pay.size()/BYTES_PER_POINT - 1));

    // create a new partition with a known part_color so that other tasks can find
    // the color space will consist of the single color '0'
    DomainColoring dc;
    Domain color_space = Domain::from_rect<1>(LegionRuntime::Arrays::Rect<1>(0, 0));
    dc[0] = Domain::from_rect<1>(alloc_bounds);

    IndexPartition alloc_ip = runtime->create_index_partition(ctx,
                    is,
                    color_space,
                    dc,
                    DISJOINT_KIND, // trivial
                    PID_ALLOCED_DATA
                    );

    // now we get the name of the logical subregion we just created and inline map
    // it to generate our output
    LogicalPartition alloc_lp = runtime->get_logical_partition(curr_lr, alloc_ip);
    IndexSpace alloc_is = runtime->get_index_subspace(alloc_ip, DomainPoint::from_point<1>(0));
    LogicalRegion alloc_lr = runtime->get_logical_subregion(alloc_lp, alloc_is);

    // printf("inline mapping %lld size %lld\n",bounds_lo, pay.size()/BYTES_PER_POINT);
    // std::cout << "created subregion "<< alloc_lr.get_index_space().get_id() << " in [" << curr_lr.get_index_space().get_id() <<"," << subregion_index <<  "]" << std::endl; //<< alloc_lr << " region " << curr_lr <<

#ifdef DEBUG_DATAFLOW
    std::set<Color> colors;
    runtime->get_index_space_partition_colors(ctx,curr_lr.get_index_space(), colors);

    DEBUG_PRINT((stdout,"callback %d has colors %d subregion_index %d tree_id\n", info.callbackID, colors.size(), subregion_index));
#endif
    // tell the default mapper that we want exactly this region to be mapped
    // otherwise, its heuristics may cause it to try to map the (huge) parent
    
    InlineLauncher launcher(RegionRequirement(alloc_lr,
                WRITE_DISCARD,
                EXCLUSIVE,
                curr_lr,
                DefaultMapper::EXACT_REGION)
          .add_field(FID_PAYLOAD));
    pr = runtime->map_region(ctx, launcher);

    // this would be done as part of asking for an accessor, but do it explicitly for
    // didactic purposes - while inline mappings can often be expensive due to stalls or 
    // data movement, this should be fast because we are just allocating new space
    pr.wait_until_valid();

  // }
    
#endif

#ifdef DEBUG_DATAFLOW
    
    Domain dom_out_deb = runtime->get_index_space_domain(ctx, is);
    auto rect_all_deb = dom_out_deb.get_rect<1>();
    
    if(rect_all_deb.volume()*BYTES_PER_POINT < pay.size()){
      fprintf(stderr,"ERROR: output region too small for the payload\n\t out[%d] volume %d payload %d callback %d\n",i,rect_all_deb.volume(), pay.size(),info.callbackID);
      assert(false);
      return -1;
    }
#endif

    Domain dom_out = runtime->get_index_space_domain(ctx, is);
    LegionRuntime::Arrays::Rect<1> rect_all = dom_out.get_rect<1>();
    DEBUG_PRINT((stderr,"out size before BTR %d lo %lld hi %lld\n", pay.size()/BYTES_PER_POINT, rect_all.lo[0], rect_all.hi[0]));

#if USE_VIRTUAL_MAPPING
    bufferToRegion(pay.buffer(), pay.size()/BYTES_PER_POINT, rect_all, pr);
#else
    bufferToRegion(pay.buffer(), pay.size()/BYTES_PER_POINT, rect_all, regions[i+info.lenInput]);
    //runtime->unmap_region(ctx,regions[i+info.lenInput]);
#endif

    //runtime->unmap_region(ctx, pr);
    delete [] pay.buffer();
    
  }

  return 0;
  
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

void printEdges(std::set<EdgeTaskId> toResolveEdges){
  std::set<EdgeTaskId>::iterator edges;

  for(edges=toResolveEdges.begin(); edges != toResolveEdges.end(); edges++){
    EdgeTaskId ed = *edges;
    printf("\t edge %d %d\n", ed.first, ed.second);
  }
}

void Controller::init(){
  // fprintf(stderr,"controller run %d\n", gasnet_mynode());

  if(input_initialization != NULL)
    mInitial_inputs = input_initialization(legion_argc, legion_argv);

  for(uint32_t i=0; i < alltasks.size(); i++){
    taskmap[alltasks[i].id()] = alltasks[i];
  }

  RegionsIndexType input_block_size = 0;

  LaunchData init_launch;
  init_launch.vparts.resize(1);

  std::set<BabelFlow::TaskId> initTasks;
  std::set<EdgeTaskId> current_inputs;
  std::set<EdgeTaskId> current_outputs;

  std::vector<DomainSelection> boxes(mInitial_inputs.size());

  printf("intial size %d\n", mInitial_inputs.size());

  RegionsIndexType bsize = sizeof(FunctionType*) + 6*sizeof(GlobalIndexType) + sizeof(FunctionType);

#if !USE_VIRTUAL_MAPPING
  bsize += sizeof(RegionsIndexType);
#endif

  RegionsIndexType index = 0;
  uint32_t input_count = 0;
  std::map<BabelFlow::TaskId,BabelFlow::Payload>::iterator it;

  for(it = mInitial_inputs.begin(); it != mInitial_inputs.end(); it++){

    BabelFlow::TaskId task_id = it->first;
    BabelFlow::Payload& in_pay = it->second;
    initTasks.insert(task_id);

    boxes[input_count] = *(DomainSelection*)in_pay.buffer();
    DomainSelection& box = boxes[input_count];


    RegionsIndexType blocksize = (box.high[0]-box.low[0]+1)*(box.high[1]-box.low[1]+1)*(box.high[2]-box.low[2]+1)*sizeof(FunctionType);

    assert((blocksize+bsize) % BYTES_PER_POINT == 0);

    RegionsIndexType num_elmts = (blocksize+bsize)/BYTES_PER_POINT;

    if(input_block_size < num_elmts)
      input_block_size = num_elmts;

    LegionRuntime::Arrays::Rect<1> subrect(LegionRuntime::Arrays::Point<1>(index),LegionRuntime::Arrays::Point<1>(index+num_elmts-1));
    init_launch.vparts[0].coloring[input_count] = Domain::from_rect<1>(subrect);
    init_launch.vparts[0].input = true;

    VPartId vpart_id;
    vpart_id.round_id = launch_data.size();
    vpart_id.part_id = 0;
    vpart_id.color = input_count;

    init_launch.arg_map.set_point(DomainPoint::from_point<1>(LegionRuntime::Arrays::Point<1>(input_count)), TaskArgument(&(boxes[input_count]),sizeof(DomainSelection)));

    vpart_map[std::make_pair(BabelFlow::TNULL,task_id)] = vpart_id;
    current_inputs.insert(std::make_pair(BabelFlow::TNULL,task_id));

    index += num_elmts;
    input_count++;
  }

  init_launch.vparts[0].local_index = index;

  DEBUG_PRINT((stdout,"init data region end %d\n", index-1));

  init_launch.callback = 0; // TODO could be confused with relay...
  init_launch.n_tasks = mInitial_inputs.size();

  launch_data.push_back(init_launch);

  double ts_start = Realm::Clock::current_time_in_microseconds();

  DEBUG_PRINT((stderr,"input_block_size %d\n", input_block_size));

  // Evaluate all the launches, regions and partitions give the current input launch and tasks
  Utils::compute_launch_data(input_block_size, initTasks, current_inputs, current_outputs, taskmap, launch_data, vpart_map);

  std::cout << "Crawling time = " << std::fixed << (Realm::Clock::current_time_in_microseconds()-ts_start)/1000000.f << std::endl;

}


void Controller::top_level_task(const LegionRuntime::HighLevel::Task *task,
                    const std::vector<PhysicalRegion> &phy_regions,
                    Context ctx, HighLevelRuntime *runtime){
  init();
  double ts_start, ts_end;
  double init_time_start = Realm::Clock::current_time_in_microseconds();
  
  FieldSpace pay_fs = runtime->create_field_space(ctx);
  {
    FieldAllocator allocator = runtime->create_field_allocator(ctx, pay_fs);
    allocator.allocate_field(sizeof(RegionPointType),FID_PAYLOAD);
  }
 
  std::map<EdgeTaskId,LogicalRegion > launch_regions;

  LaunchData& init_launch = launch_data[0];

  { // Begin initialization (Data load) 

  LegionRuntime::Arrays::Rect<1> color_bounds(LegionRuntime::Arrays::Point<1>(0),LegionRuntime::Arrays::Point<1>(mInitial_inputs.size()-1));
  Domain color_domain = Domain::from_rect<1>(color_bounds);

  std::map<BabelFlow::TaskId,BabelFlow::Payload>::iterator it;

  IndexSpace data_is = runtime->create_index_space(ctx, Domain::from_rect<1>(LegionRuntime::Arrays::Rect<1>(0, init_launch.vparts[0].local_index-1)));//index-1)));
  LogicalRegion data_lr = runtime->create_logical_region(ctx, data_is, pay_fs);

  EdgeTaskId ved = std::make_pair(0, 0);
  launch_regions[ved] = data_lr;
  DEBUG_PRINT((stdout,"added input region 0, 0 (%d)\n", data_lr.get_tree_id()));
  // init_launch.callback = 0;
  // init_launch.n_tasks = mInitial_inputs.size();

  IndexPartition disjoint_ip = runtime->create_index_partition(ctx, data_is, color_domain,
                                    init_launch.vparts[0].coloring, true/*disjoint*/);
  LogicalPartition disjoint_lp = runtime->get_logical_partition(ctx, data_lr, disjoint_ip);

  IndexLauncher init_launcher(LOAD_TASK_ID, color_domain,
                              TaskArgument(NULL, 0), init_launch.arg_map);
  init_launcher.add_region_requirement(RegionRequirement(disjoint_lp, 0/*projection ID*/,
                        WRITE_DISCARD, EXCLUSIVE, data_lr));
  init_launcher.region_requirements[0].add_field(FID_PAYLOAD);

  FutureMap fm = runtime->execute_index_space(ctx, init_launcher);
  fm.wait_all_results();

  //launch_data.push_back(init_launch);

  } // End initialization (Data Load)

  std::vector<FutureMap> all_launches;

  std::cout << "Init time = " << std::fixed << (Realm::Clock::current_time_in_microseconds()-init_time_start)/1000000.f << std::endl;

  std::cout << "Total launches " << launch_data.size() << std::endl;

  // This is the timing of the top_level_task launches
  ts_start = Realm::Clock::current_time_in_microseconds();

  for(int i=1; i < launch_data.size(); i++){
    LaunchData& launch = launch_data[i];
    
    DEBUG_PRINT((stdout,"Launch %d callback %d partititions size %d\n", i, launch.callback, launch.vparts.size()));
    DEBUG_PRINT((stdout,"coloring size %d ntasks %u\n",launch.vparts[0].coloring.size(), launch.n_tasks));

    LegionRuntime::Arrays::Rect<1> color_bounds(LegionRuntime::Arrays::Point<1>(0),LegionRuntime::Arrays::Point<1>(launch.n_tasks-1));
    Domain color_domain = Domain::from_rect<1>(color_bounds);

    IndexLauncher curr_launcher(GENERIC_TASK_ID, color_domain,
                                  TaskArgument(&i, sizeof(int)), ArgumentMap());//launch.arg_map);

    for(int v=0; v < launch.vparts.size(); v++){
      VirtualPartition& vpart = launch.vparts[v];

      EdgeTaskId ved = std::make_pair(vpart.id.round_id, vpart.id.part_id);
      DEBUG_PRINT((stdout,"Region req %d last index %d\n", v, vpart.local_index));
      IndexSpace curr_is;
      LogicalRegion curr_lr;

      if(launch_regions.find(ved) == launch_regions.end()){
        curr_is = runtime->create_index_space(ctx, Domain::from_rect<1>(LegionRuntime::Arrays::Rect<1>(0,vpart.local_index-1)));
        curr_lr = runtime->create_logical_region(ctx, curr_is, pay_fs);
        launch_regions[ved] = curr_lr;
        //DEBUG_PRINT((stdout,"adding new region %d, %d (%d)\n", vpart.id.round_id, vpart.id.part_id, curr_lr.get_tree_id()));
      }else{
        curr_lr = launch_regions[ved];
        curr_is = curr_lr.get_index_space();
        //DEBUG_PRINT((stdout,"using existing region %d, %d (%d)\n", vpart.id.round_id, vpart.id.part_id, curr_lr.get_tree_id()));
      }

      // std::set<Color> col;
      //   for(std::map<Color,Domain>::iterator itc = vpart.coloring.begin(); itc != vpart.coloring.end(); ++itc) {
      //     col.insert(itc->first);
      //     Rect<1> rdom = itc->second.get_rect<1>();
      //     printf("\t[%d %d]\n", rdom.lo[0], rdom.hi[0]);
      //   }
      // printColors(col);

      if(vpart.input){

#if USE_VIRTUAL_MAPPING
        IndexPartition curr_ip = runtime->get_index_partition(ctx, curr_is, 0);
#else
        IndexPartition curr_ip = runtime->create_index_partition(ctx, curr_is, color_domain, vpart.coloring, false/*disjoint*/);
#endif

       LogicalPartition curr_lp = runtime->get_logical_partition(ctx, curr_lr, curr_ip);

#if USE_VIRTUAL_MAPPING
        if(i>1){
          curr_launcher.add_region_requirement(RegionRequirement(curr_lp, PFID_USE_DATA_TASK/*projection ID*/,
                              READ_ONLY, EXCLUSIVE, curr_lr, DefaultMapper::EXACT_REGION));
        }
        else{
            curr_launcher.add_region_requirement(RegionRequirement(curr_lp, 0/*projection ID*/,
                              READ_ONLY, EXCLUSIVE, curr_lr, DefaultMapper::EXACT_REGION));
        }

#else
      curr_launcher.add_region_requirement(RegionRequirement(curr_lp, 0/*projection ID*/,
                              READ_ONLY, EXCLUSIVE, curr_lr, DefaultMapper::EXACT_REGION));
#endif
        

      }else{
        IndexPartition curr_ip = runtime->create_index_partition(ctx, curr_is, color_domain,
                                      vpart.coloring, true/*disjoint*/);

        LogicalPartition curr_lp = runtime->get_logical_partition(ctx, curr_lr, curr_ip);

#if USE_VIRTUAL_MAPPING
        curr_launcher.add_region_requirement(RegionRequirement(curr_lp, 0/*projection ID*/,
                              WRITE_DISCARD, EXCLUSIVE, curr_lr, DefaultMapper::VIRTUAL_MAP));
#else
        curr_launcher.add_region_requirement(RegionRequirement(curr_lp, 0/*projection ID*/,
                              WRITE_DISCARD, EXCLUSIVE, curr_lr, DefaultMapper::EXACT_REGION));
#endif

      }

      curr_launcher.region_requirements[v].add_field(FID_PAYLOAD);

    }

    all_launches.push_back(runtime->execute_index_space(ctx, curr_launcher));//.wait_all_results();

  }

  ts_end = Realm::Clock::current_time_in_microseconds();
  std::cout << std::fixed << "Dataflow time = " << (ts_end-ts_start)/1000000.f << std::endl;

  runtime->destroy_field_space(ctx, pay_fs);

  // wait for all the launchers to finish and destroy things that need runtime alive
  /*for(auto& launch : all_launches){
    launch.wait_all_results();
  }

  launch_data.clear();
  */
}

int Controller::initialize(const BabelFlow::TaskGraph& graph, const BabelFlow::TaskMap* task_map, int argc, char **argv){
  // Assume there is only one controller, get all the tasks in the graph
  alltasks = graph.localGraph(0, task_map);

  for (int i = 1; i < argc; i++){
    if (!strcmp(argv[i],"-d")){
      data_size[0] = atoi(argv[++i]);
      data_size[1] = atoi(argv[++i]);
      data_size[2] = atoi(argv[++i]);
    }
  }

  legion_argc = argc;
  legion_argv = argv;
  return 1;
}

#if 0
void Controller::mapper_registration(Machine machine, HighLevelRuntime *rt,
                         const std::set<Processor> &local_procs)
{
  for (std::set<Processor>::const_iterator it = local_procs.begin();
       it != local_procs.end(); it++){
    rt->replace_default_mapper(new DataFlowMapper(machine, rt, *it), *it);

  }
}
#endif

int Controller::run(std::map<BabelFlow::TaskId,BabelFlow::Payload>& initial_inputs){
  mInitial_inputs = initial_inputs;

  return HighLevelRuntime::start(legion_argc, legion_argv);
}

int Controller::registerCallback(BabelFlow::CallbackId id, Callback func){
  assert (id > 0); // Callback 0 is reserved for relays
  
  if (mCallbacks.size() <= id)
    mCallbacks.resize(id+1);
  
  mCallbacks[id] = func;
  
  return 1;
}

Controller::Controller()
{
//  HighLevelRuntime::set_top_level_task_id(TOP_LEVEL_TASK_ID);
//  HighLevelRuntime::register_legion_task<top_level_task>(TOP_LEVEL_TASK_ID,
//                      Processor::LOC_PROC, true/*single*/, false/*index*/,
//                      AUTO_GENERATE_ID, TaskConfigOptions(), "top_level");

  {
    TaskVariantRegistrar top_level_registrar(TOP_LEVEL_TASK_ID);
    top_level_registrar.add_constraint(ProcessorConstraint(Processor::LOC_PROC));
    top_level_registrar.set_replicable();
    top_level_registrar.set_inner();
    Runtime::preregister_task_variant<top_level_task>(top_level_registrar,
                                                      "Top Level Task");
    Runtime::set_top_level_task_id(TOP_LEVEL_TASK_ID);
  }

  HighLevelRuntime::register_legion_task<int, generic_task>(GENERIC_TASK_ID,
                      Processor::LOC_PROC, true/*single*/, false/*index*/,
                      AUTO_GENERATE_ID, TaskConfigOptions(true/*leaf*/), "generic_task");
/*
  {
    TaskVariantRegistrar generic_task_registrar(GENERIC_TASK_ID);
    generic_task_registrar.add_constraint(ProcessorConstraint(Processor::LOC_PROC));
    Runtime::preregister_task_variant<int, generic_task>(generic_task_registrar,
                                                      "generic_task");
  }*/

#if USE_VIRTUAL_MAPPING
  Runtime::preregister_projection_functor(PFID_USE_DATA_TASK,
            new UseDataProjectionFunctor);
#endif

  // Currently using Default mapper 

  //HighLevelRuntime::set_registration_callback(mapper_registration);
  
}
