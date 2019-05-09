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
#include <thread>
#include <mutex>
#include <sys/time.h>
#include <stdint.h>
#include "legion.h"

#include "Utils.h"
#include "datatypes.h"
#include "DataFlowMapper.h"
#include "Controller.h"
#include "default_mapper.h"
#include "cgmapper.h"

//#include "idxio/read_block.h"

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

#define USE_AFFINE_ACCESSOR 1

//#define USE_SINGLE_REGION 1

#define TEMP_PMT_HARDCODED 1
#define IO_READ_BLOCK 1

//#define OUTPUT_SIZE 5000000
#ifndef DATA_SIZE_POINT
#define DATA_SIZE_POINT 200*200*200
#endif

struct ShardArgs{
  int rank;
  uint32_t n_pb;
  uint32_t offset;
  int max_shard;
  int round;

  template <class Archive>
  void save( Archive & ar ) const
  {
    ar(rank, n_pb, offset, max_shard, round);
  }

  template <class Archive>
  void load( Archive & ar )
  {
    ar(rank, n_pb, offset, max_shard, round);
  }
};

#include <cereal/types/map.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/complex.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/archives/json.hpp>
#include <cereal/archives/portable_binary.hpp>

struct ExternalInfo{

  uint32_t pb_index;
  uint32_t edge_index;
  int owner;

  template <class Archive>
  void save( Archive & ar ) const
  {
    ar(pb_index, edge_index, owner);
  }

  template <class Archive>
  void load( Archive & ar )
  {
    ar(pb_index, edge_index, owner);
  }
};

struct SerTask{
  BabelFlow::TaskId id;
  BabelFlow::CallbackId callback;
  std::vector<BabelFlow::TaskId> incoming;
  std::vector<std::vector<BabelFlow::TaskId> > outgoing;

  template <class Archive>
  void save( Archive & ar ) const
  {
    ar(id, callback, incoming, outgoing);
  }

  template <class Archive>
  void load( Archive & ar ) 
  {
    ar(id, callback, incoming, outgoing);
  }
};

class ShardMetadata{
private:

    friend class cereal::access;

    template <class Archive>
    void serialize( Archive & ar )
    {
      shard_ordered_ser.resize(shard_ordered.size());

      for(int i=0; i<shard_ordered.size(); i++){
        shard_ordered_ser[i].incoming = shard_ordered[i].incoming();
        shard_ordered_ser[i].outgoing = shard_ordered[i].outputs();
        shard_ordered_ser[i].id = shard_ordered[i].id();
        shard_ordered_ser[i].callback = shard_ordered[i].callback();
      }
      ar( sargs, CEREAL_NVP(externals_map), CEREAL_NVP(shard_ordered_ser), CEREAL_NVP(shared_edges));
      
    }
public:
    ShardMetadata() = default;

    ShardArgs sargs;

    std::map<EdgeTaskId, ExternalInfo> externals_map;
    std::vector<BabelFlow::Task> shard_ordered;
    std::map<EdgeTaskId, EdgeTaskId> shared_edges;
    std::vector<SerTask> shard_ordered_ser;

    template <class Archive>
    void deserialize( Archive & ar )
    { 
      ar( sargs, CEREAL_NVP(externals_map), CEREAL_NVP(shard_ordered_ser), CEREAL_NVP(shared_edges));

      shard_ordered.resize(shard_ordered_ser.size());

      for(int i=0; i<shard_ordered.size(); i++){
        shard_ordered[i].incoming() = shard_ordered_ser[i].incoming;
        shard_ordered[i].outputs() = shard_ordered_ser[i].outgoing;
        shard_ordered[i].id(shard_ordered_ser[i].id);
        shard_ordered[i].callback(shard_ordered_ser[i].callback);
      }
    }

};

#if TEMP_PMT_HARDCODED
BabelFlow::Payload make_local_block(FunctionType* data, 
                           GlobalIndexType low[3], GlobalIndexType high[3], 
                           FunctionType threshold);
#endif

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

const BabelFlow::TaskGraph* graph;
const BabelFlow::TaskMap* task_map;

std::vector<InputDomainSelection> boxes;
//std::vector<std::map<BabelFlow::TaskId, BabelFlow::Task> > shard_tasks;

// std::vector<std::map<EdgeTaskId, ExternalInfo> > externals_map;

// std::vector<std::vector<BabelFlow::Task> > shard_ordered;

//std::vector<std::set<BabelFlow::TaskId> > boot_tasks;

class FieldHelperBase {
protected:
  // this is shared by all FieldHelper<T>'s
  static FieldID next_static_id;
};

/*static*/ FieldID FieldHelperBase::next_static_id = 10000;


template <typename T>
class FieldHelper : protected FieldHelperBase {
public:
  static const FieldID ASSIGN_STATIC_ID = AUTO_GENERATE_ID - 1;

  FieldHelper(const char *_name, FieldID _fid = ASSIGN_STATIC_ID)
    : name(_name), fid(_fid)
  {
    if(fid == ASSIGN_STATIC_ID)
      fid = next_static_id++;
  }

  ~FieldHelper(void) {}

  operator FieldID(void) const
  {
    assert(fid != AUTO_GENERATE_ID);
    return fid;
  }

  void allocate(Runtime *runtime, Context ctx, FieldSpace fs)
  {
    FieldAllocator fa = runtime->create_field_allocator(ctx, fs);
    fid = fa.allocate_field(sizeof(T), fid);
    runtime->attach_name(fs, fid, name);
  }

  template <typename AT>
  RegionAccessor<AT, T> accessor(const PhysicalRegion& pr)
  {
    assert(fid != AUTO_GENERATE_ID);
    return pr.get_field_accessor(fid).template typeify<T>().template convert<AT>();
  }

  template <typename AT>
  RegionAccessor<AT, T> fold_accessor(const PhysicalRegion& pr)
  {
    assert(fid != AUTO_GENERATE_ID);
    std::vector<FieldID> fields;
    pr.get_fields(fields);
    assert((fields.size() == 1) && (fields[0] == fid));
    return pr.get_accessor().template typeify<T>().template convert<AT>();
  }

protected:
  const char *name;
  FieldID fid;
};

FieldHelper<EdgeTaskId> fid_edge_shard("edge_shard");
FieldHelper<PhaseBarrier> fid_done_barrier("done_barrier");

void print_domain(Context ctx, Runtime *runtime, LogicalRegion newlr){
  IndexSpace newis = newlr.get_index_space();
  Domain dom_new = runtime->get_index_space_domain(ctx, newis);
  Rect<1> rect_new = dom_new.get_rect<1>();
  printf("domain volume %zu lo %d hi %d\n", rect_new.volume(), (int)rect_new.lo, (int)rect_new.hi);

}

#if TEMP_PMT_HARDCODED
RegionsIndexType estimate_output_size(BabelFlow::CallbackId cid, int ro, RegionsIndexType input_block_size){

  RegionsIndexType output_size;

  if(cid ==1){
    if(ro == 0)
      output_size = input_block_size/9;///9;
    else
      output_size = input_block_size+input_block_size/4;
  }
  else if(cid == 2){
    output_size = input_block_size;//input_block_size+input_block_size/9;//10
  }
  else if(cid == 3){
    output_size = input_block_size+input_block_size/4;
  }
  else
    output_size = input_block_size;
  
  return output_size;

}
#else
RegionsIndexType estimate_output_size(BabelFlow::CallbackId cid, int ro, RegionsIndexType input_block_size){

  if(cid == 1)
    return input_block_size;
  else
    return input_block_size;

}
#endif

// Tasks responsible to make copies of the first region into the others
int relay_task(const Task *task,
                             const std::vector<PhysicalRegion> &regions,
                             Context ctx, Runtime *runtime){

  RegionAccessor<AccessorType::Generic, RegionPointType> acc_in =
      regions[0].get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>();
  
  RegionAccessor<AccessorType::Affine<1>, RegionPointType> aff_in =
      acc_in.convert<AccessorType::Affine<1> >();

  IndexSpace is = task->regions[0].region.get_index_space();
  Rect<1> rect = runtime->get_index_space_domain(ctx, is).get_rect<1>();
    

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

    Rect<1> my_bounds = runtime->get_index_space_domain(curr_is).get_rect<1>();
    RegionsIndexType bounds_lo = my_bounds.lo[0];
   
    // remember rectangle bounds are inclusive on both sides
    Rect<1> alloc_bounds(my_bounds.lo, bounds_lo + size - 1);

    DomainColoring dc;
    Domain color_space = Domain::from_rect<1>(Rect<1>(0, 0));
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

    RegionAccessor<AccessorType::Generic, RegionPointType> acc_out =
      pr.get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>();
    RegionAccessor<AccessorType::Affine<1>, RegionPointType> aff_out =
      acc_out.convert<AccessorType::Affine<1> >();

    Rect<1> curr_rect = runtime->get_index_space_domain(ctx, alloc_is).get_rect<1>();
   //RegionsIndexType curr_full_size = RegionsIndexType(curr_rect.hi[0]-curr_rect.lo[0]+1);

    //memset(&aff_out[RegionsIndexType(curr_rect.lo[0])],0,curr_full_size*BYTES_PER_POINT);
    memcpy(&aff_out[RegionsIndexType(curr_rect.lo[0])], &aff_in[RegionsIndexType(rect.lo[0])], size*BYTES_PER_POINT);

#else

    RegionAccessor<AccessorType::Generic, RegionPointType> acc_out =
      regions[i+1].get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>();
    RegionAccessor<AccessorType::Affine<1>, RegionPointType> aff_out =
      acc_out.convert<AccessorType::Affine<1> >();

    IndexSpace curr_is = task->regions[i+1].region.get_index_space();
    Rect<1> curr_rect = runtime->get_index_space_domain(ctx, curr_is).get_rect<1>();
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
int Controller::regionToBuffer(char*& buffer, RegionsIndexType& size, RegionsIndexType offset,
      const PhysicalRegion& region){ 
  
  RegionAccessor<AccessorType::Generic, RegionPointType> acc_in =
    region.get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>();
  
  //  bool affine = acc_in.can_convert<AccessorType::Affine<1> >();
  //Domain dom_incall = runtime->get_index_space_domain(*ctx, region.get_logical_region().get_index_space());
  //Rect<1> dom_rect = dom_incall.get_rect<1>();

  //printf("RTB volume %d my vol %llu size %llu\n", dom_rect.volume() , RegionsIndexType(dom_rect.hi-dom_rect.lo)+1, size);
  //  assert(elem_rect_in.volume() >= size);

#if USE_AFFINE_ACCESSOR

    RegionAccessor<AccessorType::Affine<1>, RegionPointType> aff_in = 
      acc_in.convert<AccessorType::Affine<1> >();

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

#else
    
    RegionsIndexType size_offset = offset/BYTES_PER_POINT+sizeof(RegionsIndexType)/BYTES_PER_POINT;

    Rect<1> elem_rect_size((Point<1>(offset/BYTES_PER_POINT)),(Point<1>(size_offset-1)));
    
    RegionsIndexType i=0;
    for (GenericPointInRectIterator<1> pir(elem_rect_size); pir; pir++){
      RegionsIndexType p = acc_in.read(DomainPoint::from_point<1>(pir.p));
      //printf("point in %d\n", i);
      ((RegionPointType*)&size)[i++] = p;
    }

    buffer = (char*)malloc(BYTES_PER_POINT*size);

    memcpy(buffer, &size, sizeof(RegionsIndexType));
    //printf("size found %d\n", size);

    Rect<1> elem_rect_in((Point<1>(size_offset)),
                         (Point<1>(size_offset+size-1)));

    i=0;
    for (GenericPointInRectIterator<1> pir(elem_rect_in); pir; pir++){
      ((RegionPointType*)buffer)[i++] = acc_in.read(DomainPoint::from_point<1>(pir.p));
    }
#endif
  
  return 1;
}

// Copy the content of a buffer into a Region
int Controller::bufferToRegion(char* buffer, RegionsIndexType size, Rect<1> rect, const PhysicalRegion& region){ //, Context* ctx, Legion::Runtime* runtime){
  
  RegionAccessor<AccessorType::Generic, RegionPointType> acc_in =
     region.get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>();
  
  RegionsIndexType offset = RegionsIndexType(rect.lo);

  //bool affine = acc_in.can_convert<AccessorType::Affine<1> >();
  
  //Domain dom_incall = runtime->get_index_space_domain(*ctx, region.get_logical_region().get_index_space());
  //Rect<1> dom_rect = dom_incall.get_rect<1>();

  // printf("BTR volume %d my vol %d size %d\n", dom_rect.volume() ,  RegionsIndexType(dom_rect.hi- dom_rect.lo)+1, size);
  
  //assert(elem_rect_in.volume() >= size);

#if USE_AFFINE_ACCESSOR
    RegionAccessor<AccessorType::Affine<1>, RegionPointType> aff_in =
      acc_in.convert<AccessorType::Affine<1> >();
  
 #if USE_VIRTUAL_MAPPING
    memcpy(&aff_in[offset], buffer, size*BYTES_PER_POINT); //dom_rect.lo],buffer, size);
 #else

    RegionsIndexType full_size = RegionsIndexType(rect.hi[0]-rect.lo[0]);
    
    //memset(&aff_in[offset],0,full_size*BYTES_PER_POINT);

    //printf("serializing off %lld size %lld\n", offset, size);
    memcpy(&aff_in[offset], &size, sizeof(RegionsIndexType));
    memcpy(&aff_in[(RegionsIndexType)(offset+sizeof(RegionsIndexType)/BYTES_PER_POINT)], buffer, size*BYTES_PER_POINT);

    // char offname[128];
    // sprintf(offname,"btr_%d.raw", offset);
    // std::ofstream outresfile(offname,std::ofstream::binary);
   
    //  outresfile.write(reinterpret_cast<char*>(buffer),size*BYTES_PER_POINT);

    //  outresfile.close();
 #endif

#else

      RegionsIndexType size_offset = offset/BYTES_PER_POINT+sizeof(RegionsIndexType)/BYTES_PER_POINT;

      Rect<1> elem_rect_size((Point<1>(offset/BYTES_PER_POINT)),(Point<1>(size_offset-1)));
      Rect<1> elem_rect_in((Point<1>(size_offset)),
                           (Point<1>(size_offset+size-1)));

      //Rect<1> elem_rect_in(Point<1>(0),Point<1>(size-1));
      // RegionAccessor<AccessorType::Generic, char> acc_in =
      // region.get_field_accessor(FID_PAYLOAD).typeify<char>();
      
      RegionsIndexType i=0;
      for (GenericPointInRectIterator<1> pir(elem_rect_size); pir; pir++){
        //printf("point %d\n",i);
        acc_in.write(DomainPoint::from_point<1>(pir.p), ((RegionPointType*)&size)[i++]);
      }

      i=0;
      //printf("extent %d %d size %d actual size %d\n",elem_rect_in.lo[0], elem_rect_in.hi[0], elem_rect_in.hi[0]-elem_rect_in.lo[0], size);
      for (GenericPointInRectIterator<1> pir(elem_rect_in); pir; pir++){
        //printf("point %d\n",i);
        acc_in.write(DomainPoint::from_point<1>(pir.p), ((RegionPointType*)(buffer))[i++]);
      }
    
    }
#endif

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

#if 0
// Task for data loading
bool Controller::load_task(const Task *task,
                   const std::vector<PhysicalRegion> &regions,
                   Context ctx, Runtime *runtime){
   //printf("entering arglen %d\n", task->local_arglen);
  Domain dom_incall = runtime->get_index_space_domain(ctx, task->regions[0].region.get_index_space());
  Rect<1> elem_rect_in = dom_incall.get_rect<1>();

  //printf("from task %lld volume %d!\n", task->index_point.point_data[0], elem_rect_in.volume());
  //assert(task->local_arglen == sizeof(InputDomainSelection));
  assert(task->arglen == sizeof(InputDomainSelection));
  
  InputDomainSelection box = *(InputDomainSelection*)task->args;//task->local_args;//args;
  /*if(elem_rect_in.volume() == 0){
    fprintf(stdout,"invalid rect %llu %llu\n", elem_rect_in.lo,elem_rect_in.hi);
    fprintf(stdout,"invalid data %d %d %d - %d %d %d \n", box.low[0],box.low[1],box.low[2],box.high[0],box.high[1],box.high[2]);
    assert(false);
    }*/

  FunctionType threshold = (FunctionType)(-1)*FLT_MAX;
  char* dataset = NULL;
  //fprintf(stderr, "input initialization started\n");

  for (int i = 1; i < legion_argc; i++){
#if TEMP_PMT_HARDCODED
    if (!strcmp(legion_argv[i],"-t"))
#else
    if (!strcmp(legion_argv[i],"-i"))
#endif
      threshold = atof(legion_argv[++i]);
    if (!strcmp(legion_argv[i],"-f"))
      dataset = legion_argv[++i];
  }

  DEBUG_PRINT((stdout,"Load data %d %d %d - %d %d %d thr %f\n", box.low[0],box.low[1],box.low[2],box.high[0],box.high[1],box.high[2], threshold));

  char* data_block;

#if IO_READ_BLOCK
  data_block = read_block(dataset,box.low,box.high);
#else
  box.low[0] = 0;
  box.low[1] = 0;
  box.low[2] = 0;
  box.high[0] = 1;
  box.high[1] = 1;
  box.high[2] = 1;

  data_block = (char*)malloc(sizeof(FunctionType));
  FunctionType v = 1;
  memcpy(data_block, &v, sizeof(FunctionType));
  fprintf(stderr, "NO IO READ\n");
#endif

#if TEMP_PMT_HARDCODED
  BabelFlow::Payload pay = make_local_block((FunctionType*)(data_block), box.low, box.high, threshold);
#else

  #if IO_READ_BLOCK
    uint32_t block_size = (box.high[0]-box.low[0]+1)*(box.high[1]-box.low[1]+1)*(box.high[2]-box.low[2]+1);
    uint32_t input_size = sizeof(InputDomainSelection)+sizeof(float)+block_size*sizeof(FunctionType);
    char* input = (char*)malloc(input_size);

    memcpy(input, &box, sizeof(InputDomainSelection));
    memcpy(input+sizeof(InputDomainSelection), &threshold, sizeof(float));
    memcpy(input+sizeof(float)+sizeof(InputDomainSelection), data_block, block_size);

    BabelFlow::Payload pay(input_size, input);
  #else
    BabelFlow::Payload pay(sizeof(FunctionType), data_block);
  #endif

#endif

  // char filename[128];
  // sprintf(filename,"dump_%d_%d_%d.raw", box.low[0],box.low[1],box.low[2]);
  // std::ofstream outfile (filename,std::ofstream::binary);

  // outfile.write (pay.buffer(),pay.size());

  // outfile.close();

  // char filename[128];
  // sprintf(filename, "outblock_%d.raw", task->index_point.point_data[0]);
  // std::ofstream outfile (filename,std::ofstream::binary);
  // outfile.write(pay.buffer(), pay.size());
  // outfile.close();

  //printf("volume in %d size %d\n", task->index_point.point_data[0], pay.size());
  assert(elem_rect_in.volume() >= pay.size()/BYTES_PER_POINT);

  bufferToRegion(pay.buffer(), pay.size()/BYTES_PER_POINT, elem_rect_in, regions[0]);//&ctx, runtime);
  //runtime->unmap_region(ctx,regions[0]);

  // char offname[128];
  //   sprintf(offname,"read_%d.raw", RegionsIndexType(elem_rect_in.lo));
  //   std::ofstream outresfile(offname,std::ofstream::binary);
   
  //    outresfile.write(reinterpret_cast<char*>(pay.buffer()),pay.size());

  //    outresfile.close();

  delete [] pay.buffer();

  return true;
}

#endif

// Generic task. Accordingly with the task descriptor received takes a number input regions, 
// copy them into arrays, executes a callback, copy the output arrays into output regions
int Controller::generic_task(const Task *task,
                   const std::vector<PhysicalRegion> &regions,
                   Context ctx, Runtime *runtime){

// #if USE_VIRTUAL_MAPPING
//   TaskInfo info = *(TaskInfo*)task->local_args;
// #else
//   TaskInfo info = *(TaskInfo*)task->local_args;
// #endif

  double g_start = Realm::Clock::current_time_in_microseconds();

  TaskInfo info = *(TaskInfo*)task->args;

  DEBUG_PRINT((stderr, "generic %d (%d) n_regs %d cb %d in %d out %d\n", info.id, task->get_unique_id(), regions.size(), info.callbackID, info.lenInput, info.lenOutput));

  if(info.callbackID == 0){ // Is a relay task
    DEBUG_PRINT((stderr, "<<<<< I'm a relay task %d\n", info.id));

    if(info.lenInput > 1){
      fprintf(stderr, "Relay task with more than one input!\n");
      assert(false);
    }

    relay_task(task, regions, ctx, runtime);
    DEBUG_PRINT((stderr,"task %d DONE\n", info.id));

    return 0;

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

    // std::set<Color> colors;
    // runtime->get_index_space_partition_colors(ctx,lri.get_index_space(), colors);
      
    Domain dom_incall = runtime->get_index_space_domain(ctx, newlr.get_index_space());
    
    Rect<1> rect_incall = dom_incall.get_rect<1>();

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

    DEBUG_PRINT((stderr,"%d: IN region %d\n", info.id, lri.get_tree_id()));
    
  }
  
  std::vector<BabelFlow::Payload> outputs;
  
  for(int i=0; i < info.lenOutput; i++){
    // The callback will allocate the buffer
    BabelFlow::Payload pay(0, NULL);
    outputs.push_back(pay);
  }

  double g_elaps = Realm::Clock::current_time_in_microseconds()-g_start;

  // Execute the callbacks
  double t_start = Realm::Clock::current_time_in_microseconds();
  mCallbacks[info.callbackID](inputs,outputs,info.id);
  double t_end = Realm::Clock::current_time_in_microseconds();

#if PROFILING_TIMING
  std::cout << std::fixed << gasnet_mynode() << ",callback," << (int)info.callbackID << ","<< (t_end-t_start)/1000000.0f <<std::endl;
#endif

  double g2_start = Realm::Clock::current_time_in_microseconds();

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
    Rect<1> my_bounds = runtime->get_index_space_domain(is).get_rect<1>();
    RegionsIndexType bounds_lo = my_bounds.lo[0];
   
    // remember rectangle bounds are inclusive on both sides
    Rect<1> alloc_bounds(my_bounds.lo, (const RegionsIndexType)(bounds_lo + pay.size()/BYTES_PER_POINT - 1));

    // create a new partition with a known part_color so that other tasks can find
    // the color space will consist of the single color '0'
    DomainColoring dc;
    Domain color_space = Domain::from_rect<1>(Rect<1>(0, 0));
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
    Rect<1> rect_all_deb = dom_out_deb.get_rect<1>();
    
    if(rect_all_deb.volume()*BYTES_PER_POINT < pay.size()){
      fprintf(stderr,"ERROR: output region too small for the BabelFlow::Payload\n\t out[%d] volume %d BabelFlow::Payload %d callback %d\n",i,rect_all_deb.volume(), pay.size(),info.callbackID);
      assert(false);
      return -1;
    }
#endif

    Domain dom_out = runtime->get_index_space_domain(ctx, is);
    Rect<1> rect_all = dom_out.get_rect<1>();
    DEBUG_PRINT((stderr,"out size before BTR %d lo %lld hi %lld\n", pay.size()/BYTES_PER_POINT, rect_all.lo[0], rect_all.hi[0]));

#if USE_VIRTUAL_MAPPING
    bufferToRegion(pay.buffer(), pay.size()/BYTES_PER_POINT, rect_all, pr);
    
#else
    // RegionAccessor<AccessorType::Generic, RegionPointType> acc_in =
    //  regions[i+info.lenInput].get_field_accessor(FID_PAYLOAD).typeify<RegionPointType>();

    //   Domain dom_incall = runtime->get_index_space_domain(ctx, regions[i+info.lenInput].get_logical_region().get_index_space());
    //   Rect<1> dom_rect = dom_incall.get_rect<1>();
    //   for (GenericPointInRectIterator<1> pir(dom_rect); pir; pir++){
    //     acc_in.write(DomainPoint::from_point<1>(pir.p), 0);
    //   }

    //   printf("zeroize done\n");

    bufferToRegion(pay.buffer(), pay.size()/BYTES_PER_POINT, rect_all, regions[i+info.lenInput]);

    //runtime->unmap_region(ctx,regions[i+info.lenInput]);
#endif

    DEBUG_PRINT((stderr,"%d: OUT region %d\n", info.id, regions[i+info.lenInput].get_logical_region().get_tree_id()));
    
    runtime->unmap_region(ctx, pr);
    delete [] pay.buffer();
    
  }

  if(regions.size() > info.lenOutput+info.lenInput){
    int last_out = info.lenOutput+info.lenInput-1;

    for(int i=last_out+1; i < regions.size(); i++){
      CopyLauncher copy_launcher;
      copy_launcher.add_copy_requirements(
          RegionRequirement(task->regions[last_out].region, READ_ONLY,
                            EXCLUSIVE, task->regions[last_out].region),
          RegionRequirement(task->regions[i].region, WRITE_DISCARD,
                            EXCLUSIVE, task->regions[i].region));
      copy_launcher.add_src_field(0, FID_PAYLOAD);
      copy_launcher.add_dst_field(0, FID_PAYLOAD);
      
      runtime->issue_copy_operation(ctx, copy_launcher);

      DEBUG_PRINT((stderr,"copied region %d to %d\n", last_out, i));
    }

  }

  DEBUG_PRINT((stderr,"task %d DONE\n", info.id));

  g_elaps += Realm::Clock::current_time_in_microseconds()-g2_start;

#if PROFILING_TIMING
  std::cout << std::fixed << gasnet_mynode() << ",task_prep," << (int)info.callbackID << ","<< (g_elaps)/1000000.0f <<std::endl;
#endif
  
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

std::set<EdgeTaskId> find_boot_tasks(const std::vector<BabelFlow::Task>& lt, const std::vector<BabelFlow::TaskId>& task_ids, std::set<BabelFlow::TaskId>& shard_boot){

  std::set<EdgeTaskId> externals;

  for(uint32_t j = 0; j < lt.size(); j++){
      
      BabelFlow::Task task = lt[j];

      uint32_t count_pb = 0;
    
      for(int i=0; i < task.incoming().size(); i++){
        BabelFlow::TaskId in_tid = task.incoming()[i];

        if(in_tid == BabelFlow::TNULL) {
          shard_boot.insert(task.id());
          continue;
        }

        if(std::find(task_ids.begin(), task_ids.end(), in_tid) == task_ids.end()){        
          count_pb++;
          EdgeTaskId edge;
          edge.first = in_tid;
          edge.second = task.id();
          externals.insert(edge);
        }
    
      }

      if(count_pb == task.incoming().size()){ // only external inputs
        shard_boot.insert(task.id());
      }

  }

  return externals;
}

std::map<EdgeTaskId, ExternalInfo> compute_externals(BabelFlow::TaskId shardId, std::vector<BabelFlow::Task>& ordered_tasks){
  std::vector<BabelFlow::TaskId> task_ids = task_map->tasks(shardId);

  std::vector<BabelFlow::Task> lt = graph->localGraph(shardId, task_map);

  std::map<BabelFlow::TaskId, BabelFlow::Task> temp_map;
  
  for(int i=0; i < lt.size(); i++){
    temp_map[lt[i].id()] = lt[i];
  }

  std::set<BabelFlow::TaskId> boot_tasks;

  std::set<EdgeTaskId> externals = find_boot_tasks(lt, task_ids, boot_tasks);

  //Utils::reorder_tasks(ordered_tasks, externals, boot_tasks[shardId], temp_map, shardId);

#if TEMP_PMT_HARDCODED
  Utils::reorder_tasks(ordered_tasks, temp_map, boot_tasks);
#else
  ordered_tasks.insert(ordered_tasks.end(), lt.begin(), lt.end());
#endif

  // std::map<BabelFlow::TaskId, BabelFlow::Task> temp_map;

  // std::set<BabelFlow::TaskId> shard_boot;

  // for(int i=0; i < ordered_tasks.size(); i++)
  //   temp_map[lt[i].id()] = ordered_tasks[i];

  // shard_tasks.push_back(temp_map);

  std::map<EdgeTaskId, ExternalInfo> pbs;
  std::set<EdgeTaskId> int_pbs;

  int f_counter = 0;
  //uint32_t count_pb = 0;
  for(uint32_t j = 0; j < ordered_tasks.size(); j++){
      
      BabelFlow::Task& task = ordered_tasks[j];

      for(int i=0; i < task.incoming().size(); i++){
        BabelFlow::TaskId in_tid = task.incoming()[i];

        if(in_tid == BabelFlow::TNULL) {
          //shard_boot.insert(task.id());
          continue;
        }

        EdgeTaskId edge;
        edge.first = in_tid;
        edge.second = task.id();

        if(std::find(task_ids.begin(), task_ids.end(), in_tid) == task_ids.end()){        
          pbs[edge].pb_index = BabelFlow::TNULL;
          //pbs[edge].edge_index = count_pb++;
        }
        else 
          int_pbs.insert(edge);

      }

      for(int ro=0; ro < task.outputs().size(); ro++){
        for(int inro=0; inro < task.outputs()[ro].size(); inro++){

          BabelFlow::TaskId out_tid = task.outputs()[ro][inro];

          //if(out_tid == BabelFlow::TNULL) continue;

          EdgeTaskId edge;
          edge.first = task.id();
          edge.second = out_tid;

          if(std::find(task_ids.begin(), task_ids.end(), out_tid) == task_ids.end()){
            pbs[edge].pb_index = BabelFlow::TNULL;
            //pbs[edge].edge_index = count_pb++;
            // count_pb++;
          }
          else
            int_pbs.insert(edge);

        }
      }

  }

#ifdef DEBUG_DATAFLOW
  printf("%d internals: ", shardId);
  for(std::set<EdgeTaskId>::iterator it = int_pbs.begin(); it != int_pbs.end(); it++){
    printf("(%d,%d) ", it->first, it->second);
  }
  printf("\n");

  printf("%d externals: ", shardId);
  for(std::map<EdgeTaskId, ExternalInfo>::iterator it = pbs.begin(); it != pbs.end(); it++){
    printf("(%d,%d) ", it->first.first, it->first.second);
  }
  printf("\n");
#endif

  return pbs;

}

void compute_ownership_index(int max_shard, std::vector<std::map<EdgeTaskId, ExternalInfo> >& externals_map,
  std::vector<std::vector<BabelFlow::Task> >& shard_ordered, std::map<EdgeTaskId, EdgeTaskId>& shared_edges){

  std::map<EdgeTaskId, int > pbs;

  shard_ordered.resize(max_shard);
  externals_map.resize(max_shard);

  for(int shard = 0; shard < max_shard; shard++){

    std::vector<BabelFlow::TaskId> task_ids = task_map->tasks(shard);
    shard_ordered[shard].clear();
    externals_map[shard].clear();
    
    std::vector<BabelFlow::Task>& ordered_tasks = shard_ordered[shard];

    const std::map<EdgeTaskId, ExternalInfo> shard_ext = compute_externals(shard, ordered_tasks);//externals_map[shard.rank];

    externals_map[shard] = shard_ext;

    std::vector<BabelFlow::Task>& lt = ordered_tasks;//shard_ordered[shard];//graph->localGraph(shard, task_map);

    int f_counter = 0;

    for(uint32_t j = 0; j < lt.size(); j++){
        
        BabelFlow::Task& task = lt[j];

        for(int i=0; i < task.incoming().size(); i++){
          BabelFlow::TaskId in_tid = task.incoming()[i];

          if(std::find(task_ids.begin(), task_ids.end(), in_tid) == task_ids.end()){
            EdgeTaskId edge;
            edge.first = in_tid;
            edge.second = task.id();

            if(pbs.find(edge) == pbs.end()){
             // PhaseBarrier pb_done = runtime->create_phase_barrier(ctx, 1);
              pbs[edge] = -1;
            }
          }
        }

        for(int ro=0; ro < task.outputs().size(); ro++){

          BabelFlow::TaskId out_tid0 = task.outputs()[ro][0];
          EdgeTaskId edge0;
          edge0.first = task.id();
          edge0.second = out_tid0;
          bool share_edge = task.outputs()[ro].size() > 1;

          for(int inro=0; inro < task.outputs()[ro].size(); inro++){

            BabelFlow::TaskId out_tid = task.outputs()[ro][inro];

            //if(out_tid == BabelFlow::TNULL) continue;

            if(std::find(task_ids.begin(), task_ids.end(), out_tid) == task_ids.end()){

              EdgeTaskId edge;
              edge.first = task.id();
              edge.second = out_tid;

              if(share_edge)
                shared_edges[edge] = edge0;

              if(pbs.find(edge) == pbs.end()){
                pbs[edge] = -1;
                
              }

              pbs[edge] = shard;

            }

          }
        }

      }

  }

  int el = 0;
  for(std::map<EdgeTaskId, int >::iterator it = pbs.begin(); it != pbs.end(); it++){
    
    for(int i=0; i<externals_map.size(); i++){
      std::map<EdgeTaskId, ExternalInfo>& extmap = externals_map[i];

      if(extmap.find(it->first) != extmap.end()){
        extmap[it->first].pb_index = el;
        extmap[it->first].owner = it->second;
      }
    }
    
    el++;
    //printf("PB %d->%d\n", it->first.first, it->first.second);
    //f_counter++;
  }

}

bool shard_main_task(const Task *task,
                    const std::vector<PhysicalRegion> &regions,
                    Context ctx, Runtime *runtime)
{

  // This is the timing of the top_level_task launches
  double ts_start = Realm::Clock::current_time_in_microseconds();

  // const ShardArgs& shard = *(const ShardArgs *)(task->args);
 
  ShardMetadata metadata;

  std::string input_str((const char*)task->args, task->arglen);
  std::istringstream is(input_str);
  {
    cereal::PortableBinaryInputArchive ar(is);
    // ar( metadata );
    metadata.deserialize(ar);
  }

  ShardArgs shard = metadata.sargs;

  //printf("%d: SHARDS N %d\n", shard.rank, shard.max_shard);

  std::map<EdgeTaskId, ExternalInfo>& externals_map = metadata.externals_map;
  std::vector<BabelFlow::Task>& shard_ordered = metadata.shard_ordered;
  std::map<EdgeTaskId, EdgeTaskId>& shared_edges = metadata.shared_edges; 

  //printf("shard_ordered %d \n", shard_ordered.size());

  /*
  std::vector<std::map<EdgeTaskId, ExternalInfo> > externals_map;
  std::vector<std::vector<BabelFlow::Task> > shard_ordered;
  std::map<EdgeTaskId, EdgeTaskId> shared_edges; 

  compute_ownership_index(shard.max_shard, externals_map, shard_ordered, shared_edges);
  
  // cereal::JSONOutputArchive output(std::cout);
  ShardMetadata test;
  test.externals_map = externals_map[shard.rank];
  test.shard_ordered = shard_ordered[shard.rank];
  test.shared_edges = shared_edges;

  // char shardname[128];
  // sprintf(shardname, "shard_%d", shard.rank);
  // output(cereal::make_nvp(shardname, test));

  std::ostringstream os;
  {    
    cereal::PortableBinaryOutputArchive ar(os);
    ar( test );
  } 

  std::string data = os.str();

  printf("SERIALIZED size = %d\n", data.size());
*/

  RegionsIndexType input_block_size = DATA_SIZE_POINT/shard.max_shard;

  /*  if (externals_map.size() == 0){
    printf("EMPTY externals for rank %d\n", shard.rank);
    assert(false);
    }*/

  //std::cout << "in spmd_main_task, shard = " << shard.rank << " externals = " << shard.n_pb << " regs " << regions.size() << std::endl;

  //std::map<BabelFlow::TaskId, BabelFlow::Task>& tasks = shard_tasks[shard.rank];

  RegionAccessor<AccessorType::Affine<1>, EdgeTaskId> fa_edge = fid_edge_shard.accessor<AccessorType::Affine<1> >(regions[0]);
  RegionAccessor<AccessorType::Affine<1>, PhaseBarrier> fa_done = fid_done_barrier.accessor<AccessorType::Affine<1> >(regions[0]);

  const LogicalRegion& pb_lr = regions[0].get_logical_region();
  // std::set<Color> colors;
  // runtime->get_index_space_partition_colors(ctx,pb_lr.get_index_space(), colors);
  // printf("colors %d\n", colors.size());

  // WHAT is fold_accessor??

  FieldSpace pay_fs = runtime->create_field_space(ctx);
  {
    FieldAllocator allocator = runtime->create_field_allocator(ctx, pay_fs);
    allocator.allocate_field(sizeof(RegionPointType),FID_PAYLOAD);
  }

  std::map<EdgeTaskId, std::pair<PhaseBarrier, int> > pbs;
  //std::set<EdgeTaskId> externals;

  std::map<EdgeTaskId, LogicalRegion> used_regions;

  IndexSpace pb_is = regions[0].get_logical_region().get_index_space();
  Domain pb_dom = runtime->get_index_space_domain(ctx, pb_is);
  Rect<1> pb_rec = pb_dom.get_rect<1>();
  
  //printf("%d: externals_map size %d\n", shard.rank, externals_map.size());
  std::vector<BabelFlow::Task>& ordered_tasks = shard_ordered;//[shard.rank];

  const std::map<EdgeTaskId, ExternalInfo>& shard_ext = externals_map;//[shard.rank];//compute_externals(shard.rank, ordered_tasks);//externals_map[shard.rank];

  int c=2;
  //  printf("%d: shard_ext size %d\n", shard.rank, shard_ext.size());
  std::map<EdgeTaskId, ExternalInfo>::const_iterator it;

  std::map<EdgeTaskId, std::vector<int> > copy_map;

  for(it=shard_ext.begin(); it != shard_ext.end(); it++){

    // for(uint32_t i=shard.offset; i < shard.offset+shard.n_pb; i++){
    //   for(uint32_t j=0; j < pb_rec.volume(); j++){
    //     //printf("%d: edge %d %d looking for %d %d\n", shard.rank, fa_edge[i].first, fa_edge[i].second, it->first.first, it->first.second);
    //     if(fa_edge[j] == it->first){
    //       pbs[it->first].first = fa_done[j];
    //       printf("EDGE %d %d found index %d\n", it->first.first, it->first.second, j);
    //       pbs[it->first].second = i - shard.offset+2;//it->second.edge_index; 
    //       pb_found = true;
    //       break;
    //     }

    //   }
    // }

    //  assert(pb_found);

    pbs[it->first].first = fa_done[it->second.pb_index];
    //printf("EDGE %d %d index %d\n", it->first.first, it->first.second, c+2);//it->second.edge_index);
    pbs[it->first].second = c;//it->second.edge_index; //c+2; // local index of the region (skip edge, pb and input fields)
    
    std::map<EdgeTaskId, EdgeTaskId>::iterator its = shared_edges.find(it->first);
    if(its != shared_edges.end()){
      EdgeTaskId original = its->second;

      copy_map[original].push_back(c);

      //printf("copy edge %d %d to %d %d (%d) region ID %d size %d\n", original.first, original.second, it->first.first,it->first.second, c, task->regions[c].region.get_tree_id(), task->regions.size());
    }

    c++;

  }
  
  EdgeTaskId in_edge;
  in_edge.first = BabelFlow::TNULL;
  in_edge.second = shard.rank;
  used_regions[in_edge] = regions[1].get_logical_region(); // input region
  
  // printf("%d Ordered: ", shard.rank);
  // for(int i=0; i < ordered_tasks.size(); i++)
  //    printf("%d ", ordered_tasks[i].id());
  // printf("\n");
  
  std::vector<BabelFlow::TaskId> resolved_tasks;
  std::vector<BabelFlow::TaskId> unresolved_tasks;

  uint32_t edge_index = 2;

  for(int i=0; i < ordered_tasks.size(); i++){
    BabelFlow::Task& int_task = ordered_tasks[i];
    int resolved = 0;
    // Shard Inputs
    for(int in=0; in < int_task.incoming().size(); in++){

      BabelFlow::TaskId tid = int_task.incoming()[in];

      EdgeTaskId edge = std::make_pair(tid, int_task.id());

      std::map<EdgeTaskId, std::pair<PhaseBarrier, int> >::iterator it = pbs.find(edge);

      LogicalRegion curr_lr;
      bool pbs_found = false;

      if(it != pbs.end()){ // edge is external
        
        pbs_found = true;
        //printf("set PB\n");
        // use corresponding pb and region at pb_idx
        if(tid != BabelFlow::TNULL){
          int pb_idx = it->second.second;
          
          // printf("%d: pre-in %d (%d,%d) pbs %d pb_idx == %d\n", int_task.id(), curr_lr.get_tree_id(), edge.first, edge.second, pbs_found, pb_idx);

          curr_lr = regions[pb_idx].get_logical_region();
          //curr_lr = regions[edge_index++].get_logical_region();
          used_regions[edge] = curr_lr;
          //pbs[edge].second = edge_index++;
        }else{
          
          assert(used_regions.find(edge) != used_regions.end());

          curr_lr = used_regions[edge];
        }

        resolved++;

      }
      else{ // edge is internal
        std::map<EdgeTaskId, LogicalRegion>::iterator found = used_regions.find(edge);

        if(found != used_regions.end()){
          curr_lr = found->second;
          resolved++;
        }
        else {
          fprintf(stderr, "INTERNAL EDGE %d %d NOT FOUND\n", edge.first, edge.second);
          assert(false);
        }
      }

      // if(resolved){
      //   //printf("PUTTING IN %d %d\n", edge.first, edge.second);
      assert(used_regions.find(edge) != used_regions.end());
      used_regions[edge] = curr_lr;

      //printf("%d: pre-in %d (%d,%d) pbs %d\n", int_task.id(), curr_lr.get_tree_id(), edge.first, edge.second, pbs_found);
      //   //std::cout << int_task.id() << ": in " << curr_lr.get_tree_id() << "(" << edge.first << ","<<edge.second << ")" << " pbs " << pbs_found <<std::endl;
      // }
      // else
      //   printf("UNRESOLVED INPUT!!!!-<<<<<<< %d -> %d\n", tid, int_task.id());
      // else 
      //   std::cout << int_task.id() << ": UNRESOLVED in " << curr_lr.get_tree_id() << "(" << edge.first << ","<<edge.second << ")" << " pbs " << pbs_found <<std::endl;
    }

    // if(resolved == int_task.incoming().size())
    //   resolved_tasks.push_back(int_task.id());
    // else
    //   unresolved_tasks.push_back(int_task.id());

    // std::cout << pb_lr << std::endl;
    // std::cout << runtime->get_index_partition(ctx, pb_lr.get_index_space(), DomainPoint::from_point<1>(0)) << std::endl;

    // Shard Outputs
    for(int ro=0; ro < int_task.outputs().size(); ro++){

      LogicalRegion curr_lr;

      bool pbs_out = false;

      for(int inro=0; inro < int_task.outputs()[ro].size(); inro++){

        BabelFlow::TaskId tid = int_task.outputs()[ro][inro];

        // if(tid == BabelFlow::TNULL)
        //   continue;

        EdgeTaskId edge = std::make_pair(int_task.id(), tid);

        std::map<EdgeTaskId, std::pair<PhaseBarrier, int> >::iterator it = pbs.find(edge);

        bool pbs_found = false;

        if(it != pbs.end()){ // edge is external
            
            int pb_idx = it->second.second;
            pbs_found= true;
            pbs_out = true;
            //printf("PBS edge %d %d index %d\n", edge.first, edge.second, pb_idx);

            curr_lr = regions[pb_idx].get_logical_region();
            //curr_lr = regions[edge_index++].get_logical_region();
        }
      }

      for(int inro=0; inro < int_task.outputs()[ro].size(); inro++){

        BabelFlow::TaskId tid = int_task.outputs()[ro][inro];

        // if(tid == BabelFlow::TNULL)
        //   continue;

        EdgeTaskId edge = std::make_pair(int_task.id(), tid);

        std::map<EdgeTaskId, std::pair<PhaseBarrier, int> >::iterator it = pbs.find(edge);

        bool pbs_found = false;

        if(!pbs_out){
          if(it != pbs.end()){ // edge is external
              //if(inro == 0){
                int pb_idx = it->second.second;
                pbs_found= true;

                //printf("PBS edge %d %d index %d\n", edge.first, edge.second, pb_idx);

                curr_lr = regions[pb_idx].get_logical_region();
              //}
              //curr_lr = regions[edge_index++].get_logical_region();
          }else{ // edge is internal
              std::map<EdgeTaskId, LogicalRegion>::iterator found = used_regions.find(edge);

              if(found != used_regions.end()){ // Use existing region
                curr_lr = found->second;
                //printf("reuse region\n");
              }
              else if(inro == 0){ // create new region

                //printf("create new region\n");
                IndexSpace curr_is = runtime->create_index_space(ctx, Domain::from_rect<1>(Rect<1>(0, estimate_output_size(int_task.callback(), ro, input_block_size))));//OUTPUT_SIZE-1)));

                curr_lr = runtime->create_logical_region(ctx, curr_is, pay_fs);
                
              }
          }
        }

        //printf("PUTTING OUT %d %d\n", edge.first, edge.second);
        used_regions[edge] = curr_lr;

        //printf("%d: pre-out %d (%d,%d) pbs %d\n", int_task.id(), curr_lr.get_tree_id(), edge.first, edge.second, pbs_found);
        //std::cout << int_task.id() << ": out " << curr_lr.get_tree_id() << "(" << edge.first << ","<<edge.second << ")" << " pbs " << pbs_found <<std::endl;
        
      }
    }

  }

  double init_time = (Realm::Clock::current_time_in_microseconds()-ts_start)/1000000.f ;
#if DETAILED_TIMING
  std::cout << "Shard " << shard.rank << " init_time = " << std::fixed << init_time << std::endl;
#endif

  std::map<EdgeTaskId, Future > copy_futures;

  for(int i=0; i < ordered_tasks.size(); i++){
    BabelFlow::Task& int_task = ordered_tasks[i];

    //TaskInfoExt info;
    TaskInfo info;
    info.id = int_task.id();
    info.callbackID = int_task.callback();
    info.lenInput = int_task.incoming().size();
    info.lenOutput = int_task.outputs().size();
    // for(int ro=0; ro < int_task.outputs().size(); ro++){
      
    //   if(int_task.outputs()[ro].size() > 0 && int_task.outputs()[ro][0] != BabelFlow::TNULL)
    //     info.lenOutput++;
    // }

    //info.lenOutput = int_task.outputs().size();
    //info.n_pbs = 0;

    TaskLauncher launcher(GENERIC_TASK_ID,
        TaskArgument(&info, sizeof(TaskInfo)), // WAS TaskInfoExt
        Predicate::TRUE_PRED,
        0 /*default mapper*/,
        CGMapper::TAG_LOCAL_SHARD);

    EdgeTaskId edge_future(BabelFlow::TNULL, BabelFlow::TNULL);

    // Shard Inputs
    for(int in=0; in < int_task.incoming().size(); in++){
      BabelFlow::TaskId tid = int_task.incoming()[in];

      EdgeTaskId edge = std::make_pair(tid, int_task.id());

      std::map<EdgeTaskId, std::pair<PhaseBarrier, int> >::iterator it = pbs.find(edge);
      /*
      if(used_regions.find(edge) == used_regions.end())
        printf("UNRESOLVED IN TASK LAUNCH %d %d \n", edge.first, edge.second);
      */

      LogicalRegion curr_lr = used_regions[edge];
      bool pbs_found = false;

      if(it != pbs.end()){ // edge is external
        
        pbs_found = true;
        //printf("set PB\n");
        // use corresponding pb and region at pb_idx
        if(tid != BabelFlow::TNULL){
          // int pb_idx = it->second.second;

          PhaseBarrier& pb = it->second.first;
          //curr_lr = regions[pb_idx].get_logical_region();

          //std::cout << edge.first << "," << edge.second <<" bf wait pb " << pb << std::endl;
          if(shard.round > 0)
            pb = runtime->advance_phase_barrier(ctx, pb);

          pb = runtime->advance_phase_barrier(ctx, pb);
          launcher.add_wait_barrier(pb);
          //std::cout << edge.first << "," << edge.second <<" af wait pb " << pb <<std::endl;
          
        }

      }
    
      DEBUG_PRINT((stderr,"%d: in %d (%d,%d) pbs %d\n", int_task.id(), curr_lr.get_tree_id(), edge.first, edge.second, pbs_found));
      //std::cout << int_task.id() << ": in " << curr_lr.get_tree_id() << "(" << edge.first << ","<<edge.second << ")" << " pbs " << pbs_found <<std::endl;

      //if(!pbs_found)
        launcher.add_region_requirement(RegionRequirement(curr_lr, READ_ONLY, EXCLUSIVE, curr_lr)
            .add_field(FID_PAYLOAD));
      // else
      //   launcher.add_region_requirement(RegionRequirement(curr_lr, READ_WRITE, SIMULTANEOUS, curr_lr)
      //        .add_field(FID_PAYLOAD));
        
    }

    // std::cout << pb_lr << std::endl;
    // std::cout << runtime->get_index_partition(ctx, pb_lr.get_index_space(), DomainPoint::from_point<1>(0)) << std::endl;

    // Shard Outputs
    for(int ro=0; ro < int_task.outputs().size(); ro++){
      
      LogicalRegion curr_lr;

      bool found_out = false;
      for(int inro=0; inro < int_task.outputs()[ro].size(); inro++){

        BabelFlow::TaskId tid = int_task.outputs()[ro][inro];

        //if(tid == BabelFlow::TNULL) continue;

        EdgeTaskId edge = std::make_pair(int_task.id(), tid);

        if(used_regions.find(edge) != used_regions.end()){
          curr_lr = used_regions[edge];
          found_out = true;
        }
        else
          printf("%d: NOT FOUND OUT %d %d\n", int_task.id(), edge.first, edge.second);

      }

      // if(!found_out)
      //   printf("OUT NOT RESOLVED\n");

      for(int inro=0; inro < int_task.outputs()[ro].size(); inro++){

        BabelFlow::TaskId tid = int_task.outputs()[ro][inro];

        //if(tid == BabelFlow::TNULL) continue;

        EdgeTaskId edge = std::make_pair(int_task.id(), tid);

        std::map<EdgeTaskId, std::pair<PhaseBarrier, int> >::iterator it = pbs.find(edge);

        bool pbs_found = false;

        if(it != pbs.end()){ // edge is external
          //int pb_idx = it->second.second;
          pbs_found= true;
          //printf("set PB\n");
          // use corresponding pb and region at pb_idx
          PhaseBarrier& pb = it->second.first;
          //printf("idx %d pb_idx %d reg size %d\n", pb_idx, curr_pb, regions.size());

          //std::cout << edge.first << "," << edge.second <<" bf arrival pb " << pb << std::endl;
          if(shard.round > 0)
            pb = runtime->advance_phase_barrier(ctx, pb);

          launcher.add_arrival_barrier(pb);
          pb = runtime->advance_phase_barrier(ctx, pb);
          //std::cout << edge.first << "," << edge.second <<" af arrival pb " << pb << std::endl;
        }
        else if(!found_out){
          curr_lr = used_regions[edge];
        }

        std::map<EdgeTaskId, std::vector<int> >::iterator itc = copy_map.find(edge);

        if(itc != copy_map.end()){
          edge_future = itc->first;
        }

        DEBUG_PRINT((stderr,"%d: out %d (%d,%d) pbs %d\n", int_task.id(), curr_lr.get_tree_id(), edge.first, edge.second, pbs_found));
        // std::cout << int_task.id() << ": out " << curr_lr.get_tree_id() << "(" << edge.first << ","<<edge.second << ")" << " pbs " << pbs_found <<std::endl;

      }

      if(curr_lr.get_tree_id() != 0){
        //printf("%d: req out %d \n", int_task.id(), curr_lr.get_tree_id());
        launcher.add_region_requirement(RegionRequirement(curr_lr, WRITE_DISCARD, EXCLUSIVE, curr_lr)
             .add_field(FID_PAYLOAD));

        if(edge_future != EdgeTaskId(BabelFlow::TNULL,BabelFlow::TNULL)){
          //printf("FOUND EDGE to copy from %d %d\n", edge_future.first, edge_future.second);

          std::vector<int>& cr = copy_map[edge_future];

          for(int c=0; c < cr.size()-1; c++){
            //printf("COPY FROM %d TO REGION %d (%d)\n", curr_lr.get_tree_id(), cr[c], task->regions[cr[c]].region.get_tree_id());
            launcher.add_region_requirement(RegionRequirement(task->regions[cr[c]].region, WRITE_DISCARD, EXCLUSIVE, task->regions[cr[c]].region)
             .add_field(FID_PAYLOAD));
          }

        }
      }
        
    }

    DEBUG_PRINT((stderr,"LAUNCHING %d\n", int_task.id()));
    
    runtime->execute_task(ctx, launcher);

  }

  // std::map<EdgeTaskId, std::vector<EdgeTaskId> >::iterator itc;

  // for(itc = copy_map.begin(); itc != copy_map.end(); itc++){
  //   copy_futures[itc->first].get_result<int>();
  //   {
  //     LogicalRegion original = used_regions[itc->first];

  //     for(int i=0; i < itc->second.size(); i++){
  //       EdgeTaskId copyedge = itc->second[i];
  //       LogicalRegion copy_lr = used_regions[copyedge];

  //       CopyLauncher copy_launcher;
  //       copy_launcher.add_copy_requirements(
  //           RegionRequirement(original, READ_ONLY,
  //                             EXCLUSIVE, original),
  //           RegionRequirement(copy_lr, WRITE_DISCARD,
  //                             EXCLUSIVE, copy_lr));
  //       copy_launcher.add_src_field(0, FID_PAYLOAD);
  //       copy_launcher.add_dst_field(0, FID_PAYLOAD);

  //       runtime->issue_copy_operation(ctx, copy_launcher);
  //     }
  //   }
  // }


  return true;
}

void Controller::top_level_task(const Task *task,
                    const std::vector<PhysicalRegion> &phy_regions,
                    Context ctx, Runtime *runtime){

  std::cout << "START TOP LEVEL" << std::endl;
  double ts_start, ts_end;
  double init_time_start = Realm::Clock::current_time_in_microseconds();
  
  FieldSpace pay_fs = runtime->create_field_space(ctx);
  {
    FieldAllocator allocator = runtime->create_field_allocator(ctx, pay_fs);
    allocator.allocate_field(sizeof(RegionPointType),FID_PAYLOAD);
  }
 
  std::vector<LogicalRegion> in_lr;

  // Begin initialization (Data load) 

  RegionsIndexType bsize = sizeof(FunctionType*) + 6*sizeof(GlobalIndexType) + sizeof(FunctionType);
  RegionsIndexType input_block_size = 0;

#if !USE_VIRTUAL_MAPPING
  bsize += sizeof(RegionsIndexType);
#endif
  
  std::vector<Future> load_futures;
  
  MustEpochLauncher must_load;

  for(int i=0; i < boxes.size(); i++){

    InputDomainSelection& box = boxes[i];

    RegionsIndexType blocksize = (box.high[0]-box.low[0]+1)*(box.high[1]-box.low[1]+1)*(box.high[2]-box.low[2]+1)*sizeof(FunctionType);
      
    assert((blocksize+bsize) % BYTES_PER_POINT == 0);

#if TEMP_PMT_HARDCODED
    RegionsIndexType num_elmts = (blocksize+bsize)/BYTES_PER_POINT;
#else
    RegionsIndexType num_elmts = (blocksize+sizeof(InputDomainSelection)+sizeof(float))/BYTES_PER_POINT;
#endif

    if(input_block_size < num_elmts)
      input_block_size = num_elmts;

    IndexSpace data_is = runtime->create_index_space(ctx, Domain::from_rect<1>(Rect<1>(0, input_block_size-1)));
    LogicalRegion data_lr = runtime->create_logical_region(ctx, data_is, pay_fs);

    //printf("input_block_size %d\n", input_block_size);

    in_lr.push_back(data_lr);

    TaskLauncher launcher(LOAD_TASK_ID,
        TaskArgument(&box, sizeof(InputDomainSelection)),
        Predicate::TRUE_PRED, 0 /*default mapper*/, CGMapper::TAG_LOCAL_SHARD); //CGMapper::SHARD_TAG(i));

    launcher.add_region_requirement(RegionRequirement(data_lr, WRITE_DISCARD, EXCLUSIVE, data_lr));//, CGMapper::SHARD_TAG(i)));
    launcher.region_requirements[0].add_field(FID_PAYLOAD);

    must_load.add_single_task(DomainPoint::from_point<1>(i), launcher);
  
  }

  FutureMap fm_load = runtime->execute_must_epoch(ctx, must_load);
  //fm_load.wait_all_results();

  /*
  for(int i=0; i < boxes.size(); i++) {
    bool shard_ok = fm_load.get_result<bool>(DomainPoint::from_point<1>(i));
    if(!shard_ok) {
      std::cout << "error returned from shard load " << i << std::endl;
      assert(false);
    }
    printf("LOADED input shard %d\n", i);
  }
  */
    // Future f = runtime->execute_task(ctx, launcher);
    // load_futures.push_back(f);

    //runtime->execute_task(ctx, launcher).get_result<LogicalRegion>();
  // }


  // //}

  // // now wait on all the futures
  // for(std::vector<Future>::iterator it = load_futures.begin();
  //   it != load_futures.end();
  //   it++)
  //   it->get_result<LogicalRegion>();
 
  //launch_data.push_back(init_launch);

  // End initialization (Data Load)

  std::vector<ShardArgs> sargs_vec;

  int num_shards = mInitial_inputs.size();

  // launch init tasks on each shard that create the needed instances
  std::vector<Future> futures;
  uint32_t offset_count = 0;

  std::vector<std::map<EdgeTaskId, ExternalInfo> > externals_map;
  std::vector<std::vector<BabelFlow::Task> > shard_ordered;
  std::map<EdgeTaskId, EdgeTaskId> shared_edges;

  compute_ownership_index(num_shards, externals_map, shard_ordered, shared_edges);

  for(int shard=0; shard < num_shards; shard++){
    //std::vector<BabelFlow::Task> temp_tasks;
    std::map<EdgeTaskId, ExternalInfo>& s_ext = externals_map[shard];//compute_externals(shard, temp_tasks);

    // externals_map[shard] = s_ext;

    uint32_t out_counter = s_ext.size();

    ShardArgs sargs;
    sargs.rank = shard;
    sargs.offset = offset_count;
    sargs.max_shard = num_shards;

    offset_count += out_counter;
    sargs.n_pb = out_counter;

    sargs_vec.push_back(sargs);

  }

  FieldSpace fs_blks = runtime->create_field_space(ctx);
  fid_edge_shard.allocate(runtime, ctx, fs_blks);
  fid_done_barrier.allocate(runtime, ctx, fs_blks);

  IndexSpace lr_is = runtime->create_index_space(ctx, Domain::from_rect<1>(Rect<1>(0, offset_count-1)));
  LogicalRegion lr_blks = runtime->create_logical_region(ctx, lr_is, fs_blks);

  //DomainColoring pb_color = Utils::partitionInput(1, lr_blks, ctx, runtime);

  // Rect<1> pb_color_bounds(Point<1>(0),Point<1>(offset_count-1));
  // Domain pb_color_domain = Domain::from_rect<1>(pb_color_bounds);

  // Blockify<1> grid2blk_map(Point<1>(1));
  // IndexPartition pb_disjoint_ip = runtime->create_index_partition(ctx,
  //                lr_is,
  //                grid2blk_map,
  //                0 /*color*/);

  // IndexPartition pb_disjoint_ip = runtime->create_index_partition(ctx, lr_is, pb_color_domain,
  //                                   pb_color, true/*disjoint*/);
  // LogicalPartition pb_disjoint_lp = runtime->get_logical_partition(ctx, lr_blks, pb_disjoint_ip);

  // printf("after lp\n");
  //for(int shard=0; shard < num_shards; shard++){

  {  
    int init_args[2] = {num_shards, 0};
    TaskLauncher launcher(INIT_SHARDS_TASK_ID,
      TaskArgument(init_args, sizeof(int)*2),
        //TaskArgument(&sargs_vec[shard], sizeof(ShardArgs)),
        Predicate::TRUE_PRED,
        0 /*default mapper*/);//,
        // CGMapper::SHARD_TAG(shard));

    launcher.add_region_requirement(RegionRequirement(lr_blks, WRITE_DISCARD, EXCLUSIVE, lr_blks)
            .add_field(fid_edge_shard));

    // we'll use "reductions" to allow each shard to fill in barriers for blocks it owns
    launcher.add_region_requirement(RegionRequirement(lr_blks,
            WRITE_DISCARD, //BarrierCombineReductionOp::redop_id,
            EXCLUSIVE, lr_blks)
            .add_field(fid_done_barrier));

    Future f = runtime->execute_task(ctx, launcher);
    futures.push_back(f);
  //}

  // now wait on all the futures
  for(std::vector<Future>::iterator it = futures.begin();
    it != futures.end();
    it++)
    it->get_void_result();
  
  }

  // std::map<EdgeTaskId, EdgeTaskId>::iterator its;

  // for(its = shared_edges.begin(); its != shared_edges.end(); its++){
  //   printf("SHARED EDGE %d %d == %d %d\n", its->first.first, its->first.second, its->second.first, its->second.second);
  // }
#if DETAILED_TIMING
  std::cout << "Init time = " << std::fixed << (Realm::Clock::current_time_in_microseconds()-init_time_start)/1000000.f << std::endl;
#endif

  double preparation_time = 0;
  double temp_start = 0;
 // This is the timing of the top_level_task launches
  ts_start = Realm::Clock::current_time_in_microseconds();
for(int round=0; round < 1; round++){
   temp_start= Realm::Clock::current_time_in_microseconds(); 

  std::map<EdgeTaskId, LogicalRegion > used_regions;
  MustEpochLauncher must;

  std::vector<TaskLauncher> launchers;
  std::vector<std::string> metadatas(num_shards);
  preparation_time += Realm::Clock::current_time_in_microseconds() - temp_start;
  for(int shard = 0; shard < num_shards; shard++){
    temp_start= Realm::Clock::current_time_in_microseconds();
    ShardArgs& sargs = sargs_vec[shard];

    sargs.round = round;

    std::map<EdgeTaskId, ExternalInfo>& s_ext = externals_map[shard];

    ShardMetadata this_metadata;
    this_metadata.externals_map = externals_map[shard];
    this_metadata.shard_ordered = shard_ordered[shard];
    this_metadata.shared_edges = shared_edges;
    this_metadata.sargs = sargs;
    // char shardname[128];
    // sprintf(shardname, "shard_%d", shard.rank);
    // output(cereal::make_nvp(shardname, test));

    std::ostringstream os;
    {    
      cereal::PortableBinaryOutputArchive ar(os);
      ar( this_metadata );
    } 

    metadatas[shard] = os.str();

    TaskLauncher launcher(SHARD_MAIN_TASK_ID,
        TaskArgument(metadatas[shard].c_str(), metadatas[shard].size()),
        Predicate::TRUE_PRED,
        0 /*default mapper*/, CGMapper::SHARD_TAG(shard));

    launcher.add_region_requirement(RegionRequirement(lr_blks, READ_ONLY, EXCLUSIVE, lr_blks)
                              .add_field(fid_edge_shard)
				    .add_field(fid_done_barrier)
            );//.add_flags(NO_ACCESS_FLAG));

    // LogicalRegion lr_solblk = runtime->get_logical_subregion_by_color(ctx,
    //                       disjoint_lp,
    //                       shard);

    launcher.add_region_requirement(RegionRequirement(in_lr[shard], READ_ONLY, EXCLUSIVE, in_lr[shard], 
						      CGMapper::SHARD_TAG(shard)).add_field(FID_PAYLOAD));//.add_flags(NO_ACCESS_FLAG));//,

    // launcher.add_region_requirement(RegionRequirement(lr_solblk, READ_ONLY, EXCLUSIVE, data_lr, 
    //   CGMapper::SHARD_TAG(shard)).add_field(FID_PAYLOAD));//,
                    // CGMapper::SHARD_TAG(shard))
                //.add_field(fid_payload));
                //.add_flags(NO_ACCESS_FLAG));

    DEBUG_PRINT((stderr,"%d SIMULTANEOUS [%d]: ", shard, s_ext.size()));

    uint32_t edge_count = 2;

#if TEMP_PMT_HARDCODED
    const RegionsIndexType EST_OUTPUT_SIZE = (DATA_SIZE_POINT/num_shards) * 1.1f;
#else
    const RegionsIndexType EST_OUTPUT_SIZE = 2048*2048*20;
#endif


// #if USE_SINGLE_REGION
//     DomainColoring coloring;
//     uint32_t lo = 0;
//     uint32_t hi = EST_OUTPUT_SIZE;

// #endif

    for(std::map<EdgeTaskId, ExternalInfo>::iterator it=s_ext.begin();it != s_ext.end(); it++){

      EdgeTaskId edge;
      // std::map<EdgeTaskId, EdgeTaskId>::iterator its = shared_edges.find(it->first);
      // if(its != shared_edges.end()){
      //   edge = its->second;
      //   printf("using first %d %d\n", edge.first, edge.second);
      // }
      // else
        edge = it->first;

      LogicalRegion curr_lr;
      std::map<EdgeTaskId, LogicalRegion >::iterator found = used_regions.find(edge);

      it->second.edge_index = edge_count++;
      
      if(found == used_regions.end()){
        IndexSpace curr_is = runtime->create_index_space(ctx, Domain::from_rect<1>(Rect<1>(0, EST_OUTPUT_SIZE-1)));
        curr_lr = runtime->create_logical_region(ctx, curr_is, pay_fs);
        used_regions[edge] = curr_lr;
        // used_regions[it->first].second = shard;
      }
      else
        curr_lr = found->second;

      DEBUG_PRINT((stderr,"[%d-%d r %d (%d)] ", it->first.first, it->first.second, curr_lr.get_tree_id(), it->second.owner));

      launcher.add_region_requirement(RegionRequirement(curr_lr, READ_WRITE, SIMULTANEOUS, curr_lr, CGMapper::SHARD_TAG(it->second.owner)).add_field(FID_PAYLOAD).add_flags(NO_ACCESS_FLAG));
    }
    DEBUG_PRINT((stderr,"\n"));

    // for(int i=0; i < sargs.n_pb; i++){

    //   IndexSpace curr_is = runtime->create_index_space(ctx, Domain::from_rect<1>(Rect<1>(0, OUTPUT_SIZE-1)));
    //   LogicalRegion curr_lr = runtime->create_logical_region(ctx, curr_is, pay_fs);
    //   printf("%d ", curr_lr.get_tree_id());

    //   launcher.add_region_requirement(RegionRequirement(curr_lr, READ_WRITE, SIMULTANEOUS, curr_lr).add_field(FID_PAYLOAD));
    // }
    
    must.add_single_task(DomainPoint::from_point<1>(shard), launcher);

    //launchers.push_back(launcher);
  }

  // for(int shard = 0; shard < num_shards; shard++){
  //    must.add_single_task(DomainPoint::from_point<1>(shard), launchers[shard]);
  // }
  preparation_time += Realm::Clock::current_time_in_microseconds() - temp_start;

#if PROFILING_TIMING
  std::cout << std::fixed << gasnet_mynode() << ",shard_prep,-1,"<<(preparation_time)/1000000.f << std::endl;
#endif

  FutureMap fm = runtime->execute_must_epoch(ctx, must);

  bool ok = true;
  for(int shard = 0; shard < num_shards; shard++) {
    bool shard_ok = fm.get_result<bool>(DomainPoint::from_point<1>(shard));
    if(!shard_ok) {
      std::cout << "error returned from shard " << shard << std::endl;
      ok = false;
    }
  double shard_end_time = (Realm::Clock::current_time_in_microseconds())/1000000.f ;

#if DETAILED_TIMING
  std::cout << "Shard " << shard << " shard_end_time = " << std::fixed << shard_end_time << std::endl;
#endif

  }

#if DETAILED_TIMING
  double round_end_time = (Realm::Clock::current_time_in_microseconds())/1000000.f ;
  std::cout << "ROUND " << round << " round_end_time = " << std::fixed << round_end_time << std::endl;
#endif
}

#if DETAILED_TIMING
  ts_end = Realm::Clock::current_time_in_microseconds();
  std::cout << std::fixed << "Dataflow time = " << (ts_end-ts_start)/1000000.f << std::endl;
#endif
  // TODO destroy all the regions
  //runtime->destroy_field_space(ctx, pay_fs);

}

void init_shards_task(const Task *task,
                    const std::vector<PhysicalRegion> &regions,
                    Context ctx, Runtime *runtime)
{
  //const ShardArgs& shard = *(const ShardArgs *)(task->args);
  const int* init_args = (const int *)(task->args);
  int max_shard = init_args[0];//*(const int *)(task->args);
  int round = init_args[1];

  //std::cout << "init_shards_task shard=" << shard.rank << std::endl;

  RegionAccessor<AccessorType::Affine<1>, EdgeTaskId> fa_edge = fid_edge_shard.accessor<AccessorType::Affine<1> >(regions[0]);
  RegionAccessor<AccessorType::Affine<1>, PhaseBarrier> fa_done = fid_done_barrier.fold_accessor<AccessorType::Affine<1> >(regions[1]);

  std::map<EdgeTaskId, std::pair<PhaseBarrier, int> > pbs;

  std::vector<std::map<EdgeTaskId, ExternalInfo> > externals_map;
  std::vector<std::vector<BabelFlow::Task> > shard_ordered;
  std::map<EdgeTaskId, EdgeTaskId> shared_edges;

  compute_ownership_index(max_shard, externals_map, shard_ordered, shared_edges);

  for(int shard = 0; shard < max_shard; shard++){

    std::vector<BabelFlow::TaskId> task_ids = task_map->tasks(shard);
    std::vector<BabelFlow::Task>& ordered_tasks = shard_ordered[shard];

    const std::map<EdgeTaskId, ExternalInfo>& shard_ext = externals_map[shard];//compute_externals(shard, ordered_tasks);//externals_map[shard.rank];

    //externals_map.push_back(shard_ext);

    std::vector<BabelFlow::Task>& lt = ordered_tasks;//shard_ordered[shard];//graph->localGraph(shard, task_map);

    int f_counter = 0;

    for(uint32_t j = 0; j < lt.size(); j++){
        
        BabelFlow::Task& task = lt[j];

        for(int i=0; i < task.incoming().size(); i++){
          BabelFlow::TaskId in_tid = task.incoming()[i];

          if(std::find(task_ids.begin(), task_ids.end(), in_tid) == task_ids.end()){
            EdgeTaskId edge;
            edge.first = in_tid;
            edge.second = task.id();

            if(pbs.find(edge) == pbs.end()){
              PhaseBarrier pb_done = runtime->create_phase_barrier(ctx, 1);
              pbs[edge].first = pb_done;
            }
          }
        }

        for(int ro=0; ro < task.outputs().size(); ro++){
          for(int inro=0; inro < task.outputs()[ro].size(); inro++){

            BabelFlow::TaskId out_tid = task.outputs()[ro][inro];

            //if(out_tid == BabelFlow::TNULL) continue;

            if(std::find(task_ids.begin(), task_ids.end(), out_tid) == task_ids.end()){

              EdgeTaskId edge;
              edge.first = task.id();
              edge.second = out_tid;

              if(pbs.find(edge) == pbs.end()){

                PhaseBarrier pb_done = runtime->create_phase_barrier(ctx, 1);
                pbs[edge].first = pb_done;
                
              }

              pbs[edge].second = shard;

            }

          }
        }

      }

  }


  // TODO this part can be skipped if search in task
  int el = 0;
  for(std::map<EdgeTaskId, std::pair<PhaseBarrier, int> >::iterator it = pbs.begin(); it != pbs.end(); it++){
    //uint32_t el = shard.offset + f_counter;

    fa_edge[el] = it->first;
    fa_done[el] = it->second.first;
    
    // for(int i=0; i<externals_map.size(); i++){
    //   std::map<EdgeTaskId, ExternalInfo>& extmap = externals_map[i];

    //   if(extmap.find(it->first) != extmap.end()){
    //     extmap[it->first].pb_index = el;
    //     extmap[it->first].owner = it->second.second;

    //     fa_owner[el] = it->second.second;
    //   }
    // }
    
    el++;
    // printf("PB %d->%d\n", it->first.first, it->first.second);
    //f_counter++;
  }

}

#if 0
int Controller::initialize(BabelFlow::SuperTask* st, int argc, char **argv){

  legion_argc = argc;
  legion_argv = argv;

  std::map<BabelFlow::TaskId, BabelFlow::SuperTask*>::iterator it = st->tasks.begin();

  for(it; it != st->tasks.end(); it++) {

    BabelFlow::SuperTask* shard_task = it->second;
    shards.push_back(shard_task);

  }

  /*
  for(std::map<BabelFlow::TaskId, BabelFlow::SuperTask*>::iterator sit = st->tasks.begin();
        sit != st->tasks.end(); sit++){

    BabelFlow::SuperTask* h1 = sit->second;

    printf("Cluster %d\n", h1->id());

    for(std::map<BabelFlow::TaskId, BabelFlow::SuperTask*>::iterator sit2 = h1->tasks.begin();
        sit2 != h1->tasks.end(); sit2++){
       BabelFlow::SuperTask* h2 = sit2->second;

       printf("\tSub-cluster %d\n", h2->id());

       for(std::map<BabelFlow::TaskId, BabelFlow::SuperTask*>::iterator sit3 = h2->tasks.begin();
        sit3 != h2->tasks.end(); sit3++){
         BabelFlow::SuperTask* h3 = sit3->second;

         printf("\t\tSub-cluster %d\n", h3->id());

         h3->find_externals();
       }

       printf("externals %d\n", h2->id());
       h2->find_externals(true);

       char name[128];
       sprintf(name, "h2_%d.dot", h2->id());
       FILE* output1 = fopen(name,"w");
       h2->output_graph(output1);
       fclose(output1);
    }

    printf("externals %d\n", h1->id());
    h1->find_externals(true);

    char name[128];
    sprintf(name, "h1_%d.dot", h1->id());
    FILE* output1 = fopen(name,"w");
    h1->output_graph(output1);
    fclose(output1);
  }

  st->find_externals(true);

  FILE* output0 = fopen("h0.dot","w");
  st->output_graph(output0);
  fclose(output0);

  exit(0);
  */
}
#endif 

int Controller::initialize(const BabelFlow::TaskGraph* _graph, const BabelFlow::TaskMap* _task_map, int argc, char **argv){
  // Assume there is only one controller, get all the tasks in the graph
  //alltasks = graph.localGraph(0, task_map);

  int valence = 2;
  uint32_t block_decomp[3];

  for (int i = 1; i < argc; i++){
    if (!strcmp(argv[i],"-d")){
      data_size[0] = atoi(argv[++i]);
      data_size[1] = atoi(argv[++i]);
      data_size[2] = atoi(argv[++i]);
    }
    if (!strcmp(argv[i],"-m"))
      valence = atoi(argv[++i]);
    if (!strcmp(argv[i],"-p")){
      block_decomp[0] = atoi(argv[++i]);
      block_decomp[1] = atoi(argv[++i]);
      block_decomp[2] = atoi(argv[++i]);
    }
  }

  legion_argc = argc;
  legion_argv = argv;

  graph = _graph;
  task_map = _task_map;

/*  
  std::vector<SuperTask> stasks(nclusters);
  std::map<BabelFlow::TaskId, BabelFlow::TaskId> shared_map;

  BabelFlow::TaskId stasks_id = 900000000;

  KWayMergeClustering clustering(valence);
  std::map<BabelFlow::TaskId, BabelFlow::Task> clustered;
  HierarchicalCluster clusters = clustering.clusterize(graph, task_map, clustered, nclusters);


  for(int i=0; i < nclusters; i++){
     HierarchicalCluster& hc = clusters.clusters[i];
     stasks[i].id(stasks_id++);

     printf("Cluster %d\n", stasks[i].id());

     for(int j=0; j < hc.clusters.size(); j++){
        HierarchicalCluster ihc = hc.clusters[j];

        SuperTask nstask;
        nstask.id(stasks_id++);

        printf("\tSub-cluster %d\n", nstask.id());

        for(std::set<BabelFlow::TaskId>::iterator it=ihc.ids.begin(); it != ihc.ids.end(); it++){
          nstask.addTask(clustered[*it]);
          printf("\t\ttask %d\n", *it);
        }

        //nstask.find_externals();

        stasks[i].addTask(nstask);
     }

     //stasks[i].find_externals();
   }
*/



  // for(BabelFlow::TaskId i=0; i < nclusters; i++){
  //   stasks[i].id(STASK_ID_OFFSET+i);

  //   for(int j=0; j < valence; j++){
  //     std::vector<BabelFlow::Task> tasks = 
  //       graph.localGraph(i*valence+j, task_map);
      
  //     printf("putting %d's tasks into %d\n", i*valence+j, i);
  //     for(int k=0; k < tasks.size(); k++){
  //       stasks[i].addTask(tasks[k]);
  //       shared_map[tasks[k].id()] = i;
  //     // std::copy(tasks.begin(), tasks.end(), 
  //     //   std::inserter(stasks[i].local_ids, stasks[i].local_ids.begin()));
  //       alltasks.push_back(tasks[k]);
  //      //alltasks.insert(alltasks.end(), tasks.begin(), tasks.end());
  //     }
  //   }

  //   stasks[i].find_externals();

  //   // char filename[128];
  //   // sprintf(filename, "cluster_%d.dot", i);
  //   // FILE* output = fopen(filename,"w");
  //   // stasks[i].output_graph(output);
  //   // fclose(output);

  // }

  // printf("Done first level\n");

  // std::map<BabelFlow::TaskId, BabelFlow::Task>::iterator stIt;

  // // Second hierarchy clustering
  // for(BabelFlow::TaskId i=0; i < nclusters; i++){
  //   SuperTask& stask = stasks[i];

  //   std::set<BabelFlow::TaskId> clustered;

  //   for(stIt = stask.tasks.begin(); stIt != stask.tasks.end(); stIt++){

  //       if(clustered.find(stIt->first) == clustered.end()) 
  //         continue;

  //       BabelFlow::Task& task = stIt->second;

  //       SuperTask nstask;
  //       nstask.id(STASK_ID_OFFSET+nclusters+i);
  //       // One step in
  //       for(int ro=0; ro < task.outputs().size(); ro++){

  //         std::vector<BabelFlow::TaskId> new_outputs;
  //         for(int inro=0; inro < task.outputs()[ro].size(); inro++){
  //           BabelFlow::TaskId out_tid = task.outputs()[ro][inro];

  //           if(stask.local_ids.find(out_tid) != stask.local_ids.end()){
  //             BabelFlow::Task& m_task = stask.tasks[out_tid];
              
  //             // add to inner subtask
  //             nstask.addTask(m_task);
  //             clustered.insert(m_task.id());

  //           }
  //         }

  //       }

  //       nstask.find_externals();
  //       stask.addTask(nstask);

  //       char filename[128];
  //       sprintf(filename, "cluster_%d.dot", i);
  //       FILE* output = fopen(filename,"w");
  //       stasks[i].output_graph(output);
  //       fclose(output);
  //   }

  //   printf("Done second level\n");

  //   for(std::set<BabelFlow::TaskId>::iterator cIt = clustered.begin(); cIt != clustered.end(); cIt++)
  //     stask.tasks.erase(*cIt);

  // }

  // Debug exploration
  // for(BabelFlow::TaskId i=0; i < nclusters; i++){
  //   SuperTask& stask = stasks[i];

  //   for(int i=0; i< stask.incoming().size(); i++){
  //       BabelFlow::TaskId tid = stask.incoming()[i];

  //       BabelFlow::TaskId cid = shared_map[tid];
  //       printf("%d is an external input form cluster %d\n", tid, cid);

  //     }

  //     for(int ro=0; ro < stask.outputs().size(); ro++){
  //       for(int inro=0; inro < stask.outputs()[ro].size(); inro++){

  //         BabelFlow::TaskId tid = stask.outputs()[ro][inro];
  //         BabelFlow::TaskId cid = shared_map[tid];
  //         printf("%d is an external output form cluster %d\n", tid, cid);

  //        }
  //      }

  // }

  return 1;
}

void Controller::mapper_registration(Machine machine, Runtime *rt,
                         const std::set<Processor> &local_procs)
{
  for (std::set<Processor>::const_iterator it = local_procs.begin();
       it != local_procs.end(); it++){
    rt->replace_default_mapper(new DataFlowMapper(machine, rt, *it), *it);

  }
}

int Controller::run(std::map<BabelFlow::TaskId,BabelFlow::Payload>& initial_inputs){
  mInitial_inputs = initial_inputs;
 // fprintf(stderr,"controller run %d\n", gasnet_mynode());
  if(input_initialization != NULL)
    mInitial_inputs = input_initialization(legion_argc, legion_argv);

  for(uint32_t i=0; i < alltasks.size(); i++){
    taskmap[alltasks[i].id()] = alltasks[i];
  }

  boxes.resize(mInitial_inputs.size());

#if IO_READ_BLOCK
  RegionsIndexType input_block_size = 0;
  LaunchData init_launch;
  init_launch.vparts.resize(1);
  std::set<BabelFlow::TaskId> initTasks;
  std::set<EdgeTaskId> current_inputs;
  std::set<EdgeTaskId> current_outputs;
  //std::vector<InputDomainSelection> boxes(mInitial_inputs.size());

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

    boxes[input_count] = *(InputDomainSelection*)in_pay.buffer();
    InputDomainSelection& box = boxes[input_count];

    RegionsIndexType blocksize = (box.high[0]-box.low[0]+1)*(box.high[1]-box.low[1]+1)*(box.high[2]-box.low[2]+1)*sizeof(FunctionType);
    
    assert((blocksize+bsize) % BYTES_PER_POINT == 0);

#if TEMP_PMT_HARDCODED
    RegionsIndexType num_elmts = (blocksize+bsize)/BYTES_PER_POINT;
#else
    RegionsIndexType num_elmts = (blocksize+sizeof(InputDomainSelection)+sizeof(float))/BYTES_PER_POINT;
#endif

   if(input_block_size < num_elmts)
      input_block_size = num_elmts;

    Rect<1> subrect(Point<1>(index),Point<1>(index+num_elmts-1));
    init_launch.vparts[0].coloring[input_count] = Domain::from_rect<1>(subrect);
    init_launch.vparts[0].input = true;

    VPartId vpart_id;
    vpart_id.round_id = launch_data.size();
    vpart_id.part_id = 0;
    vpart_id.color = input_count;

    //init_launch.arg_map.set_point(DomainPoint::from_point<1>(Point<1>(input_count)), TaskArgument(&boxes[input_count],sizeof(InputDomainSelection)));
    init_launch.arg_map[DomainPoint::from_point<1>(Point<1>(input_count))] = TaskArgument(&boxes[input_count],sizeof(InputDomainSelection));

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
 // Utils::compute_launch_data(input_block_size, initTasks, current_inputs, current_outputs, taskmap, launch_data, vpart_map);

  //std::cout << "Crawling time = " << std::fixed << (Realm::Clock::current_time_in_microseconds()-ts_start)/1000000.f << std::endl;

#else
  for(int i=0; i<boxes.size(); i++){
    boxes[i].low[0] = 0;
    boxes[i].low[1] = 0;
    boxes[i].low[2] = 0;
    boxes[i].high[0] = 1;
    boxes[i].high[1] = 1;
    boxes[i].high[2] = 1;
  }
#endif

  return Runtime::start(legion_argc, legion_argv);
}

int Controller::registerCallback(BabelFlow::CallbackId id, Callback func){
  assert (id > 0); // Callback 0 is reserved for relays
  
  if (mCallbacks.size() <= id)
    mCallbacks.resize(id+1);
  
  mCallbacks[id] = func;
  
  return 1;
}

static void update_mappers(Machine machine, Runtime *runtime,
                           const std::set<Processor> &local_procs)
{
  for(std::set<Processor>::const_iterator it = local_procs.begin();
      it != local_procs.end();
      it++) 
    runtime->replace_default_mapper(new CGMapper(machine, runtime, *it), *it);

  //printf("local procs %d\n",local_procs.size());
}

Controller::Controller()
{
  Runtime::set_top_level_task_id(TOP_LEVEL_TASK_ID);

  {
    TaskVariantRegistrar registrar(TOP_LEVEL_TASK_ID, "top_level");
    registrar.add_constraint(ProcessorConstraint(Processor::LOC_PROC));
    Runtime::preregister_task_variant<top_level_task>(registrar, "top_level");
  }
  
  // Runtime::set_top_level_task_id(TOP_LEVEL_TASK_ID);
  // Runtime::register_legion_task<top_level_task>(TOP_LEVEL_TASK_ID,
  //                     Processor::LOC_PROC, true/*single*/, false/*index*/,
  //                     AUTO_GENERATE_ID, TaskConfigOptions(), "top_level");
  {
    TaskVariantRegistrar tvr(INIT_SHARDS_TASK_ID, "init_shards_task");
    tvr.add_constraint(ProcessorConstraint(Processor::LOC_PROC));
    tvr.set_leaf(true);
    Runtime::preregister_task_variant<init_shards_task>(tvr, "init_shards_task");
  }

  {
    TaskVariantRegistrar tvr(SHARD_MAIN_TASK_ID, "shard_main_task");
    tvr.add_constraint(ProcessorConstraint(Processor::LOC_PROC));
    Runtime::preregister_task_variant<bool, shard_main_task>(tvr, "shard_main_task");
  }

  Runtime::register_legion_task<int, generic_task>(GENERIC_TASK_ID,
                      Processor::LOC_PROC, true/*single*/, false/*index*/,
                      AUTO_GENERATE_ID, TaskConfigOptions(false/*leaf*/), "generic_task");

//  Runtime::register_legion_task<bool, load_task>(LOAD_TASK_ID,
//                      Processor::LOC_PROC, true/*single*/, false/*index*/,
//                      AUTO_GENERATE_ID, TaskConfigOptions(false/*leaf*/), "load_task");

#if USE_VIRTUAL_MAPPING
  Runtime::preregister_projection_functor(PFID_USE_DATA_TASK,
             new UseDataProjectionFunctor);
#endif
  Runtime::set_registration_callback(update_mappers);

  //Runtime::set_registration_callback(mapper_registration);
  
}
