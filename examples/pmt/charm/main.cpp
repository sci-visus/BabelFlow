/*
 * main.cpp
 *
 *  Created on: Dec 14, 2014
 *      Author: bremer5
 */

#include <cstdio>
#include <unistd.h>
#include <cstdlib>
#include <sstream>

#include "RelayTask.h"
#include "charm/CharmTask.h"
#include "charm/Controller.h"
//#include "charm/CharmTaskGraph.h"

#include <cstdio>
#include <cassert>
#include <cstdlib>
#include <cfloat>
#include <fstream>

#include "../KWayTaskMap.h"
#include "../KWayMerge.h"
#include "MergeTree.h"
#include "AugmentedMergeTree.h"
#include "TypeDefinitions.h"
#include "SortedJoinAlgorithm.h"
#include "SortedUnionFindAlgorithm.h"
#include "LocalCorrectionAlgorithm.h"

#include "../idxio/read_block.h"

#include "pmt.decl.h"

template <typename T>
struct DomainBlock{
  GlobalIndexType low[3];
  GlobalIndexType high[3];
  T* data;
};

class DomainSelection{
 public:
  GlobalIndexType low[3];
  GlobalIndexType high[3];

  void pup(PUP::er &p) 
  {
    PUParray(p,low,3);
    PUParray(p,high,3);
  }

};



/* readonly */ CProxy_Main mainProxy;

using namespace DataFlow;
using namespace charm;


//! The number of bits used for prefixing scatter tasks
static const uint8_t sPrefixSize = 4;

//! The number of non-prefix bits
static const uint8_t sPostfixSize = sizeof(TaskId)*8 - sPrefixSize;

//! Bit mask for scatter tasks
static const TaskId sPrefixMask = ((1 << sPrefixSize) - 1) << sPostfixSize;

int local_compute(std::vector<Payload>& inputs,
                  std::vector<Payload>& output, TaskId task){

  DomainSelection* box = (DomainSelection*)inputs[0].buffer();

  char filename[128];
  sprintf(filename, "/Users/steve/Research/SCI/workspace/datasets/pmt/blob.idx");      
  char* data_block = read_block(filename,box->low,box->high);

  float threshold = 0;
  Payload new_pay = make_local_block((FunctionType*)(data_block), box->low, box->high, threshold);
 
  for (int i=0; i<inputs.size(); i++){
    delete[] (char*)inputs[i].buffer();
  }
  inputs.clear();

  inputs.push_back(new_pay);

  // char filename[128];
  // sprintf(filename,"dump_%d.raw", task);
  // std::ofstream outfile (filename,std::ofstream::binary);

  // outfile.write (inputs[0].buffer(),inputs[0].size());

  // outfile.close();
  
  sorted_union_find_algorithm(inputs, output, task);
  
  // MergeTree t;
  
  // fprintf(stderr,"LOCAL COMPUTE performed by task %d\n", task);
  // t.decode(output[0]);
  
  // t.writeToFile(task);
  
  // Deleting input data
  for (int i=0; i<inputs.size(); i++){
    delete[] (char*)inputs[i].buffer();
  }
  inputs.clear();
  
  return 1;
}


int join(std::vector<Payload>& inputs,
         std::vector<Payload>& output, TaskId task){
  
  // for(int i=0; i<inputs.size(); i++){
  //   char filename[128];
  
  //   sprintf(filename,"dump_%d_in_%d.raw", task, i);
  //   std::ofstream outfile (filename,std::ofstream::binary);

  //   outfile.write (inputs[i].buffer(),inputs[i].size());

  //   outfile.close();
  // }
  //fprintf(stderr, "Task : %d : Started with join algorithm\n", task);
  sorted_join_algorithm(inputs, output, task);
  //fprintf(stderr, "Task : %d : Done with join algorithm\n", task);
  
  // MergeTree join_tree;
  
  // join_tree.decode(output[0]);
  // join_tree.writeToFile(task+1000);
  
  // Deleting input data
  for (int i=0; i<inputs.size(); i++){
    delete[] (char*)inputs[i].buffer();
  }
  inputs.clear();
  
  return 0;
}

int local_correction(std::vector<Payload>& inputs,
                     std::vector<Payload>& output, TaskId task){
  
  // if(inputs[1].size() < inputs[0].size()){
  //   iter_swap(inputs.begin(), inputs.begin() + 1);
  // }
  // printf("size %d %d\n",inputs[0].size(),inputs[1].size() );
  //if ((task & ~sPrefixMask) == 237)
  local_correction_algorithm(inputs, output, task);
  
  // Deleting input data
  for (int i=0; i<inputs.size(); i++){
    delete[] (char*)inputs[i].buffer();
  }
  inputs.clear();
  
  //fprintf(stderr,"CORRECTION performed by task %d\n", task & ~sPrefixMask);
  return 1;
}

int write_results(std::vector<Payload>& inputs,
                  std::vector<Payload>& output, TaskId task){
  
  AugmentedMergeTree t;
  t.decode(inputs[0]);
  
  t.id(task & ~sPrefixMask);
  // t.writeToFile(task & ~sPrefixMask);
  //printf("after write to file %d.dot\n", task & ~sPrefixMask);
  
  t.computeSegmentation();
  //t.writeToFileBinary(task & ~sPrefixMask);
  //fprintf(stderr,"done segmentation\n");
  // Deleting input data
  for (int i=0; i<inputs.size(); i++){
    delete[] (char*)inputs[i].buffer();
  }
  inputs.clear();
  
  assert(output.size() == 0);
  //fprintf(stderr,"WRITING RESULTS performed by %d\n", task & ~sPrefixMask);
  return 1;
}




template<typename T>
T* extractBlock(T* data, GlobalIndexType* dim, GlobalIndexType* start, GlobalIndexType* end){
  uint32_t dim_block[3];
  
  for(int i=0; i < 3; i++){
      dim_block[i] = end[i]-start[i]+1;
  }
  
  T* block = (T*)malloc(sizeof(T)*dim_block[0]*dim_block[1]*dim_block[2]);
  
  uint32_t bidx = 0;
  
  for (uint32_t z = start[2]; z < end[2]+1; z++) {
    for (uint32_t y = start[1]; y < end[1]+1; y++) {
      for (uint32_t x = start[0]; x < end[0]+1; x++) {
        
            //printf("ext %d %d %d\n", x, y, z);
            uint32_t idx = x + y * dim[0] + z * dim[0]*dim[1];
            
            block[bidx++] = data[idx];
            
          }
      }
  }

  //  printf("dims %d %d %d\n", dim_block[0], dim_block[1], dim_block[2]);
  //printf("written %d\n", bidx);
  
  return block;
  
}

template<typename T>
std::vector<DomainBlock<T> > blockify(T* data, uint32_t* num_blocks, GlobalIndexType* dim, int share_face){
  
  std::vector<DomainBlock<T> > blocks;
  
  /*uint32_t num_blocks[3] = {(dim[0]+(dim_block[0]-1))/dim_block[0],
    (dim[1]+(dim_block[1]-1))/dim_block[1],
    (dim[2]+(dim_block[2]-1))/dim_block[2]};
  */
  uint32_t dim_block[3] = {dim[0]/num_blocks[0], dim[1]/num_blocks[1],dim[2]/num_blocks[2]};
  
  int num_blocks_count = num_blocks[0]*num_blocks[1]*num_blocks[2];
  
//  printf("Blockify size %dx%dx%d into %dx%dx%d blocks...\n", dim[0], dim[1], dim[2], dim_block[0], dim_block[1], dim_block[2]);
//  printf("Num %dx%dx%d=%d blocks...\n", num_blocks[0], num_blocks[1], num_blocks[2], num_blocks[0]*num_blocks[1]*num_blocks[2]);
    
  GlobalIndexType b_start[3];
  GlobalIndexType b_end[3];
  GlobalIndexType b_dim[3];
  
  for (uint32_t z = 0, idx = 0; z < num_blocks[2]; ++z) {
    b_start[2] = z * dim_block[2];
    b_dim[2] = ((b_start[2] + dim_block[2]) <= dim[2]) ?
    dim_block[2] : (dim[2] - b_start[2]);
    b_end[2] = b_start[2] + b_dim[2] -1;
    
    if(b_end[2] + share_face < dim[2]) b_end[2] = b_end[2] + share_face;
    
    for (uint32_t y = 0; y < num_blocks[1]; ++y) {
      b_start[1] = y * dim_block[1];
      b_dim[1] = ((b_start[1] + dim_block[1]) <= dim[1]) ?
      dim_block[1] : (dim[1] - b_start[1]);
      b_end[1] = b_start[1] + b_dim[1] -1;
      
      if(b_end[1] + share_face < dim[1]) b_end[1] = b_end[1] + share_face;
      
      for (uint32_t x = 0; x < num_blocks[0]; ++x, ++idx) {
        b_start[0] = x * dim_block[0];
        b_dim[0] = ((b_start[0] + dim_block[0]) <= dim[0]) ?
        dim_block[0] : (dim[0] - b_start[0]);
        b_end[0] = b_start[0] + b_dim[0] - 1;
        
        if(b_end[0] + share_face < dim[0]) b_end[0] = b_end[0] + share_face;
        
        DomainBlock<T> block;
        memcpy(block.low, b_start, sizeof(GlobalIndexType)*3);
        memcpy(block.high, b_end, sizeof(GlobalIndexType)*3);

       // printf("block %d start (%d %d %d) end (%d %d %d)\n", idx, b_start[0],b_start[1], b_start[2], b_end[0],b_end[1], b_end[2]);
        
        block.data = extractBlock<T>(data, dim, b_start, b_end);
        
        blocks.push_back(block);
        
      }
    }
  }
  
  return  blocks;
}

std::vector<DomainSelection> blockify_nodata(uint32_t* num_blocks, GlobalIndexType* dim, int share_face){
  
  std::vector<DomainSelection> blocks;
  
  /*uint32_t num_blocks[3] = {(dim[0]+(dim_block[0]-1))/dim_block[0],
    (dim[1]+(dim_block[1]-1))/dim_block[1],
    (dim[2]+(dim_block[2]-1))/dim_block[2]};
  */
  uint32_t dim_block[3] = {dim[0]/num_blocks[0], dim[1]/num_blocks[1],dim[2]/num_blocks[2]};
  
  int num_blocks_count = num_blocks[0]*num_blocks[1]*num_blocks[2];
  
  printf("Blockify size %dx%dx%d into %dx%dx%d blocks...\n", dim[0], dim[1], dim[2], dim_block[0], dim_block[1], dim_block[2]);
  printf("Num %dx%dx%d=%d blocks...\n", num_blocks[0], num_blocks[1], num_blocks[2], num_blocks[0]*num_blocks[1]*num_blocks[2]);
    
  GlobalIndexType b_start[3];
  GlobalIndexType b_end[3];
  GlobalIndexType b_dim[3];
  
  for (uint32_t z = 0, idx = 0; z < num_blocks[2]; ++z) {
    b_start[2] = z * dim_block[2];
    b_dim[2] = ((b_start[2] + dim_block[2]) <= dim[2]) ?
    dim_block[2] : (dim[2] - b_start[2]);
    b_end[2] = b_start[2] + b_dim[2] -1;

    if(b_end[2] + share_face < dim[2]) b_end[2] = b_end[2] + share_face;
    
    for (uint32_t y = 0; y < num_blocks[1]; ++y) {
      b_start[1] = y * dim_block[1];
      b_dim[1] = ((b_start[1] + dim_block[1]) <= dim[1]) ?
      dim_block[1] : (dim[1] - b_start[1]);
      b_end[1] = b_start[1] + b_dim[1] -1;
      
      if(b_end[1] + share_face < dim[1]) b_end[1] = b_end[1] + share_face;
      
      for (uint32_t x = 0; x < num_blocks[0]; ++x, ++idx) {
        b_start[0] = x * dim_block[0];
        b_dim[0] = ((b_start[0] + dim_block[0]) <= dim[0]) ?
        dim_block[0] : (dim[0] - b_start[0]);
        b_end[0] = b_start[0] + b_dim[0] - 1;
        
        if(b_end[0] + share_face < dim[0]) b_end[0] = b_end[0] + share_face;
        
        DomainSelection block;
        memcpy(block.low, b_start, sizeof(GlobalIndexType)*3);
        memcpy(block.high, b_end, sizeof(GlobalIndexType)*3);

       // printf("block %d start (%d %d %d) end (%d %d %d)\n", idx, b_start[0],b_start[1], b_start[2], b_end[0],b_end[1], b_end[2]);
        
        blocks.push_back(block);
        
      }
    }
  }
  
  return  blocks;
}

std::map<TaskId,Payload> input_initialization_nodata(int argc, char* argv[]){
  GlobalIndexType data_size[3];        // {x_size, y_size, z_size}
  uint32_t block_decomp[3];     // block decomposition
  int nblocks;                  // number of blocks per input task
  int share_face = 1;           // share a face among the blocks
  uint32_t valence = 2;
  FunctionType threshold = (FunctionType)(-1)*FLT_MAX;
  char* dataset = NULL;
  //fprintf(stderr, "input initialization started\n");
  for (int i = 1; i < argc; i++){
    if (!strcmp(argv[i],"-d")){
      data_size[0] = atoi(argv[++i]);
      data_size[1] = atoi(argv[++i]);
      data_size[2] = atoi(argv[++i]);
    }
    if (!strcmp(argv[i],"-p")){
      block_decomp[0] = atoi(argv[++i]);
      block_decomp[1] = atoi(argv[++i]);
      block_decomp[2] = atoi(argv[++i]);
    }
    if (!strcmp(argv[i],"-m"))
      valence = atoi(argv[++i]);
    if (!strcmp(argv[i],"-t"))
      threshold = atof(argv[++i]);
    if (!strcmp(argv[i],"-f"))
      dataset = argv[++i];
  }
  
  KWayMerge graph(block_decomp, valence);
  KWayTaskMap task_map(1, &graph);
  std::map<TaskId,Payload> initial_input;
  MergeTree::setDimension(data_size);
  
  GlobalIndexType tot_size = data_size[0]*data_size[1]*data_size[2]*sizeof(float);
  
  std::vector<DomainSelection> blocks = blockify_nodata(block_decomp, data_size, share_face);
  nblocks = blocks.size();
  /*
  FILE* output = fopen("graph.dot","w");
  graph.output_graph(1, &task_map, output);
  fclose(output);
  */
  std::vector<DataFlow::Task> alltasks = graph.localGraph(0, &task_map);
  std::map<TaskId,DataFlow::Task> taskmap;
  
  std::vector<DataFlow::Task> leafTasks;
  
  for(uint32_t i=0; i < alltasks.size(); i++){
    taskmap[alltasks[i].id()] = alltasks[i];
    
    if(alltasks[i].incoming().size() > 0 && alltasks[i].incoming()[0] == TNULL){
      leafTasks.push_back(alltasks[i]);
      //    printf("leaf task %d\n", alltasks[i].id());
    }
  }
  
  //std::map<TaskId,Payload> initial_input;
  
  int in_length = leafTasks.size();
  
  // Set input for leaf tasks
  for(int i=0; i < in_length; i++){
    
    DataFlow::Task& task = leafTasks[i];
    //    printf("input task %d callback %d\n", task.id(), task.callback());

    size_t size = sizeof(DomainSelection);
    char* input = (char*)malloc(size);

    memcpy(input, &blocks[i], size);
    Payload pay(size, input);

    initial_input[task.id()] = pay;
  }
  
  // fprintf(stderr,"input initialization done\n");
  
  return initial_input;
}


std::map<TaskId,Payload> input_initialization(int argc, char* argv[]){
  GlobalIndexType data_size[3];        // {x_size, y_size, z_size}
  uint32_t block_decomp[3];     // block decomposition
  int nblocks;                  // number of blocks per input task
  int share_face = 1;           // share a face among the blocks
  uint32_t valence = 2;
  FunctionType threshold = (FunctionType)(-1)*FLT_MAX;
  char* dataset = NULL;
  //fprintf(stderr, "input initialization started\n");
  for (int i = 1; i < argc; i++){
    if (!strcmp(argv[i],"-d")){
      data_size[0] = atoi(argv[++i]);
      data_size[1] = atoi(argv[++i]);
      data_size[2] = atoi(argv[++i]);
    }
    if (!strcmp(argv[i],"-p")){
      block_decomp[0] = atoi(argv[++i]);
      block_decomp[1] = atoi(argv[++i]);
      block_decomp[2] = atoi(argv[++i]);
    }
    if (!strcmp(argv[i],"-m"))
      valence = atoi(argv[++i]);
    if (!strcmp(argv[i],"-t"))
      threshold = atof(argv[++i]);
    if (!strcmp(argv[i],"-f"))
      dataset = argv[++i];
  }
  
  KWayMerge graph(block_decomp, valence);
  KWayTaskMap task_map(1, &graph);
  std::map<TaskId,Payload> initial_input;
  MergeTree::setDimension(data_size);
  
  std::ifstream in;
  in.open(dataset);
  
  GlobalIndexType tot_size = data_size[0]*data_size[1]*data_size[2]*sizeof(float);
  float *data = (float*)malloc(tot_size);
  
  in.read((char*)data, tot_size);
  
  std::vector<DomainBlock<float> > blocks = blockify<float>(data, block_decomp, data_size, share_face);
  nblocks = blocks.size();
  /*
  FILE* output = fopen("graph.dot","w");
  graph.output_graph(1, &task_map, output);
  fclose(output);
  */
  std::vector<DataFlow::Task> alltasks = graph.localGraph(0, &task_map);
  std::map<TaskId,DataFlow::Task> taskmap;
  
  std::vector<DataFlow::Task> leafTasks;
  
  for(uint32_t i=0; i < alltasks.size(); i++){
    taskmap[alltasks[i].id()] = alltasks[i];
    
    if(alltasks[i].incoming().size() > 0 && alltasks[i].incoming()[0] == TNULL){
      leafTasks.push_back(alltasks[i]);
      //    printf("leaf task %d\n", alltasks[i].id());
    }
  }
  
  //std::map<TaskId,Payload> initial_input;
  
  int in_length = leafTasks.size();
  
  // Set input for leaf tasks
  for(int i=0; i < in_length; i++){
    
    DataFlow::Task& task = leafTasks[i];
    //  printf("input task %d callback %d threshold %f\n", task.id(), task.callback(), threshold);
      /*
    std::ofstream out;
    int datasize = (blocks[i].high[0]-blocks[i].low[0]+1)*(blocks[i].high[1]-blocks[i].low[1]+1)*(blocks[i].high[2]-blocks[i].low[2]+1);
    char name[128];
    sprintf(name, "data_block%d.raw", i);
    printf("data_block%d.raw\n", i);
    out.open(name,std::ofstream::binary);
    out.write((char*)blocks[i].data, datasize);
    out.close();
      */
    initial_input[task.id()] =  make_local_block((FunctionType*)(blocks[i].data), blocks[i].low, blocks[i].high, threshold);//inputs[i];
    
    free(blocks[i].data);
  }

  in.close();
  free(data);
  
  //fprintf(stderr,"input initialization done\n");
  
  return initial_input;
}

DataFlow::Callback registered_callback(DataFlow::CallbackId id)
{
  switch (id) {
    case 0:
      return DataFlow::relay_message;
    case 1:
      return local_compute;
    case 2:
      return join;
    case 3:
      return local_correction;
    case 4:
      return write_results;
    default:
      assert(false);
      break;
  }
  return NULL;
}

DataFlow::TaskGraph* make_task_graph(DataFlow::Payload buffer)
{
  return DataFlow::charm::make_task_graph_template<KWayMerge>(buffer);
}


class Main : public CBase_Main
{
public:

  uint32_t mSum;

  //! The main constructor that constructs *and* starts the dataflow
  Main(CkArgMsg* m)
  {

      if (m->argc < 9) {
      fprintf(stderr,"Usage: %s <input_data> <Xdim> <Ydim> <Zdim> \
              <dx> <dy> <dz> <fanin> <threshold>\n", m->argv[0]);
      return ;
    }
    /*
    int num_entries = 4;
    gasnet_nodeinfo_t ginfo;
    int rank = gasnet_getNodeInfo(&ginfo, num_entries) ;//gasnet_mynode();
    printf("I'm node %d argc %d\n", ginfo.supernode, argc);
    */
    
    GlobalIndexType data_size[3]={0,0,0};        // {x_size, y_size, z_size}
    uint32_t block_decomp[3]={0,0,0};     // block decomposition
    int nblocks;                  // number of blocks per input task
    int share_face = 1;           // share a face among the blocks
    uint32_t valence = 2;
    FunctionType threshold = (FunctionType)(-1)*FLT_MAX;
    char* dataset = NULL;
    
    for (int i = 1; i < m->argc; i++){
      if (!strcmp(m->argv[i],"-d")){
        data_size[0] = atoi(m->argv[++i]);                                                                                                              
        data_size[1] = atoi(m->argv[++i]);                                                                                                              
        data_size[2] = atoi(m->argv[++i]); 
      }
      if (!strcmp(m->argv[i],"-p")){
        block_decomp[0] = atoi(m->argv[++i]);                                                                                                           
        block_decomp[1] = atoi(m->argv[++i]);                                                                                                           
        block_decomp[2] = atoi(m->argv[++i]);
      }
      if (!strcmp(m->argv[i],"-m"))
        valence = atoi(m->argv[++i]);
      if (!strcmp(m->argv[i],"-t"))
        threshold = atof(m->argv[++i]);
      if (!strcmp(m->argv[i],"-f"))
        dataset = m->argv[++i];
    }
    
    int tot_blocks = block_decomp[0]* block_decomp[1]* block_decomp[2];
    printf("run config %d %d %d nprocs %d \n",block_decomp[0], block_decomp[1], block_decomp[2],tot_blocks);    
    KWayMerge graph(block_decomp, valence);
    KWayTaskMap task_map(1, &graph);
    std::map<TaskId,Payload> initial_input;
    MergeTree::setDimension(data_size);

    DataFlow::charm::Controller controller;
    
    DataFlow::charm::Controller::ProxyType proxy;
    proxy = controller.initialize(graph.serialize(),tot_blocks);

    std::map<TaskId,Payload> inputs = input_initialization_nodata(m->argc, m->argv);

    std::map<TaskId,Payload>::iterator it;

    int i=0;
    for (it=inputs.begin();it != inputs.end(); it++) {

      std::vector<char> buffer(it->second.size());

      uint32_t size = it->second.size();
      buffer.assign((char*)&size, (char*)(it->second.buffer()));

      proxy[graph.gId(i++)].addInput(TNULL,buffer);
    }
    // controller.registerInputInitialization(&input_initialization_nodata);
    
  }

  Main(CkMigrateMessage *m) {}

  void done() {fprintf(stderr,"DONE\n");CkExit();}
};


#include "pmt.def.h"

