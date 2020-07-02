/*
 * BinarySwap.cpp
 *
 *  Created on: Sep 3, 2016
 *      Author: spetruzza
 */

#include <cassert>
#include <cmath>

#include "BinarySwap.h"

using namespace BabelFlow;


uint32_t BinarySwap::sDATASET_DIMS[3];
uint32_t BinarySwap::sDATA_DECOMP[3];


BinarySwap::BinarySwap(uint32_t block_dim[3])
{
  init(block_dim);
}

void BinarySwap::init(uint32_t block_dim[3])
{
  uint32_t f;
  
  b_decomp[0] = block_dim[0];
  b_decomp[1] = block_dim[1];
  b_decomp[2] = block_dim[2];

  mLvlOffset.push_back(0);

  n_blocks = b_decomp[0]*b_decomp[1]*b_decomp[2];

  if(!fastIsPow2(n_blocks))
  {
    fprintf(stderr, "Num blocks not power of two!");
    assert(false);
  }  

  // Now we compute how many rounds
  mRounds = std::log2(n_blocks);

  for(int i=0; i < mRounds; i++)
  {
    mLvlOffset.push_back(mLvlOffset.back()+n_blocks);
  }

  // for (uint8_t i=0;i<mRounds;i++)
  //   fprintf(stderr,"Lvl %d: dim [%d,%d,%d], offset %d, factors [%d,%d,%d]\n",
  //          i,mLvlDim[i][0],mLvlDim[i][1],mLvlDim[i][2],mLvlOffset[i],
  //          mFactors[i][0],mFactors[i][1],mFactors[i][2]);

}

Payload BinarySwap::serialize() const
{
  uint32_t* buffer = new uint32_t[6];

  buffer[0] = b_decomp[0];
  buffer[1] = b_decomp[1];
  buffer[2] = b_decomp[2];

  buffer[3] = BinarySwap::sDATASET_DIMS[0];
  buffer[4] = BinarySwap::sDATASET_DIMS[1];
  buffer[5] = BinarySwap::sDATASET_DIMS[2];

  // TODO threshold

  //printf("serializing %d %d %d , f %d\n", buffer[0], buffer[1], buffer[2], buffer[3]);

  return Payload(6*sizeof(uint32_t),(char*)buffer);
}

void BinarySwap::deserialize(Payload buffer)
{
  assert (buffer.size() == 6*sizeof(uint32_t));
  uint32_t *tmp = (uint32_t *)(buffer.buffer());

  b_decomp[0] = tmp[0];
  b_decomp[1] = tmp[1];
  b_decomp[2] = tmp[2];

  //printf("DEserializing %d %d %d , f %d\n", tmp[0], tmp[1], tmp[2], tmp[3]);

  uint32_t* decomp = tmp;
  uint32_t* dim = tmp+3;

  BinarySwap::sDATASET_DIMS[0] = dim[0];
  BinarySwap::sDATASET_DIMS[1] = dim[1];
  BinarySwap::sDATASET_DIMS[2] = dim[2];
  
  BinarySwap::sDATA_DECOMP[0] = decomp[0];
  BinarySwap::sDATA_DECOMP[1] = decomp[1];
  BinarySwap::sDATA_DECOMP[2] = decomp[2];
  
  // memcpy(dim, buffer.buffer(), sizeof(uint32_t)*3);

  init(decomp);

  delete[] buffer.buffer();
}

Task BinarySwap::task(uint64_t gId) const{

  Task t(toTId(gId));
  Task* it = &t;

  std::vector<TaskId> incoming;
  std::vector<std::vector<TaskId> > outgoing;

  uint8_t lvl = level(it->id());

  if (lvl == 0) { // If this is a leaf node
    it->callback(1); 

    incoming.resize(1); // One dummy input
    incoming[0] = TNULL;
    it->incoming(incoming);
  } 
  else {
    it->callback(2);

    incoming.resize(2);

    if(it->id() % fastPow2(lvl) < fastPow2(lvl-1))
    {
      incoming[0] = roundId(it->id(),lvl-1);
      incoming[1] = roundId(it->id(),lvl-1) + fastPow2(lvl-1); 
    }
    else
    {
      incoming[0] = roundId(it->id(),lvl-1) - fastPow2(lvl-1); 
      incoming[1] = roundId(it->id(),lvl-1);
    }

    it->incoming(incoming);
  }
  
  if(lvl == mRounds)
    it->callback(3);

  // Set outputs
  if(lvl < mRounds){
    outgoing.resize(2);
    outgoing[0].resize(1);
    outgoing[1].resize(1);

    if(it->id() % fastPow2(lvl+1) < fastPow2(lvl))
    {
      //printf("t %d L %d %d\n", it->id(), pow2(lvl+1), pow2(lvl));
      outgoing[0][0] = roundId(it->id(),lvl+1);
      outgoing[1][0] = roundId(it->id(),lvl+1) + fastPow2(lvl); 
    }
    else
    {
      //printf("t %d R %d %d\n", it->id(), pow2(lvl+1), pow2(lvl));
      outgoing[0][0] = roundId(it->id(),lvl+1) - fastPow2(lvl); 
      outgoing[1][0] = roundId(it->id(),lvl+1);
    }
  }
  else{
    outgoing.resize(0);
    outgoing.clear();
  }

  it->outputs(outgoing);

  // if(incoming.size()==2){
  //   fprintf(stderr,"Task %d (l%d, cb %d): incoming %d %d outputs %d %d\n",
  //         baseId(it->id()),round(it->id()),it->callback(),baseId(incoming[0]),baseId(incoming[1]),baseId(outgoing[0][0]),
  //         baseId(outgoing[1][0]));      
  // }

  return t;
}

std::vector<Task> BinarySwap::localGraph(ShardId id,
                                         const TaskMap* task_map) const
{
  TaskId i;

  // First get all the ids we need
  std::vector<TaskId> ids = task_map->tasks(id);

  // The create the required number of tasks
  std::vector<Task> tasks;

  //! Now assign all the task ids
  for (TaskId i=0;i<ids.size();i++)
    tasks.push_back(task(ids[i]));

  return tasks;
}

int BinarySwap::output_graph(ShardId count, 
                             const TaskMap* task_map, FILE* output)
{
  fprintf(output,"digraph G {\n");
  fprintf(output,"\tordering=out;\n\trankdir=TB;ranksep=0.8;\n");

  for (uint8_t i=0;i<=mRounds;i++)
    fprintf(output,"f%d [label=\"level %d\"]",i,i);


  fprintf(output,"f0 ");
  for (uint8_t i=1;i<=mRounds;i++) {
    fprintf(output," -> f%d",i);
  }
  fprintf(output,"\n\n");

  std::vector<Task> tasks;
  std::vector<Task>::iterator tIt;
  std::vector<TaskId>::iterator it;

  for (uint32_t i=0;i<count;i++) {
    tasks = localGraph(i,task_map);

    for (tIt=tasks.begin();tIt!=tasks.end();tIt++) {
      TaskId::InnerTaskId tid = tIt->id();
      if (level(tIt->id()) == 0)
        fprintf(output, "%d [label=\"(%d ,%d)\",color=red]\n", tid, tid, tIt->callback());
      else
        fprintf(output, "%d [label=\"(%d ,%d)\",color=black]\n", tid, tid, tIt->callback());

      for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {
        if (*it != TNULL)
          fprintf(output, "%d -> %d\n", TaskId::InnerTaskId(*it), tid);
      }
    }

    // for (tIt=tasks.begin();tIt!=tasks.end();tIt++)
    //   fprintf(output,"{rank = same; f%d; %d}\n",
    //           level(tIt->id()),tIt->id());

  }

  fprintf(output,"}\n");
  return 1;
}
