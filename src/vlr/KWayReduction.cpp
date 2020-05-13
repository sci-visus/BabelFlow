/*
 * KWayReduction.cpp
 *
 *  Created on: Aug 30, 2016
 *      Author: spetruzza
 */

#include <cassert>
#include <sstream>

#include "KWayReduction.h"


using namespace BabelFlow;


KWayReduction::KWayReduction(const std::string& config)
{
  std::stringstream cmd(config);

  uint32_t dim[3];

  cmd >> dim[0];
  cmd >> dim[1];
  cmd >> dim[2];

  uint32_t factor;

  cmd >> factor;
  
  init(dim, factor);
}

KWayReduction::KWayReduction(uint32_t block_dim[3], uint32_t factor) : mFactor(factor)
{
  init(block_dim, factor);
}

void KWayReduction::init(uint32_t block_dim[3], uint32_t factor)
{
  uint32_t f;
  uint32_t dim[3];
  dim[0] = block_dim[0];
  dim[1] = block_dim[1];
  dim[2] = block_dim[2];

  mLvlOffset.push_back(0);
  mLvlDim.push_back(std::vector<uint32_t>(3));
  mLvlDim[0][0] = dim[0];
  mLvlDim[0][1] = dim[1];
  mLvlDim[0][2] = dim[2];

  assert ((factor == 2) || (factor == 4) || (factor == 8) || 
          (factor == 16) || (factor == 32));

  // Now we compute how many rounds
  mRounds = 1;

  while (dim[0]*dim[1]*dim[2] > 1) {
    mLvlOffset.push_back(mLvlOffset.back()+dim[0]*dim[1]*dim[2]);
    mRounds++;

    mFactors.push_back(std::vector<uint8_t>(3));
    mFactors.back()[0] = mFactors.back()[1] = mFactors.back()[2] = 1;

    f = 1;

    while ((f < factor) && (dim[0]*dim[1]*dim[2] > 1)) {
      if ((dim[0] >= dim[1]) && (dim[0] >= dim[2])) {
        dim[0] = (dim[0]+1) / 2;
        mFactors.back()[0] *= 2;
      }
      else if (dim[1] >= dim[2]) {
        dim[1] = (dim[1] + 1) / 2;
        mFactors.back()[1] *= 2;
      }
      else {
        dim[2] = (dim[2]+1) / 2;
        mFactors.back()[2] *= 2;
      }

      f *= 2;
    }

    mLvlDim.push_back(std::vector<uint32_t>(3));
    mLvlDim.back()[0] = dim[0];
    mLvlDim.back()[1] = dim[1];
    mLvlDim.back()[2] = dim[2];
  }
  mLvlOffset.push_back(mLvlOffset.back()+1);

  mFactors.push_back(std::vector<uint8_t>(3));
  mFactors.back()[0] = mFactors.back()[1] = mFactors.back()[2] = 1;

  // for (uint8_t i=0;i<mRounds;i++)
  //   fprintf(stderr,"Lvl %d: dim [%d,%d,%d], offset %d, factors [%d,%d,%d]\n",
  //          i,mLvlDim[i][0],mLvlDim[i][1],mLvlDim[i][2],mLvlOffset[i],
  //          mFactors[i][0],mFactors[i][1],mFactors[i][2]);

}

Payload KWayReduction::serialize() const
{
  uint32_t* buffer = new uint32_t[8];

  buffer[0] = mLvlDim[0][0];
  buffer[1] = mLvlDim[0][1];
  buffer[2] = mLvlDim[0][2];

  buffer[3] = KWayReduction::sDATASET_DIMS[0];
  buffer[4] = KWayReduction::sDATASET_DIMS[1];
  buffer[5] = KWayReduction::sDATASET_DIMS[2];
  buffer[6] = mFactor;

  // TODO threshold

  //printf("serializing %d %d %d , f %d\n", buffer[0], buffer[1], buffer[2], buffer[3]);

  return Payload(8*sizeof(uint32_t),(char*)buffer);
}

void KWayReduction::deserialize(Payload buffer)
{
  assert (buffer.size() == 8*sizeof(uint32_t));
  uint32_t *tmp = (uint32_t *)(buffer.buffer());

  // mLvlDim[0][0] = tmp[0];
  // mLvlDim[0][1] = tmp[1];
  // mLvlDim[0][2] = tmp[2];
  mFactor = tmp[6];

  //printf("DEserializing %d %d %d , f %d\n", tmp[0], tmp[1], tmp[2], tmp[3]);

  uint32_t* decomp = tmp;
  uint32_t* dim = tmp+3;

  KWayReduction::sDATASET_DIMS[0] = dim[0];
  KWayReduction::sDATASET_DIMS[1] = dim[1];
  KWayReduction::sDATASET_DIMS[2] = dim[2];

  // memcpy(dim, buffer.buffer(), sizeof(uint32_t)*3);

  init(decomp, mFactor);

  delete[] buffer.buffer();
}

TaskId KWayReduction::size() const
{
  return mLvlOffset[mRounds];
}

Task KWayReduction::task(uint64_t gId) const { 
  Task t(gId);
  Task* it = &t;

  std::vector<TaskId> incoming;
  std::vector<std::vector<TaskId> > outgoing;

  if (it->id() < mLvlOffset[1]) { // If this is a leaf node
    it->callback(1); // Local compute

    incoming.resize(1); // One dummy input
    incoming[0] = TNULL;
    it->incoming(incoming);

    outgoing.resize(1);

    outgoing[0].resize(1);
    outgoing[0][0] = reduce(it->id()); // parent

    it->outputs(outgoing);

    //fprintf(stderr,"Leaf %d: outputs %d (%d,%d)\n",it->id(),
    //        outgoing[0][0],baseId(outgoing[1][0]),round(outgoing[1][0]));

  } // end-if leaf node
  else {
    //fprintf(stderr,"ID = %d\n",it->id());
    uint8_t lvl = level(it->id());

    // Join computation
    it->callback(2);

    // Directly compute all the incoming
    incoming = expand(it->id());
    it->incoming(incoming);

    outgoing.resize(1);

    outgoing[0].resize(1);
    if (it->id() == mLvlOffset.back()-1){  // if this is the root
      it->callback(3);
      outgoing.resize(0);
      //outgoing[0][0] = TNULL; // parent
    }
    else
      outgoing[0][0] = reduce(it->id()); // parent

    it->outputs(outgoing);

    //fprintf(stderr,"Merge %d: incoming %d %d outputs %d (%d,%d)\n",
    //        it->id(),incoming[0],incoming[1],outgoing[0][0],
    //        baseId(outgoing[1][0]),round(outgoing[1][0]));

  }
  
  return t;

}

std::vector<Task> KWayReduction::localGraph(ShardId id,
                                        const TaskMap* task_map) const
{
  // First get all the ids we need
  std::vector<TaskId> ids = task_map->tasks(id);

  // The create the required number of tasks
  std::vector<Task> tasks;

  //! Now assign all the task ids
  for (TaskId i=0;i<ids.size();i++)
    tasks.push_back(task(ids[i]));

  return tasks;
}

int KWayReduction::output_graph(ShardId count, 
                            const TaskMap* task_map, FILE* output)
{
  fprintf(output,"digraph G {\n");
  fprintf(output,"\trankdir=TB;ranksep=0.8;\n");

  for (uint8_t i=0;i<mRounds;i++)
    fprintf(output,"f%d [label=\"level %d\"]",i,i);


  fprintf(output,"f0 ");
  for (uint8_t i=1;i<mRounds;i++) {
    fprintf(output," -> f%d",i);
  }
  fprintf(output,"\n\n");

  std::vector<Task> tasks;
  std::vector<Task>::iterator tIt;
  std::vector<TaskId>::iterator it;

  for (uint32_t i=0;i<count;i++) {
    tasks = localGraph(i,task_map);


    for (tIt=tasks.begin();tIt!=tasks.end();tIt++) {
      if (round(tIt->id()) == 0)
        fprintf(output,"%d [label=\"(%d, %d) ,%d)\",color=red]\n",
                tIt->id(),baseId(tIt->id()),round(tIt->id()),tIt->callback());
      else
        fprintf(output,"%d [label=\"(%d, %d) ,%d)\",color=black]\n",
                tIt->id(),baseId(tIt->id()),round(tIt->id()),tIt->callback());

      for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {
        if (*it != TNULL)
          fprintf(output,"%d -> %d\n",*it,tIt->id());
      }
    }

    for (tIt=tasks.begin();tIt!=tasks.end();tIt++)
      fprintf(output,"{rank = same; f%d; %d}\n",
              level(baseId(tIt->id())),tIt->id());

  }

  fprintf(output,"}\n");
  return 1;
}



uint8_t KWayReduction::level(TaskId id) const
{
  uint8_t l = 0;

  // assert (gatherTask(id));

  // Figure out what level we are on
  while (id >= mLvlOffset[l+1])
    l++;

  return l;
}

TaskId KWayReduction::roundId(TaskId id, uint8_t round) const
{
  return (id | (round << sPostfixSize));
}


TaskId KWayReduction::reduce(TaskId source) const
{
  uint32_t l = level(source);

  // Map the global id to an in-level grid row-major id
  if (l > 0)
    source -= mLvlOffset[l];

  assert (l < mLvlOffset.size()-1);

  // Map this to an in-level row major id on the next level
  source = gridReduce(source,l);

  // Finally map it back to a global id
  source += mLvlOffset[l+1];

  return source;
}

std::vector<TaskId> KWayReduction::expand(TaskId source) const
{
  uint32_t l = level(source);

  assert (l > 0);

  // Map the global id to an in-level grid row-major id
  if (l > 0)
    source -= mLvlOffset[l];


  // Get the row-major indices for the lower level
  std::vector<TaskId> up = gridExpand(source,l);

  // Convert them to global indices
  for (uint32_t i=0;i<up.size();i++)
    up[i] += mLvlOffset[l-1];

  return up;
}


TaskId KWayReduction::gridReduce(TaskId source, uint8_t lvl) const
{
  TaskId p[3];

  assert (lvl < mLvlDim.size()-1);

  // Compute the current index
  p[0] = source % mLvlDim[lvl][0];
  p[1] = ((source - p[0]) / mLvlDim[lvl][0]) % mLvlDim[lvl][1];
  p[2] = source / (mLvlDim[lvl][0]*mLvlDim[lvl][1]);

  // Adapt the indices
  p[0] = p[0] / mFactors[lvl][0];
  p[1] = p[1] / mFactors[lvl][1];
  p[2] = p[2] / mFactors[lvl][2];


  // Compute the new index
  source = (p[2]*mLvlDim[lvl+1][1]  + p[1])*mLvlDim[lvl+1][0] + p[0];

  return source;
}

std::vector<TaskId> KWayReduction::gridExpand(TaskId source, uint8_t lvl) const
{
  TaskId p[3];
  std::vector<TaskId> up;

  assert (lvl > 0);

  // Compute the current index
  p[0] = source % mLvlDim[lvl][0];
  p[1] = ((source - p[0]) / mLvlDim[lvl][0]) % mLvlDim[lvl][1];
  p[2] = source / (mLvlDim[lvl][0]*mLvlDim[lvl][1]);

  // Adapt the indices
  p[0] = p[0] * mFactors[lvl-1][0];
  p[1] = p[1] * mFactors[lvl-1][1];
  p[2] = p[2] * mFactors[lvl-1][2];

  // Compute the new indices
  for (TaskId i=0;i<mFactors[lvl-1][2];i++) {
    for (TaskId j=0;j<mFactors[lvl-1][1];j++) {
      for (TaskId k=0;k<mFactors[lvl-1][0];k++) {

        if ((p[0]+k < mLvlDim[lvl-1][0]) && (p[1]+j < mLvlDim[lvl-1][1]) && (p[2]+i < mLvlDim[lvl-1][2]))
            up.push_back(((p[2]+i) * mLvlDim[lvl-1][1] + (p[1]+j))*mLvlDim[lvl-1][0] + (p[0]+k));
      }
    }
  }

  return up;
}



