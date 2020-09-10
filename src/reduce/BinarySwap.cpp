/*
 * BinarySwap.cpp
 *
 *  Created on: Sep 3, 2016
 *      Author: spetruzza
 * 
 *  Updated on: Jul 7, 2020
 *      Author: sshudler
 */

#include <cassert>
#include <cmath>

#include "BinarySwap.h"

using namespace BabelFlow;


uint32_t BinarySwap::sDATASET_DIMS[3];

//-----------------------------------------------------------------------------

BinarySwap::BinarySwap(uint32_t nblks)
{
  init(nblks);
}

//-----------------------------------------------------------------------------

void BinarySwap::init(uint32_t nblks)
{
  mLvlOffset.push_back(0);

  n_blocks = nblks;

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

//-----------------------------------------------------------------------------

Payload BinarySwap::serialize() const
{
  uint32_t num_elems_in_buff = 4;
  uint32_t* buffer = new uint32_t[num_elems_in_buff];

  buffer[0] = n_blocks;

  buffer[1] = BinarySwap::sDATASET_DIMS[0];
  buffer[2] = BinarySwap::sDATASET_DIMS[1];
  buffer[3] = BinarySwap::sDATASET_DIMS[2];

  // TODO threshold

  //printf("serializing %d %d %d , f %d\n", buffer[0], buffer[1], buffer[2], buffer[3]);

  return Payload(num_elems_in_buff*sizeof(uint32_t),(char*)buffer);
}

//-----------------------------------------------------------------------------

void BinarySwap::deserialize(Payload buffer)
{
  uint32_t num_elems_in_buff = 4;
  assert (buffer.size() == num_elems_in_buff*sizeof(uint32_t));
  uint32_t *tmp = (uint32_t *)(buffer.buffer());

  //printf("DEserializing %d %d %d , f %d\n", tmp[0], tmp[1], tmp[2], tmp[3]);

  BinarySwap::sDATASET_DIMS[0] = tmp[1];
  BinarySwap::sDATASET_DIMS[1] = tmp[2];
  BinarySwap::sDATASET_DIMS[2] = tmp[3];
  
  // memcpy(dim, buffer.buffer(), sizeof(uint32_t)*3);

  init(tmp[0]);

  delete[] buffer.buffer();
}

//-----------------------------------------------------------------------------

Task BinarySwap::task(uint64_t gId) const{

  Task t(toTId(gId));
  Task* it = &t;

  std::vector<TaskId> incoming;
  std::vector<std::vector<TaskId> > outgoing;

  uint8_t lvl = level(it->id());

  if (lvl == 0) { // If this is a leaf node
    it->callback( TaskCB::LEAF_TASK_CB, queryCallback( TaskCB::LEAF_TASK_CB ) ); 

    incoming.resize(1); // One dummy input
    incoming[0] = TNULL;
    it->incoming(incoming);
  } 
  else {
    it->callback( TaskCB::MID_TASK_CB, queryCallback( TaskCB::MID_TASK_CB ) );

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
    it->callback( TaskCB::ROOT_TASK_CB, queryCallback( TaskCB::ROOT_TASK_CB ) );

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

//-----------------------------------------------------------------------------

std::vector<Task> BinarySwap::localGraph(ShardId id,
                                         const TaskMap* task_map) const
{
  // Get all the ids we need from the TaskMap
  std::vector<TaskId> tids = task_map->tasks( id );
  std::vector<Task> tasks( tids.size() );
  // Assign all the task ids
  for( uint32_t i = 0; i < tids.size(); ++i )
    tasks[i] = task( gId( tids[i] ) );

  return tasks;
}

//-----------------------------------------------------------------------------

void BinarySwap::outputDot( const std::vector< std::vector<Task> >& tasks_v, 
                            std::ostream& outs, 
                            const std::string& eol ) const
{
  for( uint32_t i = 0; i <= mRounds; ++i )
    outs << "f" << i << " [label=\"level " << i << "\"]" << eol <<std::endl;
  
  if( mRounds > 0 )
  {
    outs << "f0 ";
    for( uint32_t i = 1; i <= mRounds; ++i )
      outs << " -> f" << i;
    outs << eol << std::endl;
    outs << eol << std::endl;
  }

  for( uint32_t i = 0; i < tasks_v.size(); ++i )
  {
    for( const Task& tsk : tasks_v[i] )
    {
      outs << tsk.id() << " [label=\"" << tsk.id() << "," << uint32_t(tsk.callbackId()) 
           << "\",color=" << (level(tsk.id()) == 0 ? "red" : "black") << "]" << eol << std::endl;

      // Print incoming edges
      for( const TaskId& tid : tsk.incoming() )
      {
        if( tid != TNULL )
          outs << tid << " -> " << tsk.id() << eol << std::endl;
      }

      // Print outgoing edges
      for( uint32_t i = 0; i < tsk.fanout(); ++i )
      {
        for( const TaskId& tid : tsk.outgoing(i) )
        {
          if( tid != TNULL )
            outs << tsk.id() << " -> " << tid << eol << std::endl;
        }
      }
    }
  }
}

//-----------------------------------------------------------------------------