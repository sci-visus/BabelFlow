/*
 * SingleTaskGraph.cpp
 *
 *  Created on: Jul 16, 2020
 *      Author: sshudler
 */

#include "SingleTaskGraph.h"


namespace BabelFlow
{


//-----------------------------------------------------------------------------

std::vector<Task> SingleTaskGraph::localGraph( ShardId id, const TaskMap* task_map ) const
{
  std::vector<TaskId> tids = task_map->tasks( id );
  std::vector<Task> tasks( tids.size() );
  for( uint32_t i = 0; i < tids.size(); ++i )
    tasks[i] = task( gId( tids[i] ) );
  
  return tasks;
}

//-----------------------------------------------------------------------------

Task SingleTaskGraph::task( uint64_t gId ) const
{
  Task t( gId );

  t.callback( TaskCB::SINGLE_TASK_CB, queryCallback( TaskCB::SINGLE_TASK_CB ) );  // Only one callback function 

  // Single input from controller
  std::vector<TaskId> incoming( m_numInputSrcs, TNULL );
  t.incoming( incoming );
  
  // An output with destination TNULL signifies an output to be extracted
  // from this task or to be linked to another graph
  std::vector< std::vector<TaskId> > outgoing;
  outgoing.resize( m_numOutputData );
  for( auto& tskid_v : outgoing )
    tskid_v.resize( m_numOutputDest, TNULL );

  t.outputs( outgoing );

  return t;
}

//-----------------------------------------------------------------------------

Payload SingleTaskGraph::serialize() const
{
  uint32_t num_elems = 4;
  uint32_t* buffer = new uint32_t[num_elems];

  buffer[0] = m_nRanks;
  buffer[1] = m_numInputSrcs;
  buffer[2] = m_numOutputData;
  buffer[3] = m_numOutputDest;

  return Payload( num_elems*sizeof(uint32_t), (char*)buffer );
}

//-----------------------------------------------------------------------------

void SingleTaskGraph::deserialize( Payload buffer )
{
  uint32_t num_elems = 4;
  assert( buffer.size() == num_elems*sizeof(uint32_t) );

  uint32_t* buff_ptr = (uint32_t *)(buffer.buffer());

  m_nRanks = buff_ptr[0];
  m_numInputSrcs = buff_ptr[1];
  m_numOutputData = buff_ptr[2];
  m_numOutputDest = buff_ptr[3];

  buffer.reset();  
}

//-----------------------------------------------------------------------------

}   // end namespace BabelFlow
