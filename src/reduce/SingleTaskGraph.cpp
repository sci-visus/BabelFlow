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

  t.callback( TaskCB::SINGLE_TASK_CB, TaskGraph::queryCallback( type(), TaskCB::SINGLE_TASK_CB ) );  // Only one callback function 

  // Single input from controller
  std::vector<TaskId> incoming( 1 );
  incoming[0] = TNULL;
  t.incoming( incoming );
  
  // An output with destination TNULL signifies an output to be extracted
  // from this task or to be linked to another graph
  std::vector< std::vector<TaskId> > outgoing;
  outgoing.resize(1);
  outgoing[0].resize(1, TNULL);
  t.outputs( outgoing );

  return t;
}

//-----------------------------------------------------------------------------

Payload SingleTaskGraph::serialize() const
{
  return Payload();   // Nothing to serialize
}

//-----------------------------------------------------------------------------

void SingleTaskGraph::deserialize( Payload buffer )
{
  // Nothing to do here since nothing was serialized
}

//-----------------------------------------------------------------------------

}   // end namespace BabelFlow
