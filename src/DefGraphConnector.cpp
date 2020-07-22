/*
 * DefGraphConnector.cpp
 *
 *  Created on: Jul 13, 2020
 *      Author: sshudler
 */

#include <algorithm>

#include "DefGraphConnector.h"


namespace BabelFlow
{


//-----------------------------------------------------------------------------

DefGraphConnector::DefGraphConnector( ShardId shard_id, TaskGraph* src_gr, TaskGraph* dst_gr, TaskMap* src_map, TaskMap* dst_map )
 : TaskGraphConnector( shard_id, src_gr, dst_gr, src_map, dst_map )
{
  init();
}

//-----------------------------------------------------------------------------

std::vector<TaskId> DefGraphConnector::getOutgoingConnectedTasks( const TaskId& task_id ) const
{
  std::vector<TaskId> rv;
  auto iter = m_outConnectorMap.find( task_id );
  if( iter != m_outConnectorMap.end() )
  {
    rv.push_back( iter->second );
  }

  return rv;
}

//-----------------------------------------------------------------------------

std::vector<TaskId> DefGraphConnector::getIncomingConnectedTasks( const TaskId& task_id ) const
{
  auto iter = m_inConnectorMap.find( task_id );
  if( iter != m_inConnectorMap.end() )
    return iter->second;

  return std::vector<TaskId>();
}

//-----------------------------------------------------------------------------

void DefGraphConnector::init()
{
  std::vector<Task> src_ts = m_srcGraph->localGraph( m_shardId, m_srcMap );
  std::vector<Task> dst_ts = m_dstGraph->localGraph( m_shardId, m_dstMap );
  
  std::vector<TaskId> output_ts, input_ts;

  std::for_each( src_ts.begin(), src_ts.end(), [&output_ts](Task& t) { if( isRootTask(t) ) output_ts.push_back( t.id() ); } );
  std::for_each( dst_ts.begin(), dst_ts.end(), [&input_ts](Task& t) { if( isLeafTask(t) ) input_ts.push_back( t.id() ); } );

  // Sanity check -- source graph should have at least one root and destination graph - at least one leaf task
  assert( output_ts.size() > 0 );
  assert( input_ts.size() > 0 );

  // We assume the task ids of both the roots and the leafs are monotonically increasing
  std::sort( output_ts.begin(), output_ts.end() );
  std::sort( input_ts.begin(), input_ts.end() );

  // Default mapping is just a cyclic one-to-one 
  for( uint32_t i = 0; i < output_ts.size(); ++i )
  {
    TaskId& out_tid = output_ts[i];
    TaskId& in_tid = input_ts[ i % input_ts.size() ];

    m_outConnectorMap[ out_tid ] = in_tid;
    m_inConnectorMap[ in_tid ].push_back( out_tid );
  }
}

//-----------------------------------------------------------------------------

bool DefGraphConnector::isRootTask( const Task& tsk )
{
  bool is_out_task = true;

  for( uint32_t i = 0; i < tsk.fanout(); ++i )
  {
    const std::vector<TaskId>& outg = tsk.outgoing(i);
    auto res = std::find_if( outg.begin(), outg.end(), [&](const TaskId& tid) { return tid != TNULL; } );
    if( res != outg.end() )
    {
      is_out_task = false;
      break;
    }
  }
  
  return is_out_task;
}

//-----------------------------------------------------------------------------

bool DefGraphConnector::isLeafTask( const Task& tsk )
{
  auto res = std::find_if( tsk.incoming().begin(), tsk.incoming().end(), [&](const TaskId& tid) { return tid != TNULL; } );
  return res == tsk.incoming().end();
}

//-----------------------------------------------------------------------------


}  // namespace BabelFlow
