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

DefGraphConnector::DefGraphConnector( ShardId controller_cnt, TaskGraph* src_gr, TaskGraph* dst_gr, TaskMap* src_map, TaskMap* dst_map )
 : TaskGraphConnector( controller_cnt, src_gr, dst_gr, src_map, dst_map )
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
  std::vector<Task> src_ts; 
  std::vector<Task> dst_ts;
  
  // Get tasks for each local graph
  for( uint32_t i = 0; i < m_controllerCount; ++i )
  {
    std::vector<Task> local_src_gr = m_srcGraph->localGraph( i, m_srcMap );
    std::vector<Task> local_dst_gr = m_dstGraph->localGraph( i, m_dstMap );

    src_ts.insert( src_ts.end(), local_src_gr.begin(), local_src_gr.end() );
    dst_ts.insert( dst_ts.end(), local_dst_gr.begin(), local_dst_gr.end() );
  }
  
  /////
  //std::cout << "DefGraphConnector - no. source tasks: " << src_ts.size() << std::endl;
  //std::cout << "DefGraphConnector - no. destination tasks: " << dst_ts.size() << std::endl;
  /////

  std::vector<TaskId> output_ts, input_ts;

  std::for_each( src_ts.begin(), src_ts.end(), [&output_ts](Task& t) { if( isRootTask(t) ) output_ts.push_back( t.id() ); } );
  std::for_each( dst_ts.begin(), dst_ts.end(), [&input_ts](Task& t) { if( isLeafTask(t) ) input_ts.push_back( t.id() ); } );

  // Sanity check -- source graph should have at least one root and destination graph - at least one leaf task
  assert( output_ts.size() > 0 );
  assert( input_ts.size() > 0 );

  // We assume the task ids of both the roots and the leafs are monotonically increasing
  std::sort( output_ts.begin(), output_ts.end() );
  std::sort( input_ts.begin(), input_ts.end() );

  /////
  // if( m_taskId == 0 )
  // {
  //   std::cout << "DefGraphConnector - root tasks:" << std::endl;
  //   for( uint32_t i = 0; i < output_ts.size(); ++i )
  //     std::cout << output_ts[i] << " ";
  //   std::cout << std::endl; 

  //   std::cout << "DefGraphConnector - leaf tasks:" << std::endl;
  //   for( uint32_t i = 0; i < input_ts.size(); ++i )
  //     std::cout << input_ts[i] << " ";
  //   std::cout << std::endl; 
  // }
  /////

  // Default mapping is just a cyclic one-to-one 
  for( uint32_t i = 0; i < output_ts.size(); ++i )
  {
    TaskId& out_tid = output_ts[i];
    TaskId& in_tid = input_ts[ i % input_ts.size() ];

    m_outConnectorMap[ out_tid ] = in_tid;
    m_inConnectorMap[ in_tid ].push_back( out_tid );
  }

  /////
  // if( m_taskId == 0 )
  // {
  //   std::cout << "DefGraphConnector - m_outConnectorMap:" << std::endl;
  //   for( auto iter = m_outConnectorMap.begin(); iter != m_outConnectorMap.end(); ++iter )
  //     std::cout << iter->first << " --> " << iter->second << std::endl;
    
  //   std::cout << "DefGraphConnector - m_inConnectorMap:" << std::endl;
  //   for( auto iter = m_inConnectorMap.begin(); iter != m_inConnectorMap.end(); ++iter )
  //   {
  //     std::cout << iter->first << " <--  ";
  //     auto ts_v = iter->second;
  //     for( auto jter = ts_v.begin(); jter != ts_v.end(); ++jter )
  //       std::cout << *jter << ";  ";
  //     std::cout << std::endl;
  //   }
  // }
  /////
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
