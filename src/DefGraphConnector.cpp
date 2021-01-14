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

DefGraphConnector::DefGraphConnector( TaskGraph* src_gr, 
                                      uint32_t src_gr_id, 
                                      TaskGraph* dst_gr,
                                      uint32_t dst_gr_id )
 : TaskGraphConnector( src_gr, src_gr_id, dst_gr, dst_gr_id )
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
  std::vector<TaskId> output_ts( m_srcGraph->numOfRoots() );
  std::vector<TaskId> input_ts( m_dstGraph->numOfLeafs() );

  for( uint32_t i = 0; i < output_ts.size(); ++i )
    output_ts[i] = m_srcGraph->root( i );

  for( uint32_t i = 0; i < input_ts.size(); ++i )
    input_ts[i] = m_dstGraph->leaf( i );

  /////
  // std::cout << "DefGraphConnector - no. source tasks: " << src_ts.size() << std::endl;
  // std::cout << "DefGraphConnector - no. destination tasks: " << dst_ts.size() << std::endl;
  /////

  // Default mapping is just a cyclic one-to-one 
  for( uint32_t i = 0; i < output_ts.size(); ++i )
  {
    TaskId& out_tid = output_ts[i];
    TaskId& in_tid = input_ts[ i % input_ts.size() ];

    // Set the graph ids
    out_tid.graphId() = m_srcGraphId;
    in_tid.graphId() = m_dstGraphId;

    m_outConnectorMap[ out_tid ] = in_tid;
    m_inConnectorMap[ in_tid ].push_back( out_tid );
  }
}

//-----------------------------------------------------------------------------


}  // namespace BabelFlow
