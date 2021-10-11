/*
 * MultiGraphConnector.cpp
 *
 *  Created on: Mar 28, 2021
 *      Author: sshudler
 */

#include <algorithm>

#include "MultiGraphConnector.h"


namespace BabelFlow
{

//-----------------------------------------------------------------------------

static inline void serialize_uint( char*& buff, uint32_t val )
{
  memcpy( buff, &val, sizeof(uint32_t) );
  buff += sizeof(uint32_t);
}

//-----------------------------------------------------------------------------

MultiGraphConnector::MultiGraphConnector( const std::vector<TaskGraph*>& gr_vec )
{
  m_grPairsVec.resize( gr_vec.size() - 1 );
  for( unsigned int i = 0; i < m_grPairsVec.size(); ++i )
    m_grPairsVec[i] = std::make_pair( i, i + 1 );
  
  init( gr_vec );
}

//-----------------------------------------------------------------------------

MultiGraphConnector::MultiGraphConnector( const std::vector<TaskGraph*>& gr_vec, 
                                          const std::vector<GraphPair>& gr_pair_vec )
 : m_grPairsVec( gr_pair_vec )
{ 
  init( gr_vec );
}

//-----------------------------------------------------------------------------

std::vector<TaskId> MultiGraphConnector::getOutgoingConnections( const TaskId& task_id ) const
{
  auto iter = m_outConnectorMap.find( task_id );
  if( iter != m_outConnectorMap.end() )
    return iter->second;
  
  return std::vector<TaskId>();
}

//-----------------------------------------------------------------------------

std::vector<TaskId> MultiGraphConnector::getIncomingConnections( const TaskId& task_id ) const
{
  auto iter = m_inConnectorMap.find( task_id );
  if( iter != m_inConnectorMap.end() )
    return iter->second;

  return std::vector<TaskId>();
}

//-----------------------------------------------------------------------------

void MultiGraphConnector::connectTasks( const TaskId& from, const TaskId& to )
{
  m_outConnectorMap[ from ].push_back( to );
  m_inConnectorMap[ to ].push_back( from );
}

//-----------------------------------------------------------------------------

Payload MultiGraphConnector::serialize() const
{
  uint32_t nelems_in_buffer = 1 + 2 * m_grPairsVec.size();
  uint32_t* buffer = new uint32_t[nelems_in_buffer];

  buffer[0] = m_grPairsVec.size();

  for( uint32_t i = 0; i < m_grPairsVec.size(); ++i ) 
  {
    buffer[2*i + 1] = m_grPairsVec[i].first;
    buffer[2*i + 2] = m_grPairsVec[i].second;
  }

  return Payload( nelems_in_buffer*sizeof(uint32_t), (char*)buffer );
}

//-----------------------------------------------------------------------------

void MultiGraphConnector::deserialize( Payload pl )
{
  uint32_t* buffer = (uint32_t*)(pl.buffer());

  m_grPairsVec.resize( buffer[0] );

  for( uint32_t i = 0; i < m_grPairsVec.size(); ++i ) 
  {
    m_grPairsVec[i] = std::make_pair( buffer[2*i + 1], buffer[2*i + 2] );
  }

  pl.reset();
}

//-----------------------------------------------------------------------------

void MultiGraphConnector::init( const std::vector<TaskGraph*>& gr_vec )
{
  for( GraphPair& p : m_grPairsVec )
  {
    const TaskGraph* src_gr = gr_vec[p.first];
    const TaskGraph* dst_gr = gr_vec[p.second];

    std::vector<TaskId> output_ts( src_gr->numOfRoots() );
    std::vector<TaskId> input_ts( dst_gr->numOfLeafs() );

    for( uint32_t i = 0; i < output_ts.size(); ++i )
      output_ts[i] = src_gr->root( i );

    for( uint32_t i = 0; i < input_ts.size(); ++i )
      input_ts[i] = dst_gr->leaf( i );

    // Future refactoring: put the code below in connection policy class
    // Default mapping is just a cyclic one-to-one 
    for( uint32_t i = 0; i < output_ts.size(); ++i )
    {
      TaskId& out_tid = output_ts[i];
      TaskId& in_tid = input_ts[ i % input_ts.size() ];

      // Set the graph ids, since the ids returned in root() and leaf() above are
      // from seperate graphs (not from a composed graph)
      out_tid.graphId() = p.first;
      in_tid.graphId() = p.second;

      connectTasks( out_tid, in_tid );
    }
  }  
}

//-----------------------------------------------------------------------------


}  // namespace BabelFlow
