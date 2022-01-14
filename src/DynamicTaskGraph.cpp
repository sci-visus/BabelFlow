/*
 * ComposableTaskGraph.cpp
 *
 *  Created on: Dec 14, 2021
 *      Author: sshudler
 */
 
#include <vector>
#include <algorithm>
#include <cassert>
#include <sstream>

#include "DynamicTaskGraph.h"
#include "ComposableTaskGraph.h"


// #define DYNAMIC_TGRAPH_DEBUG


namespace BabelFlow
{

REG_TGRAPH_TYPE(DynamicTaskGraph)

//-----------------------------------------------------------------------------

static inline void serialize_uint( char*& buff, uint32_t val )
{
  memcpy( buff, &val, sizeof(uint32_t) );
  buff += sizeof(uint32_t);
}

//-----------------------------------------------------------------------------

static inline void serialize_payl( char*& buff, const Payload& pl )
{
  std::size_t pl_sz = pl.size();
  memcpy( buff, pl.buffer(), pl_sz );
  buff += pl_sz;
}

//-----------------------------------------------------------------------------

DynamicTaskGraph::DynamicTaskGraph( std::vector<TaskGraph*>& gr_vec )
 : m_subGraphs( gr_vec ), m_currGraphIdx( 0 )
{
  std::vector<BabelFlow::TaskGraph*> grps{ m_subGraphs[m_currGraphIdx] };

  m_compGraph = ComposableTaskGraph( grps );
}

//-----------------------------------------------------------------------------

std::vector<Task> DynamicTaskGraph::localGraph( ShardId id, const TaskMap* task_map ) const
{
  return m_compGraph.localGraph( id, task_map );
}

//-----------------------------------------------------------------------------

Task DynamicTaskGraph::task( uint64_t gId ) const 
{ 
  return m_compGraph.task( gId );
}

//-----------------------------------------------------------------------------

uint64_t DynamicTaskGraph::gId( TaskId tId ) const
{
  return m_compGraph.gId( tId );
}

//-----------------------------------------------------------------------------

uint32_t DynamicTaskGraph::size() const
{
  return m_compGraph.size();
}

//-----------------------------------------------------------------------------

uint32_t DynamicTaskGraph::numOfLeafs() const
{
  return m_compGraph.numOfLeafs();
}

//-----------------------------------------------------------------------------

uint32_t DynamicTaskGraph::numOfRoots() const
{
  return m_compGraph.numOfRoots();
}

//-----------------------------------------------------------------------------

TaskId DynamicTaskGraph::leaf( uint32_t idx ) const
{
  return m_compGraph.leaf( idx );
}

//-----------------------------------------------------------------------------

TaskId DynamicTaskGraph::root( uint32_t idx ) const
{
  return m_compGraph.root( idx );
}

//-----------------------------------------------------------------------------

Payload DynamicTaskGraph::serialize() const
{
  Payload gr_vec_pl = ComposableTaskGraph::serializeGraphVec( m_subGraphs );
  Payload comp_gr_pl = m_compGraph.serialize();

  uint32_t total_sz = (3*sizeof(uint32_t)) + gr_vec_pl.size() + comp_gr_pl.size();

  char* buff = new char[total_sz];
  char* bf_ptr = buff;

  serialize_uint( bf_ptr, m_currGraphIdx );

  serialize_uint( bf_ptr, gr_vec_pl.size() );
  serialize_payl( bf_ptr, gr_vec_pl );
  
  serialize_uint( bf_ptr, comp_gr_pl.size() );
  serialize_payl( bf_ptr, comp_gr_pl );

  gr_vec_pl.reset();
  comp_gr_pl.reset();

  return Payload( int32_t(total_sz), buff );
}

//-----------------------------------------------------------------------------

void DynamicTaskGraph::deserialize( Payload pl, bool clean_mem )
{
  char* buff_ptr = pl.buffer();
  m_currGraphIdx = *(uint32_t*)( buff_ptr );
  buff_ptr += sizeof(uint32_t);
  uint32_t gr_vec_sz = *(uint32_t*)( buff_ptr );
  buff_ptr += sizeof(uint32_t);
  Payload gr_vec_pl( int32_t(gr_vec_sz), buff_ptr );
  buff_ptr += gr_vec_sz;
  
  ComposableTaskGraph::deserializeGraphVec( m_subGraphs, gr_vec_pl, false );

  uint32_t comp_pl_sz = *(uint32_t*)( buff_ptr );
  buff_ptr += sizeof(uint32_t);
  Payload comp_payl( int32_t(comp_pl_sz), buff_ptr );

  m_compGraph.deserialize( comp_payl, false );
  
  pl.reset( clean_mem );
}

//-----------------------------------------------------------------------------

bool DynamicTaskGraph::extend(const TaskGraph::OutputsMap& outputs)
{
  std::vector<BabelFlow::TaskGraph*> graphs = m_compGraph.getGraphs();

  // First byte of the payload is an extensionn flag
  char extension_flag = *(outputs.begin()->second[0].buffer());   // 0 -- no extensions
                                                                  // 1 -- repeat current graph
                                                                  // 2 -- move to the next graph
  if (extension_flag == 0)
    return false;
  
  if (extension_flag == 2)
    m_currGraphIdx++;

  if (m_currGraphIdx >= m_subGraphs.size() )
    return false;

  graphs.push_back( m_subGraphs[m_currGraphIdx] );

  m_compGraph = ComposableTaskGraph( graphs );
  
  return true;
}

//-----------------------------------------------------------------------------

}   // namespace BabelFlow
