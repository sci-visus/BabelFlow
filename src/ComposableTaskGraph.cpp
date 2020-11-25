/*
 * ComposableTaskGraph.cpp
 *
 *  Created on: Jul 7, 2020
 *      Author: sshudler
 */
 
#include <vector>
#include <algorithm>
#include <cassert>
#include <sstream>

#include "ComposableTaskGraph.h"
#include "DefGraphConnector.h"
#include "reduce/RadixKExchange.h"
#include "reduce/KWayReduction.h"


// #define COMPOSABLE_TGRAPH_DEBUG


namespace BabelFlow
{

//-----------------------------------------------------------------------------

ComposableTaskGraph::ComposableTaskGraph( std::vector<TaskGraph*>& gr_vec, std::vector<TaskGraphConnector*>& gr_connectors  )
 : m_graphs( gr_vec ), m_connectors( gr_connectors )
{
  init();
}

//-----------------------------------------------------------------------------

void ComposableTaskGraph::init()
{
  // Place holder, at the moment performs just a sanity check

  uint32_t num_graphs = m_graphs.size();
  uint32_t num_connectors = m_connectors.size();
  assert( num_graphs = num_connectors + 1 );
}

//-----------------------------------------------------------------------------

std::vector<Task> ComposableTaskGraph::allGraph() const
{
  std::vector<Task> all_tasks( size() );
  for( uint32_t i = 0; i < all_tasks.size(); ++i )
  {
    all_tasks[i] = task( (uint64_t)i );
  }

  return all_tasks;
}

//-----------------------------------------------------------------------------

std::vector<Task> ComposableTaskGraph::localGraph( ShardId id, const TaskMap* task_map ) const
{
  std::vector<TaskId> tids = task_map->tasks(id);
  std::vector<Task> tasks( tids.size() );
  for( uint32_t i = 0; i < tids.size(); ++i )
    tasks[i] = task( tids[i] );         // Do not use gId() since it will collapse TaskId into just a uint32_t, but we
                                        // need to know the graph id in the task() function
  
#ifdef COMPOSABLE_TGRAPH_DEBUG
  {
    std::stringstream ss;
    ss << "comp_gr_tasks_" << id << ".html";
    outputTasksHtml( tasks, ss.str() );
  }
#endif

  return tasks;
}

//-----------------------------------------------------------------------------

Task ComposableTaskGraph::task( uint64_t gId ) const 
{ 
  assert( gId < size() );

  uint64_t cnt = 0;
  for( uint32_t i = 0; i < m_graphs.size(); ++i ) 
  {
    const TaskGraph* gr = m_graphs[i];
    if( cnt <= gId && gId < cnt + gr->size() )
      return task( TaskId( gId - cnt, i ) );
    cnt += gr->size();
  }

  // We shouldn't reach this point
}

//-----------------------------------------------------------------------------

uint64_t ComposableTaskGraph::gId( TaskId tId ) const
{
  uint64_t cnt = 0;
  for( uint32_t i = 0; i < tId.graphId(); ++i ) 
  {
    cnt += m_graphs[i]->size();
  }

  return cnt + tId.tid();
}

//-----------------------------------------------------------------------------

Task ComposableTaskGraph::task( const TaskId& task_id ) const
{
  uint32_t graph_id = task_id.graphId();
  assert( graph_id < m_graphs.size() );
  TaskGraph* gr = m_graphs[graph_id];
  TaskId orig_task_id( task_id.tid() );
  Task tsk = gr->task( gr->gId( orig_task_id ) );

  // The next line ensures that the id in the task itself contains the graph_id;
  // in the lines below same thing is repeated for all incoming and outgoing tasks
  tsk.id( TaskId( tsk.id().tid(), graph_id ) );
  
  // Convert task ids of incoming tasks
  for( TaskId& tid : tsk.incoming() )
  {
    if( tid != TNULL )
      tid.graphId() = graph_id;
  }

  // Convert task ids of outgoing tasks
  for( uint32_t i = 0; i < tsk.fanout(); ++i )
  {  
    for( TaskId& tid : tsk.outgoing(i) )
    {
      if( tid != TNULL )
        tid.graphId() = graph_id;
    }
  }

  // If task t has TNULL output connect it to a leaf task in graph_id + 1,
  // use m_connector[graph_id] for that:
  if( graph_id < m_connectors.size() )
  {
    std::vector<TaskId> connected_tsk = m_connectors[graph_id]->getOutgoingConnectedTasks( tsk.id() );
    if( connected_tsk.size() > 0 )
    {
#ifdef COMPOSABLE_TGRAPH_DEBUG
      {
        std::cout << "ComposableTaskGraph - graph_id = " << graph_id 
                  << ", found outgoing connection: " << tsk.id() << " --> ";
        for( TaskId& tid : connected_tsk ) std::cout << tid << "  ";
        std::cout << std::endl;
      }
#endif

      // Each output data has to be sent to connected tasks in the next graph
      for( TaskId& tid : connected_tsk ) 
        tid.graphId() = graph_id + 1;

      // Right now the assumption is that if a task is a root task in graph_id, then it only has TNULL ouputs,
      // which means we should replace all these outputs with connected_tsk
      for( uint32_t i = 0; i < tsk.fanout(); ++i )
      {
        auto& out_dest_v = tsk.outgoing( i );
        uint32_t num_connected_tasks = std::min( out_dest_v.size(), connected_tsk.size() );
        for( uint32_t j = 0; j < num_connected_tasks; ++j )
          out_dest_v[j] = connected_tsk[j];
      }
    }
  }

  // If task tsk has TNULL input (leaf task in graph_id) connect it to the root task in graph_id - 1.
  // Use m_connector[graph_id - 1] for that.
  if( graph_id > 0 )
  {
    std::vector<TaskId> connected_tsk = m_connectors[graph_id - 1]->getIncomingConnectedTasks( tsk.id() );
    if( connected_tsk.size() > 0 )
    {
#ifdef COMPOSABLE_TGRAPH_DEBUG
      {
        std::cout << "ComposableTaskGraph - graph_id = " << graph_id 
                  << ", found incoming connection: " << tsk.id() << " <-- ";
        for( TaskId& tid : connected_tsk ) std::cout << tid << "  ";
        std::cout << std::endl;
      }
#endif

      for( TaskId& tid : connected_tsk ) 
        tid.graphId() = graph_id - 1;

      // Right now the assumption is that if a task is a leaf task in graph_id, then it only has TNULL inputs,
      // which means we should replace all these inputs with connected_tsk
      auto& in_srcs_v = tsk.incoming();
      uint32_t num_connected_tasks = std::min( in_srcs_v.size(), connected_tsk.size() );
      uint32_t i = 0;
      for( uint32_t j = in_srcs_v.size() - num_connected_tasks; j < in_srcs_v.size(); ++j, ++i )
        in_srcs_v[j] = connected_tsk[i];
    }
  }
  
  
  return tsk;
}

//-----------------------------------------------------------------------------

uint32_t ComposableTaskGraph::size() const
{
  uint32_t total_sz = 0;
  std::for_each( m_graphs.begin(), m_graphs.end(), [&total_sz](TaskGraph* gr) { total_sz += gr->size(); } );
  return total_sz;
}

//-----------------------------------------------------------------------------

Payload ComposableTaskGraph::serialize() const
{
  // Serialization order:
  // - Num graphs
  // - Payload size (x num graphs times)
  // - Buffer for each graph
  // - Graph connector types (num connectors = num graphs - 1)
  uint32_t payl_sz = 0;
  std::vector<Payload> payloads( m_graphs.size() );
  
  // Serialize each graph separately and count the size of the payloads
  for( uint32_t i = 0; i < m_graphs.size(); ++i )
  {
    Payload pl = m_graphs[i]->serialize();
    payloads[i] = pl;
    payl_sz += pl.size();
  }
  
  uint32_t total_sz = (m_connectors.size() + 1 + 2 * m_graphs.size())*sizeof(uint32_t) + payl_sz;
  
  char* buff = new char[total_sz];
  char* bf_ptr = buff;
  char* bf_pl_ptr = buff + (1 + 2 * m_graphs.size())*sizeof(uint32_t);
  
  // Write the number of graphs
  uint32_t num_graphs = m_graphs.size();
  memcpy( bf_ptr, &num_graphs, sizeof(uint32_t) );
  bf_ptr += sizeof(uint32_t);
  
  // Write the payload sizes and the payloads themselves
  for( uint32_t i = 0; i < payloads.size(); ++i )
  {
    Payload& pl = payloads[i];
    uint32_t pl_size = pl.size();
    uint32_t gr_type = m_graphs[i]->type();

    memcpy( bf_ptr, &pl_size, sizeof(uint32_t) );
    bf_ptr += sizeof(uint32_t);

    memcpy( bf_ptr, &gr_type, sizeof(uint32_t) );
    bf_ptr += sizeof(uint32_t);
    
    memcpy( bf_pl_ptr, pl.buffer(), pl_size );
    bf_pl_ptr += pl_size;
    
    pl.reset();
  }
  
  for( uint32_t i = 0; i < m_connectors.size(); ++i )
  {
    uint32_t connector_type = m_connectors[i]->type();
    memcpy( bf_pl_ptr, &connector_type, sizeof(uint32_t) );
    bf_pl_ptr += sizeof(uint32_t);
  }

  return Payload( int32_t(total_sz), buff );
}

//-----------------------------------------------------------------------------

void ComposableTaskGraph::deserialize( Payload pl )
{
  // Deserialization order:
  // - Num graphs
  // - Payload sizes
  // - Payload for each graph
  uint32_t* buff_ptr = (uint32_t*)(pl.buffer());
  
  m_graphs.resize( buff_ptr[0] );
  
  // Ptr into the payload part of the buffer
  char* bf_ptr = pl.buffer() + (1 + 2 * m_graphs.size())*sizeof(uint32_t);
  
  for( uint32_t i = 0; i < m_graphs.size(); ++i )
  {
    uint32_t pl_size = buff_ptr[2*i+1];
    uint32_t gr_type = buff_ptr[2*i+2];

    char* tmp_buff = new char[pl_size];
    memcpy( tmp_buff, bf_ptr, pl_size );
    Payload pay_ld( int32_t(pl_size), tmp_buff );
    bf_ptr += pl_size;

    switch( gr_type )
    {
      case 1:
        m_graphs[i] = new RadixKExchange();
        break;
      case 2:
        m_graphs[i] = new KWayReduction();
        break;
      default:
        assert(false);
    }

    m_graphs[i]->deserialize( pay_ld );
    m_graphs[i]->setGraphId( i );
  }
  
  // bf_ptr now points to connector type part
  m_connectors.resize( m_graphs.size() - 1 );
  uint32_t* conn_types_ptr = (uint32_t*)bf_ptr;
  for( uint32_t i = 0; i < m_connectors.size(); ++i )
  {
    if( conn_types_ptr[i] == 0 )  // Def connector
    {
      m_connectors[i] = new DefGraphConnector( m_graphs[i], i, m_graphs[i+1], i+1 );
    }
  }

  init();
  
  pl.reset();
}

//-----------------------------------------------------------------------------


}   // namespace BabelFlow
