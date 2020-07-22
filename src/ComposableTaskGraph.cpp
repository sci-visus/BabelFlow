/*
 * ComposableTaskGraph.cpp
 *
 *  Created on: Jul 7, 2020
 *      Author: sshudler
 */
 
#include <vector>
#include <algorithm>
#include <cassert>

#include "ComposableTaskGraph.h"


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
  uint32_t num_graphs = m_graphs.size();
  uint32_t num_connectors = m_connectors.size();
  assert( num_graphs = num_connectors + 1 );
  // Run localGraph for graph1 and graph2
  // When terminal (root) nodes in graph1 are found save their tids
  // Do the same for leaf nodes in graph2

  //for( uint32_t i = 0; i < m_graphs.size(); ++i )
  //{
  //  std::vector<Task> gr_tasks = m_graphs[i]->localGraph( shard_id, );
  //}
}

//-----------------------------------------------------------------------------

std::vector<Task> ComposableTaskGraph::localGraph(ShardId id, const TaskMap* task_map) const
{
  std::vector<TaskId> tids = task_map->tasks(id);
  std::vector<Task> tasks( tids.size() );
  for( uint32_t i = 0; i < tids.size(); ++i )
    tasks[i] = task( tids[i] );         // Do not use gId() since it will collapse TaskId into just a uint32_t, but we
                                        // need to know the graph id in the task() function
    //tasks[i] = task( gId( tids[i] ) );
  
  return tasks;
}

//-----------------------------------------------------------------------------

// gid problematic!! How to get graph id?
Task ComposableTaskGraph::task(const TaskId& task_id) const
{
  uint32_t graph_id = task_id.graphId();
  assert( graph_id < m_graphs.size() );
  TaskGraph* gr = m_graphs[graph_id];
  Task t = gr->task( gr->gId( task_id ) );
  TaskId orig_task_id = t.id();
  // The next line ensures that the id in the task itself contains the graph_id;
  // in the lines below same thing is repeated for all incoming and outgoing tasks
  t.id( TaskId( t.id().tid(), graph_id ) );
  
  // Convert task ids of incoming tasks
  std::for_each( t.incoming().begin(), t.incoming().end(), [&](TaskId& tid) { tid.graphId() = graph_id; } );
  
  // Convert task ids of outgoing tasks
  std::for_each( t.outputs().begin(), t.outputs().end(), 
  [&](std::vector<TaskId>& outg)
  { 
    std::for_each( outg.begin(), outg.end(), [&](TaskId& tid) { tid.graphId() = graph_id; } ); 
  }
  );
  
  // If task t has zero outputs (root task), or a TNULL output, connect it to a leaf task in graph_id + 1,
  // use m_connector[graph_id] for that:
  if( graph_id < m_connectors.size() )
  {
    std::vector<TaskId> connected_tsk = m_connectors[graph_id]->getOutgoingConnectedTasks( orig_task_id );
    if( connected_tsk.size() > 0 )
    {
      // Each output data has to be sent to connected tasks in the next graph
      std::for_each( connected_tsk.begin(), connected_tsk.end(), [&](TaskId& tid) { tid.graphId() = graph_id + 1; } );
      std::for_each( t.outputs().begin(), t.outputs().end(), 
      [&](std::vector<TaskId>& outg)
      { 
        outg.insert( outg.end(), connected_tsk.begin(), connected_tsk.end() );
      }
      );
    }
  }

  // If task t has TNULL input (leaf task in graph_id) connect it to the root task in graph_id - 1.
  // Use m_connector[graph_id - 1] for that.
  if( graph_id > 0 )
  {
    std::vector<TaskId> connected_tsk = m_connectors[graph_id - 1]->getIncomingConnectedTasks( orig_task_id );
    if( connected_tsk.size() > 0 )
    {
      std::for_each( connected_tsk.begin(), connected_tsk.end(), [&](TaskId& tid) { tid.graphId() = graph_id - 1; } );
      t.incoming().insert( t.incoming().end(), connected_tsk.begin(), connected_tsk.end() );
    }
  }
  
  
  return t;
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
  uint32_t payl_sz = 0;
  std::vector<Payload> payloads( m_graphs.size() );
  
  // Serialize each graph separately and count the size of the payloads
  for( uint32_t i = 0; i < m_graphs.size(); ++i )
  {
    Payload pl = m_graphs[i]->serialize();
    payloads[i] = pl;
    payl_sz += pl.size();
  }
  
  uint32_t total_sz = (1 + m_graphs.size())*sizeof(uint32_t) + payl_sz;
  
  char* buff = new char[total_sz];
  char* bf_ptr = buff;
  char* bf_pl_ptr = buff + (total_sz  - payl_sz);
  
  // Write the number of graphs
  uint32_t num_graphs = m_graphs.size();
  memcpy( bf_ptr, &num_graphs, sizeof(uint32_t) );
  bf_ptr += sizeof(uint32_t);
  
  // Write the payload sizes and the payloads themselves
  for( uint32_t i = 0; i < payloads.size(); ++i )
  {
    Payload& pl = payloads[i];
    uint32_t pl_size = pl.size();

    memcpy( bf_ptr, &pl_size, sizeof(uint32_t) );
    bf_ptr += sizeof(uint32_t);
    
    memcpy( bf_pl_ptr, pl.buffer(), pl_size );
    bf_pl_ptr += pl_size;
    
    delete[] pl.buffer();
  }
  
  return Payload( int32_t(total_sz), buff );
}

//-----------------------------------------------------------------------------

void ComposableTaskGraph::deserialize(Payload pl)
{
  // Deserialization order:
  // - Num graphs
  // - Payload sizes
  // - Payload for each graph
  uint32_t* buff_ptr = (uint32_t*)(pl.buffer());
  
  // Sanity check -- the number of graphs in this object should be the same as the number
  // of graphs about to be deserialized
  assert( buff_ptr[0] == m_graphs.size() );
  
  char* bf_ptr = pl.buffer() + (1 + m_graphs.size())*sizeof(uint32_t);  // ptr into the payload part of the buffer
  
  for( uint32_t i = 0; i < m_graphs.size(); ++i )
  {
    Payload pl( int32_t(buff_ptr[i+1]), bf_ptr );
    bf_ptr += buff_ptr[i+1];
    m_graphs[i]->deserialize( pl );
  }
  
  init();
  
  delete[] pl.buffer();
}

//-----------------------------------------------------------------------------


}   // namespace BabelFlow
