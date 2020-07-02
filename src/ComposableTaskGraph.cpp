
#include <vector>
#include <algorithm>

#include "ComposableTaskGraph.h"


namespace BabelFlow
{


//-----------------------------------------------------------------------------

ComposableTaskGraph::ComposableTaskGraph( TaskGraph* graph1, TaskGraph* graph2 )
 : m_graph1( graph1 ), m_graph2( graph2 )
{
  init();
}

//-----------------------------------------------------------------------------

void ComposableTaskGraph::init()
{
  // Run localGraph for graph1 and graph2
  // When terminal (root) nodes in graph1 are found save their tids
  // Do the same for leaf nodes in graph2
}

//-----------------------------------------------------------------------------

std::vector<Task> ComposableTaskGraph::localGraph(ShardId id, const TaskMap* task_map) const
{
  std::vector<TaskId> tids = task_map->tasks(id);
  std::vector<Task> tasks( tids.size() );
  for( uint32_t i = 0; i < tids.size(); ++i )
    tasks[i] = task( tids[i] );
    //tasks[i] = task( gId( tids[i] ) );
  
  return tasks;
}

//-----------------------------------------------------------------------------

// gid problematic!! How to get graph id?
Task ComposableTaskGraph::task(const TaskId& task_id) const
{
  uint32_t graph_id = task_id.graphId();
  TaskGraph* gr = (graph_id == 1) ? m_graph1 : m_graph2;
  Task t = gr->task( gr->gId( task_id ) );
  // The next line ensures that the id in the task itself is set to a [graph_id, orig_tid] pair;
  // in the lines below same thing is repeated for all incoming and outgoing tasks
  t.id( TaskId( t.id().tid(), graph_id ) );
  
  // Convert task ids of incoming tasks
  std::for_each( t.incoming().begin(), t.incoming().end(), [=](TaskId& tid) { tid.graphId() = graph_id; } );
  
  // Convert task ids of outgoing tasks
  std::for_each( t.outputs().begin(), t.outputs().end(), [=](std::vector<TaskId>& outg)
  { std::for_each( outg.begin(), outg.end(), [=](TaskId& tid) { tid.graphId() = graph_id; } ); }
  );
  
  // If a task in graph1 has zero outputs, or a TNULL output connect it to leaf task in graph2
  // For now assume the number of roots in graph1 and leafs in graph2 are equal -- if not get a mapping
  // policy in the constructor
  
  return t;
}

//-----------------------------------------------------------------------------

uint32_t ComposableTaskGraph::size() const
{
  // Should glue tasks be included? Do we need them?
  return m_graph1->size() + m_graph2->size();
}

//-----------------------------------------------------------------------------

Payload ComposableTaskGraph::serialize() const
{
  Payload payl_gr1 = m_graph1->serialize();
  Payload payl_gr2 = m_graph2->serialize();
  int32_t sz1 = payl_gr1.size();
  int32_t sz2 = payl_gr2.size();
  int32_t total_sz = sz1 + sz2 + 2*sizeof(int32_t);
  
  char* buff = new char[total_sz];
  
  memcpy( buff, &sz1, sizeof(int32_t) );
  memcpy( buff + sizeof(int32_t), &sz2, sizeof(int32_t) );
  memcpy( buff + 2*sizeof(int32_t), payl_gr1.buffer(), sz1 );
  memcpy( buff + 2*sizeof(int32_t) + sz1, payl_gr2.buffer(), sz2 );
  
  delete[] payl_gr1.buffer();
  delete[] payl_gr2.buffer();
  
  return Payload( total_sz, buff );
}

//-----------------------------------------------------------------------------

void ComposableTaskGraph::deserialize(Payload buffer)
{
  int32_t* buff_ptr = (int32_t*)(buffer.buffer());
  int32_t sz1 = buff_ptr[0];
  int32_t sz2 = buff_ptr[1];
  
  Payload payl_gr1( sz1, buffer.buffer() + 2*sizeof(int32_t) );
  Payload payl_gr2( sz2, buffer.buffer() + 2*sizeof(int32_t) + sz1 );
  
  m_graph1->deserialize( payl_gr1 );
  m_graph2->deserialize( payl_gr2 );
  
  init();
  
  delete[] buffer.buffer();
}

//-----------------------------------------------------------------------------


}   // namespace BabelFlow
