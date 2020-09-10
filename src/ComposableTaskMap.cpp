/*
 * ComposableTaskMap.cpp
 *
 *  Created on: Jul 7, 2020
 *      Author: sshudler
 */

#include <vector>
#include <algorithm>

#include "ComposableTaskMap.h"
#include "ComposableTaskGraph.h"


namespace BabelFlow
{


//-----------------------------------------------------------------------------

ComposableTaskMap::ComposableTaskMap( const std::vector<TaskMap*>& task_maps )
 : m_taskMaps( task_maps )
{
}

//-----------------------------------------------------------------------------

ShardId ComposableTaskMap::shard(TaskId id) const
{
  uint32_t graph_id = id.graphId();
  
  assert( graph_id < m_taskMaps.size() );
  
  return m_taskMaps[graph_id]->shard( id );
}

//-----------------------------------------------------------------------------

std::vector<TaskId> ComposableTaskMap::tasks(ShardId id) const
{
  std::vector<TaskId> rv;
  
  for( uint32_t i = 0; i < m_taskMaps.size(); ++i )
  {
    std::vector<TaskId> tasks = m_taskMaps[i]->tasks( id );
    // Add the graph id to each TaskId
    std::for_each( tasks.begin(), tasks.end(), [&](TaskId& tid) { tid.graphId() = i; } );
    rv.insert( rv.end(), tasks.begin(), tasks.end() );
  }
  
  return rv;
}

//-----------------------------------------------------------------------------


}   // namespace BabelFlow
