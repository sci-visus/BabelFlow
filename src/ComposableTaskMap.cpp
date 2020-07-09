#include <vector>
#include <algorithm>

#include "ComposableTaskMap.h"
#include "ComposableTaskGraph.h"


namespace BabelFlow
{


//-----------------------------------------------------------------------------

ComposableTaskMap::ComposableTaskMap( TaskMap* map1, TaskMap* map2 )
 : m_TMap1( map1 ), m_TMap2( map2 )
{
}

//-----------------------------------------------------------------------------

ShardId ComposableTaskMap::shard(TaskId id) const
{
  uint32_t graph_id = id.graphId();
  
  assert( graph_id == 1 || graph_id == 2 );
  
  TaskMap* tm = (graph_id == 1) ? m_TMap1 : m_TMap2;
  return tm->shard( id );
}

//-----------------------------------------------------------------------------

std::vector<TaskId> ComposableTaskMap::tasks(ShardId id) const
{
  std::vector<TaskId> tids_gr_1 = m_TMap1->tasks( id );
  std::vector<TaskId> tids_gr_2 = m_TMap2->tasks( id );
  
  // Transform the task ids into pairs of [graphId, TaskId]
  std::for_each( tids_gr_1.begin(), tids_gr_1.end(), [](TaskId& tid) { tid.graphId() = 1; } );
  std::for_each( tids_gr_2.begin(), tids_gr_2.end(), [](TaskId& tid) { tid.graphId() = 2; } );
  
  // Append all the ids into one array
  tids_gr_1.insert( tids_gr_1.end(), tids_gr_2.begin(), tids_gr_2.end() );
  
  return tids_gr_1;
}

//-----------------------------------------------------------------------------


}   // namespace BabelFlow
