//
// Created by Li, Jixian on 2019-05-28.
//

#include <iostream>
#include "MapReduceGraph.h"

using namespace BabelFlow;

std::vector<BabelFlow::Task> MapReduceGraph::localGraph(BabelFlow::ShardId id, const BabelFlow::TaskMap *task_map) const
{
  std::vector<TaskId> ids = task_map->tasks(id);
  std::vector<Task> tasks(ids.size());
  for (int i = 0; i < ids.size(); ++i)
    tasks[i] = task(ids[i]);
  return tasks;
}

BabelFlow::Task MapReduceGraph::task(uint64_t gId) const
{
  Task task(gId);
  std::vector<TaskId> incoming;
  std::vector<std::vector<TaskId>> outgoing;

  // distributor
  if (gId == 0) {
    outgoing.resize(n_worker);
    for (int i = 0; i < n_worker; ++i) {
      outgoing[i].push_back(i + 1);
    }
    incoming.resize(1, TNULL);
    task.incoming() = incoming;
    task.outputs() = outgoing;
    task.callback( MapReduceGraph::SPLIT_LOAD, TaskGraph::queryCallback( type(), MapReduceGraph::SPLIT_LOAD ) );
  }
    // collector
  else if (gId == size() - 2) {
    incoming.resize(n_worker);
    for (int i = 0; i < n_worker; ++i)
      incoming[i] = i + 1;
    task.incoming() = incoming;
    outgoing.resize(1);
    outgoing[0].push_back(size() - 1);
    task.outputs() = outgoing;
    task.callback( MapReduceGraph::RED_FUNC, TaskGraph::queryCallback( type(), MapReduceGraph::RED_FUNC ) );
  }
    // printer
  else if (gId == size() - 1) {
    incoming.resize(1);
    incoming[0] = size() - 2;
    task.incoming(incoming);
    task.callback( MapReduceGraph::PRINT_FUNC, TaskGraph::queryCallback( type(), MapReduceGraph::PRINT_FUNC ) );
  }
    // worker
  else {
    incoming.resize(1);
    incoming[0] = 0;
    outgoing.resize(1);
    outgoing[0].push_back(size() - 2);
    task.incoming() = incoming;
    task.outputs() = outgoing;
    task.callback( MapReduceGraph::MAP_FUNC, TaskGraph::queryCallback( type(), MapReduceGraph::MAP_FUNC ) );
  }
  return task;
}

BabelFlow::Payload MapReduceGraph::serialize() const
{
  int *buffer = new int[1];
  buffer[0] = n_worker;
  return Payload(sizeof(int), reinterpret_cast<char *>(buffer));
}

void MapReduceGraph::deserialize(BabelFlow::Payload buffer)
{
  assert(buffer.size() == sizeof(int));
  int *tmp = reinterpret_cast<int *>(buffer.buffer());
  n_worker = tmp[0];
  delete[] buffer.buffer();
}
