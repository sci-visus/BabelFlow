//
// Created by Jixian Li on 2019-07-27.
//

#include "DoNothingTaskGraph.h"

using namespace BabelFlow;
using namespace std;


DoNothingTaskGraph::DoNothingTaskGraph(BabelFlow::ShardId count) : n_controllers(count) {

}

std::vector<BabelFlow::Task>
DoNothingTaskGraph::localGraph(BabelFlow::ShardId id, const BabelFlow::TaskMap *task_map) const {
  auto local_tids = task_map->tasks(id);
  vector<Task> local_tasks;
  for (auto &&tid:local_tids) {
    auto t = task(gId(tid));
    local_tasks.push_back(t);
  }
  return local_tasks;
}

BabelFlow::Task DoNothingTaskGraph::task(uint64_t gId) const {
  Task t(gId);
  vector<TaskId> incoming;
  vector<vector<TaskId> > outgoing;
  if (gId < n_controllers) {
    incoming.resize(1);
    incoming[0] = TNULL;
    outgoing.resize(1);
    outgoing[0].resize(1);
    outgoing[0][0] = gId + n_controllers;
    t.incoming() = incoming;
    t.outputs() = outgoing;
  } else {
    incoming.resize(1);
    incoming[0] = gId - n_controllers;
    t.incoming() = incoming;
    t.callback(1);
  }
  return t;
}

BabelFlow::Payload DoNothingTaskGraph::serialize() const {
  char *buffer = new char(sizeof(ShardId));
  memcpy(buffer, &n_controllers, sizeof(ShardId));
  return Payload(sizeof(ShardId), buffer);
}

void DoNothingTaskGraph::deserialize(BabelFlow::Payload buffer) {
  memcpy(&n_controllers, buffer.buffer(), sizeof(ShardId));
  delete[] buffer.buffer();
}


