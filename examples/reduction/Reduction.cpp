/*
 * Reduction.cpp
 *
 *  Created on: Dec 15, 2014
 *      Author: bremer5
 */

#include <cmath>
#include <sstream>

#include "Reduction.h"

using namespace DataFlow;

Reduction::Reduction(uint32_t leafs, uint32_t valence) : TaskGraph(),
    mValence(valence)
{
  // Find the number of leafs that is a power of valence
  mLeafs = 1;
  mLevels = 0;

  while (mLeafs < leafs) {
    mLeafs *= mValence;
    mLevels++;
  }
}

Reduction::Reduction(std::string config)
{
  std::stringstream cmd(config);

  uint32_t leafs;

  cmd >> leafs;
  cmd >> mValence;

  // Find the number of leafs that is a power of valence
  mLeafs = 1;
  mLevels = 0;

  while (mLeafs < leafs) {
    mLeafs *= mValence;
    mLevels++;
  }
}



std::vector<Task> Reduction::localGraph(ShardId id, const TaskMap* task_map) const
{
  TaskId i;

  //fprintf(stderr, "LocalGraph %d \n",id);

  // First get all the ids we need
  std::vector<TaskId> ids = task_map->tasks(id);

  // The create the required number of tasks
  std::vector<Task> tasks(ids.size());
  std::vector<Task>::iterator it;

  //! Now assign all the task ids
  for (i=0;i<ids.size();i++)
    tasks[i].id(ids[i]);

  std::vector<TaskId> incoming(mValence); // There will be at most valence many incoming
  std::vector<std::vector<TaskId> > outgoing(1); // and one output
  outgoing[0].resize(1); // With one target

  //! Now assign the callback functions as well as the incoming and outgoing
  for (it=tasks.begin();it!=tasks.end();it++) {
    // First we assign the inputs

    // If we are a leaf
    if (it->id() >= (size() - leafCount())) {
      incoming.resize(1);
      incoming[0] = TNULL;

      it->incoming() = incoming;
    }
    else { // If we are not a leaf

      // We have valence many inputs
      incoming.resize(mValence);
      for (i=0;i<mValence;i++)
        incoming[i] = it->id()*mValence + i + 1;

      it->incoming() = incoming;
    }

    // Then we assign the outputs
    if (it->id() != 0) {// If we are not the root
      it->callback(1);

      outgoing[0][0] = (it->id() - 1) / mValence;
      it->outputs() = outgoing;
    }
    else {  //If we are the root
      it->callback(2);
    }
  }// end-for all tasks

  return tasks;
}

DataFlow::Task Reduction::task(uint64_t gId) const
{
  DataFlow::Task task(gId);
  std::vector<DataFlow::TaskId> incoming; // There will be at most valence many incoming
  std::vector<std::vector<DataFlow::TaskId> > outgoing(1); // and one output
  uint32_t i;

  if (gId < size() - leafCount()) {
    incoming.resize(mValence);
    for (i=0;i<mValence;i++)
      incoming[i] = task.id()*mValence + i + 1;

    task.incoming(incoming);
  }
  else { // Otherwise we expect one external input
    incoming.resize(1,TNULL);
  }

  // Then we assign the outputs
  if (task.id() != 0) {// If we are not the root
    task.callback(1); // We do a reduction
    outgoing.resize(1);
    outgoing[0].resize(1);
    outgoing[0][0] = (task.id() - 1) / mValence;
  }
  else {
    task.callback(2); // Otherwise we report the result
    outgoing.clear();
  }

  task.incoming(incoming);
  task.outputs(outgoing);

  return task;
}

DataFlow::Payload Reduction::serialize() const
{
  uint32_t* buffer = new uint32_t[3];

  buffer[0] = mLeafs;
  buffer[1] = mValence;
  buffer[2] = mLevels;

  return Payload(3*sizeof(uint32_t),(char*)buffer);
}

void Reduction::deserialize(DataFlow::Payload buffer)
{
  assert (buffer.size() == 3*sizeof(uint32_t));
  uint32_t *tmp = (uint32_t *)(buffer.buffer());

  mLeafs = tmp[0];
  mValence = tmp[1];
  mLevels = tmp[2];

  delete[] buffer.buffer();
}
