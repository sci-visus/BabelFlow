/*
 * Copyright (c) 2017 University of Utah 
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <cmath>
#include "Broadcast.h"
#include <iostream>
#include <sstream>

using namespace DataFlow;

Broadcast::Broadcast(uint32_t endpoints, uint32_t valence) : TaskGraph(),
    mValence(valence)
{
  // Find the number of endpoints that is a power of valence
  mEndpoints = 1;
  mLevels = 0;

  while (mEndpoints < endpoints) {
    mEndpoints *= mValence;
    mLevels++;
  }
}

Broadcast::Broadcast(std::string config)
{
  std::stringstream cmd(config);

  uint32_t endpoints;

  cmd >> endpoints;
  cmd >> mValence;

  // Find the number of endpoints that is a power of valence
  mEndpoints = 1;
  mLevels = 0;

  while (endpoints < endpoints) {
    mEndpoints *= mValence;
    mLevels++;
  }
}

DataFlow::Task Broadcast::task(uint64_t gId) const
{
  DataFlow::Task task(gId);

  std::vector<TaskId> incoming(1); // There will always be 1 incoming
  std::vector<std::vector<TaskId> > outgoing(1); // and one output
  outgoing[0].resize(mValence); // That goes to valence many other tasks
  uint32_t i;

  // If this is a leaf
  if (task.id() >= (size() - pow(mValence,mLevels)))
    task.callback(1);
  else { // If we are not the leaf
    task.callback(0); // We are a relay task

    // And we have valence many outputs
    for (i=0;i<mValence;i++)
      outgoing[0][i] = task.id()*mValence + i + 1;

    task.outputs() = outgoing;
  }

  // If we are not the root we have one incoming
  if (task.id() > 0) {
    incoming[0] = (task.id()-1) / mValence;
    task.incoming() = incoming;
  }
  else { // If we are the root we have one outside input
    incoming[0] = TNULL;
    task.incoming() = incoming;
  }

  return task;

}

std::vector<Task> Broadcast::localGraph(ShardId id, const TaskMap* task_map) const
{
  // First get all the ids we need
  std::vector<TaskId> ids = task_map->tasks(id);

  // Create the required number of tasks
  std::vector<DataFlow::Task> tasks(ids.size());
  for (int i=0; i< ids.size(); i++) {
    tasks[i] = task(ids[i]);
  }

  return tasks;
}

DataFlow::Payload Broadcast::serialize() const
{
  uint32_t* buffer = new uint32_t[3];

  buffer[0] = mEndpoints;
  buffer[1] = mValence;
  buffer[2] = mLevels;

  return Payload(3*sizeof(uint32_t),(char*)buffer);
}

void Broadcast::deserialize(DataFlow::Payload buffer)
{
  assert (buffer.size() == 3*sizeof(uint32_t));
  uint32_t *tmp = (uint32_t *)(buffer.buffer());

  mEndpoints = tmp[0];
  mValence = tmp[1];
  mLevels = tmp[2];

  delete[] buffer.buffer();
}


