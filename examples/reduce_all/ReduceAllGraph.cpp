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
#include <sstream>

#include "ReduceAllGraph.h"

using namespace BabelFlow;

ReduceAllGraph::ReduceAllGraph(uint32_t leafs, uint32_t valence) : TaskGraph(),
                                                                   mValence(valence) {
  // Find the number of leafs that is a power of valence
  mLeafs = 1;
  mLevels = 0;

  while (mLeafs < leafs) {
    mLeafs *= mValence;
    mLevels++;
  }
}

ReduceAllGraph::ReduceAllGraph(std::string config) {
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

std::vector<Task> ReduceAllGraph::localGraph(ShardId id, const TaskMap *task_map) const {
  // First get all the ids we need
  std::vector<TaskId> ids = task_map->tasks(id);

//  printf("shard %d has %d tasks: ", id, ids.size());

  // Create the required number of tasks
  std::vector<BabelFlow::Task> tasks(ids.size());
  for (int i = 0; i < ids.size(); i++) {
    tasks[i] = task(ids[i]);
//    printf("%d ",ids[i]);
  }
//  printf("\n");

  return tasks;
}

BabelFlow::Task ReduceAllGraph::task(uint64_t gId) const {
  BabelFlow::Task task(gId);
  std::vector<BabelFlow::TaskId> incoming; // There will be at most valence many incoming
  std::vector<std::vector<BabelFlow::TaskId> > outgoing(1); // and one output
  uint32_t i;

  if (gId < reductionSize()) {
    bool leaf_task = false;
    if (gId < reductionSize() - leafCount()) {
      leaf_task = true;
      incoming.resize(mValence);
      for (i = 0; i < mValence; i++)
        incoming[i] = task.id() * mValence + i + 1;

      task.incoming(incoming);
    } else { // Otherwise we expect one external input
      incoming.resize(1, TNULL);
    }

    // Then we assign the outputs
    if (task.id() != 0) { // If we are not the root of the reduction
      task.callback(!leaf_task ? LOCAL_COMPUTE_TASK : REDUCTION_TASK);

      outgoing.resize(1);
      outgoing[0].resize(1);
      outgoing[0][0] = (task.id() - 1) / mValence;
    } else {
      task.callback(COMPLETE_REDUCTION_TASK); // Otherwise we start the broadcast
      outgoing.resize(1);
      outgoing[0].resize(mValence);

      for (i = 0; i < mValence; i++)
        outgoing[0][i] = reductionSize() + i;
    }
  } else {
    outgoing.resize(1);
    outgoing[0].resize(mValence);
    incoming.resize(1);

    // If this is a leaf
    if (task.id() >= (size() - leafCount())) {
      task.callback(RESULT_REPORT_TASK);    // Report result
//      for (i = 0; i < mValence; i++)
//        outgoing[0][i] = TNULL;
      outgoing.clear();
    }
    else { // If we are not the leaf
      task.callback(RESULT_BROADCAST_TASK); // We are a relay task

      // And we have valence many outputs
      for (i = 0; i < mValence; i++) {
        outgoing[0][i] = (task.id() - reductionSize() + 1) * mValence + i + reductionSize();
        //printf("task %d out[%d] %d\n", task.id(), i, outgoing[0][i]);
      }
      task.outputs() = outgoing;
    }

    // If we are the task connecting to the root of the reduction
    if (task.id() < (reductionSize() + mValence))
      incoming[0] = 0;
    else { // if we are the broadcast tasks
      incoming[0] = (task.id() - 1) / mValence + leafCount() - 1;
      //printf("task %d incoming %d\n", task.id(), incoming[0]);
    }

    task.incoming() = incoming;
  }

//  printf("task %d in: ", task.id());
//  for(auto in : incoming)
//    printf("%d ", in);
//  printf("\n");

  task.incoming(incoming);
  task.outputs(outgoing);

  return task;
}

BabelFlow::Payload ReduceAllGraph::serialize() const {
  uint32_t *buffer = new uint32_t[3];

  buffer[0] = mLeafs;
  buffer[1] = mValence;
  buffer[2] = mLevels;

  return Payload(3 * sizeof(uint32_t), (char *) buffer);
}

void ReduceAllGraph::deserialize(BabelFlow::Payload buffer) {
  assert (buffer.size() == 3 * sizeof(uint32_t));
  uint32_t *tmp = (uint32_t *) (buffer.buffer());

  mLeafs = tmp[0];
  mValence = tmp[1];
  mLevels = tmp[2];

  delete[] buffer.buffer();
}