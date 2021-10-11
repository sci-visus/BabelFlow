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

#ifndef BROADCAST_GRAPH_H_
#define BROADCAST_GRAPH_H_

#include <stdint.h>
#include <cmath>

#include "Definitions.h"
#include "TaskGraph.h"

//! A broadcast implements a task graph describing an tree based broadcast
class BroadcastGraph : public BabelFlow::TaskGraph
{
public:
  enum TaskCB { BCAST_TASK_CB = 0, LEAF_TASK_CB = 1 };

  /*! Create a tree broadcast with at least the given number of leaf
   *  and the given fanout. All interior nodes will have callback=0
   *  all leaf task will have callback=1
   *
   * @param endpoints The minimal number of leafs
   * @param valence The fanout of the broadcast
   */
  BroadcastGraph(uint32_t endpoints, uint32_t valence);

  BroadcastGraph(std::string config);

  BroadcastGraph(){};

  //! Default destructor
  virtual ~BroadcastGraph() {}

  //! Return the taskId for the given global task id
  virtual BabelFlow::Task task(uint64_t gId) const override;

  //! Return the global id of the given task id
  virtual uint64_t gId(BabelFlow::TaskId tId) const override {return tId;}

  //! Compute the fully specified tasks for the
  //! given controller and task map
  virtual std::vector<BabelFlow::Task> localGraph(BabelFlow::ShardId id, const BabelFlow::TaskMap* task_map) const override;

  //! Return the total number of tasks
  virtual uint32_t size() const override {return (pow(mValence,mLevels+1) - 1) / (mValence-1);}

  //! Return the total number of leaf tasks
  virtual uint32_t numOfLeafs() const override { return 1; }

  //! Return the total number of root tasks
  virtual uint32_t numOfRoots() const override { return pow(mValence, mLevels); }

  //! Return the id for a leaf at the given index
  virtual BabelFlow::TaskId leaf(uint32_t idx) const override { return 0; }

  //! Return the id for a root at the given index
  virtual BabelFlow::TaskId root(uint32_t idx) const override { return size() - numOfRoots() + idx; }

  //! Serialize a task graph
  virtual BabelFlow::Payload serialize() const override;

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize(BabelFlow::Payload buffer) override;

private:

  //! The minimal number of endpoints
  uint32_t mEndpoints;

  //! The fanout
  uint32_t mValence;

  //! The number of levels
  uint32_t mLevels;
};



#endif /* BROADCAST_H_ */
