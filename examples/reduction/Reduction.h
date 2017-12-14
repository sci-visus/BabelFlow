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

#ifndef REDUCTION_H_
#define REDUCTION_H_

#include <stdint.h>
#include <cmath>

#include "Definitions.h"
#include "TaskGraph.h"

class Reduction : public DataFlow::TaskGraph
{
public:

  /*! Create a tree based reduction with at least the given number of
   *  leafs and the given fanin. All nodes but the root will
   *  have callback=1 and the root will have callback=2. Note that for
   *  simplicity this create a full tree if necessary increasing the
   *  number of leafs. Leafs will be the last leafCount() many tasks
   *
   * @param leafs: The minimal number of leafs to create
   * @param valence: The fanin of the reduction
   */
  Reduction(uint32_t leafs = 1, uint32_t valence = 1);

  //! Construct from command line arguments
  Reduction(std::string config);

  //! Default destructor
  virtual ~Reduction() {}

  //! Compute the fully specified tasks for the
  //! given controller id and task map
  virtual std::vector<DataFlow::Task> localGraph(DataFlow::ShardId id, const DataFlow::TaskMap* task_map) const;

  //! Return the taskId for the given global task id
  virtual DataFlow::Task task(uint64_t gId) const;

  //! Return the global id of the given task id
  virtual uint64_t gId(DataFlow::TaskId tId) const {return tId;}

  //! Return the total number of tasks
  DataFlow::TaskId size() const {return (pow(mValence,mLevels+1) - 1) / (mValence-1);}

  //! Return the number of leafs
  DataFlow::TaskId leafCount() const {return pow(mValence,mLevels);}

  //! Serialize a task graph
  virtual DataFlow::Payload serialize() const;

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize(DataFlow::Payload buffer);


protected:

  //! The number of leafs in the reduction
  uint32_t mLeafs;

  //! The fanout
  uint32_t mValence;

  //! The number of levels in the tree
  uint32_t mLevels;
};

#endif /* REDUCTION_H_ */
