/*
 * Reduction.h
 *
 *  Created on: Dec 15, 2014
 *      Author: bremer5
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

  //! COnstruct from command line arguments
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

  //! Return the global id of the given leaf id
  //virtual uint64_t leaf(uint64_t lId) const = 0;

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
