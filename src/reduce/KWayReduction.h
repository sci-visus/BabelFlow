/*
 * KWayReduction.h
 *
 *  Created on: Aug 30, 2016
 *      Author: spetruzza
 */

#ifndef KWayReduction_H_
#define KWayReduction_H_

#include <stdint.h>
#include <vector>
#include <cmath>

//#include "TypeDefinitions.h"
#include "../TaskGraph.h"


namespace BabelFlow 
{

class KWayReductionTaskMap;

/*! This class implements a k-way merge tree algorithm assuming
 *  a regular grid of data blocks as input. The graph will merge
 *  across the longest spatial boundary first and merge until
 *  the given k is achieved. Currently, only power-of-2 merges are
 *  supported. The graph assume four unique operations:
 *
 *  Callback 1: Local compute (one outside input), two outputs the first
 *              to the merge and the next to the first correction stage
 *
 *  Callback 2: Merge computation (up to k inputs), two outputs, the
 *              first to the next merge the second to the relay towards
 *              the next correction
 *
 *  Callback 3: Local corrections with two inputs and one output. The
 *              first input is the current local tree, the second the
 *              boundary tree. The output is the new local tree.
 *
 *  Callback 4: Output of the final local trees with one input and no
 *              outputs
 *
 *  The numbering of the nodes will be as follows. The main reduction
 *  flow is numbered 0 - pow(k,level)-1 as the default reduction. The
 *  various scatter flows will be numbered 0-n as the standard broadcast
 *  but their number will be prefixed by their level in the reduction
 *  using the first m bits (m typically = 4) of the taskid
 *
 */
class KWayReduction : public TaskGraph
{
public:
  enum TaskCB { LEAF_TASK_CB = 1, MID_TASK_CB = 2, ROOT_TASK_CB = 3 };

  friend class KWayReductionTaskMap;

  //! The number of bits used for prefixing scatter tasks
  static const uint8_t sPrefixSize = 4;

  //! The number of non-prefix bits
  static const uint8_t sPostfixSize = sizeof(TaskId::InnerTaskId)*8 - sPrefixSize;

  //! Bit mask for scatter tasks
  static const TaskId::InnerTaskId sPrefixMask = ((1 << sPrefixSize) - 1) << sPostfixSize;

  //! Dataset dimensions
  static uint32_t sDATASET_DIMS[3];

  /*! Create a k-way merge graph from the given grid of local tasks
   *  and given merge factor. The leaf task will be numbered in
   *  row major order.
   *
   * @param dim
   * @param factor
   */
  KWayReduction(uint32_t dim[3], uint32_t factor);

  KWayReduction(const std::string& config);

  KWayReduction() {}

  void init(uint32_t dim[3], uint32_t factor);

  //! Destructor
  virtual ~KWayReduction() {}

  virtual uint32_t type() const override { return 2; };

  //! Compute the fully specified tasks for the
  //! given controller id and task map
  virtual std::vector<Task> localGraph(ShardId id, const TaskMap* task_map) const override;

  //! Return the total number of tasks
  /*! This function computes the total number of tasks in the graph.
   *  However, it is important to note that the tasks are *not*
   *  numbered consecutively, so "size" cannot be used in a modulo
   *  task map, for example.
   *
   * @return The total number of tasks
   */
  virtual uint32_t size() const override { return mLvlOffset[mRounds]; }

  //! Return the total number of leaf tasks
  virtual uint32_t numOfLeafs() const override { return mLvlOffset[1]; }

  //! Return the total number of root tasks
  virtual uint32_t numOfRoots() const override { return 1; }

  //! Return the id for a leaf at the given index
  virtual TaskId leaf(uint32_t idx) const override { return idx; }

  //! Return the id for a root at the given index
  virtual TaskId root(uint32_t idx) const override { return (mLvlOffset.back()-1); }

  //! Return the total number of rounds needed to merge
  uint8_t rounds() const { return mRounds; }

  virtual Task task(uint64_t gId) const override;

  virtual uint64_t gId(TaskId tId) const override{ return tId; };

  //! Serialize a task graph
  virtual Payload serialize() const override;

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize(Payload buffer) override;

protected:
  virtual void outputDot( const std::vector< std::vector<Task> >& tasks_v, std::ostream& outs, const std::string& eol ) const override;

private:

  //! The dimension of the input grid
  std::vector<std::vector<uint32_t> > mLvlDim;

  //! The merge factor
  uint32_t mFactor;

  //! How many rounds of merging
  uint32_t mRounds;

  //! A vector storing how many tasks there are for each level and tis children
  std::vector<uint32_t> mLvlOffset;

  //! Simplification factors on a per level basis
  std::vector<std::vector<uint8_t> > mFactors;


  /*******************************************************************
   ******* Internal convenience functions
   *******************************************************************/
  // Const access to the level sizes
  const std::vector<uint32_t>& lvlOffset() const { return mLvlOffset; }

  //! Return the level of a reduction task
  uint8_t level(TaskId id) const;

  //! Return the base id (in the reduction)
  TaskId baseId(TaskId id) const { return id &= ~sPrefixMask; }

  //! Return thecomputation round this task is part of
  // TaskId round(TaskId id) const { return id >> sPostfixSize; }

  //! Compute the id of a task in a certain round
  // TaskId roundId(TaskId id, uint8_t round) const;

  //! Return whether this is a gather or scatter task
  bool gatherTask(TaskId id) const { return (id & sPrefixMask) == 0; }

  //! Function to map a global id in the reduction to its parent
  TaskId reduce(TaskId source) const;

  //! Function to compute the children of the given task id
  std::vector<TaskId> expand(TaskId source) const;

  //! Function to map row-major indices from grid of round n to grid of round n+1
  TaskId gridReduce(TaskId source, uint8_t lvl) const;

  //! Function to map row-major indices from grid of round n+1 to round n
  std::vector<TaskId> gridExpand(TaskId source, uint8_t lvl) const;
};


}   // namespace babelflow

#endif /* KWayReduction_H_ */
