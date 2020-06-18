/*
 * BinarySwap.h
 *
 *  Created on: Sep 3, 2016
 *      Author: spetruzza
 */

#ifndef BFLOW_VLR_BINARYSWAP_H_
#define BFLOW_VLR_BINARYSWAP_H_

#include <stdint.h>
#include <vector>
#include <cmath>

#include "../TaskGraph.h"


namespace BabelFlow 
{


class BinarySwapTaskMap;

class BinarySwap : public TaskGraph
{
public:

  friend class BinarySwapTaskMap;
  
  //! The number of bits used for prefixing scatter tasks
  static const uint8_t sPrefixSize = 4;

  //! The number of non-prefix bits
  static const uint8_t sPostfixSize = sizeof(TaskId)*8 - sPrefixSize;

  //! Bit mask for scatter tasks
  static const TaskId sPrefixMask = ((1 << sPrefixSize) - 1) << sPostfixSize;
  
  //! Dataset dimensions
  static uint32_t sDATASET_DIMS[3];
  
  //! Dataset decomposition
  static uint32_t sDATA_DECOMP[3];
  
  static inline uint32_t fastPow2(uint32_t exp) { return uint32_t(1) << exp; }
  
  static inline bool fastIsPow2(uint32_t x) { return x && !(x & (x - 1)); }
  
  BinarySwap(){}
  
  BinarySwap(uint32_t dim[3]);
  
  void init(uint32_t block_dim[3]);

  //! Destructor
  virtual ~BinarySwap() {}

  //! Compute the fully specified tasks for the
  //! given controller id and task map
  virtual std::vector<Task> localGraph(ShardId id, const TaskMap* task_map) const;

  //! Return the total number of tasks
  /*! This function computes the total number of tasks in the graph.
   *  However, it is important to note that the tasks are *not*
   *  numbered consecutively, so "size" cannot be used in a modulo
   *  task map, for example.
   *
   * @return The total number of tasks
   */
  TaskId size() const;

  //! Return the total number of rounds needed to merge
  uint8_t rounds() const {return mRounds;}

  //! Output the entire graph as dot file
  virtual int output_graph(ShardId count, const TaskMap* task_map, FILE* output);

  virtual Task task(uint64_t gId) const;

  virtual uint64_t gId(TaskId tId) const { return tId; }    //return baseId(tId); };

  virtual uint64_t toTId(TaskId gId) const { return gId; }  //|= sPrefixMask; };

  //! Serialize a task graph
  virtual Payload serialize() const;

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize(Payload buffer);

private:

  uint32_t n_blocks;
  uint32_t b_decomp[3];

  //! How many rounds of merging
  uint32_t mRounds;

  //! A vector storing how many tasks there are for each level and tis children
  std::vector<uint32_t> mLvlOffset;


  /*******************************************************************
   ******* Internal convenience functions
   *******************************************************************/
  // Const access to the level sizes
  const std::vector<uint32_t>& lvlOffset() const {return mLvlOffset;}

  //! Return the level of a reduction task
  uint8_t level(TaskId id) const;

  //! Compute the id of a task in a certain round
  TaskId roundId(TaskId id, uint8_t round) const;

  //! Return the base id (in the reduction)
  TaskId baseId(TaskId id) const {return id; }//&= ~sPrefixMask;}

  //! Return thecomputation round this task is part of
  //BabelFlow::TaskId round(BabelFlow::TaskId id) const {return id >> sPostfixSize;}

};


}   // end namespace BabelFlow


#endif /* BFLOW_VLR_BINARYSWAP_H_ */
