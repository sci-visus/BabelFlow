#ifndef BFLOW_VLR_RADIXKEXCHANGE_H_
#define BFLOW_VLR_RADIXKEXCHANGE_H_

#include <stdint.h>
#include <vector>
#include <cmath>

#include "../TaskGraph.h"


namespace BabelFlow 
{


class RadixKExchangeTaskMap;

class RadixKExchange : public TaskGraph
{
public:

  friend class RadixKExchangeTaskMap;
  
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
  
  RadixKExchange() {}
  
  RadixKExchange(uint32_t dim[3], const std::vector<uint32_t>& radix_v);

  //! Destructor
  virtual ~RadixKExchange() {}

  void init(uint32_t block_dim[3], const std::vector<uint32_t>& radix_v);
  
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
  virtual TaskId size() const { return (totalLevels() + 1) * m_Nblocks; }

  //! Return the total number of levels in the radix-k exchange
  uint32_t totalLevels() const { return m_Radices.size(); }

  //! Output the entire graph as dot file
  virtual int output_graph_dot(ShardId count, const TaskMap* task_map, FILE* output, const std::string &eol);

  virtual Task task(uint64_t gId) const;

  virtual uint64_t gId(TaskId tId) const { return tId; }    //return baseId(tId); };

  virtual uint64_t toTId(TaskId gId) const { return gId; }  //|= sPrefixMask; };

  //! Serialize a task graph
  virtual Payload serialize() const;

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize(Payload buffer);

private:

  uint32_t m_Nblocks;
  uint32_t m_BlkDecomp[3];
  
  //! A vector of radices 
  std::vector<uint32_t> m_Radices;
  
  //! Prexif prod vector of radices
  std::vector<uint32_t> m_RadicesPrefixProd;

  //! A vector storing how many tasks there are for each level and tis children
  std::vector<uint32_t> m_LvlOffset;


  /*******************************************************************
   ******* Internal convenience functions
   *******************************************************************/
  // Const access to the level sizes
  const std::vector<uint32_t>& lvlOffset() const { return m_LvlOffset; }

  //! Return the level of a RadixK exchange task
  uint32_t level(TaskId id) const { baseId(id) / m_Nblocks; }

  //! Compute the id of a task at a certain level
  TaskId idAtLevel(TaskId id, uint32_t level) const { return (baseId(id)%m_Nblocks + m_Nblocks*level); }
  
  //! Compute radix group members (task id's group) for given level
  void getRadixNeighbors(TaskId id, uint32_t level, std::vector<TaskId>& neighbors) const;

  //! Return the base id (in the reduction)
  TaskId baseId(TaskId id) const { return id; }//&= ~sPrefixMask;}

};


}   // end namespace BabelFlow


#endif /* BFLOW_VLR_RADIXKEXCHANGE_H_ */
