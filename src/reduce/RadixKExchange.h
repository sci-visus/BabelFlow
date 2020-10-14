/*
 * RadixKExchange.h
 *
 *  Created on: Jul 7, 2020
 *      Author: sshudler
 */
 
#ifndef BFLOW_RED_RADIXKEXCHANGE_H_
#define BFLOW_RED_RADIXKEXCHANGE_H_

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
  enum TaskCB { LEAF_TASK_CB = 1, MID_TASK_CB = 2, ROOT_TASK_CB = 3 };

  friend class RadixKExchangeTaskMap;
  
  //! Dataset dimensions
  static uint32_t sDATASET_DIMS[3];
 
  RadixKExchange() {}
  
  RadixKExchange(uint32_t nblks, const std::vector<uint32_t>& radix_v);

  //! Destructor
  virtual ~RadixKExchange() {}

  void init(uint32_t nblks, const std::vector<uint32_t>& radix_v);
  
  // virtual uint32_t type() const override { return 1; };

  //! Compute the fully specified tasks for the
  //! given controller id and task map
  virtual std::vector<Task> localGraph(ShardId id, const TaskMap* task_map) const override;

  virtual std::vector<Task> allGraph() const override;

  virtual Task task(uint64_t gId) const override;

  virtual uint64_t gId(TaskId tId) const override { return tId; }    //return baseId(tId); };

  //! Return the total number of tasks
  /*! This function computes the total number of tasks in the graph.
   *  However, it is important to note that the tasks are *not*
   *  numbered consecutively, so "size" cannot be used in a modulo
   *  task map, for example.
   *
   * @return The total number of tasks
   */
  virtual uint32_t size() const override { return (totalLevels() + 1) * m_Nblocks; }

  //! Serialize a task graph
  virtual Payload serialize() const override;

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize(Payload buffer) override;

  //! Return the total number of levels in the radix-k exchange
  uint32_t totalLevels() const { return m_Radices.size(); }

  virtual uint64_t toTId(TaskId gId) const { return gId; }  //|= sPrefixMask; };

protected:
  virtual void outputDot( const std::vector< std::vector<Task> >& tasks_v, std::ostream& outs, const std::string& eol ) const override;

private:

  uint32_t m_Nblocks;
  
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
  uint32_t level(TaskId id) const { return (baseId(id) / m_Nblocks); }

  //! Compute the id of a task at a certain level
  TaskId idAtLevel(TaskId id, uint32_t level) const { return (baseId(id)%m_Nblocks + m_Nblocks*level); }
  
  //! Compute radix group members (task id's group) for given level
  void getRadixNeighbors(TaskId id, uint32_t level, bool isOutgoing, std::vector<TaskId>& neighbors) const;

  //! Return the base id (in the reduction)
  TaskId baseId(TaskId id) const { return id; }//&= ~sPrefixMask;}

};


}   // end namespace BabelFlow


#endif /* BFLOW_VLR_RADIXKEXCHANGE_H_ */
