/*
 * SingleTaskGraph.h
 *
 *  Created on: Jul 16, 2020
 *      Author: sshudler
 */

#ifndef BFLOW_RED_SINGLE_TASK_GRAPH_H_
#define BFLOW_RED_SINGLE_TASK_GRAPH_H_

#include <vector>

#include "../TaskGraph.h"



namespace BabelFlow
{

class SingleTaskGraph : public TaskGraph
{
public:
  enum TaskCB { SINGLE_TASK_CB = 11 };

  SingleTaskGraph( uint32_t n_ranks = 1 ) 
  : m_nRanks( n_ranks ), m_numInputSrcs( 1 ), m_numOutputData( 1 ), m_numOutputDest( 1 ) {}

  SingleTaskGraph( uint32_t n_ranks, uint32_t input_srcs, uint32_t output_data_items, uint32_t output_dest )
  : m_nRanks( n_ranks ), m_numInputSrcs( input_srcs ), 
    m_numOutputData( output_data_items ), m_numOutputDest( output_dest ) {}

  //! Destructor
  virtual ~SingleTaskGraph() {}

  //! Compute the fully specified tasks for the given controller
  virtual std::vector<Task> localGraph(ShardId id, const TaskMap* task_map) const override;

  //! Compute fully specified tasks for the whole graph (in all controllers)
  virtual std::vector<Task> allGraph() const override;

  //! Return the task for the given global task id
  virtual Task task(uint64_t gId) const override;

  //! Return the global id of the given task id
  virtual uint64_t gId(TaskId tId) const override { return tId; }

  //! Return the total number of tasks (or some reasonable upper bound)
  virtual uint32_t size() const override { return 1; }

  //! Serialize a task graph
  virtual Payload serialize() const override;

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize(Payload buffer) override;

protected:
  uint32_t m_nRanks;
  uint32_t m_numInputSrcs;
  uint32_t m_numOutputData;
  uint32_t m_numOutputDest;

};  // end class SingleTaskGraph

}   // end namespace BabelFlow


#endif    // #ifndef BFLOW_RED_SINGLE_TASK_GRAPH_H_
