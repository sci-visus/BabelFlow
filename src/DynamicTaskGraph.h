/*
 * DynamicTaskGraph.h
 *
 *  Created on: Dec 14, 2021
 *      Author: sshudler
 */
 
#ifndef DYNAMIC_TASK_GRAPH_H__
#define DYNAMIC_TASK_GRAPH_H__

#include <vector>
#include "TaskGraph.h"
#include "ComposableTaskGraph.h"


namespace BabelFlow
{

class DynamicTaskGraph : public TaskGraph
{
public:
  // using GraphTerm = std::pair<TaskGraph*, uint32_t>;

  DynamicTaskGraph() = default;
  
  //! Def ctor: All the connectors between the graphs are going to be default connectors
  DynamicTaskGraph( std::vector<TaskGraph*>& gr_vec );
  
  virtual ~DynamicTaskGraph() {}
    
  //! Compute the fully specified tasks for the given controller
  virtual std::vector<Task> localGraph( ShardId id, const TaskMap* task_map ) const override;

  //! Return the task for the given global task id
  virtual Task task( uint64_t gId ) const override;

  //! Return the global id of the given task id
  virtual uint64_t gId( TaskId tId ) const override;

  //! Return the total number of tasks (or some reasonable upper bound)
  virtual uint32_t size() const override;

  //! Return the total number of leaf tasks
  virtual uint32_t numOfLeafs() const override;

  //! Return the total number of root tasks
  virtual uint32_t numOfRoots() const override;

  //! Return the id for a leaf at the given index
  virtual TaskId leaf( uint32_t idx ) const override;

  //! Return the id for a root at the given index
  virtual TaskId root( uint32_t idx ) const override;

  //! Serialize a task graph
  virtual Payload serialize() const override;

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize( Payload buffer, bool clean_mem = true ) override;

  virtual bool extend(const TaskGraph::OutputsMap& outputs) override;

private:
  
  std::vector<TaskGraph*>     m_subGraphs;
  uint32_t                    m_currGraphIdx;
  ComposableTaskGraph         m_compGraph;
};  // class DynamicTaskGraph

}   // namespace BabelFlow


#endif    // #ifndef DYNAMIC_TASK_GRAPH_H__
