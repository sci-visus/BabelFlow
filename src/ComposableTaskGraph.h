/*
 * ComposableTaskGraph.h
 *
 *  Created on: Jul 7, 2020
 *      Author: sshudler
 */
 
#ifndef COMPOSABLE_TASK_GRAPH_H__
#define COMPOSABLE_TASK_GRAPH_H__

#include <vector>
#include "TaskGraph.h"
#include "TaskGraphConnector.h"


namespace BabelFlow
{

class ComposableTaskGraph : public TaskGraph
{
public:
  ComposableTaskGraph() = default;
  
  //! Def ctor: All the connectors between the graphs are going to be default connectors
  ComposableTaskGraph( std::vector<TaskGraph*>& gr_vec );

  ComposableTaskGraph( std::vector<TaskGraph*>& gr_vec, const TaskGraphConnectorPtr& gr_connector );
  
  virtual ~ComposableTaskGraph() {}
    
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
  virtual void deserialize( Payload buffer ) override;

private:
  Task task( const TaskId& tid ) const;
  
  std::vector<TaskGraph*>     m_graphs;
  TaskGraphConnectorPtr       m_connector;

};  // class ComposableTaskGraph

}   // namespace BabelFlow


#endif    // #ifndef COMPOSABLE_TASK_GRAPH_H__
