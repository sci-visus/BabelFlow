/*
 * MultiGraphConnector.h
 *
 *  Created on: Jul 13, 2020
 *      Author: sshudler
 */
 
#ifndef MULTI_GRAPH_CONNECTOR_H__
#define MULTI_GRAPH_CONNECTOR_H__

#include <vector>
#include <unordered_map>
#include <utility>

#include "TaskGraphConnector.h"


namespace BabelFlow
{

class MultiGraphConnector : public TaskGraphConnector
{
public:
  using GraphPair = std::pair<uint32_t, uint32_t>;

  MultiGraphConnector() = default;
  
  MultiGraphConnector( const std::vector<TaskGraph*>& gr_vec );

  MultiGraphConnector( const std::vector<TaskGraph*>& gr_vec, 
                       const std::vector<GraphPair>& gr_pair_vec );

  virtual ~MultiGraphConnector() {}

  virtual std::vector<TaskId> getOutgoingConnections( const TaskId& task_id ) const override;

  virtual std::vector<TaskId> getIncomingConnections( const TaskId& task_id ) const override;

  virtual void connectTasks( const TaskId& from, const TaskId& to ) override;

  virtual uint32_t type() const override { return 0; }

  //! Serialize this task graph connector
  virtual Payload serialize() const override;

  //! Deserialize this task graph connector
  virtual void deserialize( Payload buffer ) override;

  void init( const std::vector<TaskGraph*>& gr_vec );

protected:
  std::vector<GraphPair> m_grPairsVec;
  std::unordered_map< TaskId, std::vector<TaskId> >   m_outConnectorMap;
  std::unordered_map< TaskId, std::vector<TaskId> >   m_inConnectorMap;
};  // class MultiGraphConnector


}   // namespace BabelFlow


#endif    // #ifndef MULTI_GRAPH_CONNECTOR_H__
