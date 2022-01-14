/*
 * TaskGraphConnector.h
 *
 *  Created on: Jul 13, 2020
 *      Author: sshudler
 */
 
#ifndef TASK_GRAPH_CONNECTOR_H__
#define TASK_GRAPH_CONNECTOR_H__


#include <vector>
#include <memory>

#include "Definitions.h"
#include "TaskGraph.h"



namespace BabelFlow
{

class TaskGraphConnector
{
public:
  TaskGraphConnector() {}

  virtual ~TaskGraphConnector() {}

  virtual std::vector<TaskId> getOutgoingConnections( const TaskId& task_id ) const = 0;

  virtual std::vector<TaskId> getIncomingConnections( const TaskId& task_id ) const = 0;

  virtual void connectTasks( const TaskId& from, const TaskId& to ) = 0;

  virtual uint32_t type() const = 0;

  //! Serialize this task graph connector
  virtual Payload serialize() const = 0;

  //! Deserialize this task graph connector
  virtual void deserialize( Payload buffer, bool clean_mem = true ) = 0;

protected:
};  // class TaskGraphConnector

using TaskGraphConnectorPtr = std::shared_ptr<TaskGraphConnector>;

}   // namespace BabelFlow

#endif    // #ifndef TASK_GRAPH_CONNECTOR_H__
