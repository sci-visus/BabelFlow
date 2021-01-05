/*
 * DefGraphConnector.h
 *
 *  Created on: Jul 13, 2020
 *      Author: sshudler
 */
 
#ifndef DEF_GRAPH_CONNECTOR_H__
#define DEF_GRAPH_CONNECTOR_H__

#include <vector>
#include <unordered_map>

#include "TaskGraphConnector.h"


namespace BabelFlow
{

class DefGraphConnector : public TaskGraphConnector
{
public:
  DefGraphConnector() = default;
  
  DefGraphConnector( TaskGraph* src_gr, 
                     uint32_t src_gr_id, 
                     TaskGraph* dst_gr,
                     uint32_t dst_gr_id );

  virtual ~DefGraphConnector() {}

  virtual std::vector<TaskId> getOutgoingConnectedTasks( const TaskId& task_id ) const override;

  virtual std::vector<TaskId> getIncomingConnectedTasks( const TaskId& task_id ) const override;

  virtual uint32_t type() const override { return 0; };

protected:
  void init();

  std::unordered_map< TaskId, TaskId > m_outConnectorMap;
  std::unordered_map< TaskId, std::vector<TaskId> > m_inConnectorMap;

};  // class DefGraphConnector


}   // namespace BabelFlow


#endif    // #ifndef TASK_GRAPH_CONNECTOR_H__
