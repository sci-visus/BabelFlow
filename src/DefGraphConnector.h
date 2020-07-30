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
  
  DefGraphConnector( ShardId controller_cnt, 
                     TaskGraph* src_gr, 
                     uint32_t src_gr_id, 
                     TaskGraph* dst_gr,
                     uint32_t dst_gr_id, 
                     TaskMap* src_map, 
                     TaskMap* dst_map );

  virtual ~DefGraphConnector() {}

  virtual std::vector<TaskId> getOutgoingConnectedTasks( const TaskId& task_id ) const override;

  virtual std::vector<TaskId> getIncomingConnectedTasks( const TaskId& task_id ) const override;

protected:
  static bool isRootTask( const Task& tsk );
  static bool isLeafTask( const Task& tsk );

  void init();

  std::unordered_map< TaskId, TaskId > m_outConnectorMap;
  std::unordered_map< TaskId, std::vector<TaskId> > m_inConnectorMap;

};  // class DefGraphConnector


}   // namespace BabelFlow


#endif    // #ifndef TASK_GRAPH_CONNECTOR_H__