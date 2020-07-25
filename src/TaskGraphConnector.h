/*
 * TaskGraphConnector.h
 *
 *  Created on: Jul 13, 2020
 *      Author: sshudler
 */
 
#ifndef TASK_GRAPH_CONNECTOR_H__
#define TASK_GRAPH_CONNECTOR_H__


#include <vector>

#include "Definitions.h"
#include "TaskGraph.h"



namespace BabelFlow
{

class TaskGraphConnector
{
public:
  TaskGraphConnector( ShardId controller_cnt, TaskGraph* src_gr, TaskGraph* dst_gr, TaskMap* src_map, TaskMap* dst_map )
  : m_controllerCount( controller_cnt ), m_srcGraph( src_gr ), m_dstGraph( dst_gr ), m_srcMap( src_map ), m_dstMap( dst_map ) {}

  virtual ~TaskGraphConnector() {}

  virtual std::vector<TaskId> getOutgoingConnectedTasks( const TaskId& task_id ) const = 0;

  virtual std::vector<TaskId> getIncomingConnectedTasks( const TaskId& task_id ) const = 0;

protected:
  ShardId     m_controllerCount;
  TaskGraph*  m_srcGraph;
  TaskGraph*  m_dstGraph;
  TaskMap*    m_srcMap;
  TaskMap*    m_dstMap;

};  // class TaskGraphConnector

}   // namespace BabelFlow

#endif    // #ifndef TASK_GRAPH_CONNECTOR_H__
