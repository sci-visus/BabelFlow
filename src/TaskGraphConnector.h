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
  TaskGraphConnector() 
  : m_srcGraph( nullptr ),
    m_srcGraphId( 0 ), 
    m_dstGraph( nullptr ), 
    m_dstGraphId( 0 ) {}

  TaskGraphConnector( TaskGraph* src_gr, 
                      uint32_t src_gr_id, 
                      TaskGraph* dst_gr,
                      uint32_t dst_gr_id )
  : m_srcGraph( src_gr ),
    m_srcGraphId( src_gr_id ), 
    m_dstGraph( dst_gr ), 
    m_dstGraphId( dst_gr_id ) {}

  virtual ~TaskGraphConnector() {}

  virtual std::vector<TaskId> getOutgoingConnectedTasks( const TaskId& task_id ) const = 0;

  virtual std::vector<TaskId> getIncomingConnectedTasks( const TaskId& task_id ) const = 0;

  virtual uint32_t type() const = 0;

protected:
  TaskGraph*  m_srcGraph;
  uint32_t    m_srcGraphId;
  TaskGraph*  m_dstGraph;
  uint32_t    m_dstGraphId;
};  // class TaskGraphConnector

}   // namespace BabelFlow

#endif    // #ifndef TASK_GRAPH_CONNECTOR_H__
