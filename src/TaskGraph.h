/*
 * Copyright (c) 2017 University of Utah 
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef TASKGRAPH_H
#define TASKGRAPH_H

#include <vector>
#include <map>
#include <cstdio>
#include <cassert>
#include <string>
#include <typeinfo>
#include <ostream>
#include <unordered_map>

#include "Task.h"
#include "Payload.h"


//#define REG_TGRAPH( cls ) \
//static bool cls ## _creator_registered = \
//TaskGraphFactory::registerCreator( std::string( typeid( cls ).name() ), &TaskGraphFactory::createT< cls > );


namespace BabelFlow 
{

class TaskMap;

/*! The task graph defines a baseclass for all algorithms
 *  that want to use the data flow for communication. It consists
 *  of pure a interface to return (subsets of) tasks.
 */
class TaskGraph
{
public:

  //! Default constructor
  TaskGraph(std::string config = "") : m_graphId( 0 ) {}

  //! Default destructor
  virtual ~TaskGraph() {}

  virtual uint32_t type() const;

  //! Compute the fully specified tasks for the given controller
  virtual std::vector<Task> localGraph(ShardId id, const TaskMap* task_map) const = 0;

  //! Return the task for the given global task id
  virtual Task task(uint64_t gId) const = 0;

  //! Return the global id of the given task id
  virtual uint64_t gId(TaskId tId) const = 0;

  //! Return the global id of the given leaf id
  //virtual uint64_t leaf(uint64_t lId) const = 0;

  //! Return the total number of leaf tasks
  virtual uint32_t numOfLeafs() const = 0;

  //! Return the total number of root tasks
  virtual uint32_t numOfRoots() const = 0;

  //! Return the id for a leaf at the given index
  virtual TaskId leaf(uint32_t idx) const = 0;

  //! Return the id for a root at the given index
  virtual TaskId root(uint32_t idx) const = 0;

  //! Return the total number of tasks (or some reasonable upper bound)
  virtual uint32_t size() const = 0;

  //! Serialize a task graph
  virtual Payload serialize() const {assert(false);return Payload();}

  //! Deserialize a task graph. This will consume the payload
  virtual void deserialize(Payload buffer) {assert(false);}

  virtual void setGraphId( uint32_t gr_id ) { m_graphId = gr_id; }

  virtual uint32_t graphId() const { return m_graphId; }

  //! Returns the callback func pointer for this graph and callback id
  virtual Callback queryCallback( CallbackId cb_id ) const;

  //! Output the entire graph as dot file
  virtual void outputGraph( ShardId count, const TaskMap* task_map, const std::string& filename ) const;

  //! Output the entire graph as dot file embedded in HTML
  virtual void outputGraphHtml( ShardId count, const TaskMap* task_map, const std::string& filename ) const;

  virtual void outputTasksHtml( const std::vector<Task>& tasks_v, const std::string& filename ) const;

  //! Register a callback for the given graph id and callback id
  static void registerCallback( uint32_t graph_id, CallbackId id, Callback func );

  //! Returns the callback func pointer for the given graph id and callback id
  static Callback queryCallback( uint32_t graph_id, CallbackId id );

protected:
  uint32_t m_graphId;

  void outputHelper( const std::vector< std::vector<Task> >& tasks_v, std::ostream& outs, bool incl_html ) const;

  virtual void outputDot( const std::vector< std::vector<Task> >& tasks_v, std::ostream& outs, const std::string& eol ) const;

  static uint32_t graphNameToTypeId( const char* gr_type_name );

  static std::unordered_map< uint32_t, std::vector<Callback> > s_callbackMap;
  static std::unordered_map< std::string, uint32_t > s_typeIdsMap;
};


/*! The TaskGraphFactory class is used to construct instances of TaskGraph only be class type name.
 *  This reflection-like capability is necessary for some advanced serialization used by ComposableTaskGraph.
 *  Since ComposableTaskGraph composes general TaskGraph's it has to have the capability to construct
 *  an instance from TaskGraph's runtime type.
 * 
 *  For this mechanism to work properly, each class derived from TaskGraph has to include the line
 *  REG_TGRAPH( cls ), cls is class name, somewhere in the first part of the file. 
 */
//class TaskGraphFactory
//{
//public:
//  typedef TaskGraph* (*CreatorFunc)();
//  typedef std::map<std::string, CreatorFunc> MapType;
//
//  static bool registerCreator(const std::string& name, CreatorFunc func);
//  static TaskGraph* createInstance(const std::string& name) { return m_map[name](); }
//  
//  template<typename T> 
//  static TaskGraph* createT() { return new T; }
//
//private:
//  static MapType m_map;
//};


/*! The task map defines an abstract baseclass to define the global
 *  Task-to-Controller map as well as the reverse
 */
class TaskMap
{
public:

  //! Default constructor
  TaskMap() {}

  //! Default destructor
  virtual ~TaskMap() {}

  //! Return which controller is assigned to the given task
  virtual ShardId shard(TaskId id) const = 0;

  //! Return the set of task assigned to the given controller
  virtual std::vector<TaskId> tasks(ShardId id) const = 0;
};


/*! The controller map defines a baseclass to define the controller
 *   to MPI_RANK map and its reverse. The default map is identity.
 *   We assume an rank can have at most one controller but not
 *   every rank must have one.
 */
class ControllerMap
{
public:

  //! Default constructor
  ControllerMap() {}

  //! Default destructor
  virtual ~ControllerMap() {}

  //! Return the MPI_RANK to which the given controller is assigned
  virtual uint32_t rank(ShardId id) const {return id;}

  //! Return the controller assigned to the given rank (could be CNULL)
  virtual ShardId controller(uint32_t rank) const {return rank;}
};
  
}

#endif /* TASKGRAPH_H_ */
