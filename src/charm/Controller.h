/*
 * controller.h
 *
 *  Created on: Dec 2, 2016
 *      Author: bremer5
 */

#ifndef CHARMCONTROLLER_H
#define CHARMCONTROLLER_H

#include <vector>
#include <cstdlib>

#include "Definitions.h"
#include "TaskGraph.h"

#include "charm_dataflow.decl.h"

namespace DataFlow {
namespace charm {


//! The Charm++ based controller
template <class TaskGraphClass>
class Controller
{
public:

  typedef CharmTask<TaskGraphClass> TaskType;
  typedef CProxy_CharmTask<TaskGraphClass> ProxyType;

  //! Default constructor
  Controller() {}

  //! Destructor
  ~Controller() {}

  //! Initialize the controller with the given graph
  ProxyType initialize(TaskGraphClass& graph);

};



template <class TaskGraphClass>
CProxy_CharmTask<TaskGraphClass> Controller<TaskGraphClass>::initialize(TaskGraphClass& graph)
{
  //fprintf(stderr,"Trying to create %d tasks\n", graph.size());

  // Create an array to hold all tasks
  ProxyType tasks = ProxyType::ckNew(graph, graph.size());

  return tasks;
}


}
}


#endif



