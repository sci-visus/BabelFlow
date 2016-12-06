/*
 * controller.h
 *
 *  Created on: Dec 2, 2016
 *      Author: bremer5
 */

#ifndef CHARMCONTROLLER_H
#define CHARMCONTROLLER_H

#include <vector>

#include "Definitions.h"
#include "TaskGraph.h"

#include "charm_dataflow.decl.h"

namespace DataFlow {
namespace charm {


//! The Charm++ based controller
template <class TaskGraphClass, class CallbackClass>
class Controller
{
public:

  typedef CharmTask<TaskGraphClass,CallbackClass> TaskType;
  typedef CProxy_CharmTask<TaskGraphClass,CallbackClass> ProxyType;

  //! Default constructor
  Controller() {}

  //! Destructor
  ~Controller() {}

  //! Initialize the controller with the given graph
  ProxyType initialize(const std::string& graph_config);

};



template <class TaskGraphClass, class CallbackClass>
CProxy_CharmTask<TaskGraphClass,CallbackClass> Controller<TaskGraphClass,CallbackClass>::initialize(const std::string& graph_config)
{
  TaskGraphClass graph(graph_config);

  fprintf(stderr,"Trying to create %d tasks\n", graph.size());

  // Create an array to hold all tasks
  ProxyType tasks = ProxyType::ckNew(graph_config, graph.size());

  return tasks;
}


}
}


#endif



