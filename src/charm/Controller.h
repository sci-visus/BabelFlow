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

  typedef CProxy_CharmDataFlowTask<TaskGraphClass,CallbackClass> ProxyType;

  //! Default constructor
  Controller() {}

  //! Destructor
  ~Controller() {}

  //! Initialize the controller with the given graph
  int initialize(const std::string& graph_config);

};



template <class TaskGraphClass, class CallbackClass>
int Controller<TaskGraphClass,CallbackClass>::initialize(const std::string& graph_config)
{
  TaskGraphClass graph(graph_config);


  ProxyType tasks = ProxyType::ckNew(graph_config);//, graph.size());


  return 1;
}


}
}


#endif



