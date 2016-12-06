/*
 * Controller.cpp
 *
 *  Created on: Dec 5, 2016
 *      Author: bremer5
 */

#include <cassert>



#include "charm/Controller.h"
#include "charm/CharmTaskGraph.h"
#include "charm/CharmTask.h"


#include "charm_dataflow.decl.h"

namespace DataFlow {
namespace charm {


int Controller::registerCallback(CallbackId id, Callback func){
  assert (id > 0); // Callback 0 is reserved for relays

  if (mCallbacks.size() <= id)
    mCallbacks.resize(id+1);

  mCallbacks[id] = func;

  return 1;
}

//! Initialize the controller with the given graph
int Controller::initialize(const TaskGraph& graph)
{

  return 1;
}



}
}
