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
#include "Payload.h"


namespace DataFlow {
namespace charm {


//! The Charm++ based controller
class Controller
{
public:

  //! Default constructor
  Controller() {}

  //! Destructor
  ~Controller() {}

  //! Initialize the controller with the given graph
  int initialize(const TaskGraph& graph);

  //! Register a callback for the given id
  int registerCallback(CallbackId id, Callback func);

private:

  static std::vector<Callback> mCallbacks;
};

}
}


#endif



