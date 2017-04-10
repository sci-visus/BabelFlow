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

#include "../Definitions.h"
#include "../TaskGraph.h"

#include "charm_dataflow.decl.h"

namespace DataFlow {

namespace charm {


//! The Charm++ based controller
class Controller
{
public:

  typedef CProxy_CharmTask ProxyType;

  //! Default constructor
  Controller() {}

  //! Destructor
  ~Controller() {}

  //! Initialize the controller with the given graph
  ProxyType initialize(Payload buffer,TaskId size);

};



}
}


#endif



