/*
 * Controller.cpp
 *
 *  Created on: Dec 14, 2016
 *      Author: bremer5
 */

#include "Controller.h"

namespace DataFlow {
namespace charm {


CProxy_CharmTask Controller::initialize(Payload buffer, TaskId size)
{
  //fprintf(stderr,"Trying to create %d tasks\n", graph.size());

  // Create an array to hold all tasks
  ProxyType tasks = ProxyType::ckNew(buffer, size);

  // As per convention, this function now owns the buffer and must
  // free the memory
  delete[] buffer.buffer();

  return tasks;
}



}
}

