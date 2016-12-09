/*
 * CharmTaskGraph.h
 *
 *  Created on: Dec 8, 2016
 *      Author: bremer5
 */

#ifndef CHARMTASKGRAPH_H
#define CHARMTASKGRAPH_H

#include "TaskGraph.h"

namespace DataFlow {
namespace charm {


inline void operator|(PUP::er &p, DataFlow::TaskGraph& graph) {
  if (!p.isUnpacking()) {
    DataFlow::Payload buffer = graph.serialize();
    p(buffer.buffer(),buffer.size());

    delete[] buffer.buffer();
  }
  else {
    // First we serialize bogus data
    DataFlow::Payload buffer = graph.serialize();

    // TO have enough room to read in the actual data
    p(buffer.buffer(),buffer.size());
    graph.deserialize(buffer);
  }
}

}
}

#endif
