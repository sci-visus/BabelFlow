/*
 * CharmTaskGraph.h
 *
 *  Created on: Dec 5, 2016
 *      Author: bremer5
 */

#ifndef CHARM_CHARMTASKGRAPH_H_
#define CHARM_CHARMTASKGRAPH_H_

#include "charm++.h"

#include "TaskGraph.h"

namespace DataFlow {
namespace charm {

class CharmTaskGraphBase
{
public:

  //! The constructor
  CharmTaskGraphBase(std::string config = "") : mConfig(config) {}

  virtual ~CharmTaskGraphBase() {}

  virtual TaskGraph* buildGraph() const {return NULL;}

  void pup(PUP::er &p) {p|mConfig;}

protected:

  //! The configuration string that can build an instance
  std::string mConfig;
};



template<class TaskGraphType>
class CharmTaskGraph : public CharmTaskGraphBase
{
public:

  //! The constructor
  CharmTaskGraph(std::string config) : CharmTaskGraphBase(config) {}

  virtual ~CharmTaskGraph() {}

};

}
}

#endif /* CHARM_CHARMTASKGRAPH_H_ */
