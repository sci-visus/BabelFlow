/*
 * KWayReductionTaskMap.h
 *
 *  Created on: Aug 30, 2016
 *      Author: spetruzza
 */

#ifndef KWAYREDCUTIONTASKMAP_H_
#define KWAYREDCUTIONTASKMAP_H_

//#include "TypeDefinitions.h"
#include "../TaskGraph.h"
#include "KWayReduction.h"


namespace BabelFlow 
{

class KWayReductionTaskMap : public TaskMap
{
public:

  //! Default constructor
  KWayReductionTaskMap()=default; // added to accommodate ascent interface
  
  KWayReductionTaskMap(ShardId controller_count, const KWayReduction* task_graph);

  //! Destructor
  ~KWayReductionTaskMap() {}

  //! Return which controller is assigned to the given task
  virtual ShardId shard(TaskId id) const;

  //! Return the set of task assigned to the given controller
  virtual std::vector<TaskId> tasks(ShardId id) const;

private:

  //! The number of controllers
  ShardId mControllerCount;

  //! A reference to the task graph
  const KWayReduction* mTaskGraph;
};

}   // namespace BabelFlow

#endif /* KWAYTASKMAP_H_ */
