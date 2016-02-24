/*
 * KWayTaskMap.h
 *
 *  Created on: Dec 18, 2014
 *      Author: bremer5
 */

#ifndef KWAYTASKMAP_H_
#define KWAYTASKMAP_H_

//#include "TypeDefinitions.h"
#include "TaskGraph.h"

#include "KWayMerge.h"

class KWayTaskMap : public TaskMap
{
public:

  //! Default constructor
  KWayTaskMap(ShardId controller_count,const KWayMerge* task_graph);

  //! Destructor
  ~KWayTaskMap() {}

  //! Return which controller is assigned to the given task
  virtual ShardId shard(TaskId id) const;

  //! Return the set of task assigned to the given controller
  virtual std::vector<TaskId> tasks(ShardId id) const;

private:

  //! The number of controllers
  ShardId mControllerCount;

  //! A reference to the task graph
  const KWayMerge* mTaskGraph;
};

#endif /* KWAYTASKMAP_H_ */
