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

class KWayTaskMap : public DataFlow::TaskMap
{
public:

  //! Default constructor
  KWayTaskMap(DataFlow::ShardId controller_count,const KWayMerge* task_graph);

  //! Destructor
  ~KWayTaskMap() {}

  //! Return which controller is assigned to the given task
  virtual DataFlow::ShardId shard(DataFlow::TaskId id) const;

  //! Return the set of task assigned to the given controller
  virtual std::vector<DataFlow::TaskId> tasks(DataFlow::ShardId id) const;

private:

  //! The number of controllers
  DataFlow::ShardId mControllerCount;

  //! A reference to the task graph
  const KWayMerge* mTaskGraph;
};

#endif /* KWAYTASKMAP_H_ */
