/*
 * ModuleMap.h
 *
 *  Created on: Dec 14, 2014
 *      Author: bremer5
 */

#ifndef MODULEMAP_H_
#define MODULEMAP_H_

#include "Definitions.h"
#include "TaskGraph.h"

class ModuloMap : public TaskMap
{
public:

  //! Default constructor
  ModuloMap(ShardId shard_count, TaskId task_count);

  //! Destructor
  virtual ~ModuloMap() {}

  //! Return which controller is assigned to the given task
  virtual ShardId shard(TaskId id) const;

  //! Return the set of task assigned to the given controller
  virtual std::vector<TaskId> tasks(ShardId id) const;

private:

  //! The total number of controllers
  ShardId mShardCount;

  //! The total number of task
  TaskId mTaskCount;

};


#endif /* MODULEMAP_H_ */
