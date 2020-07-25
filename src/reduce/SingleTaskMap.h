/*
 * SingleTaskMap.h
 *
 *  Created on: Jul 16, 2020
 *      Author: sshudler
 */

#ifndef BFLOW_RED_SINGLE_TASK_MAP_H_
#define BFLOW_RED_SINGLE_TASK_MAP_H_

#include <vector>

#include "../TaskGraph.h"
#include "SingleTaskGraph.h"


namespace BabelFlow
{


class SingleTaskMap : public TaskMap
{
public:

  //! Default constructor
  SingleTaskMap() = default; // added to accommodate Ascent interface
  
  //! Constructor
  SingleTaskMap(ShardId controller_count, const SingleTaskGraph* task_graph);

  //! Destructor
  ~SingleTaskMap() {}

  //! Return which controller is assigned to the given task
  virtual ShardId shard(TaskId id) const override;

  //! Return the set of task assigned to the given controller
  virtual std::vector<TaskId> tasks(ShardId id) const override;

private:

  //! The number of controllers
  ShardId mControllerCount;

  //! A reference to the task graph
  const SingleTaskGraph* mTaskGraph;
};

}   // end namespace BabelFlow


#endif    // #ifndef BFLOW_RED_SINGLE_TASK_GRAPH_H_
