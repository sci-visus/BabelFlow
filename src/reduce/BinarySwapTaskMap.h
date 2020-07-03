/*
 * BynarySwapTaskMap.h
 *
 *  Created on: Sep 3, 2016
 *      Author: spetruzza
 */

#ifndef BFLOW_VLR_BINARYSWAPTASKMAP_H_
#define BFLOW_VLR_BINARYSWAPTASKMAP_H_


#include "../TaskGraph.h"
#include "BinarySwap.h"


namespace BabelFlow 
{


class BinarySwapTaskMap : public TaskMap
{
public:

  //! Default constructor
  BinarySwapTaskMap() = default; // added to accommodate ascent interface
  
  //! Constructor
  BinarySwapTaskMap(ShardId controller_count,const BinarySwap* task_graph);

  //! Destructor
  ~BinarySwapTaskMap() {}

  //! Return which controller is assigned to the given task
  virtual ShardId shard(TaskId id) const;

  //! Return the set of task assigned to the given controller
  virtual std::vector<TaskId> tasks(ShardId id) const;

private:

  //! The number of controllers
  ShardId mControllerCount;

  //! A reference to the task graph
  const BinarySwap* mTaskGraph;
};


}   // end namespace BabelFlow

#endif /* BFLOW_VLR_BINARYSWAPTASKMAP_H_ */
