#ifndef BFLOW_RED_RADIXKEXCHANGETASKMAP_H_
#define BFLOW_RED_RADIXKEXCHANGETASKMAP_H_


#include "../TaskGraph.h"
#include "RadixKExchange.h"


namespace BabelFlow 
{


class RadixKExchangeTaskMap : public TaskMap
{
public:

  //! Default constructor
  RadixKExchangeTaskMap() = default; // added to accommodate ascent interface
  
  //! Constructor
  RadixKExchangeTaskMap(ShardId controller_count, const RadixKExchange* task_graph);

  //! Destructor
  ~RadixKExchangeTaskMap() {}

  //! Return which controller is assigned to the given task
  virtual ShardId shard(TaskId id) const override;

  //! Return the set of task assigned to the given controller
  virtual std::vector<TaskId> tasks(ShardId id) const override;

private:

  //! The number of controllers
  ShardId mControllerCount;

  //! A reference to the task graph
  const RadixKExchange* mTaskGraph;
};

}   // end namespace BabelFlow

#endif /* BFLOW_RED_RADIXKEXCHANGETASKMAP_H_ */
