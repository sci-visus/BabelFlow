//
// Created by Jixian Li on 2019-07-27.
//

#ifndef BABELFLOW_DONOTHINGTASKGRAPH_H
#define BABELFLOW_DONOTHINGTASKGRAPH_H

#include <TaskGraph.h>

class DoNothingTaskGraph : public BabelFlow::TaskGraph {
private:
  BabelFlow::ShardId n_controllers;
public:
  DoNothingTaskGraph(BabelFlow::ShardId count);

  std::vector<BabelFlow::Task> localGraph(BabelFlow::ShardId id, const BabelFlow::TaskMap *task_map) const override;

  BabelFlow::Task task(uint64_t gId) const override;

  uint64_t gId(BabelFlow::TaskId tId) const override;

  BabelFlow::TaskId size() const override;

  BabelFlow::Payload serialize() const override;

  void deserialize(BabelFlow::Payload buffer) override;
};


#endif //BABELFLOW_DONOTHINGTASKGRAPH_H