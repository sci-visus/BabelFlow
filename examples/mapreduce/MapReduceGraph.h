//
// Created by Li, Jixian on 2019-05-28.
//

#ifndef BABELFLOW_MAPREDUCEGRAPH_H
#define BABELFLOW_MAPREDUCEGRAPH_H


#include <TaskGraph.h>

enum CallBackTypes
{
  SPLIT_LOAD=1,
  MAP_FUNC=2,
  RED_FUNC=3,
  PRINT_FUNC=4
};

class MapReduceGraph : public BabelFlow::TaskGraph
{
public:
  explicit MapReduceGraph(int n_worker) : n_worker(n_worker)
  {}

  ~MapReduceGraph() override = default;

  std::vector<BabelFlow::Task> localGraph(BabelFlow::ShardId id, const BabelFlow::TaskMap *task_map) const override;

  BabelFlow::Task task(uint64_t gId) const override;

  uint64_t gId(BabelFlow::TaskId tid) const override
  { return tid; }

  uint32_t size() const override { return n_worker + 3; };

  BabelFlow::Payload serialize() const override;

  void deserialize(BabelFlow::Payload buffer) override;

private:
  int n_worker;
};


#endif //BABELFLOW_MAPREDUCEGRAPH_H
