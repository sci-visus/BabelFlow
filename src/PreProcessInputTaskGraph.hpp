#include <utility>

//
// Created by Li, Jixian on 2019-07-25.
//

#ifndef BABELFLOW_DOWNSIZEGHOSTTASKGRAPH_HPP
#define BABELFLOW_DOWNSIZEGHOSTTASKGRAPH_HPP

#include <TaskGraph.h>
#include <type_traits>
#include <map>

namespace BabelFlow {
  template<class BaseTaskGraph>
  class PreProcessInputTaskGraph : public TaskGraph {
    static_assert(std::is_base_of<TaskGraph, BaseTaskGraph>::value, "BaseTaskGraph must be a TaskGraph");
  public:
    ShardId n_controllers;
    BaseTaskGraph *mGraph;
    std::map<TaskId, ShardId> new_sids;
    std::map<TaskId, TaskId> new_tids;
    std::map<TaskId, uint64_t> new_gids;
    std::map<TaskId, Task> old_tasks;
    std::vector<std::vector<TaskId> > data_tasks;
    uint64_t maxGid = 0;
    TaskId maxTid = 0;
    CallbackId newCallBackId = 0;

    PreProcessInputTaskGraph(ShardId count, TaskGraph *g, TaskMap *m) :
      n_controllers(count), mGraph(g) {
      data_tasks.resize(n_controllers);
      for (ShardId sid = 0; sid < n_controllers; ++sid) {
        std::vector<Task> tasks = mGraph->localGraph(sid);
        for (auto &&task : tasks) {
          // update max
          uint64_t gid = mGraph->gid(task.id());
          if (gid > maxGid) maxGid = gid;
          TaskId tid = task.id();
          if (tid > maxTid) maxTid = tid;
          if (task.callback() >= newCallBackId) newCallBackId = task.callback() + 1;

          // record input tasks
          if (task.incoming().size() == 1 && task.incoming()[0] == TNULL) {
            // old task is a input task
            data_tasks[sid].push_back(tid);
            old_tasks[tid] = task;
          }
        }
      }

      auto next_tid = maxTid + 1;
      auto next_gid = maxGid + 1;
      // max ids are all set. prepare to add new tasks
      for (ShardId sid = 0; sid < n_controllers; ++sid) {
        for (auto &&dtask:data_tasks) {
          new_sids[next_tid] = sid;
          new_tids[dtask] = next_tid++;
          new_gids[dtask] = next_gid++;
        }
      }
    }

    std::vector<Task>
    localGraph(ShardId id, const TaskMap *task_map) const override { //// Let's try this without updated task_map
      //// may be we should just assume it's updated
      //// all we need to do is to define the callbacks
      std::vector<Task> olds = mGraph->localGraph(id, task_map);
      std::vector<Task> myTasks;
      for (auto &&old : olds) {
        if (old.incoming()[0] > maxTid) {
          auto ngid = new_gids[old.incoming()[0]];
          myTasks.push_back(task(ngid));
        }
      }
      olds.insert(olds.end(), myTasks.begin(), myTasks.end());
      return olds;
    }

    Task task(uint64_t gId) const override {
      Task task;
      std::vector<TaskId> incoming;
      std::vector<std::vector<TaskId>> outgoing;
      outgoing.resize(1);// only outputs to data task

      if (gId < maxGid) {
        task = mGraph->task(gId);
        task.incoming().resize(1);
        task.incoming()[0] = new_tids[task.id()];
      } else {
        TaskId old_tid = gid2otid(gId);
        task = Task(new_tids[old_tid]);
        task.incoming().resize(1);
        task.incoming()[0] = TNULL;
        outgoing[0].push_back(old_tid);
        task.outputs() = outgoing;
      }
      return task;
    }

    TaskId gid2otid(uint64_t gid) {
      for (auto iter = new_gids.begin(); iter != new_gids.end(); ++iter) {
        if (iter->second == gid) {
          return iter->first;
        }
      }
      return TNULL;
    }

    uint64_t gId(TaskId tId) const override {
      if (tId < maxTid) {
        return mGraph->gId(tId);
      } else {
        return maxGid + (tId - maxTid);
      }
    }

    TaskId size() const override {
      return mGraph->size() + new_tids.size();
    }

    template<class K, class V>
    Payload serializeMap(std::map<K, V> m) {
      int32_t buffer_size = sizeof(size_t) + m.size() * sizeof(K) + m.size() * sizeof(V);
      char *buffer = new char[buffer_size];

      auto *count_ptr = reinterpret_cast<size_t *>(buffer);
      count_ptr[0] = m.size();

      auto *key_ptr = reinterpret_cast<K *>(buffer + sizeof(size_t));
      auto *value_ptr = reinterpret_cast<V *>(buffer + sizeof(size_t) + sizeof(K) * m.size());

      size_t count = 0;
      for (auto iter = m.begin(); iter != m.end(); ++iter) {
        key_ptr[count] = iter->first;
        value_ptr[count] = iter->second;
        count++;
      }
      return Payload(buffer_size, buffer);
    }

    template<class K, class V>
    std::map<K, V> deserializeMap(char *buffer) {
      std::map<K, V> map_obj;
      size_t num_element = reinterpret_cast<size_t *>(buffer)[0];
      auto *key_ptr = reinterpret_cast<K *>(buffer + sizeof(size_t));
      auto *value_ptr = reinterpret_cast<V *>(buffer + sizeof(size_t) + sizeof(K) * num_element);
      for (size_t i = 0; i < num_element; ++i) {
        map_obj[key_ptr[i]] = value_ptr[i];
      }
      return map_obj;
    }

    Payload serialize() const override {
      Payload old = mGraph->serialize();
      Payload p_tids = serializeMap<TaskId, TaskId>(new_tids);
      Payload p_gids = serializeMap<TaskId, uint64_t>(new_gids);
      Payload p_sids = serializeMap<TaskId, ShardId>(new_sids);

      // merge all the payloads
      // header = |CallbackId|maxTid|maxGid|p_tids.offset|p_gids.offset|p_sids.offset|old.offset|
      int32_t header_size = sizeof(CallbackId) + sizeof(TaskId) + sizeof(uint64_t) + sizeof(size_t) * 4;
      int32_t buffer_size = header_size + p_tids.size() + p_gids.size() + p_sids.size() + old.size();

      char *buffer = new char[buffer_size];

      size_t offset = 0;
      memcpy(buffer + offset, &newCallBackId, sizeof(CallbackId));
      offset += sizeof(CallbackId);
      memcpy(buffer + offset, &maxTid, sizeof(TaskId));
      offset += sizeof(TaskId);
      memcpy(buffer + offset, &maxGid, sizeof(uint64_t));
      offset += sizeof(uint64_t);

      // compute tid offset;
      size_t tid_offset = header_size;
      memcpy(buffer + offset, &tid_offset, sizeof(size_t));
      offset += sizeof(size_t);

      // compute gid offset;
      size_t gid_offset = tid_offset + p_tids.size();
      memcpy(buffer + offset, &gid_offset, sizeof(size_t));
      offset += sizeof(size_t);

      // compute sid offset;
      size_t sid_offset = gid_offset + p_gids.size();
      memcpy(buffer + offset, &sid_offset, sizeof(size_t));
      offset += sizeof(size_t);

      // compute old offset;
      size_t old_offset = sid_offset + p_sids.size();
      memcpy(buffer + offset, &old_offset, sizeof(size_t));

      memcpy(buffer + tid_offset, p_tids.buffer(), p_tids.size());
      memcpy(buffer + gid_offset, p_gids.buffer(), p_gids.size());
      memcpy(buffer + sid_offset, p_sids.buffer(), p_sids.size());
      memcpy(buffer + old_offset, old.buffer(), old.size());

      delete[] p_tids.buffer();
      delete[] p_gids.buffer();
      delete[] p_sids.buffer();
      delete[] old.buffer();

      return Payload(buffer_size, buffer);
    }

    void deserialize(Payload payload) override {
      char *buffer = payload.buffer();
      // deserialize header
      // header = |CallbackId|maxTid|maxGid|p_tids.offset|p_gids.offset|p_sids.offset|old.offset|
      size_t tid_offset, gid_offset, sid_offset, old_offset;
      size_t offset = 0;
      memcpy(&newCallBackId, buffer + offset, sizeof(CallbackId));
      offset += sizeof(CallbackId);
      memcpy(&maxTid, buffer + offset, sizeof(TaskId));
      offset += sizeof(TaskId);
      memcpy(&maxGid, buffer + offset, sizeof(uint64_t));
      offset += sizeof(uint64_t);
      memcpy(&tid_offset, buffer + offset, sizeof(size_t));
      offset += sizeof(size_t);
      memcpy(&gid_offset, buffer + offset, sizeof(size_t));
      offset += sizeof(size_t);
      memcpy(&sid_offset, buffer + offset, sizeof(size_t));
      offset += sizeof(size_t);
      memcpy(&old_offset, buffer + offset, sizeof(size_t));
      // deserialize maps
      new_tids = deserializeMap(buffer + tid_offset);
      new_gids = deserializeMap(buffer + gid_offset);
      new_sids = deserializeMap(buffer + sid_offset);
      // deserialize old - build Payload first
      Payload old(payload.size() - old_offset, buffer + old_offset);
      mGraph->deserialize(old);
    }

  };
}


#endif //BABELFLOW_DOWNSIZEGHOSTTASKGRAPH_HPP
