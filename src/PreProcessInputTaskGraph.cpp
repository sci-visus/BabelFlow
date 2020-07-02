#include "PreProcessInputTaskGraph.hpp"


using namespace BabelFlow;


PreProcessInputTaskGraph::PreProcessInputTaskGraph( ShardId count, TaskGraph *g, TaskMap *m )
 : n_controllers(count), mGraph(g)
{
  data_tasks.resize(n_controllers);
  for (ShardId sid = 0; sid < n_controllers; ++sid) 
  {
    std::vector<Task> tasks = mGraph->localGraph(sid, m);
    for (auto &&task : tasks) 
    {
      // update max
      uint64_t gid = mGraph->gId(task.id());
      if (gid > maxGid) maxGid = gid;
      TaskId tid = task.id();
      if (tid > maxTid) maxTid = tid;
      if (task.callback() >= newCallBackId) newCallBackId = task.callback() + 1;

      // record input tasks
      if (task.incoming().size() == 1 && task.incoming()[0] == TNULL) 
      {
        // old task is a input task
        data_tasks[sid].push_back(tid);
        old_tasks[tid] = task;
        old_g2t[mGraph->gId(tid)] = tid;
      }
    }
  }

  auto next_tid = maxTid + 1;
  auto next_gid = maxGid + 1;
  // max ids are all set. prepare to add new tasks
  for (ShardId sid = 0; sid < n_controllers; ++sid) 
  {
    for (auto &&dtask:data_tasks[sid]) 
    {
      new_sids[next_tid] = sid;
      new_tids[dtask] = next_tid++;
      new_gids[dtask] = next_gid++;
    }
  }
}

std::vector<Task> PreProcessInputTaskGraph::localGraph(ShardId id, const TaskMap *task_map) const
{
  // here we assume the task_map is updated (using ModTaskMap)
  std::vector<TaskId> tids = task_map->tasks(id);
  std::vector<Task> tasks(tids.size());
  for (int i = 0; i < tids.size(); ++i)
    tasks[i] = task(gId(tids[i]));

  return tasks;
}

Task PreProcessInputTaskGraph::task(uint64_t gId) const
{
  if (gId <= maxGid) // old task
  {
    auto iter = old_g2t.find(gId);
    if (iter == old_g2t.end()) 
    {
      return mGraph->task(gId);
    } 
    else 
    {
      Task t = mGraph->task(gId);
      t.incoming()[0] = new_tids.at(iter->second);
      return t;
    }
  } 
  else 
  { // new Tasks
    auto old_tid = gid2otid(gId);
    auto new_tid = new_tids.at(old_tid);
    Task t(new_tid);
    t.incoming().resize(1);
    t.incoming()[0] = TNULL;
    t.outputs().resize(1);
    t.outputs()[0].resize(1);
    t.outputs()[0][0] = old_tid;
    t.callback(newCallBackId);
    return t;
  }
}

TaskId PreProcessInputTaskGraph::gid2otid(const uint64_t &gid) const
{
  for (auto iter = new_gids.begin(); iter != new_gids.end(); ++iter) 
  {
    if (iter->second == gid)
      return iter->first;
  }
  return TNULL;
}

template<class K, class V>
Payload PreProcessInputTaskGraph::serializeMap(const std::map<K, V> &m) const
{
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
std::map<K, V> PreProcessInputTaskGraph::deserializeMap(char *buffer)
{
  std::map<K, V> map_obj;
  size_t num_element = reinterpret_cast<size_t *>(buffer)[0];
  auto *key_ptr = reinterpret_cast<K *>(buffer + sizeof(size_t));
  auto *value_ptr = reinterpret_cast<V *>(buffer + sizeof(size_t) + sizeof(K) * num_element);
  for (size_t i = 0; i < num_element; ++i) {
    map_obj[key_ptr[i]] = value_ptr[i];
  }
  return map_obj;
}

Payload PreProcessInputTaskGraph::serialize() const
{
  Payload old = mGraph->serialize();
  Payload p_tids = serializeMap<TaskId, TaskId>(new_tids);
  Payload p_gids = serializeMap<TaskId, uint64_t>(new_gids);
  Payload p_sids = serializeMap<TaskId, ShardId>(new_sids);
  Payload p_og2t = serializeMap<uint64_t, TaskId>(old_g2t);

  // merge all the payloads
  // header = |CallbackId|maxTid|maxGid|p_tids.offset|p_gids.offset|p_sids.offset|p_og2t.offset|old.offset|
  int32_t header_size = sizeof(CallbackId) + sizeof(TaskId) + sizeof(uint64_t) + sizeof(size_t) * 5;
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

  // compute old_g2t offset;
  size_t og2t_offset = sid_offset + p_og2t.size();
  memcpy(buffer + offset, &og2t_offset, sizeof(size_t));
  offset += sizeof(size_t);

  // compute old offset;
  size_t old_offset = og2t_offset + p_sids.size();
  memcpy(buffer + offset, &old_offset, sizeof(size_t));

  memcpy(buffer + tid_offset, p_tids.buffer(), p_tids.size());
  memcpy(buffer + gid_offset, p_gids.buffer(), p_gids.size());
  memcpy(buffer + sid_offset, p_sids.buffer(), p_sids.size());
  memcpy(buffer + og2t_offset, p_og2t.buffer(), p_og2t.size());
  memcpy(buffer + old_offset, old.buffer(), old.size());

  delete[] p_tids.buffer();
  delete[] p_gids.buffer();
  delete[] p_sids.buffer();
  delete[] p_og2t.buffer();
  delete[] old.buffer();

  return Payload(buffer_size, buffer);
}

void PreProcessInputTaskGraph::deserialize(Payload payload)
{
  char *buffer = payload.buffer();
  // deserialize header
  // header = |CallbackId|maxTid|maxGid|p_tids.offset|p_gids.offset|p_sids.offset|p_og2t.offset|old.offset|
  size_t tid_offset, gid_offset, sid_offset, og2t_offset, old_offset;
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
  memcpy(&og2t_offset, buffer + offset, sizeof(size_t));
  offset += sizeof(size_t);
  memcpy(&old_offset, buffer + offset, sizeof(size_t));
  // deserialize maps
  new_tids = deserializeMap<TaskId, TaskId>(buffer + tid_offset);
  new_gids = deserializeMap<TaskId, uint64_t>(buffer + gid_offset);
  new_sids = deserializeMap<TaskId, ShardId>(buffer + sid_offset);
  old_g2t = deserializeMap<uint64_t, TaskId>(buffer + og2t_offset);
  // deserialize old - build Payload first
  Payload old(payload.size() - old_offset, buffer + old_offset);
  mGraph->deserialize(old);

  delete[] payload.buffer();
}
