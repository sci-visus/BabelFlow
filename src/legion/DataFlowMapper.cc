
#include <cstdio>
#include <cassert>
#include <cstdlib>
#include "DataFlowMapper.h"
#include "Utils.h"
#include "default_mapper.h"
#include "test_mapper.h"
#include "Controller.h"

using namespace LegionRuntime::HighLevel;
using namespace LegionRuntime::Accessor;
using namespace LegionRuntime::Arrays;

bool debug = false;

using namespace Legion::Mapping;

static int curr_proc = 0;
#if 1
DataFlowMapper::DataFlowMapper(Machine m,
                                     HighLevelRuntime *rt, Processor p)
  : DefaultMapper(rt->get_mapper_runtime(), m, p){}
#else
//ShimMapper(m, rt, rt->get_mapper_runtime(), p)//DefaultMapper(rt->get_mapper_runtime(), m, p)
{

  std::set<Processor> all_procs;
  machine.get_all_processors(all_procs);
  //  printf("There are %ld processors:\n", all_procs.size());
  
  for (std::set<Processor>::const_iterator it = all_procs.begin(); it != all_procs.end(); it++){
    Processor::Kind k = it->kind();	// Differentiate CPU and GPU processors
    switch (k){
      case Processor::LOC_PROC:		// If CPU (Latency Optimized Core)
	//        printf("adding CPU\n");
        all_cpus.push_back(*it);
        break;
      case Processor::TOC_PROC:		// If GPU (Throughput Optimized Core)
	//  printf("found GPU\n");
        break;
      default:
        //printf("found something else...\n");// Something else...?
        break;
    }
  }
  
  std::set<Processor>::iterator iter = all_procs.begin();	// Step through all processors
  //iter++;												// Skip the first one (used for main loop)
  for(iter++; iter != all_procs.end();iter++){		// Add rest to a list of available processors for mapping
    task_procs.insert(*iter);
  }
  {
    for (std::vector<Processor>::iterator itr = all_cpus.begin(); itr != all_cpus.end(); ++itr){
      Machine::MemoryQuery visible_memories(machine);
      visible_memories.has_affinity_to(*itr);
      Memory sysmem = visible_memories.only_kind(Memory::SYSTEM_MEM).first();
      //Memory sysmem = machine_interface.find_memory_kind(*itr, Memory::SYSTEM_MEM);	// Find the relevant memories
      all_sysmems[*itr] = sysmem;
    }
  }
  // Initialize our random number generator
  const size_t short_bits = 8*sizeof(unsigned short);
  long long short_mask = 0;
  for (unsigned i = 0; i < short_bits; i++)
    short_mask |= (1LL << i);
  for (int i = 0; i < 3; i++)
    random_number_generator[i] = (unsigned short)((local_proc.id & 
						   (short_mask << (i*short_bits))) >> (i*short_bits));
}
#endif

#if 0
bool DataFlowMapper::default_make_instance(MapperContext ctx, 
					  Memory target_memory, const LayoutConstraintSet &constraints,
					   Legion::Mapping::PhysicalInstance &result, MappingKind kind, bool force_new, bool meets, 
					  const RegionRequirement &req)
//--------------------------------------------------------------------------
{
  bool created = true;
  LogicalRegion target_region = 
    default_policy_select_instance_region(ctx, target_memory, req,
                                          constraints, force_new, meets);
  // TODO: deal with task layout constraints that require multiple
  // region requirements to be mapped to the same instance
  std::vector<LogicalRegion> target_regions(1, target_region);
  if (force_new || (req.privilege == REDUCE && (kind != COPY_MAPPING))) {
    if (!runtime->create_physical_instance(ctx, target_memory, 
          constraints, target_regions, result))
      return false;
  } else {
    if (!runtime->find_or_create_physical_instance(ctx, 
          target_memory, constraints, target_regions, result, created))
      return false;
  }
  if (created)
  {
    int priority = default_policy_select_garbage_collection_priority(ctx, 
            kind, target_memory, result, meets, (req.privilege == REDUCE));
    if (priority != 0)
      runtime->set_garbage_collection_priority(ctx, result,priority);
  }
  return true;
}

bool DataFlowMapper::default_create_custom_instances(MapperContext ctx,
						    Processor target_proc, Memory target_memory,
						    const RegionRequirement &req, unsigned index, 
						    std::set<FieldID> &needed_fields,
						    const TaskLayoutConstraintSet &layout_constraints,
						    bool needs_field_constraint_check,
						     std::vector<Legion::Mapping::PhysicalInstance> &instances)
//--------------------------------------------------------------------------
{
  // Before we do anything else figure out our 
  // constraints for any instances of this task, then we'll
  // see if these constraints conflict with or are satisfied by
  // any of the other constraints

  //fprintf(stderr, "ENTERING create CUSTOM instances<<<<<<<<<<<<<<<<\n");
  bool force_new_instances = false;
      LayoutConstraintID our_layout_id = 
	default_policy_select_layout_constraints(ctx, target_memory, req,
						 TASK_MAPPING, needs_field_constraint_check, force_new_instances);
      const LayoutConstraintSet &our_constraints = 
	runtime->find_layout_constraints(ctx, our_layout_id);
      for (std::multimap<unsigned,LayoutConstraintID>::const_iterator lay_it =
	     layout_constraints.layouts.lower_bound(index); lay_it !=
	     layout_constraints.layouts.upper_bound(index); lay_it++)
	{
	  // Get the constraints
        const LayoutConstraintSet &index_constraints = 
	  runtime->find_layout_constraints(ctx, lay_it->second);
        std::vector<FieldID> overlaping_fields;
        const std::vector<FieldID> &constraint_fields = 
          index_constraints.field_constraint.get_field_set();
        for (unsigned idx = 0; idx < constraint_fields.size(); idx++)
	  {
	    FieldID fid = constraint_fields[idx];
	    std::set<FieldID>::iterator finder = needed_fields.find(fid);
	    if (finder != needed_fields.end())
	      {
		overlaping_fields.push_back(fid);
		// Remove from the needed fields since we're going to handle it
		needed_fields.erase(finder);
	      }
	  }
        // If we don't have any overlapping fields, then keep going
        if (overlaping_fields.empty())
          continue;
        // Now figure out how to make an instance
        instances.resize(instances.size()+1);
        // Check to see if these constraints conflict with our constraints
        if (runtime->do_constraints_conflict(ctx, 
						    our_layout_id, lay_it->second))
	  {
	    // They conflict, so we're just going to make an instance
	    // using these constraints
	    if (!default_make_instance(ctx, target_memory, index_constraints,
				       instances.back(), TASK_MAPPING, force_new_instances, 
				       false/*meets*/, req))
	      return false;
	  }
        else if (runtime->do_constraints_entail(ctx, 
						       lay_it->second, our_layout_id))
	  {
	    // These constraints do everything we want to do and maybe more
	    // so we can just use them directly
	    if (!default_make_instance(ctx, target_memory, index_constraints,
				       instances.back(), TASK_MAPPING, force_new_instances, 
				       true/*meets*/, req))
	      return false;
	  }
        else
	  {
	    // These constraints don't do as much as we want but don't
	    // conflict so make an instance with them and our constraints 
	    LayoutConstraintSet creation_constraints = index_constraints;
	    default_policy_select_constraints(ctx, creation_constraints, 
					      target_memory, req);
	    creation_constraints.add_constraint(
						FieldConstraint(overlaping_fields,
								false/*contig*/, false/*inorder*/));
	    if (!default_make_instance(ctx, target_memory, creation_constraints,
				       instances.back(), TASK_MAPPING, force_new_instances, 
				       true/*meets*/, req))
	      return false;
	  }
	}
      // If we don't have anymore needed fields, we are done
      if (needed_fields.empty())
        return true;
      // There are no constraints for these fields so we get to do what we want
      instances.resize(instances.size()+1);
      LayoutConstraintSet creation_constraints = our_constraints;
      creation_constraints.add_constraint(
					  FieldConstraint(needed_fields, false/*contig*/, false/*inorder*/));
      if (!default_make_instance(ctx, target_memory, creation_constraints, 
				 instances.back(), TASK_MAPPING, force_new_instances, 
				 true/*meets*/,  req))
        return false;
      return true;
}


void DataFlowMapper::select_task_options(const MapperContext    ctx,
					 const Task&            task,
					 TaskOptions&     output)//Task *task)
{

  output.initial_proc = default_policy_select_initial_processor(ctx, task);
  output.inline_task = false;
  output.stealable = false;//stealing_enabled; 
  output.map_locally = true;
  
  /*
  task->inline_task = false;
  task->spawn_task = false;
  task->map_locally = false;
  task->profile_task = false;
  task->task_priority = 0;
  */  
/*
  if(task->get_depth()==0){	// If we're on the top-level task
    task->target_proc = local_proc; // Define it to the local processor
  }
  else {*/
//    std::set<Processor> all_procs;
//    machine.get_all_processors(all_procs);
/* 
 Machine::ProcessorQuery random(machine);
  random.only_kind(Processor::LOC_PROC);
  int chosen = nrand48(random_number_generator) % random.count();//generate_random_integer() % random.count();
  fprintf(stderr,"mapping on proc %d out of %d\n", chosen, random.count());
  Machine::ProcessorQuery::iterator it = random.begin();
  for (int idx = 0; idx < chosen; idx++) it++;
  task.target_proc = (*it);
*/  
//task->target_proc = (*it);

  //task->target_proc = DefaultMapper::select_random_processor(task_procs);//,                                                          
    // Processor::LOC_PROC, machine);//all_cpus[curr_proc];//DefaultMapper::select_random_processor(task_procs,
    //                                              Processor::LOC_PROC, machine);
    //    }
}

/*
void DataFlowMapper::slice_domain(const Task *task, const Domain &domain,
                                     std::vector<DomainSplit> &slices)
{
//  std::set<Processor> all_procs;
//  machine.get_all_processors(all_procs);
  
  printf("domain to slice\n");
  Utils::printDomain(domain);

  std::vector<Processor> split_set;	// Find all processors to split on
  for (unsigned idx = 0; idx < 2; idx++){ // Add the approriate number for a binary decomposition
    split_set.push_back(DefaultMapper::select_random_processor(task_procs, Processor::LOC_PROC, machine));
  }
  
  DefaultMapper::decompose_index_space(domain, split_set,1, slices); // Split the index space on colors
  for (std::vector<DomainSplit>::iterator it = slices.begin(); it != slices.end(); it++){
    Rect<1> rect = it->domain.get_rect<1>(); // Step through colors and indicate recursion or not

    if (rect.volume() == 1) // Stop recursing when only one task remains
      it->recurse = false;
    else{
      it->recurse = true;
      //printf("volume %d\n", rect.volume());
    }
  }
}

*/

 /*
bool DataFlowMapper::map_task(Task *task)
{
  //  printf("map_task\n");
 
  Memory sys_mem = all_sysmems[task->target_proc];
  assert(sys_mem.exists());
  
  for (unsigned idx = 0; idx < task->regions.size(); idx++){
    task->regions[idx].target_ranking.push_back(sys_mem);
    task->regions[idx].virtual_map = false;
    task->regions[idx].enable_WAR_optimization = true;
    task->regions[idx].reduction_list = false;
    // Make everything SOA
     task->regions[idx].blocking_factor = task->regions[idx].max_blocking_factor;
   }
  
    /*
   std::set<Memory> vis_mems;
   machine.get_visible_memories(task->target_proc, vis_mems);
   assert(!vis_mems.empty());
   for (unsigned idx = 0; idx < task->regions.size(); idx++)
   {
   std::set<Memory> mems_copy = vis_mems;
   // Assign memories in a random order
   while (!mems_copy.empty())
   {
   unsigned mem_idx = (lrand48() % mems_copy.size());
   std::set<Memory>::iterator it = mems_copy.begin();
   for (unsigned i = 0; i < mem_idx; i++)
   it++;
   task->regions[idx].target_ranking.push_back(*it);
   mems_copy.erase(it);
   }
   task->regions[idx].virtual_map = false;
   task->regions[idx].enable_WAR_optimization = false;
   task->regions[idx].reduction_list = false;
   task->regions[idx].blocking_factor = 1;
   task->regions[idx].blocking_factor = task->regions[idx].max_blocking_factor;
   }*/
  // Report successful mapping results

//}
#endif
#if 1
/*
   void DataFlowMapper::premap_task(const MapperContext      ctx,
                                    const Task&              task, 
                                    const PremapTaskInput&   input,
                                          PremapTaskOutput&  output)
    //--------------------------------------------------------------------------
    {}*/
/*
bool DataFlowMapper::pre_map_task(Task *task){
       bool all_virtual = false;
      TaskInfo info;
      if(task->arglen == sizeof(TaskInfo))
        info = *(TaskInfo*)task->args;
      else{
        info = (*(MetaDataFlow*)task->args).info;
        all_virtual = true;
      }

    for(unsigned idx = 0; idx < task->regions.size(); idx++){
      
	task->regions[idx].virtual_map = (idx >= info.lenInput || all_virtual) ;
	// task->regions[idx].early_map = true;
      task->regions[idx].enable_WAR_optimization = false;
      task->regions[idx].reduction_list = false;
      task->regions[idx].make_persistent = false;
      task->regions[idx].blocking_factor = task->regions[idx].max_blocking_factor;
    }
}
*/

 void DataFlowMapper::map_task(const MapperContext      ctx,
                                 const Task&              task,
                                 const MapTaskInput&      input,
                                       MapTaskOutput&     output)
    //--------------------------------------------------------------------------
    {
      if(debug)
	     fprintf(stderr,"Custom map_task in %s\n", get_mapper_name());
     
      Processor::Kind target_kind = task.target_proc.kind();
      // Get the variant that we are going to use to map this task
      VariantInfo chosen = default_find_preferred_variant(task, ctx,
                        true/*needs tight bound*/, true/*cache*/, target_kind);
      output.chosen_variant = chosen.variant;
      // TODO: some criticality analysis to assign priorities
      output.task_priority = 0;
      output.postmap_task = false;
      // Figure out our target processors
      default_policy_select_target_processors(ctx, task, output.target_procs);

      bool all_virtual = false;

      TaskInfo info;
      if(task.arglen == sizeof(TaskInfo))
        info = *(TaskInfo*)task.args;
      else if(task.arglen == sizeof(MetaDataFlow)){
        info = (*(MetaDataFlow*)task.args).info;
        all_virtual = true;
      }

      if(debug)
       fprintf(stderr, "MAP_TASK regions %d input %d outputs %d\n", task.regions.size(), info.lenInput, info.lenOutput);

      // See if we have an inner variant, if we do virtually map all the regions
      // We don't even both caching these since they are so simple
      // See if we have an inner variant, if we do virtually map all the regions
      // We don't even both caching these since they are so simple
      if (chosen.is_inner)
      {
        // Check to see if we have any relaxed coherence modes in which
        // case we can no longer do virtual mappings so we'll fall through
        bool has_relaxed_coherence = false;
        for (unsigned idx = 0; idx < task.regions.size(); idx++)
        {
          if (task.regions[idx].prop != EXCLUSIVE)
          {
            has_relaxed_coherence = true;
            break;
          }
        }
        if (!has_relaxed_coherence)
        {
          std::vector<unsigned> reduction_indexes;
          for (unsigned idx = 0; idx < task.regions.size(); idx++)
          {
            // As long as this isn't a reduction-only region requirement
            // we will do a virtual mapping, for reduction-only instances
            // we will actually make a physical instance because the runtime
            // doesn't allow virtual mappings for reduction-only privileges
            if (task.regions[idx].privilege == REDUCE)
              reduction_indexes.push_back(idx);
            else
              output.chosen_instances[idx].push_back(
                  Legion::Mapping::PhysicalInstance::get_virtual_instance());
          }
          if (!reduction_indexes.empty())
          {
            const TaskLayoutConstraintSet &layout_constraints =
                runtime->find_task_layout_constraints(ctx,
                                      task.task_id, output.chosen_variant);
            Memory target_memory = default_policy_select_target_memory(ctx, 
                                                         task.target_proc);
            for (std::vector<unsigned>::const_iterator it = 
                  reduction_indexes.begin(); it != 
                  reduction_indexes.end(); it++)
            {
              std::set<FieldID> copy = task.regions[*it].privilege_fields;
              if (!default_create_custom_instances(ctx, task.target_proc,
                  target_memory, task.regions[*it], *it, copy, 
                  layout_constraints, false/*needs constraint check*/, 
                  output.chosen_instances[*it]))
              {
                default_report_failed_instance_creation(task, *it, 
                                            task.target_proc, target_memory);
              }
            }
          }
          return;
        }
      }
      // First, let's see if we've cached a result of this task mapping
      const unsigned long long task_hash = compute_task_hash(task);
      std::pair<TaskID,Processor> cache_key(task.task_id, task.target_proc);
      std::map<std::pair<TaskID,Processor>,
               std::list<CachedTaskMapping> >::const_iterator 
        finder = cached_task_mappings.find(cache_key);
      // This flag says whether we need to recheck the field constraints,
      // possibly because a new field was allocated in a region, so our old
      // cached physical instance(s) is(are) no longer valid
      bool needs_field_constraint_check = false;
      Memory target_memory = default_policy_select_target_memory(ctx, 
                                                         task.target_proc);
      if (finder != cached_task_mappings.end())
      {
        bool found = false;
        bool has_reductions = false;
        // Iterate through and see if we can find one with our variant and hash
        for (std::list<CachedTaskMapping>::const_iterator it = 
              finder->second.begin(); it != finder->second.end(); it++)
        {
          if ((it->variant == output.chosen_variant) &&
              (it->task_hash == task_hash))
          {
            // Have to copy it before we do the external call which 
            // might invalidate our iterator
            output.chosen_instances = it->mapping;
            has_reductions = it->has_reductions;
            found = true;
            break;
          }
        }
        if (found)
        {
          // If we have reductions, make those instances now since we
          // never cache the reduction instances
          if (has_reductions)
          {
            const TaskLayoutConstraintSet &layout_constraints =
              runtime->find_task_layout_constraints(ctx,
                                  task.task_id, output.chosen_variant);
            for (unsigned idx = 0; idx < task.regions.size(); idx++)
            {
              if(idx >= info.lenInput || all_virtual) {
                if(info.callbackID > 0){
                  if(debug)
                    fprintf(stderr,"\tTask %d Virtual mapping region %d\n", info.id, idx);

                  output.chosen_instances[idx].push_back(
                      Legion::Mapping::PhysicalInstance::get_virtual_instance());	
                  continue;
                }
              }

              if (task.regions[idx].privilege == REDUCE)
              {
                std::set<FieldID> copy = task.regions[idx].privilege_fields;
                if (!default_create_custom_instances(ctx, task.target_proc,
                    target_memory, task.regions[idx], idx, copy, 
                    layout_constraints, needs_field_constraint_check, 
                    output.chosen_instances[idx]))
                {
                  default_report_failed_instance_creation(task, idx, 
                                              task.target_proc, target_memory);
                }
              }
            }
          }
          // See if we can acquire these instances still
          if (runtime->acquire_and_filter_instances(ctx, 
                                                     output.chosen_instances))
            return;
          // We need to check the constraints here because we had a
          // prior mapping and it failed, which may be the result
          // of a change in the allocated fields of a field space
          needs_field_constraint_check = true;
          // If some of them were deleted, go back and remove this entry
          // Have to renew our iterators since they might have been
          // invalidated during the 'acquire_and_filter_instances' call
          default_remove_cached_task(ctx, output.chosen_variant,
                        task_hash, cache_key, output.chosen_instances);
        }
      }
      // We didn't find a cached version of the mapping so we need to 
      // do a full mapping, we already know what variant we want to use
      // so let's use one of the acceleration functions to figure out
      // which instances still need to be mapped.
      std::vector<std::set<FieldID> > missing_fields(task.regions.size());
      runtime->filter_instances(ctx, task, output.chosen_variant,
                                 output.chosen_instances, missing_fields);
      // Track which regions have already been mapped 
      std::vector<bool> done_regions(task.regions.size(), false);
      if (!input.premapped_regions.empty())
        for (std::vector<unsigned>::const_iterator it = 
              input.premapped_regions.begin(); it != 
              input.premapped_regions.end(); it++)
          done_regions[*it] = true;
      const TaskLayoutConstraintSet &layout_constraints = 
        runtime->find_task_layout_constraints(ctx, 
                              task.task_id, output.chosen_variant);
      // Now we need to go through and make instances for any of our
      // regions which do not have space for certain fields
      bool has_reductions = false;
      for (unsigned idx = 0; idx < task.regions.size(); idx++)
      {
        if (done_regions[idx]){
          fprintf(stderr, "DONE REGION?? BY WHO????\n");
          continue;
        }

        if(idx >= info.lenInput || all_virtual){
          if(info.callbackID > 0){
            if(debug)
              fprintf(stderr,"\tTask %d Virtual mapping region %d\n", info.id, idx);

            output.chosen_instances[idx].push_back(
                Legion::Mapping::PhysicalInstance::get_virtual_instance());
            continue;
          }
        }

        // Skip any empty regions
        if ((task.regions[idx].privilege == NO_ACCESS) ||
            (task.regions[idx].privilege_fields.empty()) ||
            missing_fields[idx].empty())
          continue;
        // See if this is a reduction      
        if (task.regions[idx].privilege == REDUCE)
        {
          has_reductions = true;
          if (!default_create_custom_instances(ctx, task.target_proc,
                  target_memory, task.regions[idx], idx, missing_fields[idx],
                  layout_constraints, needs_field_constraint_check,
                  output.chosen_instances[idx]))
          {
            default_report_failed_instance_creation(task, idx, 
                                        task.target_proc, target_memory);
          }
          continue;
        }
        // Otherwise make normal instances for the given region
        if (!default_create_custom_instances(ctx, task.target_proc,
                target_memory, task.regions[idx], idx, missing_fields[idx],
                layout_constraints, needs_field_constraint_check,
                output.chosen_instances[idx]))
        {
          default_report_failed_instance_creation(task, idx,
                                      task.target_proc, target_memory);
        }
      }
      // Now that we are done, let's cache the result so we can use it later
      std::list<CachedTaskMapping> &map_list = cached_task_mappings[cache_key];
      map_list.push_back(CachedTaskMapping());
      CachedTaskMapping &cached_result = map_list.back();
      cached_result.task_hash = task_hash; 
      cached_result.variant = output.chosen_variant;
      cached_result.mapping = output.chosen_instances;
      cached_result.has_reductions = has_reductions;
      // We don't ever save reduction instances in our cache 
      if (has_reductions) {
        for (unsigned idx = 0; idx < task.regions.size(); idx++) {
          if (task.regions[idx].privilege != REDUCE)
            continue;
          cached_result.mapping[idx].clear();
        }
      }
    }
#endif


#if 0
void DataFlowMapper::map_must_epoch(const MapperContext           ctx,
				   const MapMustEpochInput&      input,
				   MapMustEpochOutput&     output)
//--------------------------------------------------------------------------
{
  fprintf(stderr,"Default map_must_epoch in %s\n", get_mapper_name());
  // Figure out how to assign tasks to CPUs first. We know we can't
  // do must epochs for anthing but CPUs at the moment.
  if (total_nodes > 1)
    {
      Machine::ProcessorQuery all_procs(machine);
      all_procs.only_kind(Processor::LOC_PROC);
      Machine::ProcessorQuery::iterator proc_finder = all_procs.begin();
      for (unsigned idx = 0; idx < input.tasks.size(); idx++)
        {
          if (proc_finder == all_procs.end())
	    {
	      fprintf(stderr,"Default mapper error. Not enough CPUs for must "
			       "epoch launch of task %s with %ld tasks", 
			       input.tasks[0]->get_task_name(),
			       input.tasks.size());
	      assert(false);
	    }
          output.task_processors[idx] = *proc_finder++;
        }
    }
  else
    {
      if (input.tasks.size() > local_cpus.size())
        {
          fprintf(stderr,"Default mapper error. Not enough CPUs for must "
                           "epoch launch of task %s with %ld tasks", 
                           input.tasks[0]->get_task_name(),
                           input.tasks.size());
          assert(false);
        }
      for (unsigned idx = 0; idx < input.tasks.size(); idx++)
	output.task_processors[idx] = local_cpus[idx];
    }
  // Now let's map all the constraints first, and then we'll call map
  // task for all the tasks and tell it that we already premapped the
  // constrainted instances

  // fprintf(stderr, "constraints in %d out %d\n", input.constraints.size(),output.constraint_mappings.size());
  for (unsigned cid = 0; cid < input.constraints.size(); cid++)
    {
      const MappingConstraint &constraint = input.constraints[cid];
      std::vector<Legion::Mapping::PhysicalInstance> &constraint_mapping = 
	output.constraint_mappings[cid];
      int index1 = -1, index2 = -1;
      for (unsigned idx = 0; (idx < input.tasks.size()) &&
	     ((index1 == -1) || (index2 == -1)); idx++)
        {
          if (constraint.t1 == input.tasks[idx])
            index1 = idx;
          if (constraint.t2 == input.tasks[idx])
            index2 = idx;
        }
      assert((index1 >= 0) && (index2 >= 0));
      // Figure out which memory to use
      // TODO: figure out how to use registered memory in the multi-node case
      Memory target1 = default_policy_select_target_memory(ctx,
							   output.task_processors[index1]);
      Memory target2 = default_policy_select_target_memory(ctx,
							   output.task_processors[index2]);
      // Pick our target memory
      Memory target_memory = Memory::NO_MEMORY;
      if (target1 != target2)
        {
          // See if one of them is not no access so we can pick the other
          if (constraint.t1->regions[constraint.idx1].is_no_access())
            target_memory = target2;
          else if (constraint.t2->regions[constraint.idx2].is_no_access())
            target_memory = target1;
          else
	    {
	      fprintf(stderr,"Default mapper error. Unable to pick a common "
                             "memory for tasks %s (ID %lld) and %s (ID %lld) "
                             "in a must epoch launch. This will require a "
			       "custom mapper.", constraint.t1->get_task_name(),
			       constraint.t1->get_unique_id(), 
			       constraint.t2->get_task_name(), 
			       constraint.t2->get_unique_id());
	      assert(false);
	    }
        }
      else // both the same so this is easy
	target_memory = target1;
      assert(target_memory.exists());
      // Figure out the variants that are going to be used by the two tasks    
      VariantInfo info1 = find_preferred_variant(*constraint.t1, ctx,
						 true/*needs tight bound*/, Processor::LOC_PROC);
      VariantInfo info2 = find_preferred_variant(*constraint.t2, ctx,
						 true/*needs tight_bound*/, Processor::LOC_PROC);
      // Map it the one way and filter the other so that we can make sure
      // that they are both going to use the same instance
      std::set<FieldID> needed_fields = 
	constraint.t1->regions[constraint.idx1].privilege_fields;
      needed_fields.insert(
			   constraint.t2->regions[constraint.idx2].privilege_fields.begin(),
			   constraint.t2->regions[constraint.idx2].privilege_fields.end());
        const TaskLayoutConstraintSet &layout_constraints1 = 
          runtime->find_task_layout_constraints(ctx, 
						       constraint.t1->task_id, info1.variant);
        if (!default_create_custom_instances(ctx, 
					     output.task_processors[index1], target_memory,
					     constraint.t1->regions[constraint.idx1], constraint.idx1,
					     needed_fields, layout_constraints1, true/*needs check*/,
					     constraint_mapping))
	  {
	    fprintf(stderr,"Default mapper error. Unable to make instance(s) "
                           "in memory " IDFMT " for index %d of constrained "
			     "task %s (ID %lld) in must epoch launch.",
			     target_memory.id, constraint.idx1, 
			     constraint.t1->get_task_name(),
			     constraint.t1->get_unique_id());
	    assert(false);
	  }
        // Copy the results over and make sure they are still good 
        const size_t num_instances = constraint_mapping.size();
        assert(num_instances > 0);
	std::set<FieldID> missing_fields;
        runtime->filter_instances(ctx, *constraint.t2, constraint.idx2,
					 info2.variant, constraint_mapping, missing_fields);
        if (num_instances != constraint_mapping.size())
	  {
	    fprintf(stderr,"Default mapper error. Unable to make instance(s) "
                           "for index %d of constrained task %s (ID %lld) in "
                           "must epoch launch. Most likely this is because "
                           "conflicting constraints are requested for regions "
                           "which must be mapped to the same instance. You "
			     "will need to write a custom mapper to fix this.",
			     constraint.idx2, constraint.t2->get_task_name(),
			     constraint.t2->get_unique_id());
	    assert(false);
	  }
    }
}

#endif
  /*
void DataFlowMapper::notify_mapping_result(const Mappable *mappable)
{
  if (mappable->get_mappable_kind() == Mappable::TASK_MAPPABLE)
  {
    const Task *task = mappable->as_mappable_task();
    assert(task != NULL);
//    for (unsigned idx = 0; idx < task->regions.size(); idx++)
//    {
//      printf("Mapped region %d of task %s (ID %lld) to memory %x\n",
//              idx, task->variants->name, 
//              task->get_unique_task_id(),
//              task->regions[idx].selected_memory.id);
//    }
  }
}
*/
