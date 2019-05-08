
/*
 *  DataFlowMapper.h
 *
 *  Created on: Feb 13, 2016
 *      Author: spetruzza
 */

#ifndef __DATAFLOW_MAPPER_H__
#define __DATAFLOW_MAPPER_H__

#include "legion.h"

#include "Controller.h"
#include "default_mapper.h"
//#include "shim_mapper.h"

using namespace Legion;
using namespace Legion::Mapping;
using namespace LegionRuntime::HighLevel;
using namespace LegionRuntime::Accessor;
using namespace LegionRuntime::Arrays;

struct MetaDataFlow{                                                                                                         
  TaskInfo info;                                                                                                 
  int prepare;                                                                                                               
};

class DataFlowMapper : public DefaultMapper {
public:
  DataFlowMapper(Machine machine,
                 HighLevelRuntime *rt, Processor local);
public:
/*  virtual void select_task_options(const MapperContext    ctx,
				   const Task&            task,
				   TaskOptions&     output); //Task *task);*/
/*  virtual bool default_make_instance(MapperContext ctx,
						     Memory target_memory, const LayoutConstraintSet &constraints,
				     Legion::Mapping::PhysicalInstance &result, MappingKind kind, bool force_new, bool meets,
						     const RegionRequirement &req);
  virtual bool default_create_custom_instances(MapperContext ctx,
						       Processor target_proc, Memory target_memory,
						       const RegionRequirement &req, unsigned index,
						       std::set<FieldID> &needed_fields,
						       const TaskLayoutConstraintSet &layout_constraints,
						       bool needs_field_constraint_check,
						       std::vector<Legion::Mapping::PhysicalInstance> &instances);
*/
//  virtual void slice_domain(const Task *task, const Domain &domain,
  //                          std::vector<DomainSplit> &slices);
  virtual void map_task(const MapperContext      ctx,
			const Task&              task,
			const MapTaskInput&      input,
			MapTaskOutput&     output);//Task *task);

  // virtual bool pre_map_task(Task *task);
  /*
  virtual void premap_task(const MapperContext      ctx,
                                    const Task&              task, 
                                    const PremapTaskInput&   input,
                                          PremapTaskOutput&  output);
  */
//virtual void notify_mapping_result(const Mappable *mappable);
/*   virtual void map_must_epoch(const MapperContext           ctx,
			 const MapMustEpochInput&      input,
			 MapMustEpochOutput&     output);
*/

private:
  std::map<Processor, Memory> all_sysmems;
  std::vector<Processor> all_cpus;
  std::set<Processor> task_procs;
  mutable unsigned short random_number_generator[3];
  
};

#endif // __DATAFLOW_MAPPER_H__

