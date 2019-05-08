/*
 *  datatypes.h
 *
 *  Created on: March 10, 2016
 *      Author: spetruzza
 */

#ifndef __DATA_TYPES_H__
#define __DATA_TYPES_H__

#include "legion.h"

enum TaskIDs {
  TOP_LEVEL_TASK_ID,
  GENERIC_TASK_ID,
  PREPARE_TASK_ID,
  LOAD_TASK_ID,
  INIT_SHARDS_TASK_ID,
  SHARD_MAIN_TASK_ID,
  SUM_TASK_ID,
  WRITE_TASK_ID
};

enum FieldIDs {
  FID_GEN,
  FID_PAYLOAD
};

// TODO move those definition at higher level (this comes form the PMT code, it could conflict with it!!)
//! The global index type for large grids
typedef uint32_t GlobalIndexType;
//! The function type of the values
typedef float FunctionType;

#endif // __DATA_TYPES_H__

