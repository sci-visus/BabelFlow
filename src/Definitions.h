/*
 * Definitions.h
 *
 *  Created on: Dec 12, 2014
 *      Author: bremer5
 */

#ifndef DEFINITIONS_H_
#define DEFINITIONS_H_

#include <stdint.h>
#include <cstdlib>

//! Index type used to identify tasks
typedef uint32_t TaskId;

//! The NULL element indicating an outside input
const TaskId TNULL = (TaskId)-1;

//! Index type used to identify a controller
typedef uint32_t ShardId;

//! The NULL element indicating no controller
const ShardId CNULL = (ShardId)-1;

//! Index type used to register callbacks
typedef uint8_t CallbackId;


//! A DataBlock abstracts a chunk of memory
class DataBlock
{
public:

  //! Default constructor
  DataBlock(char *b=NULL, uint32_t s=0) : buffer(b), size(s) {}

  //! Copy constructor
  DataBlock(const DataBlock& block);

  //! Makes a copy of the data block
  DataBlock clone() const;

  char* buffer;
  uint32_t size;
};



#endif /* DEFINITIONS_H_ */
