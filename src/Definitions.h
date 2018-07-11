/*
 * Copyright (c) 2017 University of Utah 
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef DEFINITIONS_H_
#define DEFINITIONS_H_

#include <stdint.h>
#include <cstdlib>

namespace BabelFlow {

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


/*
//! A DataBlock abstracts a chunk of memory
class DataBlock
{
public:

  //! Default constructor
  DataBlock(char *b=NULL, int32_t s=0) : buffer(b), size(s) {}

  //! Copy constructor
  DataBlock(const DataBlock& block);

  //! Makes a copy of the data block
  DataBlock clone() const;

  //! The point to the actual memory
  char* buffer;

  //! The size with size=-1 and buffer != NULL indicating a pointer to a class instance
  int32_t size;
};
*/

}

#endif /* DEFINITIONS_H_ */
