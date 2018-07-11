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

#ifndef MPI_PAYLOAD_H_
#define MPI_PAYLOAD_H_

#include <cassert>
#include <cstring>
#include <vector>
#include "Definitions.h"

namespace BabelFlow {

class Payload;

//! The typedef for the accepted callbacks
/*! A Callback is the only accepted function signature for a task.
 *  It takes n>=0  DataBlocks as input and produces m>=0 DataBlocks
 *  as output. The callback will assume ownership of the input buffers
 *  as is responsible for deleting any associated memory. Similarly,
 *  the callback will give up ownership of all output buffers to the
 *  caller
 *
 * @param inputs A set of DataBlocks that are the inputs
 * @param outputs A set of DataBlocks storing the outputs
 * @return 1 if successful and 0 otherwise
 */
typedef int (*Callback)(std::vector<Payload>& inputs, std::vector<Payload>& outputs, TaskId task);


class Payload {

public:

  Payload(int32_t size=0, char* buffer = NULL) : mSize(size), mBuffer(buffer) {}

  virtual ~Payload() {}

  int32_t size() const {return mSize;}

  char* buffer() const {return mBuffer;}

  void initialize(int32_t size, char* buffer) {mSize = size; mBuffer = buffer;}

  void reset() {mSize = 0; delete[] mBuffer; mBuffer = NULL;}

  virtual Payload clone() {
    char* buffer = new char[mSize];
    memcpy(buffer, mBuffer, mSize);

    Payload copy(mSize,buffer);
    return copy;
  }

  virtual void serialize() {assert (false);}

  virtual void deserialize() {assert (false);}

public:

  //! Size of the memory
  int32_t mSize;

  //! Pointer to class or block of memory
  char* mBuffer;
};

}


#endif /* MPI_PAYLOAD_H_ */
