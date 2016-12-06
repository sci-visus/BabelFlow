/*
 * Payload.h
 *
 *  Created on: Feb 25, 2016
 *      Author: bremer5
 */

#ifndef MPI_PAYLOAD_H_
#define MPI_PAYLOAD_H_

#include <cassert>
#include <cstring>
#include <vector>

namespace DataFlow {

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

protected:

  //! Size of the memory
  int32_t mSize;

  //! Pointer to class or block of memory
  char* mBuffer;
};

}


#endif /* MPI_PAYLOAD_H_ */
