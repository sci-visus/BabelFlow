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
#include <iostream>
#include <functional>

namespace BabelFlow 
{

//! Index type used to identify tasks
class TaskId
{
public:
  typedef uint32_t InnerTaskId;

  TaskId() : m_tid( 0 ), m_gr( 0 ) {}
  TaskId( uint32_t tid ) : m_tid( tid ), m_gr( 0 ) {}
  TaskId( uint32_t tid, uint32_t gr ) : m_tid( tid ), m_gr( gr ) {}
  TaskId( const TaskId& other ) : m_tid( other.tid() ), m_gr( other.graphId() ) {}

  uint32_t  tid() const     { return m_tid; }
  uint32_t& tid()           { return m_tid; }
  uint32_t  graphId() const { return m_gr; }
  uint32_t& graphId()       { return m_gr; }
  
  /*explicit*/ operator uint32_t() const       { return m_tid; }
  
  TaskId& operator=( uint32_t tid )        { m_tid = tid; return *this; }
  TaskId& operator=( const TaskId& other ) { m_tid = other.tid(); m_gr = other.graphId(); return *this; }
  
  TaskId& operator>>=( uint32_t d )        { m_tid >>= d; return *this; }
  TaskId& operator>>=( int d )             { return *this >>= uint32_t(d); }
  TaskId operator>>( uint32_t d ) const    { TaskId shifted_id(*this); shifted_id >>= d; return shifted_id; }
  TaskId operator>>( int d ) const         { return *this >> uint32_t(d); }
  
  TaskId& operator<<=( uint32_t d )        { m_tid <<= d; return *this; }
  TaskId& operator<<=( int d )             { return *this <<= uint32_t(d); }
  TaskId operator<<( uint32_t d ) const    { TaskId shifted_id(*this); shifted_id <<= d; return shifted_id; }
  TaskId operator<<( int d ) const         { return *this << uint32_t(d); }
  
  TaskId& operator&=( uint32_t d )         { m_tid &= d; return *this; }
  TaskId& operator&=( int d )              { return *this &= uint32_t(d); }
  TaskId operator&( uint32_t d ) const     { TaskId task_id(*this); task_id &= d; return task_id; }
  TaskId operator&( int d ) const          { return *this & uint32_t(d); }
  
  TaskId& operator|=( uint32_t d )         { m_tid |= d; return *this; }
  TaskId& operator|=( int d )              { return *this |= uint32_t(d); }
  TaskId operator|( uint32_t d ) const     { TaskId task_id(*this); task_id |= d; return task_id; }
  TaskId operator|( int d ) const          { return *this | uint32_t(d); }
  
  TaskId& operator+=( uint32_t d )         { m_tid += d; return *this; }
  TaskId& operator-=( uint32_t d )         { m_tid -= d; return *this; }
  
  bool operator==( const TaskId& other ) const { return m_tid == other.tid() && m_gr == other.graphId(); }
  bool operator==( int d ) const               { return m_tid == d; }
  bool operator==( uint32_t d ) const          { return m_tid == d; }

  bool operator!=( const TaskId& other ) const { return !( *this == other ); }
  bool operator!=( int d ) const               { return !( *this == d ); }
  bool operator!=( uint32_t d ) const          { return !( *this == d ); }

  bool operator<( const TaskId& other ) const { return m_gr < other.graphId() || (m_gr == other.graphId() && m_tid < other.tid()); }
  bool operator<( int d ) const               { return m_tid < d; }
  bool operator<( uint32_t d ) const          { return m_tid < d; }

  bool operator<=( const TaskId& other ) const { return ( *this < other || *this == other ); }
  bool operator<=( int d ) const               { return m_tid <= d; }
  bool operator<=( uint32_t d ) const          { return m_tid <= d; }

  bool operator>( const TaskId& other ) const { return !( *this <= other ); }
  bool operator>( int d ) const               { return m_tid > d; }
  bool operator>( uint32_t d ) const          { return m_tid > d; }

  // Prefix increment operator
  TaskId operator++() { TaskId task_id( ++m_tid, m_gr ); return task_id; }
  // Postfix increment operator
  TaskId operator++(int) { TaskId task_id( m_tid++, m_gr ); return task_id; }
  // Prefix decrement operator
  TaskId operator--() { TaskId task_id( --m_tid, m_gr ); return task_id; }
  // Postfix decrement operator
  TaskId operator--(int) { TaskId task_id( m_tid--, m_gr ); return task_id; }
  
  friend std::ostream& operator<<( std::ostream& out, const TaskId& task_id )
  {
    out << "T_" << task_id.tid() << "_" << task_id.graphId() << "_";
    return out;
  }

private:
  InnerTaskId m_tid;   // Inner task id
  InnerTaskId m_gr;    // Graph id
};


//! The NULL element indicating an outside input
const TaskId TNULL( (uint32_t)-1 );

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


// Hasher functor so we could use TaskId as a key in unordered_map
template<>
struct std::hash<BabelFlow::TaskId>
{
  std::size_t operator()( const BabelFlow::TaskId& t ) const 
  {
    std::size_t hash_a = m_uintHash( t.tid() );
    std::size_t hash_b = m_uintHash( t.graphId() );
    // Use boost::hash_combine:
    hash_a ^= hash_b + 0x9e3779b9 + (hash_a << 6) + (hash_a >> 2);
    return hash_a;
  }

  std::hash<unsigned int> m_uintHash;
};



#endif /* DEFINITIONS_H_ */
