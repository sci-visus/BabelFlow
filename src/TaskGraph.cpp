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
#include <fstream>
#include <cassert>

#include "TaskGraph.h"


//TaskGraphFactory::MapType TaskGraphFactory::m_map;

namespace BabelFlow 
{

std::unordered_map< uint32_t, std::vector<Callback> > TaskGraph::s_callbackMap;
std::unordered_map< std::string, uint32_t > TaskGraph::s_typeIdsMap;

uint32_t TaskGraph::TypeID::m_counter = 0;

//-----------------------------------------------------------------------------

void TaskGraph::registerCallback( uint32_t graph_id, CallbackId id, Callback func )
{
  std::vector<Callback>& cb_v = s_callbackMap[graph_id];
  if( cb_v.size() <= id )
    cb_v.resize( id + 1, nullptr );

  cb_v[id] = func;
}

//-----------------------------------------------------------------------------

Callback TaskGraph::queryCallback( uint32_t graph_id, CallbackId id  )
{
  auto iter = s_callbackMap.find( graph_id );
  
  // assert( iter != s_callbackMap.end() );
  if( iter == s_callbackMap.end() )
  {
    std::cerr << "TaskGraph::queryCallback - graph id " << graph_id << " not found!" << std::endl;
    exit( -1 );
  }

  std::vector<Callback>& cb_v = iter->second;

  assert( id < cb_v.size() );

  return cb_v[ id ];
}

//-----------------------------------------------------------------------------

// uint32_t TaskGraph::graphNameToTypeId( const char* gr_type_name )
// {
//   std::string tname( gr_type_name );
//   auto iter = s_typeIdsMap.find( tname );
//   uint32_t id = 0;
//   if( iter == s_typeIdsMap.end() )
//   {
//     id = s_typeIdsMap.size() + 1;
//     s_typeIdsMap[tname] = id;
//   }
//   else
//   {
//     id = iter->second;
//   }

//   return id; 
// }

//-----------------------------------------------------------------------------

uint32_t TaskGraph::type() const
{
  auto iter = s_typeIdsMap.find( typeid(*this).name() );
  assert( iter != s_typeIdsMap.end() );

  return iter->second;
}

//-----------------------------------------------------------------------------

Callback TaskGraph::queryCallback( CallbackId cb_id ) const
{
  return TaskGraph::queryCallback( graphId(), cb_id );
}

//-----------------------------------------------------------------------------

void TaskGraph::outputGraph( ShardId shard_count, const TaskMap* task_map, const std::string& filename ) const
{
  std::ofstream ofs( filename );

  std::vector< std::vector<Task> > tasks( shard_count );
  for( uint32_t i = 0; i < shard_count; ++i )
    tasks[i] = localGraph( i, task_map );

  outputHelper( tasks, ofs, false );

  ofs.close();
}

//-----------------------------------------------------------------------------

void TaskGraph::outputGraphHtml( ShardId shard_count, const TaskMap* task_map, const std::string& filename ) const
{
  std::ofstream ofs( filename );

  std::vector< std::vector<Task> > tasks( shard_count );
  for( uint32_t i = 0; i < shard_count; ++i )
    tasks[i] = localGraph( i, task_map );
  
  outputHelper( tasks, ofs, true );

  ofs.close();
}

//-----------------------------------------------------------------------------

void TaskGraph::outputTasksHtml( const std::vector<Task>& task_vec, const std::string& filename ) const
{
  std::ofstream ofs( filename );

  std::vector< std::vector<Task> > tasks( 1 );
  tasks[0] = task_vec;

  outputHelper( tasks, ofs, true );

  ofs.close();
}

//-----------------------------------------------------------------------------

void TaskGraph::outputHelper( const std::vector< std::vector<Task> >& tasks_v, std::ostream& outs, bool incl_html ) const
{
  std::string eol;

  if( incl_html )
  {
    eol = " \\";

    outs << "<!DOCTYPE html>" << std::endl;
    outs << "<meta charset=\"utf-8\">" << std::endl;
    outs << "<body>" << std::endl;
    outs << "<script src=\"https://d3js.org/d3.v4.min.js\"></script>" << std::endl;
    outs << "<script src=\"https://unpkg.com/viz.js@1.8.0/viz.js\" type=\"javascript/worker\"></script>" << std::endl;
    outs << "<script src=\"https://unpkg.com/d3-graphviz@1.3.1/build/d3-graphviz.min.js\"></script>" << std::endl;
    outs << "<div id=\"graph\" style=\"text-align: center;\"></div>" << std::endl;
    outs << "<script>" << std::endl;
    outs << std::endl;
    outs << "d3.select(\"#graph\")" << std::endl;
    outs << "  .graphviz()" << std::endl;
    outs << "    .renderDot('";
  }

  outs << "strict digraph G {" << eol << std::endl;
  // outs << "  ordering=out;" << eol << std::endl;
  outs << "  rankdir=TB;" << eol << std::endl;
  outs << "  ranksep=0.8;" << eol << std::endl;

  outputDot( tasks_v, outs, eol );
  
  outs << "}" << eol << std::endl;

  if( incl_html )
  {
    outs << "');" << std::endl;
    outs << std::endl;
    outs << "</script>" << std::endl;
    outs << "</body>" << std::endl;
    outs << "</html>" << std::endl;
  }
}

//-----------------------------------------------------------------------------

void TaskGraph::outputDot( const std::vector< std::vector<Task> >& tasks_v, std::ostream& outs, const std::string& eol ) const
{
  for( uint32_t i = 0; i < tasks_v.size(); ++i )
  {
    for( const Task& tsk : tasks_v[i] )
    {
      outs << tsk.id() << " [label=\"" << tsk.id() << "," << uint32_t(tsk.callbackId()) << "\"]" << eol << std::endl;

      // Print incoming edges
      for( const TaskId& tid : tsk.incoming() )
      {
        if( tid != TNULL )
          outs << tid << " -> " << tsk.id() << eol << std::endl;
      }

      // Print outgoing edges
      for( uint32_t i = 0; i < tsk.fanout(); ++i )
      {
        for( const TaskId& tid : tsk.outgoing(i) )
        {
          if( tid != TNULL )
            outs << tsk.id() << " -> " << tid << eol << std::endl;
        }
      }
    }
  }
}

//-----------------------------------------------------------------------------

//bool TaskGraphFactory::registerCreator(const std::string& name, TaskGraphFactory::CreatorFunc func)
//{
//  if( m_map.find( name ) != m_map.end() )
//    return false;
//
//  m_map[name] = func;
//  return true;
//}

}   // end namespace BabelFlow
