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

#include "TaskGraph.h"

using namespace BabelFlow;


//TaskGraphFactory::MapType TaskGraphFactory::m_map;

int TaskGraph::output_graph(ShardId count, const TaskMap* task_map, FILE* output)
{
  output_graph_dot(count,task_map,output,"\n");
}


int TaskGraph::output_graph_html(ShardId count, const TaskMap* task_map, FILE* output)
{
  // The following code is taken from Ascent:

  fprintf(output,
    "<!DOCTYPE html>\n"
    "<meta charset=\"utf-8\">\n"
    "<body>\n"
    "<script src=\"https://d3js.org/d3.v4.min.js\"></script>\n"
    "<script src=\"https://unpkg.com/viz.js@1.8.0/viz.js\" type=\"javascript/worker\"></script>\n"
    "<script src=\"https://unpkg.com/d3-graphviz@1.3.1/build/d3-graphviz.min.js\"></script>\n"
    "<div id=\"graph\" style=\"text-align: center;\"></div>\n"
    "<script>\n"
    "\n"
    "d3.select(\"#graph\")\n"
    "  .graphviz()\n"
    "    .renderDot('");

  // gen dot def, with proper js escaping
  // we are injected as inline js literal -- new lines need to be escaped.
  // Add \ to the end of each line in our dot output.
  output_graph_dot(count,task_map,output," \\\n");

  fprintf(output,
    "');\n"
    "\n"
    "</script>\n"
    "</body>\n"
    "</html>\n");
}


int TaskGraph::output_graph_dot(ShardId count, const TaskMap* task_map, FILE* output, const std::string &eol)
{
  fprintf(output,"digraph G {%s",eol.c_str());

  std::vector<Task> tasks;
  std::vector<Task>::iterator tIt;
  std::vector<TaskId>::iterator it;

  for (uint32_t i=0;i<count;i++) {
    tasks = localGraph(i,task_map);

    for (tIt=tasks.begin();tIt!=tasks.end();tIt++) {
      TaskId::InnerTaskId tid = TaskId::InnerTaskId( tIt->id() );
      fprintf(output, "%d [label=\"%d,%d\"]%s", tid , tid, tIt->callback(), eol.c_str());
      for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {
        if (*it != TNULL)
          fprintf(output, "%d -> %d%s", TaskId::InnerTaskId(*it), tid, eol.c_str());
      }
    }
  }

  fprintf(output,"}%s",eol.c_str());
  return 1;
}


void TaskGraph::output_graph( ShardId shard_count, const TaskMap* task_map, const std::string& filename ) const
{
  std::ofstream ofs( filename );

  std::vector< std::vector<Task> > tasks( shard_count );
  for( uint32_t i = 0; i < shard_count; ++i )
    tasks[i] = localGraph( i, task_map );

  output_helper( tasks, ofs, false );

  ofs.close();
}


void TaskGraph::output_graph_html( ShardId shard_count, const TaskMap* task_map, const std::string& filename ) const
{
  std::ofstream ofs( filename );

  std::vector< std::vector<Task> > tasks( shard_count );
  for( uint32_t i = 0; i < shard_count; ++i )
    tasks[i] = localGraph( i, task_map );
  
  output_helper( tasks, ofs, true );

  ofs.close();
}


void TaskGraph::output_tasks_html( const std::vector<Task>& task_vec, const std::string& filename ) const
{
  std::ofstream ofs( filename );

  std::vector< std::vector<Task> > tasks( 1 );
  tasks[0] = task_vec;

  output_helper( tasks, ofs, true );

  ofs.close();
}


void TaskGraph::output_helper( const std::vector< std::vector<Task> >& tasks_v, std::ostream& outs, bool incl_html ) const
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
  outs << "  ordering=out;" << eol << std::endl;
  outs << "  rankdir=TB;" << eol << std::endl;
  outs << "  ranksep=0.8;" << eol << std::endl;

  output_dot( tasks_v, outs, eol );
  
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


void TaskGraph::output_dot( const std::vector< std::vector<Task> >& tasks_v, std::ostream& outs, const std::string& eol ) const
{
  for( uint32_t i = 0; i < tasks_v.size(); ++i )
  {
    for( const Task& tsk : tasks_v[i] )
    {
      outs << tsk.id() << " [label=\"" << tsk.id() << "," << uint32_t(tsk.callback()) << "\"]" << eol << std::endl;

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

//bool TaskGraphFactory::registerCreator(const std::string& name, TaskGraphFactory::CreatorFunc func)
//{
//  if( m_map.find( name ) != m_map.end() )
//    return false;
//
//  m_map[name] = func;
//  return true;
//}
