
#include <cassert>
#include <cmath>

#include "RadixKExchange.h"

using namespace BabelFlow;


uint32_t RadixKExchange::sDATASET_DIMS[3];
uint32_t RadixKExchange::sDATA_DECOMP[3];

RadixKExchange::RadixKExchange( uint32_t block_dim[3], const std::vector<uint32_t>& radix_v )
{
  init(block_dim, radix_v);
}

void RadixKExchange::init( uint32_t block_dim[3], const std::vector<uint32_t>& radix_v )
{
  m_BlkDecomp[0] = block_dim[0];
  m_BlkDecomp[1] = block_dim[1];
  m_BlkDecomp[2] = block_dim[2];
  m_Nblocks = m_BlkDecomp[0]*m_BlkDecomp[1]*m_BlkDecomp[2];

  m_Radices = radix_v;
  
  // Compute radices prefix product which will be later used to map tasks to groups
  m_RadicesPrefixProd.resize(m_Radices.size(), 1);
  for( uint32_t i = 1; i < m_RadicesPrefixProd.size(); ++i )
    m_RadicesPrefixProd[i] = m_RadicesPrefixProd[i - 1] * m_Radices[i - 1];
  
  // Sanity check -- total product of radices should be equal to num of blocks
  if( m_RadicesPrefixProd.back() * m_Radices.back() != m_Nblocks )
  {
    fprintf( stderr, "Num blocks not equal to product of radices!" );
    assert( false );
  }  

  m_LvlOffset.push_back( 0 ) ;
  
  for( uint32_t i=0; i < m_Radices.size(); ++i )
  {
    m_LvlOffset.push_back( m_LvlOffset.back() + m_Nblocks );
  }
}

Payload RadixKExchange::serialize() const
{
  // What's serialized:
  // - m_blkDecomp (n = 3)
  // - sDATASET_DIMS (n = 3)
  // - num radices (n = 1)
  // - all the radices (n = m_Radices.size())
  uint32_t nelems_in_buffer = 6 + 1 + m_Radices.size();
  uint32_t* buffer = new uint32_t[nelems_in_buffer];

  buffer[0] = m_BlkDecomp[0];
  buffer[1] = m_BlkDecomp[1];
  buffer[2] = m_BlkDecomp[2];

  buffer[3] = RadixKExchange::sDATASET_DIMS[0];
  buffer[4] = RadixKExchange::sDATASET_DIMS[1];
  buffer[5] = RadixKExchange::sDATASET_DIMS[2];

  buffer[6] = m_Radices.size();

  for( uint32_t i=0; i < m_Radices.size(); ++i ) buffer[7 + i] = m_Radices[i];

  return Payload( nelems_in_buffer*sizeof(uint32_t), (char*)buffer );
}

void RadixKExchange::deserialize(Payload buffer)
{
  // Even if the radices vector is empty, there should be at least 7 elements in the buffer
  assert( buffer.size() >= 7*sizeof(uint32_t) );
  uint32_t* buff_ptr = (uint32_t *)(buffer.buffer());
  
  //printf("DEserializing %d %d %d , f %d\n", tmp[0], tmp[1], tmp[2], tmp[3]);

  RadixKExchange::sDATA_DECOMP[0] = buff_ptr[0];
  RadixKExchange::sDATA_DECOMP[1] = buff_ptr[1];
  RadixKExchange::sDATA_DECOMP[2] = buff_ptr[2];
  
  RadixKExchange::sDATASET_DIMS[0] = buff_ptr[3];
  RadixKExchange::sDATASET_DIMS[1] = buff_ptr[4];
  RadixKExchange::sDATASET_DIMS[2] = buff_ptr[5];
  
  std::vector<uint32_t> radix_v(buff_ptr[6]);
  
  for( uint32_t i=0; i < radix_v.size(); ++i ) radix_v[i] = buff_ptr[7 + i];
  
  // memcpy(dim, buffer.buffer(), sizeof(uint32_t)*3);

  init( buff_ptr, radix_v );

  delete[] buffer.buffer();
}

Task RadixKExchange::task(uint64_t gId) const
{
  Task t(toTId(gId));
  Task* it = &t;

  std::vector<TaskId> incoming;
  std::vector<std::vector<TaskId> > outgoing;

  uint32_t lvl = level( it->id() );
  
  if( lvl == 0 )        // Leaf node
  {
    it->callback(1); 

    incoming.resize(1); // One dummy input from controller
    incoming[0] = TNULL;
  } 
  else 
  {
    if( lvl == totalLevels() )    // root node -- no outputs
      it->callback(3);
    else
      it->callback(2);      // middle node
  
    // Neighbors from previous level are inputs to current level
    getRadixNeighbors( it->id(), lvl - 1, false, incoming );
  }
  
  it->incoming(incoming);
  
  // Set outputs
  if( lvl < totalLevels() )
  {
    std::vector<TaskId> out_neighbors;
    getRadixNeighbors( it->id(), lvl, true, out_neighbors );
    
    outgoing.resize( out_neighbors.size() );
    
    for( uint32_t i = 0; i < outgoing.size(); ++i )
    {
      outgoing[i].resize( 1 );  // only one destination for each outgoing message
      outgoing[i][0] = out_neighbors[i];
    }
  }

  it->outputs(outgoing);

  return t;
}

std::vector<Task> RadixKExchange::localGraph(ShardId id,
                                             const TaskMap* task_map) const
{
  TaskId i;

  // First get all the ids we need
  std::vector<TaskId> ids = task_map->tasks(id);

  // The create the required number of tasks
  std::vector<Task> tasks;

  //! Now assign all the task ids
  for( TaskId i = 0; i < ids.size(); i++ )
    tasks.push_back( task( ids[i] ) );

  return tasks;
}

int RadixKExchange::output_graph_dot(ShardId count, 
                                     const TaskMap* task_map, 
                                     FILE* output,
                                     const std::string &eol)
{
  fprintf(output, "digraph G {%s", eol.c_str());
  fprintf(output, "\tordering=out;%s\trankdir=TB;ranksep=0.8;%s", eol.c_str(), eol.c_str());

  for (uint32_t i = 0; i <= totalLevels(); ++i)
    fprintf(output, "f%d [label=\"level %d\"]", i, i);


  fprintf(output,"f0 ");
  for (uint32_t i = 0; i <= totalLevels(); ++i)
    fprintf(output, " -> f%d", i);
  fprintf(output, "%s%s", eol.c_str(), eol.c_str());

  std::vector<Task> tasks;
  std::vector<Task>::iterator tIt;
  std::vector<TaskId>::iterator it;

  for (uint32_t i=0;i<count;i++) {
    tasks = localGraph(i,task_map);

    for (tIt=tasks.begin();tIt!=tasks.end();tIt++) {
      TaskId::InnerTaskId tid = tIt->id();
      if (level(tid) == 0)
        fprintf(output, "%d [label=\"(%d ,%d)\",color=red]%s",
                tid, tid, tIt->callback(), eol.c_str());
      else
        fprintf(output,"%d [label=\"(%d ,%d)\",color=black]%s",
                tid, tid, tIt->callback(), eol.c_str());

      for (it=tIt->incoming().begin();it!=tIt->incoming().end();it++) {
        if (*it != TNULL)
          fprintf(output, "%d -> %d%s", TaskId::InnerTaskId(*it), tid, eol.c_str());
      }
    }

    // for (tIt=tasks.begin();tIt!=tasks.end();tIt++)
    //   fprintf(output,"{rank = same; f%d; %d}\n",
    //           level(tIt->id()),tIt->id());

  }

  fprintf(output, "}%s", eol.c_str());
  return 1;
}

void RadixKExchange::getRadixNeighbors(TaskId id, uint32_t level, bool isOutgoing, 
                                       std::vector<TaskId>& neighbors) const
{
  //TaskId base_id = baseId(id);
  TaskId blk_id = baseId(id) % m_Nblocks;   // base_id % m_Nblocks;
  
  // To form groups in each level envision all the tasks mapped to a virtual
  // lattice with m_Radices.size() dimensions. Each dimension represents a level
  // and the size of the dimension is m_Radices[level]. Following this logic, we
  // can map the task 'id' to a coordinate vector in the lattice (k_0, k_1, ..., k_l).
  // Members of the same group have the same coordinates, but only the l coordinate
  // changes (k_0, k_1, ..., *, k_l+1, ...). All the coordinates can be translated
  // back to id's using the m_RadicesPrefixProd vector we computed earlier. In the end,
  // we will have all the task id's for our neighbors -- the ones we send data to or
  // receive from at the the given level l.
  
  std::vector<uint32_t> lattice_coords( m_Radices.size() );
  for( uint32_t i = 0; i < lattice_coords.size(); ++i )
    lattice_coords[i] = (blk_id / m_RadicesPrefixProd[i]) % m_Radices[i];
  
  neighbors.resize( m_Radices[level] );
  
  for( uint32_t i = 0; i < neighbors.size(); ++i )
  {
    lattice_coords[level] = i;
    
    uint32_t nid = 0;
    for( uint32_t j = 0; j < lattice_coords.size(); ++j )
      nid += lattice_coords[j] * m_RadicesPrefixProd[j];
      
    neighbors[i] = nid + m_Nblocks * (level + (isOutgoing ? 1 : 0));
  }
}
