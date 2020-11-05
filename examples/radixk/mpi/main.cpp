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

#include <cstdio>
#include <unistd.h>
#include <cstdlib>
#include <sstream>
#include <fstream>

#include <mpi.h>

#include "CompositingUtils.h"
#include "ModuloMap.h"
#include "RelayTask.h"
#include "ModuloMap.h"
#include "mpi/Controller.h"

// using namespace BabelFlow;
// using namespace BabelFlow::mpi;


//-----------------------------------------------------------------------------

class BabelCompRadixK_MPI : public comp_utils::BabelCompRadixK
{
public:
  BabelCompRadixK_MPI(int32_t rank_id,
                      int32_t n_blocks,
                      int32_t fanin,
                      const std::vector<uint32_t>& radix_v)
  : comp_utils::BabelCompRadixK(rank_id, n_blocks, fanin, radix_v) {}

  virtual void Initialize(std::map<BabelFlow::TaskId, BabelFlow::Payload>& inputs) override
  {
    BabelCompRadixK::Initialize(inputs);

    m_controller.initialize( m_radGatherGraph, &m_radGatherTaskMap, MPI_COMM_WORLD, &m_contMap );
  }
  
  virtual void Execute(std::map<BabelFlow::TaskId, BabelFlow::Payload>& inputs) override
  {
    m_controller.run(inputs);
  }

protected:
  BabelFlow::mpi::Controller m_controller;
  BabelFlow::ControllerMap m_contMap;
};

//-----------------------------------------------------------------------------

void register_callbacks()
{
  static std::mutex mtx;
  static bool initialized = false;

  std::unique_lock<std::mutex> lock(mtx);

  if (!initialized)
  {
    comp_utils::register_callbacks();
    comp_utils::BabelGraphWrapper::sIMAGE_NAME = "radixk_img";
    initialized = true;
  }
}

int main(int argc, char* argv[])
{
  if (argc < 5)
  {
    std::cout << "Usage: " << argv[0] << " <fan-in> <img_prefix> <radices>" << std::endl;
    return -1;
  }

  clock_t start, finish;

  MPI_Init(&argc, &argv);

  // Find out how many controllers we need
  int nranks, my_rank;
  MPI_Comm_size(MPI_COMM_WORLD, &nranks);
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

  int32_t fanin = atoi(argv[1]);
  std::string img_prefix(argv[2]);   // e.g.,  "raw_img_<rank>.bin"
  std::vector<uint32_t> radix_v;

  for( uint32_t i = 3; i < argc; ++i )
    radix_v.push_back(atoi(argv[i]));

  if (my_rank == 0)
  {
    std::cout << "---" << std::endl;
    std::cout << "Starting program with " << nranks << " procs, fanin: " << fanin << std::endl;
    std::cout << "Image prefix:" << img_prefix << std::endl;
    std::cout << "Radices:" << std::endl;
    for( uint32_t i = 0; i < radix_v.size(); ++i )
      std::cout << radix_v[i] << " " << std::endl;
    std::cout << "---" << std::endl;
  }    

  register_callbacks();

  std::map<BabelFlow::TaskId, BabelFlow::Payload> inputs;
  
  // Read the input image
  std::stringstream img_name;
  img_name << img_prefix << my_rank << ".bin";
  std::ifstream ifs(img_name.str());
  if( !ifs.good() )
  {
    std::cout << "Couldn't open " << img_name.str() << std::endl;
    exit(-1);
  }
  ifs.seekg (0, ifs.end);
  int length = ifs.tellg();
  ifs.seekg (0, ifs.beg);
  char* buff = new char[length];
  ifs.read( buff, length );
  ifs.close();

  BabelFlow::Payload payl(length, buff);
  inputs[my_rank] = payl;
  
  start = clock();

  BabelCompRadixK_MPI radixk_mpi_wrapper(0, nranks, fanin, radix_v);
  radixk_mpi_wrapper.Initialize(inputs);
  radixk_mpi_wrapper.Execute(inputs);

  finish = clock();

  double run_time, max_run_time;
  max_run_time = run_time = (static_cast<double>(finish) - static_cast<double>(start)) / CLOCKS_PER_SEC;
  MPI_Reduce(&run_time, &max_run_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  if (my_rank == 0) 
  {
    std::cout << "Finished executing, runtime (sec): " << max_run_time << std::endl;
  }

  MPI_Finalize();
  return 0;
}


