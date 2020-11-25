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
#include <cstring>
#include <iostream>
#include <cstdlib>

#include <mpi.h>

#include "BroadcastGraph.h"
#include "BroadcastCallbacks.h"
#include "ModuloMap.h"
#include "RelayTask.h"
#include "mpi/Controller.h"

uint32_t gCount = 0;

using namespace BabelFlow;
using namespace BabelFlow::mpi;


class Point : public Payload {
public:
    int x, y, z;

    void serialize() {

    }

    void deserialize() {

    }

};

int main(int argc, char* argv[])
{
  if (argc < 3) {
    fprintf(stderr,"Usage: %s <nr-of-leafs> <fanout> \n", argv[0]);
    return 0;
  }

  MPI_Init(&argc, &argv);

  // FInd out how many controllers we need
  int mpi_width;
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_width);

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  uint32_t leafs = atoi(argv[1]);
  uint32_t valence = atoi(argv[2]);

  BroadcastGraph graph(leafs,valence);
  TaskGraph::registerCallback( 0, BroadcastGraph::LEAF_TASK_CB, print_message );
  TaskGraph::registerCallback( 0, BroadcastGraph::BCAST_TASK_CB, relay_message );

  //std::cout << "graph size " << graph.size() << "\n";
  ModuloMap task_map(mpi_width,graph.size());

  Controller master;

  graph.outputGraph( mpi_width, &task_map, "broadcast_task_graph.dot" );

  master.initialize(graph,&task_map);
  
  std::map<TaskId,Payload> inputs;

  if (rank ==0 ) {
    int32_t size = arr_length*sizeof(int);
    char* buffer = (char*)(new int[size]);

     int *arr = (int*)buffer;
    // Initialize the array with ints
    for (int i=0; i<arr_length; i++) 
      arr[i] = i;

    // Collect the sum in the first element
    for (int i=1; i<arr_length; i++) 
      arr[0] += arr[i];

    Payload data(size,buffer);
    inputs[0] = data;
    printf("Sum: %d\n", arr[0]);
  }

  master.run(inputs);

  std::vector<Point> mypoints; // --> Payload

  size_t v_size;

  char *buffer;
  Payload pay;

  
  fprintf(stderr,"Done\n");
  MPI_Finalize();
  return 0;
}


