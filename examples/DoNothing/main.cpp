//
// Created by Jixian Li on 2019-07-27.
//

#include <mpi.h>
#include <vector>


#include <mpi/Controller.h>
#include <ModuloMap.h>
#include <PreProcessInputTaskGraph.hpp>

using namespace std;
using namespace BabelFlow;
using namespace BabelFlow::mpi;

int main(int argc, char **argv) {
  MPI_Init(nullptr, nullptr);
  int mpi_size, mpi_rank;
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);


  MPI_Finalize();
  return 0;
}