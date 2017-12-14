
--------------------------------------
Overview
--------------------------------------

BabelFlow is an Embedded Domain Specific Language to describe algorithms 
using a task graph abstraction which allows them to be executed on top 
of one of several available runtime systems.

The code is released under BSD 3 licence (attached) and currently includes
example use cases using MPI and Charm++ runtime systems.

--------------------------------------
Build
--------------------------------------

The project can be build using CMake as following

```
mkdir build
cd build
cmake .. 
make
```

The default CMake configuration will build the BabelFlow using the MPI
runtime. 

To change runtime you can modify the field `RUNTIME_TYPE` in the CMake
configuration to `MPI` or `CHARM`.

To build and run your application using the Charm++ runtime you need to provide the Charm++ compiler path setting the field `CHARM_COMPILER` as follow:

`ccmake .. -DRUNTIME_TYPE=CHARM -DCHARM_COMPILER=/path/to/charmc`

--------------------------------------
Examples included
--------------------------------------

**Reduction**

The reduction graph example executes a k-way reduction dataflow. 
The tasks defined in this examples perform two simple operations: sum and print (the final result).

The executable requires 2 inputs: `<nr-of-leafs> <fan_in>`

To execute the application using the MPI runtime you can run (using for example 8 cores):

`mpirun -np 8 examples/reduction/mpi/reduction 16 2`

To execute the version built using the Charm++ runtime you can run (8-way SMP):

`examples/reduction/charm/reduction +p8 16 2`

Note that the size of the graph (e.g., number of leafs) is not bounded by
the number of cores you are using. For example we are using 8 ranks to execute
a reduction graph with 16 leafs.

This means that you can easily debug bigger scale runs on a reduced number
of computing resources.

**Broadcast**

The broadcast graph example executes a k-way broadcast dataflow.
This example defines only one task which computes the sum of the numbers
from 0 to 99 and compares the result with the input data which has been
propagated through the broadcast dataflow.

The executable requires 2 inputs: `<nr-of-leafs> <fan_out>`

To execute the application using the MPI runtime you can run (using for example 8 cores):

`mpirun -np 8 examples/broadcast/mpi/broadcast 16 2`

To execute the application using the Charm++ runtime you can run (8-way SMP):

`examples/broadcast/charm/broadcast +p8 16 2`
