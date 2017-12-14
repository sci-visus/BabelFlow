
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

The project can be build using the CMake 

```
mkdir build
cd build
cmake .. 
```
The default CMake configuration will build the BabelFlow using the MPI
runtime. 

To change runtime you can modify the field `RUNTIME_TYPE` in the CMake
configuration.

To build using the Charm++ runtime you need to provide the Charm++ compiler path
setting the `CHARM_COMPILER` as follow:

`ccmake .. -DRUNTIME_TYPE=CHARM -DCHARM_COMPILER=/path/to/charmc`

--------------------------------------
Examples included
--------------------------------------

**Reduction**

The reduction graph example executes a k-way reduction dataflow. 
The tasks defined in this examples perform a simple sum operation.

To execute the version built using the MPI runtime you can run (using for example 8 cores):
`mpirun -np 8 examples/reduction/reduction 8 2`

To execute the version built using the Charm++ runtime you can run (8-way SMP):
`examples/reduction/charm/reduction +p8 8 2`
