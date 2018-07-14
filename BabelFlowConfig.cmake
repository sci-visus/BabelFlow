# - Config file for the FooBar package
# It defines the following variables
#  BABELFLOW_INCLUDE_DIRS - include directories for DataFlow
#  BABELFLOW_LIBRARIES    - libraries to link against
#  FOOBAR_EXECUTABLE   - the bar executable
 
# Compute paths
get_filename_component(BABELFLOW_CMAKE_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)
set(BABELFLOW_INCLUDE_DIRS ${BabelFlow_DIR}/../include)
 
# Our library dependencies (contains definitions for IMPORTED targets)
if(NOT TARGET DataFlow AND NOT BABELFLOW_BINARY_DIR)
    include("${BABELFLOW_CMAKE_DIR}/BabelFlowTargets.cmake")
endif()

if(${RUNTIME_TYPE} MATCHES "MPI")
  set(BABELFLOW_LIBRARIES dataflow dataflow_mpi)
else()
  set(BABELFLOW_LIBRARIES dataflow dataflow_charm)
endif()