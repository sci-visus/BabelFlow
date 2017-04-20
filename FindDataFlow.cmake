
#
# Try to find the DataFlow libraries
# Once done this will define
#
# DATAFLOW_FOUND          - system has DataFlow/TaskGraph.h
# DATAFLOW_INCLUDE_DIR    - path to DataFlow/TaskGraph.h
# DATAFLOW_LIBRARIES      - path to libdataflow.a
#
# You can also specify the environment variable DATAFLOW_DIR or define it with
# -DDATAFLOW_DIR=... to hint at the module where to search 
#

IF (NOT DEFINED DATAFLOW_FOUND)
   SET (DATAFLOW_FOUND FALSE)
ENDIF ()

if(ENABLE_MPI)
  set(RUNTIME_SUFFIX mpi)
else()
  set(RUNTIME_SUFFIX charm)
endif()

FIND_PATH(DATAFLOW_INCLUDE_DIR DataFlow/TaskGraph.h
  HINTS
  ${DATAFLOW_DIR}/include
  ${CMAKE_SOURCE_DIR}/../DataFlow/build_${RUNTIME_SUFFIX}/
  $ENV{DATAFLOW_DIR}
  PATH_SUFFIXES include build_${RUNTIME_SUFFIX}/include
  PATHS
  ~/Library/Frameworks
  /Library/Frameworks
  /usr/local/include/
  /usr/include/
  /sw # Fink
  /opt/local # DarwinPorts
  /opt/csw # Blastwave
  /opt
)

FIND_LIBRARY(DATAFLOW_LIBRARIES_RUNTIME dataflow_${RUNTIME_SUFFIX}
       ${DATAFLOW_INCLUDE_DIR}/../lib
       /usr/lib
       /sw/lib
)

FIND_LIBRARY(DATAFLOW_LIBRARIES_CORE dataflow
       ${DATAFLOW_INCLUDE_DIR}/../lib
       /usr/lib
       /sw/lib
)

set(DATAFLOW_LIBRARIES ${DATAFLOW_LIBRARIES_CORE} ${DATAFLOW_LIBRARIES_RUNTIME})

MESSAGE("Using DATAFLOW_INCLUDE_DIR = " ${DATAFLOW_INCLUDE_DIR}) 
MESSAGE("Using DATAFLOW_LIBRARIES   = " ${DATAFLOW_LIBRARIES}) 

IF (DATAFLOW_INCLUDE_DIR AND DATAFLOW_LIBRARIES)

    SET(DATAFLOW_FOUND TRUE)
    IF (CMAKE_VERBOSE_MAKEFILE)
       MESSAGE("Using DATAFLOW_INCLUDE_DIR = " ${DATAFLOW_INCLUDE_DIR}) 
       MESSAGE("Using DATAFLOW_LIBRARIES   = " ${DATAFLOW_LIBRARIES}) 
    ENDIF (CMAKE_VERBOSE_MAKEFILE)

ELSE()
   
    MESSAGE("ERROR DataFlow library not found on the system")
 
ENDIF()

