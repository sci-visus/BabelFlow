
#
# Try to find the BabelFlow libraries
# Once done this will define
#
# BABELFLOW_FOUND          - system has BabelFlow/TaskGraph.h
# BABELFLOW_INCLUDE_DIR    - path to BabelFlow/TaskGraph.h
# BABELFLOW_LIBRARIES      - path to libbabelflow.a
#
# You can also specify the environment variable BABELFLOW_DIR or define it with
# -DBABELFLOW_DIR=... to hint at the module where to search 
#

IF (NOT DEFINED BABELFLOW_FOUND)
   SET (BABELFLOW_FOUND FALSE)
ENDIF ()

if(ENABLE_MPI)
  set(RUNTIME_SUFFIX mpi)
else()
  set(RUNTIME_SUFFIX charm)
endif()

FIND_PATH(BABELFLOW_INCLUDE_DIR BabelFlow/TaskGraph.h
  HINTS
        ${BabelFlow_DIR}/../../../include
  ${BABELFLOW_DIR}/include
  ${CMAKE_SOURCE_DIR}/../BabelFlow/build_${RUNTIME_SUFFIX}/
  $ENV{BABELFLOW_DIR}
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

FIND_LIBRARY(BABELFLOW_LIBRARIES_RUNTIME babelflow_${RUNTIME_SUFFIX}
       ${BABELFLOW_INCLUDE_DIR}/../lib
       /usr/lib
       /sw/lib
)

FIND_LIBRARY(BABELFLOW_LIBRARIES_CORE babelflow
       ${BABELFLOW_INCLUDE_DIR}/../lib
       /usr/lib
       /sw/lib
)

set(BABELFLOW_LIBRARIES ${BABELFLOW_LIBRARIES_CORE} ${BABELFLOW_LIBRARIES_RUNTIME})

MESSAGE("Using BABELFLOW_INCLUDE_DIR = " ${BABELFLOW_INCLUDE_DIR}) 
MESSAGE("Using BABELFLOW_LIBRARIES   = " ${BABELFLOW_LIBRARIES}) 

IF (BABELFLOW_INCLUDE_DIR AND BABELFLOW_LIBRARIES)

    SET(BABELFLOW_FOUND TRUE)
    IF (CMAKE_VERBOSE_MAKEFILE)
       MESSAGE("Using BABELFLOW_INCLUDE_DIR = " ${BABELFLOW_INCLUDE_DIR}) 
       MESSAGE("Using BABELFLOW_LIBRARIES   = " ${BABELFLOW_LIBRARIES}) 
    ENDIF (CMAKE_VERBOSE_MAKEFILE)

ELSE()
   
    MESSAGE("ERROR BabelFlow library not found on the system")
 
ENDIF()

