set(MPI_SRC
    Controller.cpp
)

set(MPI_HEADERS
    Controller.h
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library(dataflow_mpi ${MPI_SRC})
target_link_libraries(dataflow_mpi dataflow)

INSTALL(FILES ${MPI_HEADERS}
        DESTINATION ${CMAKE_INSTALL_PREFIX}/include/DataFlow/mpi
)

INSTALL(TARGETS dataflow_mpi
        EXPORT DataFlowTargets
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)

export(TARGETS dataflow dataflow_mpi
  FILE "${PROJECT_BINARY_DIR}/DataFlowTargets.cmake")
