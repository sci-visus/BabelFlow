set(MPI_SRC
    Controller.cpp
)

set(MPI_HEADERS
    Controller.h
)

include_directories(${MPI_CXX_HEADER_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library(babelflow_mpi ${MPI_SRC})
target_link_libraries(babelflow_mpi babelflow)

INSTALL(FILES ${MPI_HEADERS}
        DESTINATION ${CMAKE_INSTALL_PREFIX}/include/BabelFlow/mpi
)

install(TARGETS babelflow_mpi
        EXPORT BabelFlowConfig
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)

export(TARGETS babelflow_mpi babelflow NAMESPACE BabelFlow::
  FILE "${CMAKE_CURRENT_BINARY_DIR}/BabelFlowConfig.cmake")
