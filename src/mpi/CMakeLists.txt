set(MPI_SRC
    Controller.cpp
)

set(MPI_HEADERS
    Controller.h
)

include_directories(${MPI_CXX_HEADER_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

if(ENABLE_SHARED)
  add_library(babelflow_mpi SHARED ${MPI_SRC})
else()
  add_library(babelflow_mpi STATIC ${MPI_SRC})
endif()

target_link_libraries(babelflow_mpi babelflow)

install(FILES ${MPI_HEADERS}
        DESTINATION include/BabelFlow/mpi
)

install(TARGETS babelflow_mpi
        EXPORT BabelFlowConfig
        LIBRARY DESTINATION lib
)

export(TARGETS babelflow_mpi babelflow NAMESPACE BabelFlow::
  FILE "${CMAKE_CURRENT_BINARY_DIR}/BabelFlowConfig.cmake")
