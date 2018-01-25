

SET (DATAFLOW_HEADERS
    Definitions.h
    Payload.h
    Task.h
    
    TaskGraph.h 
    RelayTask.h
    
    ModuloMap.h

    HierarchicalTask.h
    HierarchicalTaskGraph.h
)

SET (DATAFLOW_SRC
    Task.cpp    
    TaskGraph.cpp
    ModuloMap.cpp
    
    HierarchicalTask.cpp
    HierarchicalTaskGraph.cpp
)

add_library(dataflow STATIC ${DATAFLOW_SRC})

if (ENABLE_MPI)
    add_subdirectory(mpi)
    set(DATAFLOW_LIBRARIES ${DATAFLOW_LIBRARIES} dataflow dataflow_mpi PARENT_SCOPE)
elseif(ENABLE_CHARM)
    add_subdirectory(charm)
    set(DATAFLOW_LIBRARIES ${DATAFLOW_LIBRARIES} dataflow dataflow_charm PARENT_SCOPE)
else()
    message(FATAL_ERROR "Unrecognized runtime")
endif()

INSTALL(FILES ${DATAFLOW_HEADERS}
        DESTINATION ${CMAKE_INSTALL_PREFIX}/include/DataFlow
)

INSTALL(TARGETS dataflow
        EXPORT DataFlowTargets
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)