

SET (BABELFLOW_HEADERS
    Definitions.h
    Payload.h
    Task.h
    
    TaskGraph.h 
    RelayTask.h
    
    ModuloMap.h

    HierarchicalTask.h
    HierarchicalTaskGraph.h
)

SET (BABELFLOW_SRC
    Task.cpp    
    TaskGraph.cpp
    ModuloMap.cpp
    
    HierarchicalTask.cpp
    HierarchicalTaskGraph.cpp
)

add_library(babelflow STATIC ${BABELFLOW_SRC})

if (ENABLE_MPI)
    add_subdirectory(mpi)
    set(BABELFLOW_LIBRARIES ${BABELFLOW_LIBRARIES} babelflow babelflow_mpi PARENT_SCOPE)
elseif(ENABLE_CHARM)
    add_subdirectory(charm)
    set(BABELFLOW_LIBRARIES ${BABELFLOW_LIBRARIES} babelflow babelflow_charm PARENT_SCOPE)
else()
    message(FATAL_ERROR "Unrecognized runtime")
endif()

INSTALL(FILES ${BABELFLOW_HEADERS}
        DESTINATION ${CMAKE_INSTALL_PREFIX}/include/BabelFlow
)

INSTALL(TARGETS babelflow
        EXPORT BabelFlowTargets
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)
