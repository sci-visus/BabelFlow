
include(GNUInstallDirs)

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
#
#INSTALL(FILES ${BABELFLOW_HEADERS}
#        DESTINATION ${CMAKE_INSTALL_PREFIX}/include/BabelFlow
#)
#
#INSTALL(TARGETS babelflow
#        EXPORT BabelFlowTargets
#        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
#)

target_include_directories(babelflow
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR})

install(TARGETS babelflow EXPORT BabelFlowConfig
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})  # This is for Windows
install(DIRECTORY ./ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/BabelFlow)

install(EXPORT BabelFlowConfig FILE BabelFlowConfig.cmake NAMESPACE BabelFlow:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/BabelFlow)

install(FILES "${PROJECT_BINARY_DIR}/BabelFlowTargets.cmake" DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/BabelFlow)
