
set(RED_HEADERS
    reduce/BinarySwap.h
    reduce/BinarySwapTaskMap.h
    reduce/KWayReduction.h
    reduce/KWayReductionTaskMap.h
    reduce/RadixKExchange.h
    reduce/RadixKExchangeTaskMap.h
)

set(RED_SRC
    reduce/BinarySwap.cpp
    reduce/BinarySwapTaskMap.cpp
    reduce/KWayReduction.cpp
    reduce/KWayReductionTaskMap.cpp
    reduce/RadixKExchange.cpp
    reduce/RadixKExchangeTaskMap.cpp
)

set(BASE_BFLOW_HEADERS
    Definitions.h
    TypeDefinitions.h
    Payload.h
    Task.h

    TaskGraph.h
    RelayTask.h

    ModuloMap.h

    HierarchicalTask.h
    HierarchicalTaskGraph.h

    PreProcessInputTaskGraph.hpp
    ModTaskMap.hpp
    
    ComposableTaskGraph.h
    ComposableTaskMap.h
)

set(BASE_BFLOW_SRC
    Task.cpp
    TaskGraph.cpp
    ModuloMap.cpp

    HierarchicalTask.cpp
    HierarchicalTaskGraph.cpp
    
    PreProcessInputTaskGraph.cpp
    
    ComposableTaskGraph.cpp
    ComposableTaskMap.cpp
)

SET(BABELFLOW_HEADERS
    ${BASE_BFLOW_HEADERS}
    ${RED_HEADERS}
)

SET(BABELFLOW_SRC
    ${BASE_BFLOW_SRC}
    ${RED_SRC}
)


if(ENABLE_SHARED)
  add_library(babelflow SHARED ${BABELFLOW_SRC})
else ()
  add_library(babelflow STATIC ${BABELFLOW_SRC})
endif()

if (ENABLE_MPI)
    add_subdirectory(mpi)
    set(BABELFLOW_LIBRARIES ${BABELFLOW_LIBRARIES} babelflow babelflow_mpi PARENT_SCOPE)
elseif (ENABLE_CHARM)
    add_subdirectory(charm)
    set(BABELFLOW_LIBRARIES ${BABELFLOW_LIBRARIES} babelflow babelflow_charm PARENT_SCOPE)
elseif (ENABLE_LEGION)
    add_subdirectory(legion)
    set(BABELFLOW_LIBRARIES ${BABELFLOW_LIBRARIES} babelflow babelflow_legion PARENT_SCOPE)
else ()
    message(FATAL_ERROR "Unrecognized runtime")
endif ()


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
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin)  # This is for Windows

install(FILES ${BASE_BFLOW_HEADERS}
        DESTINATION include/BabelFlow
)

install(FILES ${RED_HEADERS}
        DESTINATION include/BabelFlow/reduce
)

install(EXPORT BabelFlowConfig FILE BabelFlowConfig.cmake NAMESPACE BabelFlow:: DESTINATION lib/cmake/BabelFlow)
