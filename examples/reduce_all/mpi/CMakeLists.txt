set(SRC
        ../ReduceAllGraph.h
        ../ReduceAllCallbacks.h
        ../ReduceAllGraph.cpp
        main.cpp
        )

include_directories(
        ${CMAKE_SOURCE_DIR}/src
        ${BABELFLOW_INCLUDE_PATH}
        ${MPI_CXX_HEADER_DIR}
)

add_executable(reduceall ${SRC})
target_link_libraries(reduceall ${BABELFLOW_LIBRARIES})

INSTALL(TARGETS reduceall
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
    
