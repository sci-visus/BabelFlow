set(SRC
    ../ReductionGraph.h
    ../ReductionCallbacks.h
    ../ReductionGraph.cpp
    main.cpp
)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${BABELFLOW_INCLUDE_PATH}
    ${MPI_CXX_HEADER_DIR}
)

add_executable(reduction ${SRC})
target_link_libraries(reduction ${BABELFLOW_LIBRARIES})

INSTALL(TARGETS reduction
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
    
