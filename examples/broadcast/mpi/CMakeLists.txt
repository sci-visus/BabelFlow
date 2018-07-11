set(SRC
    ../BroadcastCallbacks.h
    ../BroadcastGraph.h
    ../BroadcastGraph.cpp
    main.cpp
)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${BABELFLOW_INCLUDE_PATH}
    ${MPI_CXX_HEADER_DIR}
)

add_executable(broadcast ${SRC})
target_link_libraries(broadcast ${BABELFLOW_LIBRARIES})

INSTALL(TARGETS broadcast
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
    