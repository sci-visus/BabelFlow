set(SRC
    ../BroadcastCallbacks.h
    ../BroadcastGraph.h
    ../BroadcastGraph.cpp
    main.cpp
)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${DATAFLOW_INCLUDE_PATH}
)

add_executable(broadcast ${SRC})
target_link_libraries(broadcast ${DATAFLOW_LIBRARIES})

INSTALL(TARGETS broadcast
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
    