
set(SRC
    ../BroadcastCallbacks.h
    ../BroadcastGraph.h
    ../BroadcastGraph.cpp
    main.cpp
)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
    
add_charm_module(broadcast)
add_charm_executable(broadcast broadcast ${SRC})
target_link_libraries(broadcast ${BABELFLOW_LIBRARIES})

INSTALL(TARGETS broadcast
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)

