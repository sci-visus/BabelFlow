
set(SRC
    ../CompositingUtils.h
    ../CompositingUtils.cpp
    main.cpp
)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
    
add_charm_module(radixk)
add_charm_executable(radixk radixk ${SRC})
target_link_libraries(radixk ${BABELFLOW_LIBRARIES})

    
INSTALL(TARGETS radixk
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)

