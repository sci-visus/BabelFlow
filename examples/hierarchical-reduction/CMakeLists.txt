set(SRC
    HierarchicalReduction.h
    HierarchicalReduction.cpp
    ${CMAKE_SOURCE_DIR}/examples/reduction/Reduction.cpp
    ${CMAKE_SOURCE_DIR}/examples/reduction/Reduction.h
    ${CMAKE_SOURCE_DIR}/examples/broadcast/Broadcast.cpp
    ${CMAKE_SOURCE_DIR}/examples/broadcast/Broadcast.h
    main.cpp
)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/examples/hierarchical-reduction
    ${CMAKE_SOURCE_DIR}/examples/reduction
    ${CMAKE_SOURCE_DIR}/examples/broadcast
    ${MPI_CXX_INCLUDE_PATH}
)

add_executable(hierarchical-reduction ${SRC})
target_link_libraries(hierarchical-reduction dataflow ${MPI_CXX_LIBRARIES})
    
INSTALL(TARGETS hierarchical-reduction
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
