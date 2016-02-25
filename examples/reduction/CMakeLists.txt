set(SRC
    Reduction.h
    Reduction.cpp
    main.cpp
)

set(SRC2
    Reduction.h
    Reduction.cpp
    main2.cpp
)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${MPI_CXX_INCLUDE_PATH}
)

add_executable(reduction ${SRC})
target_link_libraries(reduction dataflow ${MPI_CXX_LIBRARIES})

add_executable(reduction2 ${SRC2})
target_link_libraries(reduction2 dataflow ${MPI_CXX_LIBRARIES})
    
INSTALL(TARGETS reduction reduction2
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
