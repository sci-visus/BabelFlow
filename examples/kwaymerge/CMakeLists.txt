set(SRC
    KWayMerge.h
    KWayMerge.cpp
    KWayTaskMap.cpp
    KWayTaskMap.h
    main.cpp
)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${MPI_CXX_INCLUDE_PATH}
)

add_executable(kwaymerge ${SRC})
target_link_libraries(kwaymerge dataflow ${MPI_CXX_LIBRARIES})
    
INSTALL(TARGETS kwaymerge
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
