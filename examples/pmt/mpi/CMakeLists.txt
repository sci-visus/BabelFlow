
set(DIY_INCLUDE_DIR "${CMAKE_PREFIX_PATH}../../ParallelMergeTree/diy/include" CACHE STRING "DIY include directory")
set(DIY_LIBRARIES ${DIY_INCLUDE_DIR}/../lib/libdiy.a)

set(MERGE_TREE_SRC "../mergetree")

include_directories(
    ${DATAFLOW_INCLUDE_DIR}
    ${MPI_CXX_INCLUDE_PATH}
    ${DIY_INCLUDE_DIR}
)

set(SRC
    ../KWayMerge.h
    ../KWayMerge.cpp
    ../KWayTaskMap.h
    ../KWayTaskMap.cpp
    main.cpp
)

file(GLOB MERGE_TREE_SRCS ${MERGE_TREE_SRC}/*.cpp)
include_directories(${MERGE_TREE_SRC}/)

include_directories(${CMAKE_CURRENT_BINARY_DIR})
    
add_executable(pmt ${SRC} ${MERGE_TREE_SRCS})
target_link_libraries(pmt ${DATAFLOW_LIBRARIES} ${DIY_LIBRARIES})

    
INSTALL(TARGETS pmt
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)

