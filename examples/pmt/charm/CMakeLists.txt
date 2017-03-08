
set(HANA_DIR "${CMAKE_PREFIX_PATH}../../hana/build/install" CACHE STRING "Hana installation directory")
include_directories(${HANA_DIR}/include)
find_library(HANA_LIB NAMES hana HINTS ${HANA_DIR}/lib/hana)

set(MERGE_TREE_SRC "../mergetree")

include_directories(
    ${DATAFLOW_INCLUDE_DIR}
)

set(SRC
    ../KWayMerge.h
    ../KWayMerge.cpp
    ../KWayTaskMap.h
    ../KWayTaskMap.cpp
    ../../../idxio/read_block.cpp
    ../../../idxio/read_block.h
    main.cpp
)

file(GLOB MERGE_TREE_SRCS ${MERGE_TREE_SRC}/*.cpp)
include_directories(${MERGE_TREE_SRC})

add_charm_module(pmt)

add_charm_executable(pmt pmt ${SRC} ${MERGE_TREE_SRCS})
target_link_libraries(pmt ${DATAFLOW_LIBRARIES} ${HANA_LIB})

    
INSTALL(TARGETS pmt
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)

