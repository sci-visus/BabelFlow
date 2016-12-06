set(SRC
    ../KWayMerge.h
    ../KWayMerge.cpp
    ../KWayTaskMap.cpp
    ../KWayTaskMap.h
    main.cpp
)

message("DATAFLOW_INCLUDE_PATH = " ${DATAFLOW_INCLUDE_PATH})

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${DATAFLOW_INCLUDE_PATH}
)

add_executable(kwaymerge ${SRC})
target_link_libraries(kwaymerge ${DATAFLOW_LIBRARIES})
    
INSTALL(TARGETS kwaymerge
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
