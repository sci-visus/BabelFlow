set(PROJ mapreduce)

set(SRC
        main.cpp
        ../MapReduceGraph.cpp ../MapReduceGraph.h ../MapReduceCallback.h)

include_directories(
        ${CMAKE_SOURCE_DIR}/src
        ${BABELFLOW_INCLUDE_PATH}
        ${MPI_CXX_HEADER_DIR}
)

add_executable(${PROJ} ${SRC})
target_link_libraries(${PROJ} ${BABELFLOW_LIBRARIES})

INSTALL(TARGETS ${PROJ}
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)