include_directories(${BABELFLOW_INCLUDE_PATH} ${MPI_CXX_HEADER_DIR})

add_executable(test main.cpp DoNothingTaskGraph.cpp)
target_link_libraries(test ${BABELFLOW_LIBRARIES})
