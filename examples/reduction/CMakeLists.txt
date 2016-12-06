
include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${DATAFLOW_INCLUDE_PATH}
)


if (ENABLE_MPI)
    
    add_subdirectory(mpi)
    
    set (SRC ${SRC} ./mpi/main.cpp)

    add_executable(reduction ${SRC})
    target_link_libraries(reduction ${DATAFLOW_LIBRARIES})

elseif(ENABLE_CHARM)
 
     add_subdirectory(charm)
 
 endif()   



