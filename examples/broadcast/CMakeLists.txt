
include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${DATAFLOW_INCLUDE_PATH}
)

if (ENABLE_MPI)
    
    add_subdirectory(mpi)
    
    #set (SRC ${SRC} ./mpi/main.cpp Broadcast.cpp)

    #add_executable(broadcast ${SRC})
    
    #target_link_libraries(broadcast dataflow dataflow_mpi)

elseif(ENABLE_CHARM)
 
     add_subdirectory(charm)
 
 endif()   



