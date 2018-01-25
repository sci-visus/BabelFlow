
include_directories(
    ./
    ${CMAKE_SOURCE_DIR}/src
    ${DATAFLOW_INCLUDE_PATH}
)

if (ENABLE_MPI)
    add_subdirectory(mpi)
elseif(ENABLE_CHARM)
    add_subdirectory(charm)
 endif()



