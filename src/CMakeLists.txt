

SET (DATAFLOW_SRC
    Definitions.h
    Task.h
    Task.cpp
    
    TaskGraph.h 
    TaskGraph.cpp
    RelayTask.h
    Controller.h
    Controller.cpp
    
    ModuloMap.h
    ModuloMap.cpp
     
)

include_directories(${MPI_CXX_INCLUDE_PATH})

add_library(dataflow STATIC ${DATAFLOW_SRC})

target_link_libraries(dataflow ${MPI_CXX_LIBRARIES})