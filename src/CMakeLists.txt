

SET (DATAFLOW_HEADERS
    Definitions.h
    Payload.h
    Task.h
    
    TaskGraph.h 
    RelayTask.h
    
    ModuloMap.h

    HierarchicalTask.h
    HierarchicalTaskGraph.h
)

SET (DATAFLOW_SRC
    Task.cpp    
    TaskGraph.cpp
    ModuloMap.cpp
    
    HierarchicalTask.cpp
    HierarchicalTaskGraph.cpp
)



if (NOT DISABLE_MPI)
  SET (DATAFLOW_HEADERS ${DATAFLOW_HEADERS}
       mpi/Controller.h
  )

  SET (DATAFLOW_SRC ${DATAFLOW_SRC}
      mpi/Controller.cpp
  )


  include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${MPI_CXX_INCLUDE_PATH})
endif()


add_library(dataflow STATIC ${DATAFLOW_SRC})
  
 
  
if (NOT DISABLE_MPI)
  target_link_libraries(dataflow ${MPI_CXX_LIBRARIES})
endif()

  

INSTALL(FILES ${DATAFLOW_HEADERS}
        DESTINATION ${CMAKE_INSTALL_PREFIX}/include/DataFlow
)

INSTALL(TARGETS dataflow
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)

