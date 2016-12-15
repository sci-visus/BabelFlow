set(CHARM_SRC
    CharmTask.cpp
    Controller.cpp
)

set(CHARM_HEADERS
    CharmTask.h
    Controller.h
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})


add_charm_module(charm_dataflow)

add_dependencies(dataflow charm_dataflowCharm)


add_charm_library(charm_dataflow dataflow_charm ${CHARM_HEADERS} ${CHARM_SRC})
target_link_libraries(dataflow_charm dataflow)


INSTALL(FILES ${CHARM_HEADERS}
        DESTINATION ${CMAKE_INSTALL_PREFIX}/include/DataFlow/charm
)

#INSTALL(TARGETS dataflow_charm
#        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
#)
