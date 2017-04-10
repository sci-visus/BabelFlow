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

INSTALL(TARGETS dataflow_charm 
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)

#INSTALL(TARGETS ${PROJECT_BINARY_DIR}/src/charm/charm_dataflow.decl.h POST_BUILD 
#	DESTINATION ${CMAKE_INSTALL_PREFIX}/include/charm
#)

add_custom_command(TARGET dataflow_charm POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy
                   ${PROJECT_BINARY_DIR}/src/charm/charm_dataflow.decl.h ${CMAKE_INSTALL_PREFIX}/include/DataFlow/charm/charm_dataflow.decl.h)

add_custom_command(TARGET dataflow_charm POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy
                   ${PROJECT_BINARY_DIR}/src/charm/charm_dataflow.def.h ${CMAKE_INSTALL_PREFIX}/include/DataFlow/charm/charm_dataflow.def.h)
