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


add_charm_module(charm_babelflow)

add_dependencies(babelflow charm_babelflowCharm)

add_charm_library(charm_babelflow babelflow_charm ${CHARM_HEADERS} ${CHARM_SRC})
target_link_libraries(babelflow_charm babelflow)


INSTALL(FILES ${CHARM_HEADERS}
        DESTINATION ${CMAKE_INSTALL_PREFIX}/include/BabelFlow/charm
)

INSTALL(TARGETS babelflow_charm 
        EXPORT BabelFlowTargets
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)

export(TARGETS babelflow babelflow_charm
  FILE "${PROJECT_BINARY_DIR}/BabelFlowTargets.cmake")

add_custom_command(TARGET babelflow_charm POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy
                   ${PROJECT_BINARY_DIR}/src/charm/charm_babelflow.decl.h ${CMAKE_INSTALL_PREFIX}/include/BabelFlow/charm/charm_babelflow.decl.h)

add_custom_command(TARGET babelflow_charm POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy
                   ${PROJECT_BINARY_DIR}/src/charm/charm_babelflow.def.h ${CMAKE_INSTALL_PREFIX}/include/BabelFlow/charm/charm_babelflow.def.h)

INSTALL(TARGETS babelflow_charm
        EXPORT BabelFlowTargets
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)
