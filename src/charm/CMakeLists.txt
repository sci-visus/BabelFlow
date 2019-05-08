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
        EXPORT BabelFlowConfig
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)

export(TARGETS babelflow_charm babelflow NAMESPACE BabelFlow::
        FILE "${CMAKE_CURRENT_BINARY_DIR}/BabelFlowConfig.cmake")

install(FILES ${PROJECT_BINARY_DIR}/src/charm/charm_babelflow.decl.h ${PROJECT_BINARY_DIR}/src/charm/charm_babelflow.def.h DESTINATION ${CMAKE_INSTALL_PREFIX}/include/BabelFlow/charm/)