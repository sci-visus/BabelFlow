
find_package(Legion REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/serialization)

SET (LEGION_CONTROLLER_HEADERS
    Controller.h
    DataFlowMapper.h
    datatypes.h
    Utils.h
#    SuperTask.h
)

SET (LEGION_CONTROLLER_SRC
    Controller.cc
    DataFlowMapper.cc
    Utils.cc
    cgmapper.cc
#    SuperTask.cc
)

add_library(babelflow_legion STATIC ${LEGION_CONTROLLER_SRC})
target_link_libraries(babelflow_legion babelflow Legion::Legion)

INSTALL(FILES ${LEGION_CONTROLLER_HEADERS}
        DESTINATION ${CMAKE_INSTALL_PREFIX}/include/BabelFlow/legion
        )

install(TARGETS babelflow_legion
        EXPORT BabelFlowTargets
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
        )

export(TARGETS babelflow babelflow_legion NAMESPACE BabelFlow::
        FILE "${CMAKE_CURRENT_BINARY_DIR}/BabelFlowTargets.cmake")

install(EXPORT BabelFlowTargets FILE BabelFlowTargets.cmake DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/BabelFlow)
