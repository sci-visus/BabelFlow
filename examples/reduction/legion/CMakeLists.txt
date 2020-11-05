set(SRC
    ../ReductionGraph.h
    ../ReductionCallbacks.h
    ../ReductionGraph.cpp
    main.cpp
)

#get_target_property(BabelFlow_INCLUDE_DIRS BabelFlow::babelflow INTERFACE_INCLUDE_DIRECTORIES)

#target_link_libraries(pmt_legion pmt idxio BabelFlow::babelflow BabelFlow::babelflow_legion Legion::Legion)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${BABELFLOW_INCLUDE_PATH}
)

add_executable(reduction ${SRC})
target_link_libraries(reduction ${BABELFLOW_LIBRARIES})

INSTALL(TARGETS reduction
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
    
