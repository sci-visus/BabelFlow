set(SRC
    ../lodepng.h
    ../lodepng.cpp
    ../CompositingUtils.h
    ../CompositingUtils.cpp
    main.cpp
)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${BABELFLOW_INCLUDE_PATH}
    ${MPI_CXX_HEADER_DIR}
)

add_executable(radixk ${SRC})
target_link_libraries(radixk ${BABELFLOW_LIBRARIES})

INSTALL(TARGETS radixk
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)

