cmake_minimum_required(VERSION 2.8.4)

Project(BabelFlow)

set(CMAKE_VERBOSE_MAKEFILE TRUE)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMake")

set(CMAKE_MACOSX_RPATH 1)

set(RUNTIME_TYPE "MPI" CACHE STRING "Runtime Controller (MPI, CHARM, LEGION)")
set_property(CACHE RUNTIME_TYPE PROPERTY STRINGS MPI CHARM LEGION)

if(${RUNTIME_TYPE} MATCHES "MPI")
    set(ENABLE_MPI TRUE)
    set(ENABLE_CHARM FALSE)
    set(ENABLE_LEGION FALSE)
    message("Using MPI Runtime Controller")
elseif(${RUNTIME_TYPE} MATCHES "CHARM")
    set(ENABLE_CHARM TRUE)
    set(ENABLE_MPI FALSE)
    set(ENABLE_LEGION FALSE)
    message("Using Charm++ Runtime Controller")
elseif(${RUNTIME_TYPE} MATCHES "LEGION")
    set(ENABLE_CHARM FALSE)
    set(ENABLE_MPI FALSE)
    set(ENABLE_LEGION TRUE)
    message("Using Legion Runtime Controller")
endif()

if (ENABLE_CHARM)
    find_package(Charm REQUIRED)

    set(CMAKE_C_COMPILER ${CHARM_COMPILER}) 
    set(CMAKE_CXX_COMPILER ${CHARM_COMPILER})
 
    set(BABELFLOW_INCLUDE_PATH ${CMAKE_CURRENT_BINARY_DIR}/src/charm)
    
elseif(ENABLE_MPI)
    if(CMAKE_GENERATOR STREQUAL Xcode)
      set(APPLE_CXX_COMPILER mpicxx)
    endif()

    find_package(MPI REQUIRED)
    include_directories(${MPI_C_INCLUDE_PATH})
    set(CMAKE_CXX_COMPILER ${MPI_CXX_COMPILER})
    set(CMAKE_C_COMPILER ${MPI_C_COMPILER})
    
    set(BABELFLOW_INCLUDE_PATH ${MPI_CXX_INCLUDE_PATH})
    set(BABELFLOW_LIBRARIES ${MPI_CXX_LIBRARIES})

elseif(ENABLE_LEGION)

endif()


if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set (CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/" CACHE PATH "default install path" FORCE )
endif()

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
CHECK_CXX_COMPILER_FLAG("-std=c++0x" COMPILER_SUPPORTS_CXX0X)


if(COMPILER_SUPPORTS_CXX11)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
elseif(COMPILER_SUPPORTS_CXX0X)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
else()
        message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
endif()

add_subdirectory(src)

add_subdirectory(examples)

export(PACKAGE BabelFlow)
install(FILES CMake/FindBabelFlow.cmake DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/BabelFlow)
