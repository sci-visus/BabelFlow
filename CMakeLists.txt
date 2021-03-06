cmake_minimum_required(VERSION 2.8.4)

Project(BabelFlow)

set(CMAKE_VERBOSE_MAKEFILE TRUE)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/CMake")

set(RUNTIME_TYPE "MPI" CACHE STRING "Runtime Controller (MPI, CHARM, LEGION)")
set_property(CACHE RUNTIME_TYPE PROPERTY STRINGS MPI CHARM LEGION)

option(ENABLE_SHARED "Build shared library" ON)

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

    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)
    add_compile_options(-pthread)

    find_package(MPI REQUIRED)
    include_directories(${MPI_C_INCLUDE_PATH})
    set(CMAKE_CXX_COMPILER ${MPI_CXX_COMPILER})
    set(CMAKE_C_COMPILER ${MPI_C_COMPILER})
    
    set(BABELFLOW_INCLUDE_PATH ${MPI_CXX_INCLUDE_PATH})
    set(BABELFLOW_LIBRARIES ${MPI_CXX_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

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


# RPath Settings: only apply rpath settings for builds using shared libs
if(ENABLE_SHARED)
  # use, i.e. don't skip the full RPATH for the build tree
  set(CMAKE_SKIP_BUILD_RPATH  FALSE)

  # when building, don't use the install RPATH already
  # (but later on when installing)
  set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
  set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
  set(CMAKE_INSTALL_NAME_DIR "${CMAKE_INSTALL_PREFIX}/lib")

  # add the automatically determined parts of the RPATH
  # which point to directories outside the build tree to the install RPATH
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

  # the RPATH to be used when installing, but only if it's not a system directory
  list(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
  if("${isSystemDir}" STREQUAL "-1")
    set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
  endif()

endif()

add_subdirectory(src)

add_subdirectory(examples)

install(EXPORT BabelFlow NAMESPACE BabelFlow:: DESTINATION lib/cmake)
install(FILES BabelFlowConfig.cmake DESTINATION lib/cmake)
