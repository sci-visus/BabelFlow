function(add_charm_module MODULE)
    
    add_custom_command(OUTPUT ${MODULE}.decl.h ${MODULE}.def.h
                       DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${MODULE}.ci
                       COMMAND ${CHARM_COMPILER} -I${CMAKE_CURRENT_SOURCE_DIR}/..
                       ${CMAKE_CURRENT_SOURCE_DIR}/${MODULE}.ci)

    add_custom_target(${MODULE}Charm
                      DEPENDS ${MODULE}.decl.h ${MODULE}.def.h)
     
endfunction()

function(add_charm_library MODULE NAME)
    
    set(MYARGS ${ARGV})
    list(REMOVE_AT MYARGS 0)
    list(REMOVE_AT MYARGS 0)
    
    # Add the binary directory as include in order to find the
    # .dcl.h and .def.h files
    include_directories(${CMAKE_CURRENT_BINARY_DIR})
    add_library(${NAME} ${MYARGS})
    
    # Add dependency of new library on new Charm++ module 
    add_dependencies(${NAME} ${MODULE}Charm)

endfunction()

function(add_charm_executable MODULE NAME)
            
    set(MYARGS ${ARGV})
    list(REMOVE_AT MYARGS 0)
    list(REMOVE_AT MYARGS 0)
    
    # Add the binary directory as include in order to find the
    # .dcl.h and .def.h files
    include_directories(${CMAKE_CURRENT_BINARY_DIR})
    add_executable(${NAME} ${MYARGS})
    
    # Add dependency of new library on new Charm++ module 
    add_dependencies(${NAME} ${MODULE}Charm)
 
endfunction()

find_program(CHARM_COMPILER charmc
    PATHS ${CHARM_DIR}
    PATH_SUFFIXES charm/bin bin
)
if (NOT CHARM_COMPILER)
    message(FATAL_ERROR "ERROR: Charm++ compiler not found")
endif() 
