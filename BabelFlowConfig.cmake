# - Config file for the FooBar package
# It defines the following variables
#  BABELFLOW_INCLUDE_DIRS - include directories for DataFlow
#  BABELFLOW_LIBRARIES    - libraries to link against
#  FOOBAR_EXECUTABLE   - the bar executable
 
if(NOT BFLOW_FOUND)

  # Compute paths
  get_filename_component(BABELFLOW_CMAKE_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)
 
  # include the main exported targets
  include("${BABELFLOW_CMAKE_DIR}/BabelFlow.cmake")

  set(BFLOW_FOUND TRUE)

endif()
