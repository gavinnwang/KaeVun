enable_testing()

include(GoogleTest)
file(GLOB_RECURSE TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*test.cc")
message(STATUS "Discovered test sources: ${TEST_SOURCES}")
set(TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test)

foreach(test_source ${TEST_SOURCES})
  get_filename_component(test_name ${test_source} NAME_WE)
  add_executable(${test_name} ${test_source})
  target_include_directories(${test_name}
      PRIVATE
      ${PROJECT_SOURCE_DIR}/src
      ${TEST_INCLUDE_DIR}
  )
  target_link_libraries(${test_name} PRIVATE kv gtest_main fmt::fmt)
  gtest_discover_tests(${test_name})
endforeach ()
