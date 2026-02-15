cmake_minimum_required(VERSION 3.14)
project(furball_tests CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

find_library(ODBC_LIB odbc REQUIRED)

add_executable(furball_tests
  test_connection.cpp
  test_execution.cpp
  test_datatypes.cpp
  test_parameters.cpp
  test_getdata.cpp
  test_metadata.cpp
  test_transactions.cpp
  test_errors.cpp
  test_edge_cases.cpp
  test_catalog.cpp
  test_getfunctions.cpp
)

target_link_libraries(furball_tests PRIVATE gtest gtest_main ${ODBC_LIB})
target_include_directories(furball_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
