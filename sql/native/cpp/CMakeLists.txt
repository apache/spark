# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The experimental C++ SDK for the native data source C ABI proposed in Spark PR #59424, with its
# examples. It is not part of the Maven or SBT build of Spark.

cmake_minimum_required(VERSION 3.16)
project(spark_native_cpp_sdk VERSION 0.1.0 LANGUAGES C CXX)

option(SPARK_NATIVE_BUILD_EXAMPLES "Build the example data sources" ON)
option(SPARK_NATIVE_BUILD_TESTS "Build the tests of the SDK and of the examples" ON)

find_package(Arrow REQUIRED)
find_package(nlohmann_json 3.10 REQUIRED)

# The SDK is header-only: each data source library compiles it, so that no C++ type crosses the
# boundary of the library.
add_library(spark_native_source INTERFACE)
add_library(spark::native_source ALIAS spark_native_source)
target_include_directories(spark_native_source INTERFACE
  "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_compile_features(spark_native_source INTERFACE cxx_std_20)
target_link_libraries(spark_native_source INTERFACE
  Arrow::arrow_shared nlohmann_json::nlohmann_json)

# Builds the shared library of the native data source `name`, spark_datasource_<name>, which is
# the file name that Spark looks for, such as libspark_datasource_<name>.so on Linux. The library
# exports only spark_ds_init: hidden visibility alone would still export the instantiations of
# the templates of Arrow and of the standard library, which other libraries of the process could
# then interpose.
function(spark_add_native_data_source name)
  set(target "spark_datasource_${name}")
  add_library(${target} SHARED ${ARGN})
  target_link_libraries(${target} PRIVATE spark::native_source)
  set_target_properties(${target} PROPERTIES
    C_VISIBILITY_PRESET hidden
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON)
  if(APPLE)
    target_link_options(${target} PRIVATE "LINKER:-exported_symbol,_spark_ds_init")
  elseif(UNIX)
    set(script "${CMAKE_CURRENT_BINARY_DIR}/${target}.map")
    file(WRITE "${script}" "{\n  global: spark_ds_init;\n  local: *;\n};\n")
    target_link_options(${target} PRIVATE "LINKER:--version-script=${script}")
    set_property(TARGET ${target} APPEND PROPERTY LINK_DEPENDS "${script}")
  endif()
endfunction()

if(SPARK_NATIVE_BUILD_TESTS)
  enable_testing()
endif()

# Warnings for the code of this project only.
set(SPARK_NATIVE_WARNINGS
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic>)

if(SPARK_NATIVE_BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(SPARK_NATIVE_BUILD_EXAMPLES)
  set(SPARK_NATIVE_EXAMPLES "${CMAKE_CURRENT_SOURCE_DIR}/../../../examples/src/main/cpp")
  add_subdirectory("${SPARK_NATIVE_EXAMPLES}/native-data-source" examples/native-data-source)
  add_subdirectory("${SPARK_NATIVE_EXAMPLES}/native-data-sink" examples/native-data-sink)
endif()
