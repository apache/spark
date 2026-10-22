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

# The cpp_sdk_range data source: libspark_datasource_cpp_sdk_range.so on Linux.
spark_add_native_data_source(cpp_sdk_range range_source.cc)
target_compile_options(spark_datasource_cpp_sdk_range PRIVATE ${SPARK_NATIVE_WARNINGS})

if(SPARK_NATIVE_BUILD_TESTS)
  find_package(Threads REQUIRED)

  add_executable(range_source_test
    tests/range_source_test.cc
    range_source.cc
    "${PROJECT_SOURCE_DIR}/tests/test_main.cc")
  target_include_directories(range_source_test PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}" "${PROJECT_SOURCE_DIR}/tests")
  target_link_libraries(range_source_test PRIVATE spark::native_source Threads::Threads)
  target_compile_options(range_source_test PRIVATE ${SPARK_NATIVE_WARNINGS})
  add_test(NAME range_source_test COMMAND range_source_test)

  # An independent C client of the library, which compiles against the C header only and loads
  # the library like Spark does, without a JVM. It uses dlopen and pthreads, on Linux or macOS.
  if(UNIX)
    add_executable(c_abi_smoke tests/c_abi_smoke.c)
    target_include_directories(c_abi_smoke PRIVATE "${PROJECT_SOURCE_DIR}/include")
    set_target_properties(c_abi_smoke PROPERTIES C_STANDARD 11 C_STANDARD_REQUIRED ON)
    target_compile_options(c_abi_smoke PRIVATE
      $<$<C_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Werror>)
    target_link_libraries(c_abi_smoke PRIVATE Threads::Threads ${CMAKE_DL_LIBS})
    add_test(NAME range_c_abi_smoke
      COMMAND c_abi_smoke $<TARGET_FILE:spark_datasource_cpp_sdk_range>)
  endif()
endif()
