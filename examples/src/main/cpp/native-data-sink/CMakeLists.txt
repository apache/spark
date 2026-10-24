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

# The cpp_sdk_sink data source, which requires a POSIX filesystem.
if(NOT UNIX)
  return()
endif()

spark_add_native_data_source(cpp_sdk_sink json_sink.cc)
target_compile_options(spark_datasource_cpp_sdk_sink PRIVATE ${SPARK_NATIVE_WARNINGS})

if(SPARK_NATIVE_BUILD_TESTS)
  find_package(Threads REQUIRED)

  add_executable(json_sink_test
    tests/json_sink_test.cc
    json_sink.cc
    "${PROJECT_SOURCE_DIR}/tests/test_main.cc")
  target_include_directories(json_sink_test PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}" "${PROJECT_SOURCE_DIR}/tests")
  target_link_libraries(json_sink_test PRIVATE spark::native_source Threads::Threads)
  target_compile_options(json_sink_test PRIVATE ${SPARK_NATIVE_WARNINGS})
  add_test(NAME json_sink_test COMMAND json_sink_test)
endif()
