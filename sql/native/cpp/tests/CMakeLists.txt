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

find_package(Threads REQUIRED)

# Unit tests of the SDK, which export no data source.
add_executable(sdk_test
  test_main.cc
  abi_test.cc
  capabilities_test.cc
  predicate_test.cc
  stream_test.cc
  task_context_test.cc)
target_link_libraries(sdk_test PRIVATE spark::native_source Threads::Threads)
target_compile_options(sdk_test PRIVATE ${SPARK_NATIVE_WARNINGS})
add_test(NAME sdk_test COMMAND sdk_test)

# Tests of the actual function table of a data source, each with its own spark_ds_init.
foreach(test library_test read_test batch_write_test streaming_test)
  add_executable(${test} test_main.cc ${test}.cc)
  target_link_libraries(${test} PRIVATE spark::native_source Threads::Threads)
  target_compile_options(${test} PRIVATE ${SPARK_NATIVE_WARNINGS})
  add_test(NAME ${test} COMMAND ${test})
endforeach()
