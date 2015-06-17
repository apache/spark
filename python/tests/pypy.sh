# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

. tests/common

# Try to test with PyPy
if [ $(which pypy) ]; then
    export PYSPARK_PYTHON="pypy"
    echo "Testing with PyPy version:"
    $PYSPARK_PYTHON --version

    run_core_tests
    run_sql_tests
    run_streaming_tests
else
    echo "Skipping tests with PyPy"
    FAILED=1
fi
