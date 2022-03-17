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

import os

SPARK_HOME = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../"))
USER_HOME = os.environ.get("HOME")
ERROR_CODES = {
    "BLOCK_GENERAL": 10,
    "BLOCK_RAT": 11,
    "BLOCK_SCALA_STYLE": 12,
    "BLOCK_PYTHON_STYLE": 13,
    "BLOCK_R_STYLE": 14,
    "BLOCK_DOCUMENTATION": 15,
    "BLOCK_BUILD": 16,
    "BLOCK_MIMA": 17,
    "BLOCK_SPARK_UNIT_TESTS": 18,
    "BLOCK_PYSPARK_UNIT_TESTS": 19,
    "BLOCK_SPARKR_UNIT_TESTS": 20,
    "BLOCK_JAVA_STYLE": 21,
    "BLOCK_BUILD_TESTS": 22,
    "BLOCK_PYSPARK_PIP_TESTS": 23,
    "BLOCK_SCALA_VERSION": 24,
    "BLOCK_TIMEOUT": 124,
}
