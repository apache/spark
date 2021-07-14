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
import sys


def version_check(python_env, major_python_version):
    """
        These are various tests to test the Python container image.
        This file will be distributed via --py-files in the e2e tests.
    """
    env_version = os.environ.get('PYSPARK_PYTHON', 'python3')
    print("Python runtime version check is: " +
          str(sys.version_info[0] == major_python_version))

    print("Python environment version check is: " +
          str(env_version == python_env))
