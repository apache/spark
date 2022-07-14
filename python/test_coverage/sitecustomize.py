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

# Note that this 'sitecustomize' module is a built-in feature in Python.
# If this module is defined, it's executed when the Python session begins.
# `coverage.process_startup()` seeks if COVERAGE_PROCESS_START environment
# variable is set or not. If set, it starts to run the coverage.
try:
    import coverage
    coverage.process_startup()
except ImportError:
    pass
