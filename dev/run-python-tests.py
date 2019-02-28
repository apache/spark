#!/usr/bin/env python

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

from __future__ import print_function
import logging

from build_environment import get_build_environment, modules_to_test
from sparktestsupport.shellutils import subprocess_check_output
from test_functions import *

LOGGER = logging.getLogger()

all_python_executables = ["python2.7", "python3.6"]

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    env = get_build_environment()
    mtt = modules_to_test(env)

    circleNodeIndex = os.getenv("CIRCLE_NODE_INDEX")
    circleNodeTotal = os.getenv("CIRCLE_NODE_TOTAL")
    if circleNodeTotal is not None:
        length = len(all_python_executables)
        fromExec = int(circleNodeIndex) * length / int(circleNodeTotal)
        toExec = (int(circleNodeIndex) + 1) * length / int(circleNodeTotal)
        python_executables_for_run = all_python_executables[fromExec:toExec]
    else:
        python_executables_for_run = all_python_executables

    LOGGER.info("Testing following python executables in this run: %s", python_executables_for_run)

    modules_with_python_tests = [m for m in mtt.test_modules if m.python_test_goals]
    if modules_with_python_tests:
        run_python_tests(modules_with_python_tests, 8, python_executables_for_run, False)

        # Packaging tests create a conda environment for each python version
        # We'd like to use the same version that our executables above use
        python_exact_versions = [
            subprocess_check_output(
                [python_exec, "-c", "import platform; print(platform.python_version())"],
                universal_newlines=True).strip()
            for python_exec in python_executables_for_run
        ]
        LOGGER.info("Running python packaging tests for following python versions using conda: %s",
                    python_exact_versions)
        run_python_packaging_tests(use_conda=True, python_versions=python_exact_versions)
