#!/usr/bin/env python2

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
import os
import re
import subprocess
import time

from sparktestsupport import SPARK_HOME
from sparktestsupport.shellutils import which
from sparktestsupport.modules import all_modules


def print_red(text):
    print('\033[31m' + text + '\033[0m')


LOG_FILE = os.path.join(SPARK_HOME, "python/unit-tests.log")


def run_individual_python_test(test_name, pyspark_python=None):
    env = {'SPARK_TESTING': '1'}
    if pyspark_python:
        env["PYSPARK_PYTHON"] = pyspark_python
    print("    Running test: %s ..." % test_name, end='')
    start_time = time.time()
    with open(LOG_FILE, 'a') as log_file:
        retcode = subprocess.call(
            [os.path.join(SPARK_HOME, "bin/pyspark"), test_name],
            stderr=log_file, stdout=log_file, env=env)
    duration = time.time() - start_time
    # Exit on the first failure.
    if retcode != 0:
        with open(LOG_FILE, 'r') as log_file:
            for line in log_file:
                if not re.match('[0-9]+', line):
                    print(line, end='')
        print_red("\nHad test failures in %s; see logs." % test_name)
        exit(-1)
    else:
        print("ok (%fs)" % (duration / 10000.0))


def main():
    # TODO: do we need to remove the metastore and warehouse created by the SQL tests? Ask Ahir.
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    python_execs = [x for x in ["python2.6", "python3.4", "pypy"] if which(x)]
    if "python2.6" not in python_execs:
        print("WARNING: Not testing against `python2.6` because it could not be found; falling"
              " back to `python` instead")
        python_execs.insert(0, "python")
    print("Will test against the following Python executables: %s" % python_execs)
    start_time = time.time()
    for python_exec in python_execs:
        print("Testing with `%s`: " % python_exec, end='')
        subprocess.call([python_exec, "--version"])

        python_modules = [m for m in all_modules if m.python_test_goals]
        for module in python_modules:
            print("Running %s tests ..." % module.name)
            for test_goal in module.python_test_goals:
                run_individual_python_test(test_goal)
    total_duration = time.time() - start_time
    print("Tests passed in %f seconds" % (total_duration / 1000.0))


if __name__ == "__main__":
    main()
