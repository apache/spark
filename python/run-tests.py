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
from optparse import OptionParser
import os
import re
import subprocess
import sys
import time


# Append `SPARK_HOME/dev` to the Python path so that we can import the sparktestsupport module
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../dev/"))


from sparktestsupport import SPARK_HOME  # noqa (suppress pep8 warnings)
from sparktestsupport.shellutils import which  # noqa
from sparktestsupport.modules import all_modules  # noqa


python_modules = dict((m.name, m) for m in all_modules if m.python_test_goals if m.name != 'root')


def print_red(text):
    print('\033[31m' + text + '\033[0m')


LOG_FILE = os.path.join(SPARK_HOME, "python/unit-tests.log")


def run_individual_python_test(test_name, pyspark_python):
    env = {'SPARK_TESTING': '1', 'PYSPARK_PYTHON': which(pyspark_python)}
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
        print("ok (%is)" % duration)


def get_default_python_executables():
    python_execs = [x for x in ["python2.6", "python3.4", "pypy"] if which(x)]
    if "python2.6" not in python_execs:
        print("WARNING: Not testing against `python2.6` because it could not be found; falling"
              " back to `python` instead")
        python_execs.insert(0, "python")
    return python_execs


def parse_opts():
    parser = OptionParser(
        prog="run-tests"
    )
    parser.add_option(
        "--python-executables", type="string", default=','.join(get_default_python_executables()),
        help="A comma-separated list of Python executables to test against (default: %default)"
    )
    parser.add_option(
        "--modules", type="string",
        default=",".join(sorted(python_modules.keys())),
        help="A comma-separated list of Python modules to test (default: %default)"
    )

    (opts, args) = parser.parse_args()
    if args:
        parser.error("Unsupported arguments: %s" % ' '.join(args))
    return opts


def main():
    opts = parse_opts()
    print("Running PySpark tests. Output is in python/%s" % LOG_FILE)
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    python_execs = opts.python_executables.split(',')
    modules_to_test = []
    for module_name in opts.modules.split(','):
        if module_name in python_modules:
            modules_to_test.append(python_modules[module_name])
        else:
            print("Error: unrecognized module %s" % module_name)
            sys.exit(-1)
    print("Will test against the following Python executables: %s" % python_execs)
    print("Will test the following Python modules: %s" % [x.name for x in modules_to_test])

    start_time = time.time()
    for python_exec in python_execs:
        python_implementation = subprocess.check_output(
            [python_exec, "-c", "import platform; print(platform.python_implementation())"],
            universal_newlines=True).strip()
        print("Testing with `%s`: " % python_exec, end='')
        subprocess.call([python_exec, "--version"])

        for module in modules_to_test:
            if python_implementation not in module.blacklisted_python_implementations:
                print("Running %s tests ..." % module.name)
                for test_goal in module.python_test_goals:
                    run_individual_python_test(test_goal, python_exec)
    total_duration = time.time() - start_time
    print("Tests passed in %i seconds" % total_duration)


if __name__ == "__main__":
    main()
