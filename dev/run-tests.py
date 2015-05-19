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

import os
import re
import sys
import shutil
import subprocess
from collections import namedtuple

SPARK_PROJ_ROOT = \
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
USER_HOME_DIR = os.environ.get("HOME")

SBT_MAVEN_PROFILE_ARGS_ENV = "SBT_MAVEN_PROFILES_ARGS"
AMPLAB_JENKINS_BUILD_TOOL = os.environ.get("AMPLAB_JENKINS_BUILD_TOOL")
AMPLAB_JENKINS = os.environ.get("AMPLAB_JENKINS")

SBT_OUTPUT_FILTER = re.compile("^.*[info].*Resolving" + "|" + 
                               "^.*[warn].*Merging" + "|" +
                               "^.*[info].*Including")


def get_error_codes(err_code_file):
    """Function to retrieve all block numbers from the `run-tests-codes.sh`
    file to maintain backwards compatibility with the `run-tests-jenkins` 
    script"""
    
    with open(err_code_file, 'r') as f:
        err_codes = [e.split()[1].strip().split('=') 
                     for e in f if e.startswith("readonly")]
        return dict(err_codes)


def exit_from_command_with_retcode(cmd, retcode):
    print "[error] running", cmd, "; received return code", retcode
    sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))


def rm_r(path):
    """Given an arbitrary path properly remove it with the correct python
    construct if it exists
    - from: http://stackoverflow.com/a/9559881"""

    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.exists(path):
        os.remove(path)


def lineno():
    """Returns the current line number in our program
    - from: http://stackoverflow.com/a/3056059"""

    return inspect.currentframe().f_back.f_lineno


def run_cmd(cmd):
    """Given a command as a list of arguments will attempt to execute the
    command and, on failure, print an error message"""

    if not isinstance(cmd, list):
        cmd = cmd.split()
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        exit_from_command_with_retcode(e.cmd, e.returncode)


def set_sbt_maven_profile_args():
    """Properly sets the SBT environment variable arguments with additional
    checks to determine if this is running on an Amplab Jenkins machine"""

    # base environment values for SBT_MAVEN_PROFILE_ARGS_ENV which will be appended on
    sbt_maven_profile_args_base = ["-Pkinesis-asl"]

    sbt_maven_profile_arg_dict = {
        "hadoop1.0" : ["-Phadoop-1", "-Dhadoop.version=1.0.4"],
        "hadoop2.0" : ["-Phadoop-1", "-Dhadoop.version=2.0.0-mr1-cdh4.1.1"],
        "hadoop2.2" : ["-Pyarn", "-Phadoop-2.2"],
        "hadoop2.3" : ["-Pyarn", "-Phadoop-2.3", "-Dhadoop.version=2.3.0"],
    }

    # set the SBT maven build profile argument environment variable and ensure
    # we build against the right version of Hadoop
    if os.environ.get("AMPLAB_JENKINS_BUILD_PROFILE"):
        os.environ[SBT_MAVEN_PROFILE_ARGS_ENV] = \
            " ".join(sbt_maven_profile_arg_dict.get(ajbp, []) 
                     + sbt_maven_profile_args_base)
    else:
        os.environ[SBT_MAVEN_PROFILE_ARGS_ENV] = \
            " ".join(sbt_maven_profile_arg_dict.get("hadoop2.3", [])
                     + sbt_maven_profile_args_base)


def is_exe(path):
    """Check if a given path is an executable file
    - from: http://stackoverflow.com/a/377028"""

    return os.path.isfile(path) and os.access(path, os.X_OK)


def which(program):
    """Find and return the given program by its absolute path or 'None'
    - from: http://stackoverflow.com/a/377028"""

    fpath, fname = os.path.split(program)

    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ.get("PATH").split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None


def determine_java_executable():
    """Will return the *best* path possible for a 'java' executable or `None`"""

    java_home = os.environ.get("JAVA_HOME")

    # check if there is an executable at $JAVA_HOME/bin/java
    java_exe = which(os.path.join(java_home, "bin/java"))
    # if the java_exe wasn't set, check for a `java` version on the $PATH
    return java_exe if java_exe else which("java")


def determine_java_version(java_exe):
    """Given a valid java executable will return its version in named tuple format
    with accessors '.major', '.minor', '.patch', '.update'"""

    raw_output = subprocess.check_output([java_exe, "-version"], 
                                         stderr=subprocess.STDOUT)
    raw_version_str = raw_output.split('\n')[0] # eg 'java version "1.8.0_25"'
    version_str = raw_version_str.split()[-1].strip('"') # eg '1.8.0_25'
    version, update = version_str.split('_') # eg ['1.8.0', '25']

    JavaVersion = namedtuple('JavaVersion', 
                             ['major', 'minor', 'patch', 'update'])

    # map over the values and convert them to integers
    version_info = map(lambda x: int(x), version.split('.') + [update])

    return JavaVersion(major=version_info[0],
                       minor=version_info[1],
                       patch=version_info[2],
                       update=version_info[3])


def multi_starts_with(orig_str, *prefixes):
    """Takes a string and an abritrary number of prefixes then checks the
    original string for any of the possible prefixes passed in"""

    for s in prefixes:
        if orig_str.startswith(s):
            return True
    return False


def determine_test_suite():
    """This function current acts to determine if SQL tests need to be run in
    addition to the core test suite *or* if _only_ SQL tests need to be run
    as the git logs show that to be the only thing touched. In the future
    this function will act more generically to help further segregate the
    test suite runner (hence the function name).
    @return a set of unique test names"""
    test_suite = list()

    if AMPLAB_JENKINS:
        run_cmd(['git', 'fetch', 'origin', 'master:master'])

        raw_output = subprocess.check_output(['git', 'diff', '--name-only', 'master'])
        # remove any empty strings
        changed_files = [f for f in raw_output.split('\n') if f]

        # find any sql files
        sql_files = [f for f in changed_files
                     if multi_starts_with(f, 
                                          "sql/", 
                                          "bin/spark-sql", 
                                          "sbin/start-thriftserver.sh")]

        non_sql_files = set(changed_files).difference(set(sql_files))

        if non_sql_files:
            test_suite.append("CORE")
        if sql_files:
            print "[info] Detected changes in SQL. Will run Hive test suite."
            test_suite.append("SQL")
            if not non_sql_files:
                print "[info] Detected no changes except in SQL. Will only run SQL tests."
        return set(test_suite)
    else:
        # we aren't in the Amplab environment so merely run all tests
        test_suite.append("CORE")
        test_suite.append("SQL")
        return set(test_suite)


def set_title_and_block(title, err_block):
    os.environ["CURRENT_BLOCK"] = error_codes[err_block]
    line_str = '=' * 72

    print
    print line_str
    print title
    print line_str


def run_apache_rat_checks():
    set_title_and_block("Running Apache RAT checks", "BLOCK_RAT")
    run_cmd(["./dev/check-license"])


def run_scala_style_checks():
    set_title_and_block("Running Scala style checks", "BLOCK_SCALA_STYLE")
    run_cmd(["./dev/lint-scala"])


def run_python_style_checks():
    set_title_and_block("Running Python style checks", "BLOCK_PYTHON_STYLE")
    run_cmd(["./dev/lint-python"])


def exec_maven(mvn_args=[]):
    """Will call Maven in the current directory with the list of mvn_args passed
    in and returns the subprocess for any further processing"""

    run_cmd(["./build/mvn"] + mvn_args)


def exec_sbt(sbt_args=[]):
    """Will call SBT in the current directory with the list of mvn_args passed
    in and returns the subprocess for any further processing"""

    sbt_cmd = ["./build/sbt"] + sbt_args

    # NOTE: echo "q" is needed because sbt on encountering a build file
    # with failure (either resolution or compilation) prompts the user for
    # input either q, r, etc to quit or retry. This echo is there to make it
    # not block.
    echo_proc = subprocess.Popen(["echo", "\"q\n\""], stdout=subprocess.PIPE)
    sbt_proc = subprocess.Popen(sbt_cmd,
                                stdin=echo_proc.stdout,
                                stdout=subprocess.PIPE)
    echo_proc.wait()
    for line in iter(sbt_proc.stdout.readline, ''):
        if not SBT_OUTPUT_FILTER.match(line):
            print line,    
    retcode = sbt_proc.wait()

    if retcode > 0:
        exit_from_command_with_retcode(sbt_cmd, retcode)


def build_apache_spark():
    """Will first build Spark with Hive v0.12.0 to ensure the build is
    successful and, after, will build Spark again against Hive v0.13.1 as the
    tests are based off that"""

    set_title_and_block("Building Spark", "BLOCK_BUILD")

    sbt_maven_profile_args = os.environ.get(SBT_MAVEN_PROFILE_ARGS_ENV).split()
    hive_profile_args = sbt_maven_profile_args + ["-Phive", 
                                                  "-Phive-thriftserver"]
    # set the default maven args
    base_mvn_args = ["clean", "package", "-DskipTests"]
    # set the necessary sbt goals
    sbt_hive_goals = ["package", 
                      "assembly/assembly", 
                      "streaming-kafka-assembly/assembly"]

    # Then build with default Hive version (0.13.1) because tests are based on
    # this version
    print "[info] Compile with Hive 0.13.1"
    rm_r("lib_managed")
    print "[info] Building Spark with these arguments:", 
    print " ".join(hive_profile_args)

    if AMPLAB_JENKINS_BUILD_TOOL == "maven":
        exec_maven(hive_profile_args + base_mvn_args)
    else:
        exec_sbt(hive_profile_args + sbt_hive_goals)


def detect_binary_inop_with_mima():
    set_title_and_block("Detecting binary incompatibilities with MiMa",
                        "BLOCK_MIMA")
    run_cmd(["./dev/mima"])


def run_scala_tests(test_suite=[]):
    """Function to properly execute all tests pass in, as a list, from the
    `determine_test_suite` function"""
    set_title_and_block("Running Spark unit tests", "BLOCK_SPARK_UNIT_TESTS")

    # ensure the test_suite is a set
    if not isinstance(test_suite, set): 
        test_suite = set(test_suite)

    # if the Spark SQL tests are enabled, run the tests with the Hive profiles 
    # enabled.
    if "SQL" in test_suite:
        sbt_maven_profile_args = \
            os.environ.get(SBT_MAVEN_PROFILE_ARGS_ENV).split()
        os.environ[SBT_MAVEN_PROFILE_ARGS_ENV] = \
            " ".join(sbt_maven_profile_args + ["-Phive", "-Phive-thriftserver"])

    # if we only have changes in SQL build a custom test string
    if "SQL" in test_suite and "CORE" not in test_suite:
        sbt_maven_test_args = ["catalyst/test",
                               "sql/test",
                               "hive/test", 
                               "hive-thriftserver/test",
                               "mllib/test"]
    else:
        sbt_maven_test_args = ["test"]

    # get the latest sbt maven profile arguments
    sbt_maven_profile_args = os.environ.get(SBT_MAVEN_PROFILE_ARGS_ENV).split()

    print "[info] Running Spark tests with these arguments:",
    print " ".join(sbt_maven_profile_args), 
    print " ".join(sbt_maven_test_args)

    if AMPLAB_JENKINS_BUILD_TOOL == "maven":
        exec_maven(["test"] + sbt_maven_profile_args + ["--fail-at-end"])
    else:
        exec_sbt(sbt_maven_profile_args + sbt_maven_test_args)


def run_python_tests(test_suite=[]):
    set_title_and_block("Running PySpark tests", "BLOCK_PYSPARK_UNIT_TESTS")
    
    # Add path for Python3 in Jenkins if we're calling from a Jenkins machine
    if AMPLAB_JENKINS:
        os.environ["PATH"] = os.environ.get("PATH")+":/home/anaconda/envs/py3k/bin"

    run_cmd(["./python/run-tests"])


def run_sparkr_tests(test_suite=[]):
    set_title_and_block("Running SparkR tests", "BLOCK_SPARKR_UNIT_TESTS")

    if which("R"):
        run_cmd(["./R/install-dev.sh"])
        run_cmd(["./R/run-tests.sh"])
    else:
        print "Ignoring SparkR tests as R was not found in PATH"

if __name__ == "__main__":
    # Ensure the user home directory (HOME) is valid and is an absolute directory
    if not USER_HOME_DIR or not os.path.isabs(USER_HOME_DIR):
        print "[error] Cannot determine your home directory as an absolute path;",
        print "ensure the $HOME environment variable is set properly."
        sys.exit(1)

    os.chdir(SPARK_PROJ_ROOT)

    rm_r("./work")
    rm_r(os.path.join(USER_HOME_DIR, ".ivy2/local/org.apache.spark"))
    rm_r(os.path.join(USER_HOME_DIR, ".ivy2/cache/org.apache.spark"))

    error_codes = get_error_codes("./dev/run-tests-codes.sh")

    os.environ["CURRENT_BLOCK"] = error_codes["BLOCK_GENERAL"]

    set_sbt_maven_profile_args()

    java_exe = determine_java_executable()

    if not java_exe:
        print "[error] Cannot find a version of `java` on the system; please", 
        print "install one and retry."
        sys.exit(2)

    java_version = determine_java_version(java_exe)

    if java_version.minor < 8:
        print "[warn] Java 8 tests will not run because JDK version is < 1.8."

    test_suite = determine_test_suite()

    run_apache_rat_checks()
    
    run_scala_style_checks()

    run_python_style_checks()

    build_apache_spark()

    detect_binary_inop_with_mima()

    run_scala_tests(test_suite)

    run_python_tests()

    run_sparkr_tests()
