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

import os
import re
import sys
import shutil
import subprocess
from collections import namedtuple

SPARK_HOME = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
USER_HOME = os.environ.get("HOME")


def get_error_codes(err_code_file):
    """Function to retrieve all block numbers from the `run-tests-codes.sh`
    file to maintain backwards compatibility with the `run-tests-jenkins`
    script"""

    with open(err_code_file, 'r') as f:
        err_codes = [e.split()[1].strip().split('=')
                     for e in f if e.startswith("readonly")]
        return dict(err_codes)


ERROR_CODES = get_error_codes(os.path.join(SPARK_HOME, "dev/run-tests-codes.sh"))


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


def run_cmd(cmd):
    """Given a command as a list of arguments will attempt to execute the
    command from the determined SPARK_HOME directory and, on failure, print
    an error message"""

    if not isinstance(cmd, list):
        cmd = cmd.split()
    try:
        # prepend SPARK_HOME onto the first element of the command
        cmd[0] = os.path.join(SPARK_HOME, *filter(lambda x: x, cmd[0].split(os.path.sep)))
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        exit_from_command_with_retcode(e.cmd, e.returncode)


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
    """Will return the path of the java executable that will be used by Spark's
    tests or `None`"""

    # Any changes in the way that Spark's build detects java must be reflected
    # here. Currently the build looks for $JAVA_HOME/bin/java then falls back to
    # the `java` executable on the path

    java_home = os.environ.get("JAVA_HOME")

    # check if there is an executable at $JAVA_HOME/bin/java
    java_exe = which(os.path.join(java_home, "bin/java"))
    # if the java_exe wasn't set, check for a `java` version on the $PATH
    return java_exe if java_exe else which("java")


JavaVersion = namedtuple('JavaVersion', ['major', 'minor', 'patch', 'update'])


def determine_java_version(java_exe):
    """Given a valid java executable will return its version in named tuple format
    with accessors '.major', '.minor', '.patch', '.update'"""

    raw_output = subprocess.check_output([java_exe, "-version"],
                                         stderr=subprocess.STDOUT)
    raw_version_str = raw_output.split('\n')[0]  # eg 'java version "1.8.0_25"'
    version_str = raw_version_str.split()[-1].strip('"')  # eg '1.8.0_25'
    version, update = version_str.split('_')  # eg ['1.8.0', '25']

    # map over the values and convert them to integers
    version_info = [int(x) for x in version.split('.') + [update]]

    return JavaVersion(major=version_info[0],
                       minor=version_info[1],
                       patch=version_info[2],
                       update=version_info[3])


def set_title_and_block(title, err_block):
    os.environ["CURRENT_BLOCK"] = ERROR_CODES[err_block]
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

    sbt_output_filter = re.compile("^.*[info].*Resolving" + "|" +
                                   "^.*[warn].*Merging" + "|" +
                                   "^.*[info].*Including")

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
        if not sbt_output_filter.match(line):
            print line,
    retcode = sbt_proc.wait()

    if retcode > 0:
        exit_from_command_with_retcode(sbt_cmd, retcode)


def get_hadoop_profiles(hadoop_version):
    """Return a list of profiles indicating which Hadoop version to use from
    a Hadoop version tag."""

    sbt_maven_hadoop_profiles = {
        "hadoop1.0": ["-Phadoop-1", "-Dhadoop.version=1.0.4"],
        "hadoop2.0": ["-Phadoop-1", "-Dhadoop.version=2.0.0-mr1-cdh4.1.1"],
        "hadoop2.2": ["-Pyarn", "-Phadoop-2.2"],
        "hadoop2.3": ["-Pyarn", "-Phadoop-2.3", "-Dhadoop.version=2.3.0"],
    }

    try:
        hadoop_profiles = sbt_maven_hadoop_profiles[hadoop_version]
    except KeyError:
        print "[error] Could not find", hadoop_version, "in the list. Valid options",
        print "are 'hadoop1.0', 'hadoop2.0', 'hadoop2.2', and 'hadoop2.3'."
        sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))

    return hadoop_profiles


def get_build_profiles(hadoop_version="hadoop2.3",
                       base_profiles=True,
                       hive_profiles=False):
    """Returns a list of hadoop profiles to be used as looked up from the passed in hadoop profile
    key with the option of adding on the base and hive profiles."""

    base_profiles = ["-Pkinesis-asl"]
    hive_profiles = ["-Phive", "-Phive-thriftserver"]
    hadoop_profiles = get_hadoop_profiles(hadoop_version)

    build_profiles = hadoop_profiles
    # first, check and add the base profiles
    if base_profiles:
        build_profiles = build_profiles + base_profiles
    # second, check and add the hive profiles
    if hive_profiles:
        build_profiles = build_profiles + hive_profiles

    return build_profiles


def build_spark_maven(hadoop_version):
    build_profiles = get_build_profiles(hadoop_version, hive_profiles=True)
    mvn_goals = ["clean", "package", "-DskipTests"]
    profiles_and_goals = build_profiles + mvn_goals

    print "[info] Building Spark (w/Hive 0.13.1) using Maven with these arguments:",
    print " ".join(profiles_and_goals)

    exec_maven(profiles_and_goals)


def build_spark_sbt(hadoop_version):
    build_profiles = get_build_profiles(hadoop_version, hive_profiles=True)
    sbt_goals = ["package",
                 "assembly/assembly",
                 "streaming-kafka-assembly/assembly"]
    profiles_and_goals = build_profiles + sbt_goals

    print "[info] Building Spark (w/Hive 0.13.1) using SBT with these arguments:",
    print " ".join(profiles_and_goals)

    exec_sbt(profiles_and_goals)


def build_apache_spark(build_tool, hadoop_version):
    """Will build Spark against Hive v0.13.1 given the passed in build tool (either `sbt` or
    `maven`). Defaults to using `sbt`."""

    set_title_and_block("Building Spark", "BLOCK_BUILD")

    rm_r("lib_managed")

    if build_tool == "maven":
        build_spark_maven(hadoop_version)
    else:
        build_spark_sbt(hadoop_version)


def detect_binary_inop_with_mima():
    set_title_and_block("Detecting binary incompatibilities with MiMa",
                        "BLOCK_MIMA")
    run_cmd(["./dev/mima"])


def determine_test_modules(test_env):
    """This function current acts to determine if SQL tests need to be run in
    addition to the core test suite *or* if _only_ SQL tests need to be run
    as the git logs show that to be the only thing touched. In the future
    this function will act more generically to help further segregate the
    test suite runner (hence the function name).
    @return a set of unique test names"""
    test_suite = list()

    if test_env == "amplab_jenkins":
        target_branch = os.environ.get("ghprbTargetBranch")
        print "target_branch at", target_branch
        run_cmd(['git', 'fetch', 'origin', str(target_branch+':'+target_branch)])

        raw_output = subprocess.check_output(['git', 'diff', '--name-only', target_branch])
        # remove any empty strings
        changed_files = [f for f in raw_output.split('\n') if f]

        # find any sql files
        sql_files = [f for f in changed_files
                     if any(f.startswith(p) for p in
                            ["sql/",
                             "bin/spark-sql",
                             "sbin/start-thriftserver.sh",
                             "examples/src/main/java/org/apache/spark/examples/sql/",
                             "examples/src/main/scala/org/apache/spark/examples/sql/"])]
        mllib_files = [f for f in changed_files
                       if any(f.startswith(p) for p in
                              ["examples/src/main/java/org/apache/spark/examples/mllib/",
                               "examples/src/main/scala/org/apache/spark/examples/mllib",
                               "data/mllib/",
                               "mllib/"])]
        streaming_files = [f for f in changed_files
                           if any(f.startswith(p) for p in
                                  ["examples/scala-2.10/",
                                   "examples/src/main/java/org/apache/spark/examples/streaming/",
                                   "examples/src/main/scala/org/apache/spark/examples/streaming/",
                                   "external/",
                                   "extras/java8-tests/",
                                   "extras/kinesis-asl/",
                                   "streaming/"])]
        graphx_files = [f for f in changed_files
                        if any(f.startswith(p) for p in
                               ["examples/src/main/scala/org/apache/spark/examples/graphx/",
                                "graphx/"])]

        non_sql_files = set(changed_files).difference(set(sql_files))

        if non_sql_files:
            test_suite.append("CORE")
        if sql_files:
            print "[info] Detected changes in SQL. Will run Hive test suite."
            test_suite.append("SQL")
            if not non_sql_files:
                print "[info] Detected no changes except in SQL. Will only run SQL tests."
        if mllib_files:
            print "[info] Detected changes in MLlib. Will run MLlib test suite."
            test_suite.append("MLLIB")
        if streaming_files:
            print "[info] Detected changes in Streaming. Will run Streaming test suite."
            test_suite.append("STREAMING")
        if graphx_files:
            print "[info] Detected changes in GraphX. Will run GraphX test suite."
            test_suite.append("GRAPHX")

        return set(test_suite)
    else:
        # we aren't in the Amplab environment so simply run all tests
        test_suite.append("ALL")
        return set(test_suite)


def run_scala_tests_maven(test_profiles):
    mvn_test_goals = ["test", "--fail-at-end"]
    profiles_and_goals = test_profiles + mvn_test_goals

    print "[info] Running Spark tests using Maven with these arguments:",
    print " ".join(profiles_and_goals)

    exec_maven(profiles_and_goals)


def run_scala_tests_sbt(test_modules, test_profiles):
    if "ALL" in test_modules:
        sbt_test_goals = ["test"]
    else:
        # if we only have changes in SQL build a custom test list
        if "SQL" in test_modules and "CORE" not in test_modules:
            sbt_test_goals = ["catalyst/test",
                              "sql/test",
                              "hive/test",
                              "hive-thriftserver/test",
                              "mllib/test",
                              "examples/test"]
        if "MLLIB" in test_modules and "CORE" not in test_modules:
            sbt_test_goals = sbt_test_goals + ["mllib/test",
                                               "examples/test"]
        if "STREAMING" in test_modules and "CORE" not in test_modules:
            sbt_test_goals = sbt_test_goals + ["streaming/test",
                                               "streaming-flume/test",
                                               "streaming-flume-sink/test",
                                               "streaming-kafka/test",
                                               "streaming-mqtt/test",
                                               "streaming-twitter/test",
                                               "streaming-zeromq/test",
                                               "examples/test"]
        if "GRAPHX" in test_modules and "CORE" not in test_modules:
            sbt_test_goals = sbt_test_goals + ["graphx/test",
                                               "examples/test"]
        if not sbt_test_goals:
            sbt_test_goals = ["test"]

    profiles_and_goals = test_profiles + sbt_test_goals

    print "[info] Running Spark tests using SBT with these arguments:",
    print " ".join(profiles_and_goals)

    exec_sbt(profiles_and_goals)


def run_scala_tests(build_tool, hadoop_version, test_modules):
    """Function to properly execute all tests passed in as a set from the
    `determine_test_suites` function"""
    set_title_and_block("Running Spark unit tests", "BLOCK_SPARK_UNIT_TESTS")

    test_modules = set(test_modules)

    # if the Spark SQL tests are enabled, run the tests with the Hive profiles
    # enabled.
    if "SQL" in test_modules:
        test_profiles = get_build_profiles(hadoop_version, hive_profiles=True)
    else:
        test_profiles = get_build_profiles(hadoop_version)

    if build_tool == "maven":
        run_scala_tests_maven(test_profiles)
    else:
        run_scala_tests_sbt(test_modules, test_profiles)


def run_python_tests():
    set_title_and_block("Running PySpark tests", "BLOCK_PYSPARK_UNIT_TESTS")

    # Add path for Python3 in Jenkins if we're calling from a Jenkins machine
    if AMPLAB_JENKINS:
        os.environ["PATH"] = os.environ.get("PATH")+":/home/anaconda/envs/py3k/bin"

    run_cmd(["./python/run-tests"])


def run_sparkr_tests():
    set_title_and_block("Running SparkR tests", "BLOCK_SPARKR_UNIT_TESTS")

    if which("R"):
        run_cmd(["./R/install-dev.sh"])
        run_cmd(["./R/run-tests.sh"])
    else:
        print "Ignoring SparkR tests as R was not found in PATH"


def main():
    # Ensure the user home directory (HOME) is valid and is an absolute directory
    if not USER_HOME or not os.path.isabs(USER_HOME):
        print "[error] Cannot determine your home directory as an absolute path;",
        print "ensure the $HOME environment variable is set properly."
        sys.exit(1)

        #os.chdir(SPARK_HOME)

    rm_r(os.path.join(SPARK_HOME, "work"))
    rm_r(os.path.join(USER_HOME, ".ivy2/local/org.apache.spark"))
    rm_r(os.path.join(USER_HOME, ".ivy2/cache/org.apache.spark"))

    os.environ["CURRENT_BLOCK"] = ERROR_CODES["BLOCK_GENERAL"]

    java_exe = determine_java_executable()

    if not java_exe:
        print "[error] Cannot find a version of `java` on the system; please",
        print "install one and retry."
        sys.exit(2)

    java_version = determine_java_version(java_exe)

    if java_version.minor < 8:
        print "[warn] Java 8 tests will not run because JDK version is < 1.8."

    if os.environ.get("AMPLAB_JENKINS"):
        # if we're on the Amplab Jenkins build servers setup variables
        # to reflect the environment settings
        build_tool = os.environ.get("AMPLAB_JENKINS_BUILD_TOOL", "sbt")
        hadoop_version = os.environ.get("AMPLAB_JENKINS_BUILD_PROFILE", "hadoop2.3")
        test_env = "amplab_jenkins"
    else:
        # else we're running locally and can use local settings
        build_tool = "sbt"
        hadoop_version = "hadoop2.3"
        test_env = "local"

    print "[info] Using build tool", build_tool, "with profile", hadoop_version,
    print "under environment", test_env

    # license checks
    run_apache_rat_checks()

    # style checks
    run_scala_style_checks()
    run_python_style_checks()

    # spark build
    build_apache_spark(build_tool, hadoop_version)

    # backwards compatibility checks
    detect_binary_inop_with_mima()

    # test suites
    test_modules = determine_test_modules(test_env)
    run_scala_tests(build_tool, hadoop_version, test_modules)
    run_python_tests()
    run_sparkr_tests()

if __name__ == "__main__":
    main()
