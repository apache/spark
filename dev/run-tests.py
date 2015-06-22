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

import itertools
import os
import re
import sys
import shutil
import subprocess
from collections import namedtuple

SPARK_HOME = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
USER_HOME = os.environ.get("HOME")


# -------------------------------------------------------------------------------------------------
# Test module definitions and functions for traversing module dependency graph
# -------------------------------------------------------------------------------------------------


all_modules = []


class Module(object):
    """
    A module is the basic abstraction in our test runner script. Each module consists of a set of
    source files, a set of test commands, and a set of dependencies on other modules. We use modules
    to define a dependency graph that lets determine which tests to run based on which files have
    changed.
    """

    def __init__(self, name, dependencies, source_file_regexes, build_profile_flags=(),
                 sbt_test_goals=(), should_run_python_tests=False, should_run_r_tests=False):
        """
        Define a new module.

        :param name: A short module name, for display in logging and error messages.
        :param dependencies: A set of dependencies for this module. This should only include direct
            dependencies; transitive dependencies are resolved automatically.
        :param source_file_regexes: a set of regexes that match source files belonging to this
            module. These regexes are applied by attempting to match at the beginning of the
            filename strings.
        :param build_profile_flags: A set of profile flags that should be passed to Maven or SBT in
            order to build and test this module (e.g. '-PprofileName').
        :param sbt_test_goals: A set of SBT test goals for testing this module.
        :param should_run_python_tests: If true, changes in this module will trigger Python tests.
            For now, this has the effect of causing _all_ Python tests to be run, although in the
            future this should be changed to run only a subset of the Python tests that depend
            on this module.
        :param should_run_r_tests: If true, changes in this module will trigger all R tests.
        """
        self.name = name
        self.dependencies = dependencies
        self.source_file_prefixes = source_file_regexes
        self.sbt_test_goals = sbt_test_goals
        self.build_profile_flags = build_profile_flags
        self.should_run_python_tests = should_run_python_tests
        self.should_run_r_tests = should_run_r_tests

        self.dependent_modules = set()
        for dep in dependencies:
            dep.dependent_modules.add(self)
        all_modules.append(self)

    def contains_file(self, filename):
        return any(re.match(p, filename) for p in self.source_file_prefixes)


sql = Module(
    name="sql",
    dependencies=[],
    source_file_regexes=[
        "sql/(?!hive-thriftserver)",
        "bin/spark-sql",
    ],
    build_profile_flags=[
        "-Phive",
    ],
    sbt_test_goals=[
        "catalyst/test",
        "sql/test",
        "hive/test",
    ]
)


hive_thriftserver = Module(
    name="hive-thriftserver",
    dependencies=[sql],
    source_file_regexes=[
        "sql/hive-thriftserver",
        "sbin/start-thriftserver.sh",
    ],
    build_profile_flags=[
        "-Phive-thriftserver",
    ],
    sbt_test_goals=[
        "hive-thriftserver/test",
    ]
)


graphx = Module(
    name="graphx",
    dependencies=[],
    source_file_regexes=[
        "graphx/",
    ],
    sbt_test_goals=[
        "graphx/test"
    ]
)


streaming = Module(
    name="streaming",
    dependencies=[],
    source_file_regexes=[
        "streaming",
    ],
    sbt_test_goals=[
        "streaming/test",
    ]
)


streaming_kinesis_asl = Module(
    name="kinesis-asl",
    dependencies=[streaming],
    source_file_regexes=[
        "extras/kinesis-asl/",
    ],
    build_profile_flags=[
        "-Pkinesis-asl",
    ],
    sbt_test_goals=[
        "kinesis-asl/test",
    ]
)


streaming_zeromq = Module(
    name="streaming-zeromq",
    dependencies=[streaming],
    source_file_regexes=[
        "external/zeromq",
    ],
    sbt_test_goals=[
        "streaming-zeromq/test",
    ]
)


streaming_twitter = Module(
    name="streaming-twitter",
    dependencies=[streaming],
    source_file_regexes=[
        "external/twitter",
    ],
    sbt_test_goals=[
        "streaming-twitter/test",
    ]
)


streaming_mqtt = Module(
    name="streaming-mqtt",
    dependencies=[streaming],
    source_file_regexes=[
        "external/mqtt",
    ],
    sbt_test_goals=[
        "streaming-mqtt/test",
    ]
)


streaming_kafka = Module(
    name="streaming-kafka",
    dependencies=[streaming],
    source_file_regexes=[
        "external/kafka",
        "external/kafka-assembly",
    ],
    sbt_test_goals=[
        "streaming-kafka/test",
    ]
)


streaming_flume_sink = Module(
    name="streaming-flume-sink",
    dependencies=[streaming],
    source_file_regexes=[
        "external/flume-sink",
    ],
    sbt_test_goals=[
        "streaming-flume-sink/test",
    ]
)


streaming_flume = Module(
    name="streaming_flume",
    dependencies=[streaming],
    source_file_regexes=[
        "external/flume",
    ],
    sbt_test_goals=[
        "streaming-flume/test",
    ]
)


mllib = Module(
    name="mllib",
    dependencies=[streaming, sql],
    source_file_regexes=[
        "data/mllib/",
        "mllib/",
    ],
    sbt_test_goals=[
        "mllib/test",
    ]
)


examples = Module(
    name="examples",
    dependencies=[graphx, mllib, streaming, sql],
    source_file_regexes=[
        "examples/",
    ],
    sbt_test_goals=[
        "examples/test",
    ]
)


pyspark = Module(
    name="pyspark",
    dependencies=[mllib, streaming, streaming_kafka, sql],
    source_file_regexes=[
        "python/"
    ],
    should_run_python_tests=True
)


sparkr = Module(
    name="sparkr",
    dependencies=[sql, mllib],
    source_file_regexes=[
        "R/",
    ],
    should_run_r_tests=True
)


docs = Module(
    name="docs",
    dependencies=[],
    source_file_regexes=[
        "docs/",
    ]
)


ec2 = Module(
    name="ec2",
    dependencies=[],
    source_file_regexes=[
        "ec2/",
    ]
)


# The root module is a dummy module which is used to run all of the tests.
# No other modules should directly depend on this module.
root = Module(
    name="root",
    dependencies=[],
    source_file_regexes=[],
    # In order to run all of the tests, enable every test profile:
    build_profile_flags=
        list(set(itertools.chain.from_iterable(m.build_profile_flags for m in all_modules))),
    sbt_test_goals=[
        "test",
    ],
    should_run_python_tests=True,
    should_run_r_tests=True
)


def determine_modules_for_files(filenames):
    """
    Given a list of filenames, return the set of modules that contain those files.
    If a file is not associated with a more specific submodule, then this method will consider that
    file to belong to the 'root' module.

    >>> sorted(x.name for x in determine_modules_for_files(["python/pyspark/a.py", "sql/test/foo"]))
    ['pyspark', 'sql']
    >>> [x.name for x in determine_modules_for_files(["file_not_matched_by_any_subproject"])]
    ['root']
    """
    changed_modules = set()
    for filename in filenames:
        matched_at_least_one_module = False
        for module in all_modules:
            if module.contains_file(filename):
                changed_modules.add(module)
                matched_at_least_one_module = True
        if not matched_at_least_one_module:
            changed_modules.add(root)
    return changed_modules


def identify_changed_files_from_git_commits(patch_sha, target_branch=None, target_ref=None):
    """
    Given a git commit and target ref, use the set of files changed in the diff in order to
    determine which modules' tests should be run.

    >>> [x.name for x in determine_modules_for_files( \
            identify_changed_files_from_git_commits("fc0a1475ef", target_ref="5da21f07"))]
    ['graphx']
    >>> 'root' in [x.name for x in determine_modules_for_files( \
         identify_changed_files_from_git_commits("50a0496a43", target_ref="6765ef9"))]
    True
    """
    if target_branch is None and target_ref is None:
        raise AttributeError("must specify either target_branch or target_ref")
    elif target_branch is not None and target_ref is not None:
        raise AttributeError("must specify either target_branch or target_ref, not both")
    if target_branch is not None:
        diff_target = target_branch
        run_cmd(['git', 'fetch', 'origin', str(target_branch+':'+target_branch)])
    else:
        diff_target = target_ref
    raw_output = subprocess.check_output(['git', 'diff', '--name-only', patch_sha, diff_target])
    # Remove any empty strings
    return [f for f in raw_output.split('\n') if f]


def determine_modules_to_test(changed_modules):
    """
    Given a set of modules that have changed, compute the transitive closure of those modules'
    dependent modules in order to determine the set of modules that should be tested.

    >>> sorted(x.name for x in determine_modules_to_test([root]))
    ['root']
    >>> sorted(x.name for x in determine_modules_to_test([graphx]))
    ['examples', 'graphx']
    >>> sorted(x.name for x in determine_modules_to_test([sql]))
    ['examples', 'hive-thriftserver', 'mllib', 'pyspark', 'sparkr', 'sql']
    """
    # If we're going to have to run all of the tests, then we can just short-circuit
    # and return 'root'. No module depends on root, so if it appears then it will be
    # in changed_modules.
    if root in changed_modules:
        return [root]
    modules_to_test = set()
    for module in changed_modules:
        modules_to_test = modules_to_test.union(determine_modules_to_test(module.dependent_modules))
    return modules_to_test.union(set(changed_modules))


# -------------------------------------------------------------------------------------------------
# Functions for working with subprocesses and shell tools
# -------------------------------------------------------------------------------------------------

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
    print "[error] running", ' '.join(cmd), "; received return code", retcode
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

    fpath = os.path.split(program)[0]

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
    java_exe = which(os.path.join(java_home, "bin", "java")) if java_home else None
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


# -------------------------------------------------------------------------------------------------
# Functions for running the other build and test scripts
# -------------------------------------------------------------------------------------------------


def set_title_and_block(title, err_block):
    os.environ["CURRENT_BLOCK"] = ERROR_CODES[err_block]
    line_str = '=' * 72

    print
    print line_str
    print title
    print line_str


def run_apache_rat_checks():
    set_title_and_block("Running Apache RAT checks", "BLOCK_RAT")
    run_cmd([os.path.join(SPARK_HOME, "dev", "check-license")])


def run_scala_style_checks():
    set_title_and_block("Running Scala style checks", "BLOCK_SCALA_STYLE")
    run_cmd([os.path.join(SPARK_HOME, "dev", "lint-scala")])


def run_python_style_checks():
    set_title_and_block("Running Python style checks", "BLOCK_PYTHON_STYLE")
    run_cmd([os.path.join(SPARK_HOME, "dev", "lint-python")])


def build_spark_documentation():
    set_title_and_block("Building Spark Documentation", "BLOCK_DOCUMENTATION")
    os.environ["PRODUCTION"] = "1 jekyll build"

    os.chdir(os.path.join(SPARK_HOME, "docs"))

    jekyll_bin = which("jekyll")

    if not jekyll_bin:
        print "[error] Cannot find a version of `jekyll` on the system; please",
        print "install one and retry to build documentation."
        sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))
    else:
        run_cmd([jekyll_bin, "build"])

    os.chdir(SPARK_HOME)


def exec_maven(mvn_args=()):
    """Will call Maven in the current directory with the list of mvn_args passed
    in and returns the subprocess for any further processing"""

    run_cmd([os.path.join(SPARK_HOME, "build", "mvn")] + mvn_args)


def exec_sbt(sbt_args=()):
    """Will call SBT in the current directory with the list of mvn_args passed
    in and returns the subprocess for any further processing"""

    sbt_cmd = [os.path.join(SPARK_HOME, "build", "sbt")] + sbt_args

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
    """
    For the given Hadoop version tag, return a list of SBT profile flags for
    building and testing against that Hadoop version.
    """

    sbt_maven_hadoop_profiles = {
        "hadoop1.0": ["-Phadoop-1", "-Dhadoop.version=1.0.4"],
        "hadoop2.0": ["-Phadoop-1", "-Dhadoop.version=2.0.0-mr1-cdh4.1.1"],
        "hadoop2.2": ["-Pyarn", "-Phadoop-2.2"],
        "hadoop2.3": ["-Pyarn", "-Phadoop-2.3", "-Dhadoop.version=2.3.0"],
    }

    if hadoop_version in sbt_maven_hadoop_profiles:
        return sbt_maven_hadoop_profiles[hadoop_version]
    else:
        print "[error] Could not find", hadoop_version, "in the list. Valid options",
        print "are", sbt_maven_hadoop_profiles.keys()
        sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))


def build_spark_maven(hadoop_version):
    # Enable all of the profiles for the build:
    build_profiles = get_hadoop_profiles(hadoop_version) + root.build_profile_flags
    mvn_goals = ["clean", "package", "-DskipTests"]
    profiles_and_goals = build_profiles + mvn_goals

    print "[info] Building Spark (w/Hive 0.13.1) using Maven with these arguments:",
    print " ".join(profiles_and_goals)

    exec_maven(profiles_and_goals)


def build_spark_sbt(hadoop_version):
    # Enable all of the profiles for the build:
    build_profiles = get_hadoop_profiles(hadoop_version) + root.build_profile_flags
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
    set_title_and_block("Detecting binary incompatibilities with MiMa", "BLOCK_MIMA")
    run_cmd([os.path.join(SPARK_HOME, "dev", "mima")])


def run_scala_tests_maven(test_profiles):
    mvn_test_goals = ["test", "--fail-at-end"]
    profiles_and_goals = test_profiles + mvn_test_goals

    print "[info] Running Spark tests using Maven with these arguments:",
    print " ".join(profiles_and_goals)

    exec_maven(profiles_and_goals)


def run_scala_tests_sbt(test_modules, test_profiles):

    sbt_test_goals = set(itertools.chain.from_iterable(m.sbt_test_goals for m in test_modules))

    if not sbt_test_goals:
        return

    profiles_and_goals = test_profiles + list(sbt_test_goals)

    print "[info] Running Spark tests using SBT with these arguments:",
    print " ".join(profiles_and_goals)

    exec_sbt(profiles_and_goals)


def run_scala_tests(build_tool, hadoop_version, test_modules):
    """Function to properly execute all tests passed in as a set from the
    `determine_test_suites` function"""
    set_title_and_block("Running Spark unit tests", "BLOCK_SPARK_UNIT_TESTS")

    test_modules = set(test_modules)

    test_profiles = get_hadoop_profiles(hadoop_version) + \
        list(set(itertools.chain.from_iterable(m.build_profile_flags for m in test_modules)))
    if build_tool == "maven":
        run_scala_tests_maven(test_profiles)
    else:
        run_scala_tests_sbt(test_modules, test_profiles)


def run_python_tests():
    set_title_and_block("Running PySpark tests", "BLOCK_PYSPARK_UNIT_TESTS")

    run_cmd([os.path.join(SPARK_HOME, "python", "run-tests")])


def run_sparkr_tests():
    set_title_and_block("Running SparkR tests", "BLOCK_SPARKR_UNIT_TESTS")

    if which("R"):
        run_cmd([os.path.join(SPARK_HOME, "R", "install-dev.sh")])
        run_cmd([os.path.join(SPARK_HOME, "R", "run-tests.sh")])
    else:
        print "Ignoring SparkR tests as R was not found in PATH"


def main():
    # Ensure the user home directory (HOME) is valid and is an absolute directory
    if not USER_HOME or not os.path.isabs(USER_HOME):
        print "[error] Cannot determine your home directory as an absolute path;",
        print "ensure the $HOME environment variable is set properly."
        sys.exit(1)

    os.chdir(SPARK_HOME)

    rm_r(os.path.join(SPARK_HOME, "work"))
    rm_r(os.path.join(USER_HOME, ".ivy2", "local", "org.apache.spark"))
    rm_r(os.path.join(USER_HOME, ".ivy2", "cache", "org.apache.spark"))

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
        # add path for Python3 in Jenkins if we're calling from a Jenkins machine
        os.environ["PATH"] = "/home/anaconda/envs/py3k/bin:" + os.environ.get("PATH")
    else:
        # else we're running locally and can use local settings
        build_tool = "sbt"
        hadoop_version = "hadoop2.3"
        test_env = "local"

    print "[info] Using build tool", build_tool, "with Hadoop profile", hadoop_version,
    print "under environment", test_env

    changed_modules = None
    changed_files = None
    if test_env == "amplab_jenkins" and os.environ.get("AMP_JENKINS_PRB"):
        target_branch = os.environ["ghprbTargetBranch"]
        changed_files = identify_changed_files_from_git_commits("HEAD", target_branch=target_branch)
        changed_modules = determine_modules_for_files(changed_files)
    if not changed_modules:
        changed_modules = [root]
    print "[info] Found the following changed modules:", ", ".join(x.name for x in changed_modules)

    test_modules = determine_modules_to_test(changed_modules)

    # license checks
    run_apache_rat_checks()

    # style checks
    if not changed_files or any(f.endswith(".scala") for f in changed_files):
        run_scala_style_checks()
    if not changed_files or any(f.endswith(".py") for f in changed_files):
        run_python_style_checks()

    # determine if docs were changed and if we're inside the amplab environment
    # note - the below commented out until *all* Jenkins workers can get `jekyll` installed
    # if "DOCS" in changed_modules and test_env == "amplab_jenkins":
    #    build_spark_documentation()

    # spark build
    build_apache_spark(build_tool, hadoop_version)

    # backwards compatibility checks
    detect_binary_inop_with_mima()

    # run the test suites
    run_scala_tests(build_tool, hadoop_version, test_modules)

    if any(m.should_run_python_tests for m in test_modules):
        run_python_tests()
    if any(m.should_run_r_tests for m in test_modules):
        run_sparkr_tests()


def _test():
    import doctest
    failure_count = doctest.testmod()[0]
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
    main()
