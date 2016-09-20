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
import itertools
from optparse import OptionParser
import os
import random
import re
import sys
import subprocess
from collections import namedtuple

from sparktestsupport import SPARK_HOME, USER_HOME, ERROR_CODES
from sparktestsupport.shellutils import exit_from_command_with_retcode, run_cmd, rm_r, which
from sparktestsupport.toposort import toposort_flatten, toposort
import sparktestsupport.modules as modules


# -------------------------------------------------------------------------------------------------
# Functions for traversing module dependency graph
# -------------------------------------------------------------------------------------------------


def determine_modules_for_files(filenames):
    """
    Given a list of filenames, return the set of modules that contain those files.
    If a file is not associated with a more specific submodule, then this method will consider that
    file to belong to the 'root' module.

    >>> sorted(x.name for x in determine_modules_for_files(["python/pyspark/a.py", "sql/core/foo"]))
    ['pyspark-core', 'sql']
    >>> [x.name for x in determine_modules_for_files(["file_not_matched_by_any_subproject"])]
    ['root']
    """
    changed_modules = set()
    for filename in filenames:
        matched_at_least_one_module = False
        for module in modules.all_modules:
            if module.contains_file(filename):
                changed_modules.add(module)
                matched_at_least_one_module = True
        if not matched_at_least_one_module:
            changed_modules.add(modules.root)
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
    raw_output = subprocess.check_output(['git', 'diff', '--name-only', patch_sha, diff_target],
                                         universal_newlines=True)
    # Remove any empty strings
    return [f for f in raw_output.split('\n') if f]


def setup_test_environ(environ):
    print("[info] Setup the following environment variables for tests: ")
    for (k, v) in environ.items():
        print("%s=%s" % (k, v))
        os.environ[k] = v


def determine_modules_to_test(changed_modules):
    """
    Given a set of modules that have changed, compute the transitive closure of those modules'
    dependent modules in order to determine the set of modules that should be tested.

    Returns a topologically-sorted list of modules (ties are broken by sorting on module names).

    >>> [x.name for x in determine_modules_to_test([modules.root])]
    ['root']
    >>> [x.name for x in determine_modules_to_test([modules.build])]
    ['root']
    >>> [x.name for x in determine_modules_to_test([modules.graphx])]
    ['graphx', 'examples']
    >>> x = [x.name for x in determine_modules_to_test([modules.sql])]
    >>> x # doctest: +NORMALIZE_WHITESPACE
    ['sql', 'hive', 'mllib', 'examples', 'hive-thriftserver',
     'pyspark-sql', 'sparkr', 'pyspark-mllib', 'pyspark-ml']
    """
    modules_to_test = set()
    for module in changed_modules:
        modules_to_test = modules_to_test.union(determine_modules_to_test(module.dependent_modules))
    modules_to_test = modules_to_test.union(set(changed_modules))
    # If we need to run all of the tests, then we should short-circuit and return 'root'
    if modules.root in modules_to_test:
        return [modules.root]
    return toposort_flatten(
        {m: set(m.dependencies).intersection(modules_to_test) for m in modules_to_test}, sort=True)


def determine_tags_to_exclude(changed_modules):
    tags = []
    for m in modules.all_modules:
        if m not in changed_modules:
            tags += m.test_tags
    return tags


# -------------------------------------------------------------------------------------------------
# Functions for working with subprocesses and shell tools
# -------------------------------------------------------------------------------------------------


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


JavaVersion = namedtuple('JavaVersion', ['major', 'minor', 'patch'])


def determine_java_version(java_exe):
    """Given a valid java executable will return its version in named tuple format
    with accessors '.major', '.minor', '.patch', '.update'"""

    raw_output = subprocess.check_output([java_exe, "-version"],
                                         stderr=subprocess.STDOUT,
                                         universal_newlines=True)

    raw_output_lines = raw_output.split('\n')

    # find raw version string, eg 'java version "1.8.0_25"'
    raw_version_str = next(x for x in raw_output_lines if " version " in x)

    match = re.search('(\d+)\.(\d+)\.(\d+)', raw_version_str)

    major = int(match.group(1))
    minor = int(match.group(2))
    patch = int(match.group(3))

    return JavaVersion(major, minor, patch)

# -------------------------------------------------------------------------------------------------
# Functions for running the other build and test scripts
# -------------------------------------------------------------------------------------------------


def set_title_and_block(title, err_block):
    os.environ["CURRENT_BLOCK"] = str(ERROR_CODES[err_block])
    line_str = '=' * 72

    print('')
    print(line_str)
    print(title)
    print(line_str)


def run_apache_rat_checks():
    set_title_and_block("Running Apache RAT checks", "BLOCK_RAT")
    run_cmd([os.path.join(SPARK_HOME, "dev", "check-license")])


def run_scala_style_checks():
    set_title_and_block("Running Scala style checks", "BLOCK_SCALA_STYLE")
    run_cmd([os.path.join(SPARK_HOME, "dev", "lint-scala")])


def run_java_style_checks():
    set_title_and_block("Running Java style checks", "BLOCK_JAVA_STYLE")
    run_cmd([os.path.join(SPARK_HOME, "dev", "lint-java")])


def run_python_style_checks():
    set_title_and_block("Running Python style checks", "BLOCK_PYTHON_STYLE")
    run_cmd([os.path.join(SPARK_HOME, "dev", "lint-python")])


def run_sparkr_style_checks():
    set_title_and_block("Running R style checks", "BLOCK_R_STYLE")

    if which("R"):
        # R style check should be executed after `install-dev.sh`.
        # Since warnings about `no visible global function definition` appear
        # without the installation. SEE ALSO: SPARK-9121.
        run_cmd([os.path.join(SPARK_HOME, "dev", "lint-r")])
    else:
        print("Ignoring SparkR style check as R was not found in PATH")


def build_spark_documentation():
    set_title_and_block("Building Spark Documentation", "BLOCK_DOCUMENTATION")
    os.environ["PRODUCTION"] = "1 jekyll build"

    os.chdir(os.path.join(SPARK_HOME, "docs"))

    jekyll_bin = which("jekyll")

    if not jekyll_bin:
        print("[error] Cannot find a version of `jekyll` on the system; please",
              " install one and retry to build documentation.")
        sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))
    else:
        run_cmd([jekyll_bin, "build"])

    os.chdir(SPARK_HOME)


def get_zinc_port():
    """
    Get a randomized port on which to start Zinc
    """
    return random.randrange(3030, 4030)


def kill_zinc_on_port(zinc_port):
    """
    Kill the Zinc process running on the given port, if one exists.
    """
    cmd = ("/usr/sbin/lsof -P |grep %s | grep LISTEN "
           "| awk '{ print $2; }' | xargs kill") % zinc_port
    subprocess.check_call(cmd, shell=True)


def exec_maven(mvn_args=()):
    """Will call Maven in the current directory with the list of mvn_args passed
    in and returns the subprocess for any further processing"""

    zinc_port = get_zinc_port()
    os.environ["ZINC_PORT"] = "%s" % zinc_port
    zinc_flag = "-DzincPort=%s" % zinc_port
    flags = [os.path.join(SPARK_HOME, "build", "mvn"), "--force", zinc_flag]
    run_cmd(flags + mvn_args)
    kill_zinc_on_port(zinc_port)


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
            print(line, end='')
    retcode = sbt_proc.wait()

    if retcode != 0:
        exit_from_command_with_retcode(sbt_cmd, retcode)


def get_hadoop_profiles(hadoop_version):
    """
    For the given Hadoop version tag, return a list of Maven/SBT profile flags for
    building and testing against that Hadoop version.
    """

    sbt_maven_hadoop_profiles = {
        "hadoop2.2": ["-Phadoop-2.2"],
        "hadoop2.3": ["-Phadoop-2.3"],
        "hadoop2.4": ["-Phadoop-2.4"],
        "hadoop2.6": ["-Phadoop-2.6"],
        "hadoop2.7": ["-Phadoop-2.7"],
    }

    if hadoop_version in sbt_maven_hadoop_profiles:
        return sbt_maven_hadoop_profiles[hadoop_version]
    else:
        print("[error] Could not find", hadoop_version, "in the list. Valid options",
              " are", sbt_maven_hadoop_profiles.keys())
        sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))


def build_spark_maven(hadoop_version):
    # Enable all of the profiles for the build:
    build_profiles = get_hadoop_profiles(hadoop_version) + modules.root.build_profile_flags
    mvn_goals = ["clean", "package", "-DskipTests"]
    profiles_and_goals = build_profiles + mvn_goals

    print("[info] Building Spark (w/Hive 1.2.1) using Maven with these arguments: ",
          " ".join(profiles_and_goals))

    exec_maven(profiles_and_goals)


def build_spark_sbt(hadoop_version):
    # Enable all of the profiles for the build:
    build_profiles = get_hadoop_profiles(hadoop_version) + modules.root.build_profile_flags
    sbt_goals = ["test:package",  # Build test jars as some tests depend on them
                 "streaming-kafka-0-8-assembly/assembly",
                 "streaming-flume-assembly/assembly",
                 "streaming-kinesis-asl-assembly/assembly"]
    profiles_and_goals = build_profiles + sbt_goals

    print("[info] Building Spark (w/Hive 1.2.1) using SBT with these arguments: ",
          " ".join(profiles_and_goals))

    exec_sbt(profiles_and_goals)


def build_spark_assembly_sbt(hadoop_version):
    # Enable all of the profiles for the build:
    build_profiles = get_hadoop_profiles(hadoop_version) + modules.root.build_profile_flags
    sbt_goals = ["assembly/package"]
    profiles_and_goals = build_profiles + sbt_goals
    print("[info] Building Spark assembly (w/Hive 1.2.1) using SBT with these arguments: ",
          " ".join(profiles_and_goals))
    exec_sbt(profiles_and_goals)


def build_apache_spark(build_tool, hadoop_version):
    """Will build Spark against Hive v1.2.1 given the passed in build tool (either `sbt` or
    `maven`). Defaults to using `sbt`."""

    set_title_and_block("Building Spark", "BLOCK_BUILD")

    rm_r("lib_managed")

    if build_tool == "maven":
        build_spark_maven(hadoop_version)
    else:
        build_spark_sbt(hadoop_version)


def detect_binary_inop_with_mima(hadoop_version):
    build_profiles = get_hadoop_profiles(hadoop_version) + modules.root.build_profile_flags
    set_title_and_block("Detecting binary incompatibilities with MiMa", "BLOCK_MIMA")
    run_cmd([os.path.join(SPARK_HOME, "dev", "mima")] + build_profiles)


def run_scala_tests_maven(test_profiles):
    mvn_test_goals = ["test", "--fail-at-end"]

    profiles_and_goals = test_profiles + mvn_test_goals

    print("[info] Running Spark tests using Maven with these arguments: ",
          " ".join(profiles_and_goals))

    exec_maven(profiles_and_goals)


def run_scala_tests_sbt(test_modules, test_profiles):

    sbt_test_goals = list(itertools.chain.from_iterable(m.sbt_test_goals for m in test_modules))

    if not sbt_test_goals:
        return

    profiles_and_goals = test_profiles + sbt_test_goals

    print("[info] Running Spark tests using SBT with these arguments: ",
          " ".join(profiles_and_goals))

    exec_sbt(profiles_and_goals)


def run_scala_tests(build_tool, hadoop_version, test_modules, excluded_tags):
    """Function to properly execute all tests passed in as a set from the
    `determine_test_suites` function"""
    set_title_and_block("Running Spark unit tests", "BLOCK_SPARK_UNIT_TESTS")

    test_modules = set(test_modules)

    test_profiles = get_hadoop_profiles(hadoop_version) + \
        list(set(itertools.chain.from_iterable(m.build_profile_flags for m in test_modules)))

    if excluded_tags:
        test_profiles += ['-Dtest.exclude.tags=' + ",".join(excluded_tags)]

    if build_tool == "maven":
        run_scala_tests_maven(test_profiles)
    else:
        run_scala_tests_sbt(test_modules, test_profiles)


def run_python_tests(test_modules, parallelism):
    set_title_and_block("Running PySpark tests", "BLOCK_PYSPARK_UNIT_TESTS")

    command = [os.path.join(SPARK_HOME, "python", "run-tests")]
    if test_modules != [modules.root]:
        command.append("--modules=%s" % ','.join(m.name for m in test_modules))
    command.append("--parallelism=%i" % parallelism)
    run_cmd(command)


def run_build_tests():
    set_title_and_block("Running build tests", "BLOCK_BUILD_TESTS")
    run_cmd([os.path.join(SPARK_HOME, "dev", "test-dependencies.sh")])
    pass


def run_sparkr_tests():
    set_title_and_block("Running SparkR tests", "BLOCK_SPARKR_UNIT_TESTS")

    if which("R"):
        run_cmd([os.path.join(SPARK_HOME, "R", "run-tests.sh")])
    else:
        print("Ignoring SparkR tests as R was not found in PATH")


def parse_opts():
    parser = OptionParser(
        prog="run-tests"
    )
    parser.add_option(
        "-p", "--parallelism", type="int", default=4,
        help="The number of suites to test in parallel (default %default)"
    )

    (opts, args) = parser.parse_args()
    if args:
        parser.error("Unsupported arguments: %s" % ' '.join(args))
    if opts.parallelism < 1:
        parser.error("Parallelism cannot be less than 1")
    return opts


def main():
    opts = parse_opts()
    # Ensure the user home directory (HOME) is valid and is an absolute directory
    if not USER_HOME or not os.path.isabs(USER_HOME):
        print("[error] Cannot determine your home directory as an absolute path;",
              " ensure the $HOME environment variable is set properly.")
        sys.exit(1)

    os.chdir(SPARK_HOME)

    rm_r(os.path.join(SPARK_HOME, "work"))
    rm_r(os.path.join(USER_HOME, ".ivy2", "local", "org.apache.spark"))
    rm_r(os.path.join(USER_HOME, ".ivy2", "cache", "org.apache.spark"))

    os.environ["CURRENT_BLOCK"] = str(ERROR_CODES["BLOCK_GENERAL"])

    java_exe = determine_java_executable()

    if not java_exe:
        print("[error] Cannot find a version of `java` on the system; please",
              " install one and retry.")
        sys.exit(2)

    java_version = determine_java_version(java_exe)

    if java_version.minor < 8:
        print("[warn] Java 8 tests will not run because JDK version is < 1.8.")

    # install SparkR
    if which("R"):
        run_cmd([os.path.join(SPARK_HOME, "R", "install-dev.sh")])
    else:
        print("Cannot install SparkR as R was not found in PATH")

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
        hadoop_version = os.environ.get("HADOOP_PROFILE", "hadoop2.3")
        test_env = "local"

    print("[info] Using build tool", build_tool, "with Hadoop profile", hadoop_version,
          "under environment", test_env)

    changed_modules = None
    changed_files = None
    if test_env == "amplab_jenkins" and os.environ.get("AMP_JENKINS_PRB"):
        target_branch = os.environ["ghprbTargetBranch"]
        changed_files = identify_changed_files_from_git_commits("HEAD", target_branch=target_branch)
        changed_modules = determine_modules_for_files(changed_files)
        excluded_tags = determine_tags_to_exclude(changed_modules)
    if not changed_modules:
        changed_modules = [modules.root]
        excluded_tags = []
    print("[info] Found the following changed modules:",
          ", ".join(x.name for x in changed_modules))

    # setup environment variables
    # note - the 'root' module doesn't collect environment variables for all modules. Because the
    # environment variables should not be set if a module is not changed, even if running the 'root'
    # module. So here we should use changed_modules rather than test_modules.
    test_environ = {}
    for m in changed_modules:
        test_environ.update(m.environ)
    setup_test_environ(test_environ)

    test_modules = determine_modules_to_test(changed_modules)

    # license checks
    run_apache_rat_checks()

    # style checks
    if not changed_files or any(f.endswith(".scala")
                                or f.endswith("scalastyle-config.xml")
                                for f in changed_files):
        run_scala_style_checks()
    if not changed_files or any(f.endswith(".java")
                                or f.endswith("checkstyle.xml")
                                or f.endswith("checkstyle-suppressions.xml")
                                for f in changed_files):
        # run_java_style_checks()
        pass
    if not changed_files or any(f.endswith(".py") for f in changed_files):
        run_python_style_checks()
    if not changed_files or any(f.endswith(".R") for f in changed_files):
        run_sparkr_style_checks()

    # determine if docs were changed and if we're inside the amplab environment
    # note - the below commented out until *all* Jenkins workers can get `jekyll` installed
    # if "DOCS" in changed_modules and test_env == "amplab_jenkins":
    #    build_spark_documentation()

    if any(m.should_run_build_tests for m in test_modules):
        run_build_tests()

    # spark build
    build_apache_spark(build_tool, hadoop_version)

    # backwards compatibility checks
    if build_tool == "sbt":
        # Note: compatibility tests only supported in sbt for now
        detect_binary_inop_with_mima(hadoop_version)
        # Since we did not build assembly/package before running dev/mima, we need to
        # do it here because the tests still rely on it; see SPARK-13294 for details.
        build_spark_assembly_sbt(hadoop_version)

    # run the test suites
    run_scala_tests(build_tool, hadoop_version, test_modules, excluded_tags)

    modules_with_python_tests = [m for m in test_modules if m.python_test_goals]
    if modules_with_python_tests:
        run_python_tests(modules_with_python_tests, opts.parallelism)
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
