#!/usr/bin/env python3

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
from argparse import ArgumentParser
import os
import re
import sys
import subprocess

from sparktestsupport import SPARK_HOME, USER_HOME, ERROR_CODES
from sparktestsupport.shellutils import exit_from_command_with_retcode, run_cmd, rm_r, which
from sparktestsupport.utils import (
    determine_modules_for_files,
    determine_modules_to_test,
    determine_tags_to_exclude,
    identify_changed_files_from_git_commits,
)
import sparktestsupport.modules as modules


def setup_test_environ(environ):
    print("[info] Setup the following environment variables for tests: ")
    for (k, v) in environ.items():
        print("%s=%s" % (k, v))
        os.environ[k] = v


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


# -------------------------------------------------------------------------------------------------
# Functions for running the other build and test scripts
# -------------------------------------------------------------------------------------------------


def set_title_and_block(title, err_block):
    os.environ["CURRENT_BLOCK"] = str(ERROR_CODES[err_block])
    line_str = "=" * 72

    print("")
    print(line_str)
    print(title)
    print(line_str)


def run_apache_rat_checks():
    set_title_and_block("Running Apache RAT checks", "BLOCK_RAT")
    run_cmd([os.path.join(SPARK_HOME, "dev", "check-license")])


def run_scala_style_checks(extra_profiles):
    build_profiles = extra_profiles + modules.root.build_profile_flags
    set_title_and_block("Running Scala style checks", "BLOCK_SCALA_STYLE")
    profiles = " ".join(build_profiles)
    print("[info] Checking Scala style using SBT with these profiles: ", profiles)
    run_cmd([os.path.join(SPARK_HOME, "dev", "lint-scala"), profiles])


def run_java_style_checks(build_profiles):
    set_title_and_block("Running Java style checks", "BLOCK_JAVA_STYLE")
    # The same profiles used for building are used to run Checkstyle by SBT as well because
    # the previous build looks reused for Checkstyle and affecting Checkstyle. See SPARK-27130.
    profiles = " ".join(build_profiles)
    print("[info] Checking Java style using SBT with these profiles: ", profiles)
    run_cmd([os.path.join(SPARK_HOME, "dev", "sbt-checkstyle"), profiles])


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
    os.environ["PRODUCTION"] = "1"

    os.chdir(os.path.join(SPARK_HOME, "docs"))

    bundle_bin = which("bundle")

    if not bundle_bin:
        print(
            "[error] Cannot find a version of `bundle` on the system; please",
            " install one with `gem install bundler` and retry to build documentation.",
        )
        sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))
    else:
        run_cmd([bundle_bin, "install"])
        run_cmd([bundle_bin, "exec", "jekyll", "build"])

    os.chdir(SPARK_HOME)


def exec_maven(mvn_args=()):
    """Will call Maven in the current directory with the list of mvn_args passed
    in and returns the subprocess for any further processing"""

    flags = [os.path.join(SPARK_HOME, "build", "mvn")]
    run_cmd(flags + mvn_args)


def exec_sbt(sbt_args=()):
    """Will call SBT in the current directory with the list of mvn_args passed
    in and returns the subprocess for any further processing"""

    sbt_cmd = [os.path.join(SPARK_HOME, "build", "sbt")] + sbt_args

    sbt_output_filter = re.compile(
        b"^.*[info].*Resolving" + b"|" + b"^.*[warn].*Merging" + b"|" + b"^.*[info].*Including"
    )

    # NOTE: echo "q" is needed because sbt on encountering a build file
    # with failure (either resolution or compilation) prompts the user for
    # input either q, r, etc to quit or retry. This echo is there to make it
    # not block.
    echo_proc = subprocess.Popen(["echo", '"q\n"'], stdout=subprocess.PIPE)
    sbt_proc = subprocess.Popen(sbt_cmd, stdin=echo_proc.stdout, stdout=subprocess.PIPE)
    echo_proc.wait()
    for line in iter(sbt_proc.stdout.readline, b""):
        if not sbt_output_filter.match(line):
            print(line.decode("utf-8"), end="")
    retcode = sbt_proc.wait()

    if retcode != 0:
        exit_from_command_with_retcode(sbt_cmd, retcode)


def get_scala_profiles(scala_version):
    """
    For the given Scala version tag, return a list of Maven/SBT profile flags for
    building and testing against that Scala version.
    """
    if scala_version is None:
        return []  # assume it's default.

    sbt_maven_scala_profiles = {
        "scala2.12": ["-Pscala-2.12"],
        "scala2.13": ["-Pscala-2.13"],
    }

    if scala_version in sbt_maven_scala_profiles:
        return sbt_maven_scala_profiles[scala_version]
    else:
        print(
            "[error] Could not find",
            scala_version,
            "in the list. Valid options",
            " are",
            sbt_maven_scala_profiles.keys(),
        )
        sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))


def switch_scala_version(scala_version):
    """
    Switch the code base to use the given Scala version.
    """
    set_title_and_block("Switch the Scala version to %s" % scala_version, "BLOCK_SCALA_VERSION")

    assert scala_version is not None
    ver_num = scala_version[-4:]  # Simply extract. e.g.) 2.13 from scala2.13
    command = [os.path.join(SPARK_HOME, "dev", "change-scala-version.sh"), ver_num]
    run_cmd(command)


def get_hadoop_profiles(hadoop_version):
    """
    For the given Hadoop version tag, return a list of Maven/SBT profile flags for
    building and testing against that Hadoop version.
    """

    sbt_maven_hadoop_profiles = {
        "hadoop2": ["-Phadoop-2"],
        "hadoop3": ["-Phadoop-3"],
    }

    if hadoop_version in sbt_maven_hadoop_profiles:
        return sbt_maven_hadoop_profiles[hadoop_version]
    else:
        print(
            "[error] Could not find",
            hadoop_version,
            "in the list. Valid options",
            " are",
            sbt_maven_hadoop_profiles.keys(),
        )
        sys.exit(int(os.environ.get("CURRENT_BLOCK", 255)))


def build_spark_maven(extra_profiles):
    # Enable all of the profiles for the build:
    build_profiles = extra_profiles + modules.root.build_profile_flags
    mvn_goals = ["clean", "package", "-DskipTests"]
    profiles_and_goals = build_profiles + mvn_goals

    print("[info] Building Spark using Maven with these arguments: ", " ".join(profiles_and_goals))

    exec_maven(profiles_and_goals)


def build_spark_sbt(extra_profiles):
    # Enable all of the profiles for the build:
    build_profiles = extra_profiles + modules.root.build_profile_flags
    sbt_goals = [
        "Test/package",  # Build test jars as some tests depend on them
        "streaming-kinesis-asl-assembly/assembly",
        "connect/assembly",  # Build Spark Connect assembly
    ]
    profiles_and_goals = build_profiles + sbt_goals

    print("[info] Building Spark using SBT with these arguments: ", " ".join(profiles_and_goals))

    exec_sbt(profiles_and_goals)


def build_spark_unidoc_sbt(extra_profiles):
    set_title_and_block("Building Unidoc API Documentation", "BLOCK_DOCUMENTATION")
    # Enable all of the profiles for the build:
    build_profiles = extra_profiles + modules.root.build_profile_flags
    sbt_goals = ["unidoc"]
    profiles_and_goals = build_profiles + sbt_goals

    print(
        "[info] Building Spark unidoc using SBT with these arguments: ",
        " ".join(profiles_and_goals),
    )

    exec_sbt(profiles_and_goals)


def build_spark_assembly_sbt(extra_profiles, checkstyle=False):
    # Enable all of the profiles for the build:
    build_profiles = extra_profiles + modules.root.build_profile_flags
    sbt_goals = ["assembly/package"]
    profiles_and_goals = build_profiles + sbt_goals
    print(
        "[info] Building Spark assembly using SBT with these arguments: ",
        " ".join(profiles_and_goals),
    )
    exec_sbt(profiles_and_goals)

    if checkstyle:
        run_java_style_checks(build_profiles)

    if not os.environ.get("AMPLAB_JENKINS") and not os.environ.get("SKIP_UNIDOC"):
        build_spark_unidoc_sbt(extra_profiles)


def build_apache_spark(build_tool, extra_profiles):
    """Will build Spark with the extra profiles and the passed in build tool
    (either `sbt` or `maven`). Defaults to using `sbt`."""

    set_title_and_block("Building Spark", "BLOCK_BUILD")

    rm_r("lib_managed")

    if build_tool == "maven":
        build_spark_maven(extra_profiles)
    else:
        build_spark_sbt(extra_profiles)


def detect_binary_inop_with_mima(extra_profiles):
    build_profiles = extra_profiles + modules.root.build_profile_flags
    set_title_and_block("Detecting binary incompatibilities with MiMa", "BLOCK_MIMA")
    profiles = " ".join(build_profiles)
    print(
        "[info] Detecting binary incompatibilities with MiMa using SBT with these profiles: ",
        profiles,
    )
    run_cmd([os.path.join(SPARK_HOME, "dev", "mima"), profiles])


def run_scala_tests_maven(test_profiles):
    mvn_test_goals = ["test", "--fail-at-end"]

    profiles_and_goals = test_profiles + mvn_test_goals

    print(
        "[info] Running Spark tests using Maven with these arguments: ",
        " ".join(profiles_and_goals),
    )

    exec_maven(profiles_and_goals)


def run_scala_tests_sbt(test_modules, test_profiles):

    sbt_test_goals = list(itertools.chain.from_iterable(m.sbt_test_goals for m in test_modules))

    if not sbt_test_goals:
        return

    profiles_and_goals = test_profiles + sbt_test_goals

    print(
        "[info] Running Spark tests using SBT with these arguments: ", " ".join(profiles_and_goals)
    )

    exec_sbt(profiles_and_goals)


def run_scala_tests(build_tool, extra_profiles, test_modules, excluded_tags, included_tags):
    """Function to properly execute all tests passed in as a set from the
    `determine_test_suites` function"""
    set_title_and_block("Running Spark unit tests", "BLOCK_SPARK_UNIT_TESTS")

    # Remove duplicates while keeping the test module order
    test_modules = list(dict.fromkeys(test_modules))

    test_profiles = extra_profiles + list(
        set(itertools.chain.from_iterable(m.build_profile_flags for m in test_modules))
    )

    if included_tags:
        test_profiles += ["-Dtest.include.tags=" + ",".join(included_tags)]
    if excluded_tags:
        test_profiles += ["-Dtest.exclude.tags=" + ",".join(excluded_tags)]

    # set up java11 env if this is a pull request build with 'test-java11' in the title
    if "ghprbPullTitle" in os.environ:
        if "test-java11" in os.environ["ghprbPullTitle"].lower():
            os.environ["JAVA_HOME"] = "/usr/java/jdk-11.0.1"
            os.environ["PATH"] = "%s/bin:%s" % (os.environ["JAVA_HOME"], os.environ["PATH"])
            test_profiles += ["-Djava.version=11"]

    if build_tool == "maven":
        run_scala_tests_maven(test_profiles)
    else:
        run_scala_tests_sbt(test_modules, test_profiles)


def run_python_tests(test_modules, parallelism, with_coverage=False):
    set_title_and_block("Running PySpark tests", "BLOCK_PYSPARK_UNIT_TESTS")

    if with_coverage:
        # Coverage makes the PySpark tests flaky due to heavy parallelism.
        # When we run PySpark tests with coverage, it uses 4 for now as
        # workaround.
        parallelism = 1
        script = "run-tests-with-coverage"
    else:
        script = "run-tests"
    command = [os.path.join(SPARK_HOME, "python", script)]
    if test_modules != [modules.root]:
        command.append("--modules=%s" % ",".join(m.name for m in test_modules))
    command.append("--parallelism=%i" % parallelism)
    run_cmd(command)


def run_python_packaging_tests():
    if not os.environ.get("AMPLAB_JENKINS"):
        set_title_and_block("Running PySpark packaging tests", "BLOCK_PYSPARK_PIP_TESTS")
        command = [os.path.join(SPARK_HOME, "dev", "run-pip-tests")]
        run_cmd(command)


def run_build_tests():
    set_title_and_block("Running build tests", "BLOCK_BUILD_TESTS")
    run_cmd([os.path.join(SPARK_HOME, "dev", "test-dependencies.sh")])


def run_sparkr_tests():
    set_title_and_block("Running SparkR tests", "BLOCK_SPARKR_UNIT_TESTS")

    if which("R"):
        run_cmd([os.path.join(SPARK_HOME, "R", "run-tests.sh")])
    else:
        print("Ignoring SparkR tests as R was not found in PATH")


def parse_opts():
    parser = ArgumentParser(prog="run-tests")
    parser.add_argument(
        "-p",
        "--parallelism",
        type=int,
        default=8,
        help="The number of suites to test in parallel (default %(default)d)",
    )
    parser.add_argument(
        "-m",
        "--modules",
        type=str,
        default=None,
        help="A comma-separated list of modules to test "
        "(default: %s)" % ",".join(sorted([m.name for m in modules.all_modules])),
    )
    parser.add_argument(
        "-e",
        "--excluded-tags",
        type=str,
        default=None,
        help="A comma-separated list of tags to exclude in the tests, "
        "e.g., org.apache.spark.tags.ExtendedHiveTest ",
    )
    parser.add_argument(
        "-i",
        "--included-tags",
        type=str,
        default=None,
        help="A comma-separated list of tags to include in the tests, "
        "e.g., org.apache.spark.tags.ExtendedHiveTest ",
    )

    args, unknown = parser.parse_known_args()
    if unknown:
        parser.error("Unsupported arguments: %s" % " ".join(unknown))
    if args.parallelism < 1:
        parser.error("Parallelism cannot be less than 1")
    return args


def main():
    opts = parse_opts()
    # Ensure the user home directory (HOME) is valid and is an absolute directory
    if not USER_HOME or not os.path.isabs(USER_HOME):
        print(
            "[error] Cannot determine your home directory as an absolute path;",
            " ensure the $HOME environment variable is set properly.",
        )
        sys.exit(1)

    os.chdir(SPARK_HOME)

    rm_r(os.path.join(SPARK_HOME, "work"))
    rm_r(os.path.join(USER_HOME, ".ivy2", "local", "org.apache.spark"))
    rm_r(os.path.join(USER_HOME, ".ivy2", "cache", "org.apache.spark"))

    os.environ["CURRENT_BLOCK"] = str(ERROR_CODES["BLOCK_GENERAL"])

    java_exe = determine_java_executable()

    if not java_exe:
        print(
            "[error] Cannot find a version of `java` on the system; please",
            " install one and retry.",
        )
        sys.exit(2)

    # Install SparkR
    should_only_test_modules = opts.modules is not None
    test_modules = []
    if should_only_test_modules:
        str_test_modules = [m.strip() for m in opts.modules.split(",")]
        test_modules = [m for m in modules.all_modules if m.name in str_test_modules]

    if not should_only_test_modules or modules.sparkr in test_modules:
        # If tests modules are specified, we will not run R linter.
        # SparkR needs the manual SparkR installation.
        if which("R"):
            run_cmd([os.path.join(SPARK_HOME, "R", "install-dev.sh")])
        else:
            print("Cannot install SparkR as R was not found in PATH")

    if os.environ.get("AMPLAB_JENKINS"):
        # if we're on the Amplab Jenkins build servers setup variables
        # to reflect the environment settings
        build_tool = os.environ.get("AMPLAB_JENKINS_BUILD_TOOL", "sbt")
        scala_version = os.environ.get("AMPLAB_JENKINS_BUILD_SCALA_PROFILE")
        hadoop_version = os.environ.get("AMPLAB_JENKINS_BUILD_PROFILE", "hadoop3")
        test_env = "amplab_jenkins"
        # add path for Python3 in Jenkins if we're calling from a Jenkins machine
        # TODO(sknapp):  after all builds are ported to the ubuntu workers, change this to be:
        # /home/jenkins/anaconda2/envs/py36/bin
        os.environ["PATH"] = "/home/anaconda/envs/py36/bin:" + os.environ.get("PATH")
    else:
        # else we're running locally or GitHub Actions.
        build_tool = "sbt"
        scala_version = os.environ.get("SCALA_PROFILE")
        hadoop_version = os.environ.get("HADOOP_PROFILE", "hadoop3")
        if "GITHUB_ACTIONS" in os.environ:
            test_env = "github_actions"
        else:
            test_env = "local"

    extra_profiles = get_hadoop_profiles(hadoop_version) + get_scala_profiles(scala_version)

    print(
        "[info] Using build tool",
        build_tool,
        "with profiles",
        *(extra_profiles + ["under environment", test_env]),
    )

    changed_modules = []
    changed_files = []
    included_tags = []
    excluded_tags = []
    if should_only_test_modules:
        # We're likely in the forked repository
        is_apache_spark_ref = os.environ.get("APACHE_SPARK_REF", "") != ""
        # We're likely in the main repo build.
        is_github_prev_sha = os.environ.get("GITHUB_PREV_SHA", "") != ""
        # Otherwise, we're in either periodic job in Github Actions or somewhere else.

        # If we're running the tests in GitHub Actions, attempt to detect and test
        # only the affected modules.
        if test_env == "github_actions" and (is_apache_spark_ref or is_github_prev_sha):
            if is_apache_spark_ref:
                changed_files = identify_changed_files_from_git_commits(
                    "HEAD", target_ref=os.environ["APACHE_SPARK_REF"]
                )
            elif is_github_prev_sha:
                changed_files = identify_changed_files_from_git_commits(
                    os.environ["GITHUB_SHA"], target_ref=os.environ["GITHUB_PREV_SHA"]
                )

            modules_to_test = determine_modules_to_test(
                determine_modules_for_files(changed_files), deduplicated=False
            )

            if modules.root not in modules_to_test:
                # If root module is not found, only test the intersected modules.
                # If root module is found, just run the modules as specified initially.
                test_modules = list(set(modules_to_test).intersection(test_modules))

        changed_modules = test_modules
        if len(changed_modules) == 0:
            print("[info] There are no modules to test, exiting without testing.")
            return

    # If we're running the tests in AMPLab Jenkins, calculate the diff from the targeted branch, and
    # detect modules to test.
    elif test_env == "amplab_jenkins" and os.environ.get("AMP_JENKINS_PRB"):
        target_branch = os.environ["ghprbTargetBranch"]
        changed_files = identify_changed_files_from_git_commits("HEAD", target_branch=target_branch)
        changed_modules = determine_modules_for_files(changed_files)
        test_modules = determine_modules_to_test(changed_modules)
        excluded_tags = determine_tags_to_exclude(changed_modules)

    # If there is no changed module found, tests all.
    if not changed_modules:
        changed_modules = [modules.root]
    if not test_modules:
        test_modules = determine_modules_to_test(changed_modules)

    if opts.excluded_tags:
        excluded_tags.extend([t.strip() for t in opts.excluded_tags.split(",")])
    if opts.included_tags:
        included_tags.extend([t.strip() for t in opts.included_tags.split(",")])

    print("[info] Found the following changed modules:", ", ".join(x.name for x in changed_modules))

    # setup environment variables
    # note - the 'root' module doesn't collect environment variables for all modules. Because the
    # environment variables should not be set if a module is not changed, even if running the 'root'
    # module. So here we should use changed_modules rather than test_modules.
    test_environ = {}
    for m in changed_modules:
        test_environ.update(m.environ)
    setup_test_environ(test_environ)

    if scala_version is not None:
        # If not set, assume this is default and doesn't need to change.
        switch_scala_version(scala_version)

    should_run_java_style_checks = False
    if not should_only_test_modules:
        # license checks
        run_apache_rat_checks()

        # style checks
        if not changed_files or any(
            f.endswith(".scala") or f.endswith("scalastyle-config.xml") for f in changed_files
        ):
            run_scala_style_checks(extra_profiles)
        if not changed_files or any(
            f.endswith(".java")
            or f.endswith("checkstyle.xml")
            or f.endswith("checkstyle-suppressions.xml")
            for f in changed_files
        ):
            # Run SBT Checkstyle after the build to prevent a side-effect to the build.
            should_run_java_style_checks = True
        if not changed_files or any(
            f.endswith("lint-python") or f.endswith("tox.ini") or f.endswith(".py")
            for f in changed_files
        ):
            run_python_style_checks()
        if not changed_files or any(
            f.endswith(".R") or f.endswith("lint-r") or f.endswith(".lintr") for f in changed_files
        ):
            run_sparkr_style_checks()

    # determine if docs were changed and if we're inside the amplab environment
    # note - the below commented out until *all* Jenkins workers can get the Bundler gem installed
    # if "DOCS" in changed_modules and test_env == "amplab_jenkins":
    #    build_spark_documentation()

    if any(m.should_run_build_tests for m in test_modules) and test_env != "amplab_jenkins":
        run_build_tests()

    # spark build
    build_apache_spark(build_tool, extra_profiles)

    # backwards compatibility checks
    if build_tool == "sbt":
        # Note: compatibility tests only supported in sbt for now
        if not os.environ.get("SKIP_MIMA"):
            detect_binary_inop_with_mima(extra_profiles)
        # Since we did not build assembly/package before running dev/mima, we need to
        # do it here because the tests still rely on it; see SPARK-13294 for details.
        build_spark_assembly_sbt(extra_profiles, should_run_java_style_checks)

    # run the test suites
    run_scala_tests(build_tool, extra_profiles, test_modules, excluded_tags, included_tags)

    modules_with_python_tests = [m for m in test_modules if m.python_test_goals]
    if modules_with_python_tests and not os.environ.get("SKIP_PYTHON"):
        run_python_tests(
            modules_with_python_tests,
            opts.parallelism,
            with_coverage=os.environ.get("PYSPARK_CODECOV", "false") == "true",
        )
        run_python_packaging_tests()
    if any(m.should_run_r_tests for m in test_modules) and not os.environ.get("SKIP_R"):
        run_sparkr_tests()


def _test():
    import doctest
    import sparktestsupport.utils

    failure_count = doctest.testmod(sparktestsupport.utils)[0] + doctest.testmod()[0]
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
    main()
