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

import os
import sys
import json
import functools
import subprocess
from urllib.request import urlopen
from urllib.request import Request
from urllib.error import HTTPError, URLError

from sparktestsupport import SPARK_HOME, ERROR_CODES
from sparktestsupport.shellutils import run_cmd


def print_err(msg):
    """
    Given a set of arguments, will print them to the STDERR stream
    """
    print(msg, file=sys.stderr)


def post_message_to_github(msg, ghprb_pull_id):
    print("Attempting to post to GitHub...")

    api_url = os.getenv("GITHUB_API_BASE", "https://api.github.com/repos/apache/spark")
    url = api_url + "/issues/" + ghprb_pull_id + "/comments"
    github_oauth_key = os.environ["GITHUB_OAUTH_KEY"]

    posted_message = json.dumps({"body": msg})
    request = Request(url,
                      headers={
                          "Authorization": "token %s" % github_oauth_key,
                          "Content-Type": "application/json"
                      },
                      data=posted_message.encode('utf-8'))
    try:
        response = urlopen(request)

        if response.getcode() == 201:
            print(" > Post successful.")
    except HTTPError as http_e:
        print_err("Failed to post message to GitHub.")
        print_err(" > http_code: %s" % http_e.code)
        print_err(" > api_response: %s" % http_e.read())
        print_err(" > data: %s" % posted_message)
    except URLError as url_e:
        print_err("Failed to post message to GitHub.")
        print_err(" > urllib_status: %s" % url_e.reason[1])
        print_err(" > data: %s" % posted_message)


def pr_message(build_display_name,
               build_url,
               ghprb_pull_id,
               short_commit_hash,
               commit_url,
               msg,
               post_msg=''):
    # align the arguments properly for string formatting
    str_args = (build_display_name,
                msg,
                build_url,
                ghprb_pull_id,
                short_commit_hash,
                commit_url,
                str(' ' + post_msg + '.') if post_msg else '.')
    return '**[Test build %s %s](%stestReport)** for PR %s at commit [`%s`](%s)%s' % str_args


def run_pr_checks(pr_tests, ghprb_actual_commit, sha1):
    """
    Executes a set of pull request checks to ease development and report issues with various
    components such as style, linting, dependencies, compatibilities, etc.
    @return a list of messages to post back to GitHub
    """
    # Ensure we save off the current HEAD to revert to
    current_pr_head = run_cmd(['git', 'rev-parse', 'HEAD'], return_output=True).strip()
    pr_results = list()

    for pr_test in pr_tests:
        test_name = pr_test + '.sh'
        pr_results.append(run_cmd(['bash', os.path.join(SPARK_HOME, 'dev', 'tests', test_name),
                                   ghprb_actual_commit, sha1],
                                  return_output=True).rstrip())
        # Ensure, after each test, that we're back on the current PR
        run_cmd(['git', 'checkout', '-f', current_pr_head])
    return pr_results


def run_tests(tests_timeout):
    """
    Runs the `dev/run-tests` script and responds with the correct error message
    under the various failure scenarios.
    @return a tuple containing the test result code and the result note to post to GitHub
    """

    test_result_code = subprocess.Popen(['timeout',
                                         tests_timeout,
                                         os.path.join(SPARK_HOME, 'dev', 'run-tests')]).wait()

    failure_note_by_errcode = {
        # error to denote run-tests script failures:
        1: 'executing the `dev/run-tests` script',
        ERROR_CODES["BLOCK_GENERAL"]: 'some tests',
        ERROR_CODES["BLOCK_RAT"]: 'RAT tests',
        ERROR_CODES["BLOCK_SCALA_STYLE"]: 'Scala style tests',
        ERROR_CODES["BLOCK_JAVA_STYLE"]: 'Java style tests',
        ERROR_CODES["BLOCK_PYTHON_STYLE"]: 'Python style tests',
        ERROR_CODES["BLOCK_R_STYLE"]: 'R style tests',
        ERROR_CODES["BLOCK_DOCUMENTATION"]: 'to generate documentation',
        ERROR_CODES["BLOCK_BUILD"]: 'to build',
        ERROR_CODES["BLOCK_BUILD_TESTS"]: 'build dependency tests',
        ERROR_CODES["BLOCK_MIMA"]: 'MiMa tests',
        ERROR_CODES["BLOCK_SPARK_UNIT_TESTS"]: 'Spark unit tests',
        ERROR_CODES["BLOCK_PYSPARK_UNIT_TESTS"]: 'PySpark unit tests',
        ERROR_CODES["BLOCK_PYSPARK_PIP_TESTS"]: 'PySpark pip packaging tests',
        ERROR_CODES["BLOCK_SPARKR_UNIT_TESTS"]: 'SparkR unit tests',
        ERROR_CODES["BLOCK_TIMEOUT"]: 'from timeout after a configured wait of `%s`' % (
            tests_timeout)
    }

    if test_result_code == 0:
        test_result_note = ' * This patch passes all tests.'
    else:
        note = failure_note_by_errcode.get(
            test_result_code, "due to an unknown error code, %s" % test_result_code)
        test_result_note = ' * This patch **fails %s**.' % note

    return [test_result_code, test_result_note]


def main():
    # Important Environment Variables
    # ---
    # $ghprbActualCommit
    #   This is the hash of the most recent commit in the PR.
    #   The merge-base of this and master is the commit from which the PR was branched.
    # $sha1
    #   If the patch merges cleanly, this is a reference to the merge commit hash
    #     (e.g. "origin/pr/2606/merge").
    #   If the patch does not merge cleanly, it is equal to $ghprbActualCommit.
    #   The merge-base of this and master in the case of a clean merge is the most recent commit
    #     against master.
    ghprb_pull_id = os.environ["ghprbPullId"]
    ghprb_actual_commit = os.environ["ghprbActualCommit"]
    ghprb_pull_title = os.environ["ghprbPullTitle"].lower()
    sha1 = os.environ["sha1"]

    # Marks this build as a pull request build.
    os.environ["AMP_JENKINS_PRB"] = "true"
    # Switch to a Maven-based build if the PR title contains "test-maven":
    if "test-maven" in ghprb_pull_title:
        os.environ["AMPLAB_JENKINS_BUILD_TOOL"] = "maven"
    # Switch the Hadoop profile based on the PR title:
    if "test-hadoop2.7" in ghprb_pull_title:
        os.environ["AMPLAB_JENKINS_BUILD_PROFILE"] = "hadoop2.7"
    if "test-hadoop3.2" in ghprb_pull_title:
        os.environ["AMPLAB_JENKINS_BUILD_PROFILE"] = "hadoop3.2"
    # Switch the Hive profile based on the PR title:
    if "test-hive2.3" in ghprb_pull_title:
        os.environ["AMPLAB_JENKINS_BUILD_HIVE_PROFILE"] = "hive2.3"

    build_display_name = os.environ["BUILD_DISPLAY_NAME"]
    build_url = os.environ["BUILD_URL"]

    project_url = os.getenv("SPARK_PROJECT_URL", "https://github.com/apache/spark")
    commit_url = project_url + "/commit/" + ghprb_actual_commit

    # GitHub doesn't auto-link short hashes when submitted via the API, unfortunately. :(
    short_commit_hash = ghprb_actual_commit[0:7]

    # format: http://linux.die.net/man/1/timeout
    # must be less than the timeout configured on Jenkins. Usually Jenkins's timeout is higher
    # then this. Please consult with the build manager or a committer when it should be increased.
    tests_timeout = "500m"

    # Array to capture all test names to run on the pull request. These tests are represented
    # by their file equivalents in the dev/tests/ directory.
    #
    # To write a PR test:
    #   * the file must reside within the dev/tests directory
    #   * be an executable bash script
    #   * accept three arguments on the command line, the first being the GitHub PR long commit
    #     hash, the second the GitHub SHA1 hash, and the final the current PR hash
    #   * and, lastly, return string output to be included in the pr message output that will
    #     be posted to GitHub
    pr_tests = [
        "pr_merge_ability",
        "pr_public_classes"
    ]

    # `bind_message_base` returns a function to generate messages for GitHub posting
    github_message = functools.partial(pr_message,
                                       build_display_name,
                                       build_url,
                                       ghprb_pull_id,
                                       short_commit_hash,
                                       commit_url)

    # post start message
    post_message_to_github(github_message('has started'), ghprb_pull_id)

    pr_check_results = run_pr_checks(pr_tests, ghprb_actual_commit, sha1)

    test_result_code, test_result_note = run_tests(tests_timeout)

    # post end message
    result_message = github_message('has finished')
    result_message += '\n' + test_result_note + '\n'
    result_message += '\n'.join(pr_check_results)

    post_message_to_github(result_message, ghprb_pull_id)

    sys.exit(test_result_code)


if __name__ == "__main__":
    main()
