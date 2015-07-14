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
import sys
import json
import subprocess

from sparktestsupport import SPARK_HOME, ERROR_CODES
from sparktestsupport.shellutils import exit_from_command_with_retcode, run_cmd, rm_r


def print_err(*args):
    """
    Given a set of arguments, will print them to the STDERR stream
    """
    print(*args, file=sys.stderr)


def post_message(mssg, comments_url):
    http_code_header = "HTTP Response Code: "
    posted_message = json.dumps({"body": mssg})

    print("Attempting to post to Github...")

    # we don't want to call `run_cmd` here as, in the event of an error, we DO NOT
    # want to print the GITHUB_OAUTH_KEY into the public Jenkins logs
    curl_proc = subprocess.Popen(['curl',
                                  '--silent',
                                  '--user', 'x-oauth-basic:' + os.environ['GITHUB_OATH_KEY'],
                                  '--request', 'POST',
                                  '--data', posted_message,
                                  '--write-out', http_code_header + '%{http_code}',
                                  '--header', 'Content-Type: application/json',
                                  comments_url],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
    curl_stdout, curl_stderr = curl_proc.communicate()
    curl_returncode = curl_proc.returncode
    # find all lines relevant to the Github API response
    api_response = "\n".join([l for l in curl_stdout.split('\n')
                              if l and not l.startswith(http_code_header)])
    # find the line where `http_code_header` exists, split on ':' to get the
    # HTTP response code, and cast to an int
    http_code = int(curl_stdout[curl_stdout.find(http_code_header):].split(':')[1])

    if not curl_returncode == 0:
        print_err("Failed to post message to GitHub.")
        print_err(" > curl_status:", curl_returncode)
        print_err(" > curl_output:", curl_stdout)
        print_err(" > data:", posted_message)

    if http_code and not http_code == 201:
        print_err(" > http_code:", http_code)
        print_err(" > api_response:", api_response)
        print_err(" > data:", posted_message)

    if curl_returncode == 0 and http_code == 201:
        print(" > Post successful.")


def send_archived_logs():
    print("Archiving unit tests logs...")

    log_files = run_cmd(['find', '.',
                         '-name', 'unit-tests.log',
                         '-o', '-path', './sql/hive/target/HiveCompatibilitySuite.failed',
                         '-o', '-path', './sql/hive/target/HiveCompatibilitySuite.hiveFailed',
                         '-o', '-path', './sql/hive/target/HiveCompatibilitySuite.wrong'],
                        return_output=True)

    if log_files:
        log_archive = "unit-tests-logs.tar.gz"

        run_cmd(['tar', 'czf', log-archive, *log_files])

        jenkins_build_dir = os.environ["JENKINS_HOME"] + "/jobs/" + os.environ["JOB_NAME"] +
        "/builds/" + os.environ["BUILD_NUMBER"]

        scp_proc = subprocess.Popen(['scp', log_archive,
                                     'amp-jenkins-master:' + jenkins_build_dir + '/' + log_archive],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        scp_stdout, scp_stderr = scp_proc.communicate()
        scp_returncode = scp_proc.returncode

        if not scp_returncode == 0:
            print_err("Failed to send archived unit tests logs to Jenkins master.")
            print_err(" > scp_status:",  scp_returncode)
            print_err(" > scp_output:", scp_stdout)
        else:
            print(" > Send successful.")
    else:
        print_err(" > No log files found.")

        rm_r(log_archive)


def run_pr_tests(pr_tests, ghprb_actual_commit, sha1):
    # Ensure we save off the current HEAD to revert to
    current_pr_head = run_cmd(['git', 'rev-parse', 'HEAD'], return_output=True).strip()
    pr_results = list()

    for pr_test in pr_tests:
        pr_results.append(run_cmd(['bash', os.path.join(SPARK_HOME, 'dev', 'tests', pr_test),
                                   ghprb_actual_commit, sha1],
                                  return_output=True).strip())
        # Ensure, after each test, that we're back on the current PR
        run_cmd(['git', 'checkout', '-f', current_pr_head])
    return pr_results


def bind_message_base(build_display_name, build_url, ghprb_pull_id, short_commit_hash, commit_url):
    """
    Given base parameters to generate a strong Github message response, binds those
    parameters into a closure without the specific message and returns a function
    able to generate strong messages for a specific description.
    """
    return lambda mssg, post_mssg="":\
        '**[Test build ' + build_display_name + ' ' + mssg + '](' + build_url +
'console)** for PR ' + ghprb_pull_id + ' at commit [\`' + short_commit_hash + '\`](' +
commit_url + ')' + str(' ' + post_mssg + '.') if post_mssg else '.'


def success_result_note(mssg):
    return ' * This patch ' + mssg + '.'


def failure_result_note(mssg):
    return ' * This patch **fails ' + mssg + '**.'


def run_tests(tests_timeout):
    test_proc = subprocess.Popen(['timeout',
                                  tests_timeout,
                                  os.path.join(SPARK_HOME, 'dev', 'run-tests')]).wait()
    test_result = test_proc.returncode

    failure_note_by_errcode = {
        ERROR_CODES["BLOCK_GENERAL"]: failure_result_note('some tests'),
        ERROR_CODES["BLOCK_RAT"]: failure_result_note('RAT tests'),
        ERROR_CODES["BLOCK_SCALA_STYLE"]: failure_result_note('Scala style tests'),
        ERROR_CODES["BLOCK_PYTHON_STYLE"]: failure_result_note('Python style tests'),
        ERROR_CODES["BLOCK_DOCUMENTATION"]: failure_result_note('to generate documentation'),
        ERROR_CODES["BLOCK_BUILD"]: failure_result_note('to build'),
        ERROR_CODES["BLOCK_MIMA"]: failure_result_note('MiMa tests'),
        ERROR_CODES["BLOCK_SPARK_UNIT_TESTS"]: failure_result_note('Spark unit tests'),
        ERROR_CODES["BLOCK_PYSPARK_UNIT_TESTS"]: failure_result_note('PySpark unit tests'),
        ERROR_CODES["BLOCK_SPARKR_UNIT_TESTS"]: failure_result_note('SparkR unit tests'),
        ERROR_CODES["BLOCK_TIMEOUT"]: failure_result_note('from timeout after a configured wait' +
                                                          ' of \`' + tests_timeout + '\`')
    }

    if test_result == 0:
        test_result_note = success_result_note('passes all tests')
    else:
        test_result_note = failure_note_by_errcode(test_result)
        send_archived_logs()

    return test_result_note


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
    sha1 = os.environ["sha1"]

    # Marks this build as a pull request build.
    os.environ["AMP_JENKINS_PRB"] = "true"
    build_display_name = os.environ["BUILD_DISPLAY_NAME"]
    build_url = os.environ["BUILD_URL"]

    comments_url = "https://api.github.com/repos/apache/spark/issues/" + ghprb_pull_id + "/comments"
    pull_request_url = "https://github.com/apache/spark/pull/" + ghprb_pull_id
    commit_url = "https://github.com/apache/spark/commit/" + ghprb_actual_commit

    # GitHub doesn't auto-link short hashes when submitted via the API, unfortunately. :(
    short_commit_hash = ghprb_actual_commit[0:7]

    # format: http://linux.die.net/man/1/timeout
    # must be less than the timeout configured on Jenkins (currently 180m)
    tests_timeout = "175m"

    # Array to capture all tests to run on the pull request. These tests are held under the
    #  dev/tests/ directory.
    #
    # To write a PR test:
    #   * the file must reside within the dev/tests directory
    #   * be an executable bash script
    #   * accept three arguments on the command line, the first being the Github PR long commit
    #     hash, the second the Github SHA1 hash, and the final the current PR hash
    #   * and, lastly, return string output to be included in the pr message output that will
    #     be posted to Github
    pr_tests = ["pr_merge_ability",
                "pr_public_classes"
                # DISABLED (pwendell) "pr_new_dependencies"
                ]

    # `bind_message_base` returns a function to generate messages for Github posting
    github_message = bind_message_base(build_display_name,
                                       build_url,
                                       ghprb_pull_id,
                                       short_commit_hash,
                                       commit_url)

    # post start message
    post_message(github_message('has started'))

    pr_test_results = run_pr_tests(pr_tests, ghprb_actual_commit, sha1)

    test_results = run_tests(tests_timeout)

    # post end message
    result_message = github_message('has finished')
    result_message += '\n' + test_results
    for pr_result in pr_test_results:
        result_message += pr_result

    post_message(result_message)

    sys.exit(test_result)


if __name__ == "__main__":
    main()
