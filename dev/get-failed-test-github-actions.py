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

from argparse import ArgumentParser
import os
import os.path
import xml.etree.ElementTree as ET

modules_target = {
  "core": "core/target/test-reports/",
  "unsafe": "common/unsafe/target/test-reports/",
  "kvstore": "common/kvstore/target/test-reports/",
  "avro": "external/avro/target/test-reports/",
  "network-common": "common/network-common/target/test-reports/",
  "network-shuffle": "common/network-shuffle/target/test-reports/",
  "repl": "repl/target/test-reports/",
  "launcher": "launcher/target/test-reports/",
  "examples": "examples/target/test-reports/",
  "sketch": "common/sketch/target/test-reports/",
  "graphx": "graphx/target/test-reports/",
  "catalyst": "sql/catalyst/target/test-reports/",
  "hive-thriftserver": "sql/hive-thriftserver/target/test-reports/",
  "streaming": "streaming/target/test-reports/",
  "sql-kafka-0-10": "external/kafka-0-10-sql//target/test-reports/",
  "mllib-local": "mllib-local/target/test-reports/",
  "mllib": "mllib/target/test-reports/",
  "yarn": "resource-managers/yarn/target/test-reports/",
  "mesos": "resource-managers/target/test-reports/",
  "kubernetes": "resource-managers/kubernetes/target/test-reports/",
  "hadoop-cloud": "hadoop-cloud/target/test-reports/",
  "spark-ganglia-lgpl": "external//target/test-reports/",
  "hive": "sql/hive/target/test-reports/",
  "sql": "sql/core/target/test-reports/"
}

def parse_opts():
    parser = ArgumentParser(
        prog="get-failed-test-github-actions"
    )
    parser.add_argument(
        "-m", "--modules", type=str,
        default=None,
        help="A comma-separated list of modules to process on"
    )

    args, unknown = parser.parse_known_args()
    if unknown:
        parser.error("Unsupported arguments: %s" % ' '.join(unknown))
    if args.modules is None:
        parser.error("modules must be specified.")
    return args

def parse_test_report(file):
    root = ET.parse(file).getroot()
    has_no_failure_yet = True

    for testcase in root.findall("testcase"):
        testcase_testsuite = testcase.get("classname")
        testcase_testname = testcase.get("name")

        failure = testcase.find("failure")
        if failure is not None:
            failure_message = failure.get("message")
            stacktrace = failure.text

            if has_no_failure_yet:
               print("Failed test suite: " + testcase_testsuite)
               has_no_failure_yet = False

            print("Failed test name: " + testcase_testname)
            print("Error Message: " + failure_message)
            print("Stacktrace: " + stacktrace)

def parse_failed_testcases_from_module(module):
    if module in modules_target:
        test_report_path = "./" + modules_target[module]
        if os.path.isdir(test_report_path):
            print("[info] Parsing test report for module", module, "from path:" + test_report_path)

            files = os.listdir(test_report_path)
            for file in files:
                parse_test_report(test_report_path + file)
        else:
            print("[info] Cannot find test report for module", module, ", skipping...")
    else:
        print("[error] Cannot get the test report path for module", module)

def main():
    opts = parse_opts()

    str_test_modules = [m.strip() for m in opts.modules.split(",")]
    for module in str_test_modules:
        parse_failed_testcases_from_module(module)

if __name__ == "__main__":
    main()
