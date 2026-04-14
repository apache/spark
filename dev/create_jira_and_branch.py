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

import argparse
import subprocess
import sys
import traceback

from spark_jira_utils import create_jira_issue, fail, get_jira_client


def run_cmd(cmd):
    print(cmd)
    if isinstance(cmd, list):
        return subprocess.check_output(cmd).decode("utf-8")
    else:
        return subprocess.check_output(cmd.split(" ")).decode("utf-8")


def create_and_checkout_branch(jira_id):
    try:
        run_cmd("git checkout -b %s" % jira_id)
        print("Created and checked out branch: %s" % jira_id)
    except subprocess.CalledProcessError as e:
        fail("Failed to create branch %s: %s" % (jira_id, e))


def create_commit(jira_id, title):
    try:
        run_cmd(["git", "commit", "-a", "-m", "[%s] %s" % (jira_id, title)])
        print("Created a commit with message: [%s] %s" % (jira_id, title))
    except subprocess.CalledProcessError as e:
        fail("Failed to create commit: %s" % e)


def choose_components(asf_jira):
    components = asf_jira.project_components("SPARK")
    components = [c for c in components if not c.raw.get("archived", False)]
    for i, c in enumerate(components):
        print("%d. %s" % (i + 1, c.name))

    while True:
        try:
            choice = input("Please choose a component by number: ")
            idx = int(choice) - 1
            if 0 <= idx < len(components):
                return components[idx].name
            else:
                print("Invalid number. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")


def main():
    parser = argparse.ArgumentParser(description="Create a Spark JIRA issue.")
    parser.add_argument("title", nargs="?", help="Title of the JIRA issue")
    parser.add_argument("-p", "--parent", help="Parent JIRA ID for subtasks")
    parser.add_argument(
        "-t",
        "--type",
        help="Issue type to create when no parent is specified (e.g. Bug). Defaults to Improvement.",
    )
    parser.add_argument("-v", "--version", help="Version to use for the issue")
    parser.add_argument("-c", "--component", help="Component for the issue")
    args = parser.parse_args()

    asf_jira = get_jira_client()

    if args.parent:
        parent_issue = asf_jira.issue(args.parent)
        print("Parent issue title: %s" % parent_issue.fields.summary)
        print("Creating a subtask of %s with title: %s" % (args.parent, args.title))
    else:
        print("Creating JIRA issue with title: %s" % args.title)

    if not args.title:
        parser.error("the following arguments are required: title")

    if not args.component:
        args.component = choose_components(asf_jira)

    jira_id = create_jira_issue(asf_jira, args.title, args.component,
                                parent=args.parent, issue_type=args.type,
                                version=args.version)
    print("Created JIRA issue: %s" % jira_id)

    create_and_checkout_branch(jira_id)

    create_commit(jira_id, args.title)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(-1)
