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
import os
import re
import sys
import traceback

try:
    import jira.client

    JIRA_IMPORTED = True
except ImportError:
    JIRA_IMPORTED = False

# ASF JIRA access token
JIRA_ACCESS_TOKEN = os.environ.get("JIRA_ACCESS_TOKEN")
JIRA_API_BASE = "https://issues.apache.org/jira"


def fail(msg):
    print(msg)
    sys.exit(-1)


def get_jira_client():
    return jira.client.JIRA(
        {"server": JIRA_API_BASE}, token_auth=JIRA_ACCESS_TOKEN, timeout=(3.05, 30)
    )


def list_components():
    asf_jira = get_jira_client()
    components = asf_jira.project_components("SPARK")
    components = [c for c in components if not c.raw.get("archived", False)]
    for c in sorted(components, key=lambda x: x.name):
        print(c.name)


def main():
    parser = argparse.ArgumentParser(description="Create a Spark JIRA issue.")
    parser.add_argument("title", nargs="?", help="Title of the JIRA issue")
    parser.add_argument("-p", "--parent", help="Parent JIRA ID for subtasks")
    parser.add_argument(
        "-t",
        "--type",
        help="Issue type (e.g. Bug, Improvement). Defaults to Improvement.",
    )
    parser.add_argument("-v", "--version", help="Version to use for the issue")
    parser.add_argument("-c", "--component", help="Component for the issue")
    parser.add_argument(
        "--list-components", action="store_true", help="List available components and exit"
    )
    args = parser.parse_args()

    def check_jira_access():
        if not JIRA_IMPORTED or not JIRA_ACCESS_TOKEN:
            msg = "Cannot create JIRA ticket automatically"
            if not JIRA_IMPORTED:
                msg += " (jira-python library not installed, run 'pip install jira')"
            else:
                msg += " (JIRA_ACCESS_TOKEN env-var not set)"
            msg += ". Please create the ticket manually at %s" % JIRA_API_BASE
            fail(msg)

    if args.list_components:
        check_jira_access()
        list_components()
        return

    if not args.title:
        parser.error("the following arguments are required: title")

    if not args.component:
        parser.error("the following arguments are required: -c/--component")

    check_jira_access()

    asf_jira = get_jira_client()

    if args.version:
        affected_version = args.version
    else:
        versions = asf_jira.project_versions("SPARK")
        # Consider only x.y.z, unreleased, unarchived versions
        versions = [
            x
            for x in versions
            if not x.raw["released"]
            and not x.raw["archived"]
            and re.match(r"\d+\.\d+\.\d+", x.name)
        ]
        versions = sorted(versions, key=lambda x: x.name, reverse=True)
        if not versions:
            fail("No unreleased versions found for SPARK project.")
        affected_version = versions[0].name

    issue_dict = {
        "project": {"key": "SPARK"},
        "summary": args.title,
        "description": "",
        "versions": [{"name": affected_version}],
        "components": [{"name": args.component}],
    }

    if args.parent:
        issue_dict["issuetype"] = {"name": "Sub-task"}
        issue_dict["parent"] = {"key": args.parent}
    else:
        issue_dict["issuetype"] = {"name": args.type if args.type else "Improvement"}

    try:
        new_issue = asf_jira.create_issue(fields=issue_dict)
        print(new_issue.key)
    except Exception as e:
        fail("Failed to create JIRA issue: %s" % e)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(-1)
