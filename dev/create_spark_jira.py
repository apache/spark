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
import sys
import traceback

from spark_jira_utils import create_jira_issue, get_jira_client, list_components


def main():
    parser = argparse.ArgumentParser(description="Create a Spark JIRA issue.")
    parser.add_argument("title", nargs="?", help="Title of the JIRA issue")
    parser.add_argument(
        "-p",
        "--parent",
        help="Parent JIRA ID to create a subtask (e.g. SPARK-12345).",
    )
    parser.add_argument(
        "-t",
        "--type",
        help="Issue type (e.g. Bug, Improvement)",
    )
    parser.add_argument("-c", "--component", help="Component for the issue")
    parser.add_argument(
        "--list-components", action="store_true", help="List available components and exit"
    )
    args = parser.parse_args()

    if args.list_components:
        asf_jira = get_jira_client()
        list_components(asf_jira)
        return

    if not args.title:
        parser.error("the following arguments are required: title")

    if not args.component:
        parser.error("-c/--component is required")

    if args.parent and args.type:
        parser.error("--parent and --type cannot be used together")

    if not args.parent and not args.type:
        parser.error("-t/--type is required when not creating a subtask")

    asf_jira = get_jira_client()
    jira_id = create_jira_issue(
        asf_jira, args.title, args.component, parent=args.parent, issue_type=args.type
    )
    print(jira_id)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(-1)
