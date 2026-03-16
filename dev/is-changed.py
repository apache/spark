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

import warnings
import traceback
import os
import sys
from argparse import ArgumentParser
from sparktestsupport.utils import (
    determine_modules_for_files,
    determine_modules_to_test,
    identify_changed_files_from_git_commits,
)
import sparktestsupport.modules as modules


def parse_opts():
    parser = ArgumentParser(prog="is-changed")

    parser.add_argument(
        "-f", "--fail", action="store_true", help="Exit with 1 if there is no relevant change."
    )

    default_value = ",".join(sorted([m.name for m in modules.all_modules]))
    parser.add_argument(
        "-m",
        "--modules",
        type=str,
        default=default_value,
        help="A comma-separated list of modules to test " "(default: %s)" % default_value,
    )

    args, unknown = parser.parse_known_args()
    if unknown:
        parser.error("Unsupported arguments: %s" % " ".join(unknown))
    return args


def main():
    opts = parse_opts()

    test_modules = opts.modules.split(",")
    changed_files = []
    if os.environ.get("APACHE_SPARK_REF"):
        changed_files = identify_changed_files_from_git_commits(
            "HEAD", target_ref=os.environ["APACHE_SPARK_REF"]
        )
    elif os.environ.get("GITHUB_PREV_SHA"):
        changed_files = identify_changed_files_from_git_commits(
            os.environ["GITHUB_SHA"], target_ref=os.environ["GITHUB_PREV_SHA"]
        )

    if any(f.endswith(".jar") for f in changed_files):
        with open(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), "test-jars.txt")
        ) as jarlist:
            itrsect = set((line.strip() for line in jarlist.readlines())).intersection(
                set(changed_files)
            )
            if len(itrsect) > 0:
                raise SystemExit(
                    f"Cannot include jars in source codes ({', '.join(itrsect)}). "
                    "If they have to be added temporarily, "
                    "please add the file name into dev/test-jars.txt."
                )

    if any(f.endswith(".class") for f in changed_files):
        with open(
            os.path.join(os.path.dirname(os.path.realpath(__file__)), "test-classes.txt")
        ) as clslist:
            itrsect = set((line.strip() for line in clslist.readlines())).intersection(
                set(changed_files)
            )
            if len(itrsect) > 0:
                raise SystemExit(
                    f"Cannot include class files in source codes ({', '.join(itrsect)}). "
                    "If they have to be added temporarily, "
                    "please add the file name into dev/test-classes.txt."
                )

    changed_modules = determine_modules_to_test(
        determine_modules_for_files(changed_files), deduplicated=False
    )
    module_names = [m.name for m in changed_modules]
    if len(changed_modules) == 0:
        print("false")
        if opts.fail:
            sys.exit(1)
    elif "root" in test_modules or modules.root in changed_modules:
        print("true")
    elif len(set(test_modules).intersection(module_names)) == 0:
        print("false")
        if opts.fail:
            sys.exit(1)
    else:
        print("true")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        warnings.warn(f"Ignored exception:\n\n{traceback.format_exc()}")
        print("true")
