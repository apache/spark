#!/usr/bin/python

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
# Creates CHANGES.txt from git history.
#
# Usage:
#   First set the new release version and old CHANGES.txt version in this file.
#   Make sure you have SPARK_HOME set.
#   $  python generate-changelist.py


import os
import sys
import subprocess
import time
import traceback

SPARK_HOME = os.environ["SPARK_HOME"]
NEW_RELEASE_VERSION = "1.1.0"
PREV_RELEASE_GIT_TAG = "v1.0.0"

CHANGELIST = "CHANGES.txt"
OLD_CHANGELIST = "%s.old" % (CHANGELIST)
NEW_CHANGELIST = "%s.new" % (CHANGELIST)
TMP_CHANGELIST = "%s.tmp" % (CHANGELIST)

# date before first PR in TLP Spark repo
SPARK_REPO_CHANGE_DATE1 = time.strptime("2014-02-26", "%Y-%m-%d")
# date after last PR in incubator Spark repo
SPARK_REPO_CHANGE_DATE2 = time.strptime("2014-03-01", "%Y-%m-%d")
# Threshold PR number that differentiates PRs to TLP
# and incubator repos
SPARK_REPO_PR_NUM_THRESH = 200

LOG_FILE_NAME = "changes_%s" % time.strftime("%h_%m_%Y_%I_%M_%S")
LOG_FILE = open(LOG_FILE_NAME, 'w')


def run_cmd(cmd):
    try:
        print >> LOG_FILE, "Running command: %s" % cmd
        output = subprocess.check_output(cmd, shell=True, stderr=LOG_FILE)
        print >> LOG_FILE, "Output: %s" % output
        return output
    except:
        traceback.print_exc()
        cleanup()
        sys.exit(1)


def append_to_changelist(string):
    with open(TMP_CHANGELIST, "a") as f:
        print >> f, string


def cleanup(ask=True):
    if ask is True:
        print "OK to delete temporary and log files? (y/N): "
        response = raw_input()
    if ask is False or (ask is True and response == "y"):
        if os.path.isfile(TMP_CHANGELIST):
            os.remove(TMP_CHANGELIST)
        if os.path.isfile(OLD_CHANGELIST):
            os.remove(OLD_CHANGELIST)
        LOG_FILE.close()
        os.remove(LOG_FILE_NAME)


print "Generating new %s for Spark release %s" % (CHANGELIST, NEW_RELEASE_VERSION)
os.chdir(SPARK_HOME)
if os.path.isfile(TMP_CHANGELIST):
    os.remove(TMP_CHANGELIST)
if os.path.isfile(OLD_CHANGELIST):
    os.remove(OLD_CHANGELIST)

append_to_changelist("Spark Change Log")
append_to_changelist("----------------")
append_to_changelist("")
append_to_changelist("Release %s" % NEW_RELEASE_VERSION)
append_to_changelist("")

print "Getting commits between tag %s and HEAD" % PREV_RELEASE_GIT_TAG
hashes = run_cmd("git log %s..HEAD --pretty='%%h'" % PREV_RELEASE_GIT_TAG).split()

print "Getting details of %s commits" % len(hashes)
for h in hashes:
    date = run_cmd("git log %s -1 --pretty='%%ad' --date=iso | head -1" % h).strip()
    subject = run_cmd("git log %s -1 --pretty='%%s' | head -1" % h).strip()
    body = run_cmd("git log %s -1 --pretty='%%b'" % h)
    committer = run_cmd("git log %s -1 --pretty='%%cn <%%ce>' | head -1" % h).strip()
    body_lines = body.split("\n")

    if "Merge pull" in subject:
        # Parse old format commit message
        append_to_changelist("  %s %s" % (h, date))
        append_to_changelist("  %s" % subject)
        append_to_changelist("  [%s]" % body_lines[0])
        append_to_changelist("")

    elif "maven-release" not in subject:
        # Parse new format commit message
        # Get authors from commit message, committer otherwise
        authors = [committer]
        if "Author:" in body:
            authors = [line.split(":")[1].strip() for line in body_lines if "Author:" in line]

        # Generate GitHub PR URL for easy access if possible
        github_url = ""
        if "Closes #" in body:
            pr_num = [line.split()[1].lstrip("#") for line in body_lines if "Closes #" in line][0]
            github_url = "github.com/apache/spark/pull/%s" % pr_num
            day = time.strptime(date.split()[0], "%Y-%m-%d")
            if (day < SPARK_REPO_CHANGE_DATE1 or
                (day < SPARK_REPO_CHANGE_DATE2 and pr_num < SPARK_REPO_PR_NUM_THRESH)):
                github_url = "github.com/apache/incubator-spark/pull/%s" % pr_num

        append_to_changelist("  %s" % subject)
        append_to_changelist("  %s" % ', '.join(authors))
        # for author in authors:
        #     append_to_changelist("  %s" % author)
        append_to_changelist("  %s" % date)
        if len(github_url) > 0:
            append_to_changelist("  Commit: %s, %s" % (h, github_url))
        else:
            append_to_changelist("  Commit: %s" % h)
        append_to_changelist("")

# Append old change list
print "Appending changelist from tag %s" % PREV_RELEASE_GIT_TAG
run_cmd("git show %s:%s | tail -n +3 >> %s" % (PREV_RELEASE_GIT_TAG, CHANGELIST, TMP_CHANGELIST))
run_cmd("cp %s %s" % (TMP_CHANGELIST, NEW_CHANGELIST))
print "New change list generated as %s" % NEW_CHANGELIST
cleanup(False)
