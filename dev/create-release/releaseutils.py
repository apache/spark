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
# This file contains helper methods used in creating a release.

import re
import sys
from subprocess import Popen, PIPE

try:
    from github import Github  # noqa: F401
    from github import GithubException
except ImportError:
    print("This tool requires the PyGithub library")
    print("Install using 'pip install PyGithub'")
    sys.exit(-1)


# Contributors list file name
contributors_file_name = "contributors.txt"


# Prompt the user to answer yes or no until they do so
def yesOrNoPrompt(msg):
    response = input("%s [y/n]: " % msg)
    while response != "y" and response != "n":
        return yesOrNoPrompt(msg)
    return response == "y"


# Utility functions run git commands (written with Git 1.8.5)
def run_cmd(cmd):
    return Popen(cmd, stdout=PIPE).communicate()[0].decode("utf8")


def run_cmd_error(cmd):
    return Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[1].decode("utf8")


def tag_exists(tag):
    stderr = run_cmd_error(["git", "show", tag])
    return "error" not in stderr


# A type-safe representation of a commit
class Commit:
    def __init__(self, _hash, github_username, title, pr_number=None):
        self._hash = _hash
        self.github_username = github_username
        self.title = title
        self.pr_number = pr_number

    def get_hash(self):
        return self._hash

    def get_github_username(self):
        return self.github_username

    def get_title(self):
        return self.title

    def get_pr_number(self):
        return self.pr_number

    def __str__(self):
        closes_pr = "(Closes #%s)" % self.pr_number if self.pr_number else ""
        return "%s @%s %s %s" % (self._hash, self.github_username, self.title, closes_pr)


# Return all commits that belong to the specified tag.
#
# Under the hood, this runs a `git log` on that tag and parses the fields
# from the command output to construct a list of Commit objects. Note that
# because certain fields reside in the commit description and cannot be parsed
# through the GitHub API itself, we need to do some intelligent regex parsing
# to extract those fields.
#
# This is written using Git 1.8.5.
def get_commits(tag):
    commit_start_marker = "|=== COMMIT START MARKER ===|"
    commit_end_marker = "|=== COMMIT END MARKER ===|"
    field_end_marker = "|=== COMMIT FIELD END MARKER ===|"
    log_format = commit_start_marker + "%h" + field_end_marker + "%s" + commit_end_marker + "%b"
    output = run_cmd(["git", "log", "--quiet", "--pretty=format:" + log_format, tag])
    commits = []
    raw_commits = [c for c in output.split(commit_start_marker) if c]
    for commit in raw_commits:
        if commit.count(commit_end_marker) != 1:
            print("Commit end marker not found in commit: ")
            for line in commit.split("\n"):
                print(line)
            sys.exit(1)
        # Separate commit digest from the body
        # From the digest we extract the hash and the title
        # From the body, we extract the PR number and the github username
        [commit_digest, commit_body] = commit.split(commit_end_marker)
        if commit_digest.count(field_end_marker) != 1:
            sys.exit("Unexpected format in commit: %s" % commit_digest)
        [_hash, title] = commit_digest.split(field_end_marker)
        # The PR number and github username is in the commit message
        # itself and cannot be accessed through any GitHub API
        pr_number = None
        github_username = None
        match = re.search("Closes #([0-9]+) from ([^/\\s]+)/", commit_body)
        if match:
            [pr_number, github_username] = match.groups()
        commit = Commit(_hash, github_username, title, pr_number)
        commits.append(commit)
    return commits


# Return the full name of the specified user on GitHub
# If the user doesn't exist, return None
def get_github_name(author, github_client):
    if github_client:
        try:
            return github_client.get_user(author).name
        except GithubException as e:
            # If this is not a "not found" exception
            if e.status != 404:
                raise e
    return None
