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
# This script generates a list of contributors between two release tags.

import os
import sys

from github import Github

from releaseutils import (
    tag_exists,
    get_commits,
    yesOrNoPrompt,
    contributors_file_name,
    get_github_name,
)

# You must set the following before use!
RELEASE_TAG = os.environ.get("RELEASE_TAG", "v1.2.0-rc2")
PREVIOUS_RELEASE_TAG = os.environ.get("PREVIOUS_RELEASE_TAG", "v1.1.0")
GITHUB_OAUTH_KEY = os.environ.get("GITHUB_OAUTH_KEY")

# If the release tags are not provided, prompt the user to provide them
while not tag_exists(RELEASE_TAG):
    RELEASE_TAG = input("Please provide a valid release tag: ")
while not tag_exists(PREVIOUS_RELEASE_TAG):
    print("Please specify the previous release tag.")
    PREVIOUS_RELEASE_TAG = input(
        "For instance, if you are releasing v1.2.0, you should specify v1.1.0: "
    )

# Gather commits found in the new tag but not in the old tag.
# This filters commits based on both the git hash and the PR number.
# If either is present in the old tag, then we ignore the commit.
print("Gathering new commits between tags %s and %s" % (PREVIOUS_RELEASE_TAG, RELEASE_TAG))
release_commits = get_commits(RELEASE_TAG)
previous_release_commits = get_commits(PREVIOUS_RELEASE_TAG)
previous_release_hashes = set()
previous_release_prs = set()
for old_commit in previous_release_commits:
    previous_release_hashes.add(old_commit.get_hash())
    if old_commit.get_pr_number():
        previous_release_prs.add(old_commit.get_pr_number())
new_commits = []
for this_commit in release_commits:
    this_hash = this_commit.get_hash()
    this_pr_number = this_commit.get_pr_number()
    if this_hash in previous_release_hashes:
        continue
    if this_pr_number and this_pr_number in previous_release_prs:
        continue
    new_commits.append(this_commit)
if not new_commits:
    sys.exit("There are no new commits between %s and %s!" % (PREVIOUS_RELEASE_TAG, RELEASE_TAG))

# Prompt the user for confirmation that the commit range is correct
print("\n==================================================================================")
print("Release tag: %s" % RELEASE_TAG)
print("Previous release tag: %s" % PREVIOUS_RELEASE_TAG)
print("Number of commits in this range: %s" % len(new_commits))
print("")


def print_indented(_list):
    for x in _list:
        print("  %s" % x)


if yesOrNoPrompt("Show all commits?"):
    print_indented(new_commits)
print("==================================================================================\n")
if not yesOrNoPrompt("Does this look correct?"):
    sys.exit("Ok, exiting")

# Filter out special commits
releases = []
maintenance = []
reverts = []
filtered_commits = []


def is_release(commit_title):
    return (
        "[release]" in commit_title.lower()
        or "preparing spark release" in commit_title.lower()
        or "preparing development version" in commit_title.lower()
        or "CHANGES.txt" in commit_title
    )


def is_maintenance(commit_title):
    return "maintenance" in commit_title.lower() or "manually close" in commit_title.lower()


def is_revert(commit_title):
    return "revert" in commit_title.lower()


for c in new_commits:
    t = c.get_title()
    if not t:
        continue
    elif is_release(t):
        releases.append(c)
    elif is_maintenance(t):
        maintenance.append(c)
    elif is_revert(t):
        reverts.append(c)
    else:
        filtered_commits.append(c)

# Warn against ignored commits
if releases or maintenance or reverts:
    print("\n==================================================================================")
    if releases:
        print("Found %d release commits" % len(releases))
    if maintenance:
        print("Found %d maintenance commits" % len(maintenance))
    if reverts:
        print("Found %d revert commits" % len(reverts))
    print("* Warning: these commits will be ignored.\n")
    if yesOrNoPrompt("Show ignored commits?"):
        if releases:
            print("Release (%d)" % len(releases))
            print_indented(releases)
        if maintenance:
            print("Maintenance (%d)" % len(maintenance))
            print_indented(maintenance)
        if reverts:
            print("Revert (%d)" % len(reverts))
            print_indented(reverts)
    print("==================== Warning: the above commits will be ignored ==================\n")
prompt_msg = "%d commits left to process after filtering. Ok to proceed?" % len(filtered_commits)
if not yesOrNoPrompt(prompt_msg):
    sys.exit("Ok, exiting.")

# Initialize GitHub client
github_client = Github(GITHUB_OAUTH_KEY) if GITHUB_OAUTH_KEY else Github()

# Extract unique GitHub usernames from commits
print("\n=========================== Compiling contributor list ===========================")
github_usernames = set()
for commit in filtered_commits:
    # Get GitHub username from commit body (parsed by get_commits)
    github_username = commit.get_github_username()
    if github_username:
        github_usernames.add(github_username)
        print("  Processed commit %s by @%s" % (commit.get_hash(), github_username))
    else:
        print("  Skipping commit %s (no GitHub username found)" % commit.get_hash())

print("==================================================================================\n")
print("Found %d unique contributors" % len(github_usernames))

# For each GitHub username, get the full name from GitHub profile
contributors = []
print("\n=========================== Fetching GitHub profiles ===========================")
for username in sorted(github_usernames):
    full_name = get_github_name(username, github_client)
    if full_name:
        contributor = "%s (%s)" % (username, full_name)
    else:
        contributor = username
    contributors.append(contributor)
    print("  %s" % contributor)
print("==================================================================================\n")

# Write to contributors file
contributors_file = open(contributors_file_name, "w")
for contributor in sorted(contributors, key=str.lower):
    contributors_file.write(contributor + "\n")
contributors_file.close()
print("Contributors list is successfully written to %s!" % contributors_file_name)
