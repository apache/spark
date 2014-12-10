#!/usr/bin/env python

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
# This script automates the process of creating release notes.

import os
import re
import sys

from releaseutils import *

# You must set the following before use!
JIRA_API_BASE = os.environ.get("JIRA_API_BASE", "https://issues.apache.org/jira")
START_COMMIT = os.environ.get("START_COMMIT", "37b100")
END_COMMIT = os.environ.get("END_COMMIT", "3693ae")

# If commit range is not specified, prompt the user to provide it
if not START_COMMIT or not END_COMMIT:
    print "A commit range is required to proceed."
    if not START_COMMIT:
        START_COMMIT = raw_input("Please specify starting commit hash (inclusive): ")
    if not END_COMMIT:
        END_COMMIT = raw_input("Please specify ending commit hash (non-inclusive): ")

# Verify provided arguments
start_commit_line = get_one_line(START_COMMIT)
end_commit_line = get_one_line(END_COMMIT)
num_commits = num_commits_in_range(START_COMMIT, END_COMMIT)
if not start_commit_line: sys.exit("Start commit %s not found!" % START_COMMIT)
if not end_commit_line: sys.exit("End commit %s not found!" % END_COMMIT)
if num_commits == 0:
    sys.exit("There are no commits in the provided range [%s, %s)" % (START_COMMIT, END_COMMIT))
print "\n=================================================================================="
print "JIRA server: %s" % JIRA_API_BASE
print "Start commit (inclusive): %s" % start_commit_line
print "End commit (non-inclusive): %s" % end_commit_line
print "Number of commits in this range: %s" % num_commits
print
response = raw_input("Is this correct? [Y/n] ")
if response.lower() != "y" and response:
    sys.exit("Ok, exiting")
print "==================================================================================\n"

# Find all commits within this range
print "Gathering commits within range [%s..%s)" % (START_COMMIT, END_COMMIT)
commits = get_one_line_commits(START_COMMIT, END_COMMIT)
if not commits: sys.exit("Error: No commits found within this range!")
commits = commits.split("\n")

# Filter out special commits
releases = []
reverts = []
nojiras = []
filtered_commits = []
def is_release(commit):
    return re.findall("\[release\]", commit.lower()) or\
        "maven-release-plugin" in commit or "CHANGES.txt" in commit
def has_no_jira(commit):
    return not re.findall("SPARK-[0-9]+", commit.upper())
def is_revert(commit):
    return "revert" in commit.lower()
def is_docs(commit):
    return re.findall("docs*", commit.lower()) or "programming guide" in commit.lower()
for c in commits:
    if not c: continue
    elif is_release(c): releases.append(c)
    elif is_revert(c): reverts.append(c)
    elif is_docs(c): filtered_commits.append(c) # docs may not have JIRA numbers
    elif has_no_jira(c): nojiras.append(c)
    else: filtered_commits.append(c)

# Warn against ignored commits
def print_indented(_list):
    for x in _list: print "  %s" % x
if releases or reverts or nojiras:
    print "\n=================================================================================="
    if releases: print "Releases (%d)" % len(releases); print_indented(releases)
    if reverts: print "Reverts (%d)" % len(reverts); print_indented(reverts)
    if nojiras: print "No JIRA (%d)" % len(nojiras); print_indented(nojiras)
    print "==================== Warning: the above commits will be ignored ==================\n"
response = raw_input("%d commits left to process. Ok to proceed? [Y/n] " % len(filtered_commits))
if response.lower() != "y" and response:
    sys.exit("Ok, exiting.")

# Keep track of warnings to tell the user at the end
warnings = []

# Mapping from the invalid author name to its associated JIRA issues
# E.g. andrewor14 -> set("SPARK-2413", "SPARK-3551", "SPARK-3471")
invalid_authors = {}

# Populate a map that groups issues and components by author
# It takes the form: Author name -> { Contribution type -> Spark components }
# For instance,
# {
#   'Andrew Or': {
#     'bug fixes': ['windows', 'core', 'web ui'],
#     'improvements': ['core']
#   },
#   'Tathagata Das' : {
#     'bug fixes': ['streaming']
#     'new feature': ['streaming']
#   }
# }
#
author_info = {}
jira_options = { "server": JIRA_API_BASE }
jira_client = JIRA(options = jira_options)
print "\n=========================== Compiling contributor list ==========================="
for commit in filtered_commits:
    commit_hash = re.findall("^[a-z0-9]+", commit)[0]
    issues = re.findall("SPARK-[0-9]+", commit.upper())
    author = get_author(commit_hash)
    author = unidecode.unidecode(unicode(author, "UTF-8")).strip() # guard against special characters
    # If the author name is invalid, keep track of it along
    # with all associated issues so we can translate it later
    if is_valid_author(author):
        author = capitalize_author(author)
    else:
        if author not in invalid_authors:
            invalid_authors[author] = set()
        for issue in issues:
            invalid_authors[author].add(issue)
    date = get_date(commit_hash)
    # Parse components from the commit message, if any
    commit_components = find_components(commit, commit_hash)
    # Populate or merge an issue into author_info[author]
    def populate(issue_type, components):
        components = components or [CORE_COMPONENT] # assume core if no components provided
        if author not in author_info:
            author_info[author] = {}
        if issue_type not in author_info[author]:
            author_info[author][issue_type] = set()
        for component in components:
            author_info[author][issue_type].add(component)
    # Find issues and components associated with this commit
    for issue in issues:
        jira_issue = jira_client.issue(issue)
        jira_type = jira_issue.fields.issuetype.name
        jira_type = translate_issue_type(jira_type, issue, warnings)
        jira_components = [translate_component(c.name, commit_hash, warnings)\
            for c in jira_issue.fields.components]
        all_components = set(jira_components + commit_components)
        populate(jira_type, all_components)
    # For docs without an associated JIRA, manually add it ourselves
    if is_docs(commit) and not issues:
        populate("documentation", commit_components)
    print "  Processed commit %s authored by %s on %s" % (commit_hash, author, date)
print "==================================================================================\n"

# Write to contributors file ordered by author names
# Each line takes the format "Author name - semi-colon delimited contributions"
# e.g. Andrew Or - Bug fixes in Windows, Core, and Web UI; improvements in Core
# e.g. Tathagata Das - Bug fixes and new features in Streaming
contributors_file = open(contributors_file_name, "w")
authors = author_info.keys()
authors.sort()
for author in authors:
    contribution = ""
    components = set()
    issue_types = set()
    for issue_type, comps in author_info[author].items():
        components.update(comps)
        issue_types.add(issue_type)
    # If there is only one component, mention it only once
    # e.g. Bug fixes, improvements in MLlib
    if len(components) == 1:
        contribution = "%s in %s" % (nice_join(issue_types), next(iter(components)))
    # Otherwise, group contributions by issue types instead of modules
    # e.g. Bug fixes in MLlib, Core, and Streaming; documentation in YARN
    else:
        contributions = ["%s in %s" % (issue_type, nice_join(comps)) \
            for issue_type, comps in author_info[author].items()]
        contribution = "; ".join(contributions)
    # Do not use python's capitalize() on the whole string to preserve case
    assert contribution
    contribution = contribution[0].capitalize() + contribution[1:]
    # If the author name is invalid, use an intermediate format that
    # can be translated through translate-contributors.py later
    # E.g. andrewor14/SPARK-3425/SPARK-1157/SPARK-6672
    if author in invalid_authors and invalid_authors[author]:
        author = author + "/" + "/".join(invalid_authors[author])
    line = "%s - %s" % (author, contribution)
    contributors_file.write(line + "\n")
contributors_file.close()
print "Contributors list is successfully written to %s!" % contributors_file_name

# Prompt the user to translate author names if necessary
if invalid_authors:
    warnings.append("Found the following invalid authors:")
    for a in invalid_authors:
        warnings.append("\t%s" % a)
    warnings.append("Please run './translate-contributors.py' to translate them.")

# Log any warnings encountered in the process
if warnings:
    print "\n============ Warnings encountered while creating the contributor list ============"
    for w in warnings: print w
    print "Please correct these in the final contributors list at %s." % contributors_file_name
    print "==================================================================================\n"

