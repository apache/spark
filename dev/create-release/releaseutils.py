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
# This file contains helper methods used in creating a release.

import re
import sys
from subprocess import Popen, PIPE

try:
    from jira.client import JIRA
    from jira.exceptions import JIRAError
except ImportError:
    print "This tool requires the jira-python library"
    print "Install using 'sudo pip install jira-python'"
    sys.exit(-1)

try:
    from github import Github
    from github import GithubException
except ImportError:
    print "This tool requires the PyGithub library"
    print "Install using 'sudo pip install PyGithub'"
    sys.exit(-1)

try:
    import unidecode
except ImportError:
    print "This tool requires the unidecode library to decode obscure github usernames"
    print "Install using 'sudo pip install unidecode'"
    sys.exit(-1)

# Contributors list file name
contributors_file_name = "contributors.txt"

# Prompt the user to answer yes or no until they do so
def yesOrNoPrompt(msg):
    response = raw_input("%s [y/n]: " % msg)
    while response != "y" and response != "n":
        return yesOrNoPrompt(msg)
    return response == "y"

# Utility functions run git commands (written with Git 1.8.5)
def run_cmd(cmd): return Popen(cmd, stdout=PIPE).communicate()[0]
def run_cmd_error(cmd): return Popen(cmd, stdout=PIPE, stderr=PIPE).communicate()[1]
def get_date(commit_hash):
    return run_cmd(["git", "show", "--quiet", "--pretty=format:%cd", commit_hash])
def tag_exists(tag):
    stderr = run_cmd_error(["git", "show", tag])
    return "error" not in stderr

# A type-safe representation of a commit
class Commit:
    def __init__(self, _hash, author, title, pr_number = None):
        self._hash = _hash
        self.author = author
        self.title = title
        self.pr_number = pr_number
    def get_hash(self): return self._hash
    def get_author(self): return self.author
    def get_title(self): return self.title
    def get_pr_number(self): return self.pr_number
    def __str__(self):
        closes_pr = "(Closes #%s)" % self.pr_number if self.pr_number else ""
        return "%s %s %s %s" % (self._hash, self.author, self.title, closes_pr)

# Return all commits that belong to the specified tag.
#
# Under the hood, this runs a `git log` on that tag and parses the fields
# from the command output to construct a list of Commit objects. Note that
# because certain fields reside in the commit description and cannot be parsed
# through the Github API itself, we need to do some intelligent regex parsing
# to extract those fields.
#
# This is written using Git 1.8.5.
def get_commits(tag):
    commit_start_marker = "|=== COMMIT START MARKER ===|"
    commit_end_marker = "|=== COMMIT END MARKER ===|"
    field_end_marker = "|=== COMMIT FIELD END MARKER ===|"
    log_format =\
        commit_start_marker + "%h" +\
        field_end_marker + "%an" +\
        field_end_marker + "%s" +\
        commit_end_marker + "%b"
    output = run_cmd(["git", "log", "--quiet", "--pretty=format:" + log_format, tag])
    commits = []
    raw_commits = [c for c in output.split(commit_start_marker) if c]
    for commit in raw_commits:
        if commit.count(commit_end_marker) != 1:
            print "Commit end marker not found in commit: "
            for line in commit.split("\n"): print line
            sys.exit(1)
        # Separate commit digest from the body
        # From the digest we extract the hash, author and the title
        # From the body, we extract the PR number and the github username
        [commit_digest, commit_body] = commit.split(commit_end_marker)
        if commit_digest.count(field_end_marker) != 2:
            sys.exit("Unexpected format in commit: %s" % commit_digest)
        [_hash, author, title] = commit_digest.split(field_end_marker)
        # The PR number and github username is in the commit message
        # itself and cannot be accessed through any Github API
        pr_number = None
        match = re.search("Closes #([0-9]+) from ([^/\\s]+)/", commit_body)
        if match:
            [pr_number, github_username] = match.groups()
            # If the author name is not valid, use the github
            # username so we can translate it properly later
            if not is_valid_author(author):
                author = github_username
        # Guard against special characters
        author = unidecode.unidecode(unicode(author, "UTF-8")).strip()
        commit = Commit(_hash, author, title, pr_number)
        commits.append(commit)
    return commits

# Maintain a mapping for translating issue types to contributions in the release notes
# This serves an additional function of warning the user against unknown issue types
# Note: This list is partially derived from this link:
# https://issues.apache.org/jira/plugins/servlet/project-config/SPARK/issuetypes
# Keep these in lower case
known_issue_types = {
    "bug": "bug fixes",
    "build": "build fixes",
    "dependency upgrade": "build fixes",
    "improvement": "improvements",
    "new feature": "new features",
    "documentation": "documentation",
    "test": "test",
    "task": "improvement",
    "sub-task": "improvement"
}

# Maintain a mapping for translating component names when creating the release notes
# This serves an additional function of warning the user against unknown components
# Note: This list is largely derived from this link:
# https://issues.apache.org/jira/plugins/servlet/project-config/SPARK/components
CORE_COMPONENT = "Core"
known_components = {
    "block manager": CORE_COMPONENT,
    "build": CORE_COMPONENT,
    "deploy": CORE_COMPONENT,
    "documentation": CORE_COMPONENT,
    "ec2": "EC2",
    "examples": CORE_COMPONENT,
    "graphx": "GraphX",
    "input/output": CORE_COMPONENT,
    "java api": "Java API",
    "mesos": "Mesos",
    "ml": "MLlib",
    "mllib": "MLlib",
    "project infra": "Project Infra",
    "pyspark": "PySpark",
    "shuffle": "Shuffle",
    "spark core": CORE_COMPONENT,
    "spark shell": CORE_COMPONENT,
    "sql": "SQL",
    "streaming": "Streaming",
    "web ui": "Web UI",
    "windows": "Windows",
    "yarn": "YARN"
}

# Translate issue types using a format appropriate for writing contributions
# If an unknown issue type is encountered, warn the user
def translate_issue_type(issue_type, issue_id, warnings):
    issue_type = issue_type.lower()
    if issue_type in known_issue_types:
        return known_issue_types[issue_type]
    else:
        warnings.append("Unknown issue type \"%s\" (see %s)" % (issue_type, issue_id))
        return issue_type

# Translate component names using a format appropriate for writing contributions
# If an unknown component is encountered, warn the user
def translate_component(component, commit_hash, warnings):
    component = component.lower()
    if component in known_components:
        return known_components[component]
    else:
        warnings.append("Unknown component \"%s\" (see %s)" % (component, commit_hash))
        return component

# Parse components in the commit message
# The returned components are already filtered and translated
def find_components(commit, commit_hash):
    components = re.findall("\[\w*\]", commit.lower())
    components = [translate_component(c, commit_hash)\
        for c in components if c in known_components]
    return components

# Join a list of strings in a human-readable manner
# e.g. ["Juice"] -> "Juice"
# e.g. ["Juice", "baby"] -> "Juice and baby"
# e.g. ["Juice", "baby", "moon"] -> "Juice, baby, and moon"
def nice_join(str_list):
    str_list = list(str_list) # sometimes it's a set
    if not str_list:
        return ""
    elif len(str_list) == 1:
        return next(iter(str_list))
    elif len(str_list) == 2:
        return " and ".join(str_list)
    else:
        return ", ".join(str_list[:-1]) + ", and " + str_list[-1]

# Return the full name of the specified user on Github
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

# Return the full name of the specified user on JIRA
# If the user doesn't exist, return None
def get_jira_name(author, jira_client):
    if jira_client:
        try:
            return jira_client.user(author).displayName
        except JIRAError as e:
            # If this is not a "not found" exception
            if e.status_code != 404:
                raise e
    return None

# Return whether the given name is in the form <First Name><space><Last Name>
def is_valid_author(author):
    if not author: return False
    return " " in author and not re.findall("[0-9]", author)

# Capitalize the first letter of each word in the given author name
def capitalize_author(author):
    if not author: return None
    words = author.split(" ")
    words = [w[0].capitalize() + w[1:] for w in words if w]
    return " ".join(words)

