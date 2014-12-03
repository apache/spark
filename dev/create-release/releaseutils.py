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

# Utility functions run git commands (written with Git 1.8.5)
def run_cmd(cmd): return Popen(cmd, stdout=PIPE).communicate()[0]
def get_author(commit_hash):
    return run_cmd(["git", "show", "--quiet", "--pretty=format:%an", commit_hash])
def get_date(commit_hash):
    return run_cmd(["git", "show", "--quiet", "--pretty=format:%cd", commit_hash])
def get_one_line(commit_hash):
    return run_cmd(["git", "show", "--quiet", "--pretty=format:\"%h %cd %s\"", commit_hash])
def get_one_line_commits(start_hash, end_hash):
    return run_cmd(["git", "log", "--oneline", "%s..%s" % (start_hash, end_hash)])
def num_commits_in_range(start_hash, end_hash):
    output = run_cmd(["git", "log", "--oneline", "%s..%s" % (start_hash, end_hash)])
    lines = [line for line in output.split("\n") if line] # filter out empty lines
    return len(lines)

# Maintain a mapping for translating issue types to contributions in the release notes
# This serves an additional function of warning the user against unknown issue types
# Note: This list is partially derived from this link:
# https://issues.apache.org/jira/plugins/servlet/project-config/SPARK/issuetypes
# Keep these in lower case
known_issue_types = {
    "bug": "bug fixes",
    "build": "build fixes",
    "improvement": "improvements",
    "new feature": "new features",
    "documentation": "documentation"
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
    author_words = len(author.split(" "))
    return author_words == 2 or author_words == 3

# Capitalize the first letter of each word in the given author name
def capitalize_author(author):
    if not author: return None
    words = author.split(" ")
    words = [w[0].capitalize() + w[1:] for w in words if w]
    return " ".join(words)

# Maintain a mapping of translated author names as a cache
translated_authors = {}

# Format the given author in a format appropriate for the contributors list.
# If the author is not an actual name, search github and JIRA for potential
# replacements and log all candidates as a warning.
def translate_author(github_author, github_client, jira_client, warnings):
    if is_valid_author(github_author):
        return capitalize_author(github_author)
    # If the translated author is already cached, just return it
    if github_author in translated_authors:
        return translated_authors[github_author]
    # Otherwise, author name is not found, so we need to search for an alternative name
    candidates = set()
    github_name = get_github_name(github_author, github_client)
    jira_name = get_jira_name(github_author, jira_client)
    if is_valid_author(github_name): github_name = capitalize_author(github_name)
    if is_valid_author(jira_name): jira_name = capitalize_author(jira_name)
    if github_name: candidates.add(github_name)
    if jira_name: candidates.add(jira_name)
    # Only use the github name as a replacement automatically
    # The JIRA name may not make sense because it can belong to someone else
    if is_valid_author(github_name):
        candidates_message = " (another candidate is %s)" % jira_name if jira_name else ""
        warnings.append("Replacing github user %s with %s%s" % (github_author, github_name, candidates_message))
        translated_authors[github_name] = github_name
        return translated_authors[github_name]
    # No direct replacement, so return the original author and list any candidates found
    candidates_message = " (candidates: %s)" % nice_join(candidates) if candidates else ""
    warnings.append("Unable to find a replacement for github user %s%s" % (github_author, candidates_message))
    translated_authors[github_author] = github_author
    return translated_authors[github_author]

