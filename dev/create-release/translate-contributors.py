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

# This script translates invalid authors in the contributors list generated
# by generate-contributors.py. When the script encounters an author name that
# is considered invalid, it searches Github and JIRA in an attempt to search
# for replacements. This tool runs in two modes:
#
# (1) Interactive mode: For each invalid author name, this script presents
# all candidate replacements to the user and awaits user response. In this
# mode, the user may also input a custom name. This is the default.
#
# (2) Non-interactive mode: For each invalid author name, this script replaces
# the name with the first valid candidate it can find. If there is none, it
# uses the original name. This can be enabled through the --non-interactive flag.

import os
import sys

from releaseutils import *

# You must set the following before use!
JIRA_API_BASE = os.environ.get("JIRA_API_BASE", "https://issues.apache.org/jira")
JIRA_USERNAME = os.environ.get("JIRA_USERNAME", None)
JIRA_PASSWORD = os.environ.get("JIRA_PASSWORD", None)
GITHUB_API_TOKEN = os.environ.get("GITHUB_API_TOKEN", None)
if not JIRA_USERNAME or not JIRA_PASSWORD:
    sys.exit("Both JIRA_USERNAME and JIRA_PASSWORD must be set")
if not GITHUB_API_TOKEN:
    sys.exit("GITHUB_API_TOKEN must be set")

# Write new contributors list to <old_file_name>.final
if not os.path.isfile(contributors_file_name):
    print("Contributors file %s does not exist!" % contributors_file_name)
    print("Have you run ./generate-contributors.py yet?")
    sys.exit(1)
contributors_file = open(contributors_file_name, "r")
warnings = []

# In non-interactive mode, this script will choose the first replacement that is valid
INTERACTIVE_MODE = True
if len(sys.argv) > 1:
    options = set(sys.argv[1:])
    if "--non-interactive" in options:
        INTERACTIVE_MODE = False
if INTERACTIVE_MODE:
    print("Running in interactive mode. To disable this, provide the --non-interactive flag.")

# Setup Github and JIRA clients
jira_options = {"server": JIRA_API_BASE}
jira_client = JIRA(options=jira_options, basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))
github_client = Github(GITHUB_API_TOKEN)

# Load known author translations that are cached locally
known_translations = {}
known_translations_file_name = "known_translations"
known_translations_file = open(known_translations_file_name, "r")
for line in known_translations_file:
    if line.startswith("#"):
        continue
    [old_name, new_name] = line.strip("\n").split(" - ")
    known_translations[old_name] = new_name
known_translations_file.close()

# Open again in case the user adds new mappings
known_translations_file = open(known_translations_file_name, "a")

# Generate candidates for the given author. This should only be called if the given author
# name does not represent a full name as this operation is somewhat expensive. Under the
# hood, it makes several calls to the Github and JIRA API servers to find the candidates.
#
# This returns a list of (candidate name, source) 2-tuples. E.g.
# [
#   (NOT_FOUND, "No full name found for Github user andrewor14"),
#   ("Andrew Or", "Full name of JIRA user andrewor14"),
#   ("Andrew Orso", "Full name of SPARK-1444 assignee andrewor14"),
#   ("Andrew Ordall", "Full name of SPARK-1663 assignee andrewor14"),
#   (NOT_FOUND, "No assignee found for SPARK-1763")
# ]
NOT_FOUND = "Not found"


def generate_candidates(author, issues):
    candidates = []
    # First check for full name of Github user
    github_name = get_github_name(author, github_client)
    if github_name:
        candidates.append((github_name, "Full name of Github user %s" % author))
    else:
        candidates.append((NOT_FOUND, "No full name found for Github user %s" % author))
    # Then do the same for JIRA user
    jira_name = get_jira_name(author, jira_client)
    if jira_name:
        candidates.append((jira_name, "Full name of JIRA user %s" % author))
    else:
        candidates.append((NOT_FOUND, "No full name found for JIRA user %s" % author))
    # Then do the same for the assignee of each of the associated JIRAs
    # Note that a given issue may not have an assignee, or the assignee may not have a full name
    for issue in issues:
        try:
            jira_issue = jira_client.issue(issue)
        except JIRAError as e:
            # Do not exit just because an issue is not found!
            if e.status_code == 404:
                warnings.append("Issue %s not found!" % issue)
                continue
            raise e
        jira_assignee = jira_issue.fields.assignee
        if jira_assignee:
            user_name = jira_assignee.name
            display_name = jira_assignee.displayName
            if display_name:
                candidates.append(
                    (display_name, "Full name of %s assignee %s" % (issue, user_name)))
            else:
                candidates.append(
                    (NOT_FOUND, "No full name found for %s assignee %s" % (issue, user_name)))
        else:
            candidates.append((NOT_FOUND, "No assignee found for %s" % issue))
    # Guard against special characters in candidate names
    # Note that the candidate name may already be in unicode (JIRA returns this)
    for i, (candidate, source) in enumerate(candidates):
        try:
            candidate = unicode(candidate, "UTF-8")
        except TypeError:
            # already in unicode
            pass
        candidate = unidecode.unidecode(candidate).strip()
        candidates[i] = (candidate, source)
    return candidates

# Translate each invalid author by searching for possible candidates from Github and JIRA
# In interactive mode, this script presents the user with a list of choices and have the user
# select from this list. Additionally, the user may also choose to enter a custom name.
# In non-interactive mode, this script picks the first valid author name from the candidates
# If no such name exists, the original name is used (without the JIRA numbers).
print("\n========================== Translating contributor list ==========================")
lines = contributors_file.readlines()
contributions = []
for i, line in enumerate(lines):
    # It is possible that a line in the contributor file only has the github name, e.g. yhuai.
    # So, we need a strip() to remove the newline.
    temp_author = line.strip(" * ").split(" -- ")[0].strip()
    print("Processing author %s (%d/%d)" % (temp_author, i + 1, len(lines)))
    if not temp_author:
        error_msg = "    ERROR: Expected the following format \" * <author> -- <contributions>\"\n"
        error_msg += "    ERROR: Actual = %s" % line
        print(error_msg)
        warnings.append(error_msg)
        contributions.append(line)
        continue
    author = temp_author.split("/")[0]
    # Use the local copy of known translations where possible
    if author in known_translations:
        line = line.replace(temp_author, known_translations[author])
    elif not is_valid_author(author):
        new_author = author
        issues = temp_author.split("/")[1:]
        candidates = generate_candidates(author, issues)
        # Print out potential replacement candidates along with the sources, e.g.
        #   [X] No full name found for Github user andrewor14
        #   [X] No assignee found for SPARK-1763
        #   [0] Andrew Or - Full name of JIRA user andrewor14
        #   [1] Andrew Orso - Full name of SPARK-1444 assignee andrewor14
        #   [2] Andrew Ordall - Full name of SPARK-1663 assignee andrewor14
        #   [3] andrewor14 - Raw Github username
        #   [4] Custom
        candidate_names = []
        bad_prompts = []  # Prompts that can't actually be selected; print these first.
        good_prompts = []  # Prompts that contain valid choices
        for candidate, source in candidates:
            if candidate == NOT_FOUND:
                bad_prompts.append("    [X] %s" % source)
            else:
                index = len(candidate_names)
                candidate_names.append(candidate)
                good_prompts.append("    [%d] %s - %s" % (index, candidate, source))
        raw_index = len(candidate_names)
        custom_index = len(candidate_names) + 1
        for p in bad_prompts:
            print(p)
        if bad_prompts:
            print("    ---")
        for p in good_prompts:
            print(p)
        # In interactive mode, additionally provide "custom" option and await user response
        if INTERACTIVE_MODE:
            print("    [%d] %s - Raw Github username" % (raw_index, author))
            print("    [%d] Custom" % custom_index)
            response = raw_input("    Your choice: ")
            last_index = custom_index
            while not response.isdigit() or int(response) > last_index:
                response = raw_input("    Please enter an integer between 0 and %d: " % last_index)
            response = int(response)
            if response == custom_index:
                new_author = raw_input("    Please type a custom name for this author: ")
            elif response != raw_index:
                new_author = candidate_names[response]
        # In non-interactive mode, just pick the first candidate
        else:
            valid_candidate_names = [name for name, _ in candidates
                                     if is_valid_author(name) and name != NOT_FOUND]
            if valid_candidate_names:
                new_author = valid_candidate_names[0]
        # Finally, capitalize the author and replace the original one with it
        # If the final replacement is still invalid, log a warning
        if is_valid_author(new_author):
            new_author = capitalize_author(new_author)
        else:
            warnings.append(
                "Unable to find a valid name %s for author %s" % (author, temp_author))
        print("    * Replacing %s with %s" % (author, new_author))
        # If we are in interactive mode, prompt the user whether we want to remember this new
        # mapping
        if INTERACTIVE_MODE and \
            author not in known_translations and \
                yesOrNoPrompt(
                    "    Add mapping %s -> %s to known translations file?" % (author, new_author)):
            known_translations_file.write("%s - %s\n" % (author, new_author))
            known_translations_file.flush()
        line = line.replace(temp_author, author)
    contributions.append(line)
print("==================================================================================\n")
contributors_file.close()
known_translations_file.close()

# Sort the contributions before writing them to the new file.
# Additionally, check if there are any duplicate author rows.
# This could happen if the same user has both a valid full
# name (e.g. Andrew Or) and an invalid one (andrewor14).
# If so, warn the user about this at the end.
contributions.sort()
all_authors = set()
new_contributors_file_name = contributors_file_name + ".final"
new_contributors_file = open(new_contributors_file_name, "w")
for line in contributions:
    author = line.strip(" * ").split(" -- ")[0]
    if author in all_authors:
        warnings.append("Detected duplicate author name %s. Please merge these manually." % author)
    all_authors.add(author)
    new_contributors_file.write(line)
new_contributors_file.close()

print("Translated contributors list successfully written to %s!" % new_contributors_file_name)

# Log any warnings encountered in the process
if warnings:
    print("\n========== Warnings encountered while translating the contributor list ===========")
    for w in warnings:
        print(w)
    print("Please manually correct these in the final contributors list at %s." %
          new_contributors_file_name)
    print("==================================================================================\n")
