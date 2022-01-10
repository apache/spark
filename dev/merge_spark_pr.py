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

# Utility for creating well-formed pull request merges and pushing them to Apache
# Spark.
#   usage: ./merge_spark_pr.py    (see config env vars below)
#
# This utility assumes you already have a local Spark git folder and that you
# have added remotes corresponding to both (i) the github apache Spark
# mirror and (ii) the apache git repo.

import json
import os
import re
import subprocess
import sys
import traceback
from urllib.request import urlopen
from urllib.request import Request
from urllib.error import HTTPError

try:
    import jira.client

    JIRA_IMPORTED = True
except ImportError:
    JIRA_IMPORTED = False

# Location of your Spark git development area
SPARK_HOME = os.environ.get("SPARK_HOME", os.getcwd())
# Remote name which points to the Gihub site
PR_REMOTE_NAME = os.environ.get("PR_REMOTE_NAME", "apache-github")
# Remote name which points to Apache git
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "apache")
# ASF JIRA username
JIRA_USERNAME = os.environ.get("JIRA_USERNAME", "")
# ASF JIRA password
JIRA_PASSWORD = os.environ.get("JIRA_PASSWORD", "")
# OAuth key used for issuing requests against the GitHub API. If this is not defined, then requests
# will be unauthenticated. You should only need to configure this if you find yourself regularly
# exceeding your IP's unauthenticated request rate limit. You can create an OAuth key at
# https://github.com/settings/tokens. This script only requires the "public_repo" scope.
GITHUB_OAUTH_KEY = os.environ.get("GITHUB_OAUTH_KEY")


GITHUB_BASE = "https://github.com/apache/spark/pull"
GITHUB_API_BASE = "https://api.github.com/repos/apache/spark"
JIRA_BASE = "https://issues.apache.org/jira/browse"
JIRA_API_BASE = "https://issues.apache.org/jira"
# Prefix added to temporary branches
BRANCH_PREFIX = "PR_TOOL"


def get_json(url):
    try:
        request = Request(url)
        if GITHUB_OAUTH_KEY:
            request.add_header("Authorization", "token %s" % GITHUB_OAUTH_KEY)
        return json.load(urlopen(request))
    except HTTPError as e:
        if "X-RateLimit-Remaining" in e.headers and e.headers["X-RateLimit-Remaining"] == "0":
            print(
                "Exceeded the GitHub API rate limit; see the instructions in "
                + "dev/merge_spark_pr.py to configure an OAuth token for making authenticated "
                + "GitHub requests."
            )
        else:
            print("Unable to fetch URL, exiting: %s" % url)
        sys.exit(-1)


def fail(msg):
    print(msg)
    clean_up()
    sys.exit(-1)


def run_cmd(cmd):
    print(cmd)
    if isinstance(cmd, list):
        return subprocess.check_output(cmd).decode("utf-8")
    else:
        return subprocess.check_output(cmd.split(" ")).decode("utf-8")


def continue_maybe(prompt):
    result = input("\n%s (y/n): " % prompt)
    if result.lower() != "y":
        fail("Okay, exiting")


def clean_up():
    if "original_head" in globals():
        print("Restoring head pointer to %s" % original_head)
        run_cmd("git checkout %s" % original_head)

        branches = run_cmd("git branch").replace(" ", "").split("\n")

        for branch in list(filter(lambda x: x.startswith(BRANCH_PREFIX), branches)):
            print("Deleting local branch %s" % branch)
            run_cmd("git branch -D %s" % branch)


# merge the requested PR and return the merge hash
def merge_pr(pr_num, target_ref, title, body, pr_repo_desc):
    pr_branch_name = "%s_MERGE_PR_%s" % (BRANCH_PREFIX, pr_num)
    target_branch_name = "%s_MERGE_PR_%s_%s" % (BRANCH_PREFIX, pr_num, target_ref.upper())
    run_cmd("git fetch %s pull/%s/head:%s" % (PR_REMOTE_NAME, pr_num, pr_branch_name))
    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, target_ref, target_branch_name))
    run_cmd("git checkout %s" % target_branch_name)

    had_conflicts = False
    try:
        run_cmd(["git", "merge", pr_branch_name, "--squash"])
    except Exception as e:
        msg = "Error merging: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and 'git add' conflicting files... Finished?"
        continue_maybe(msg)
        had_conflicts = True

    commit_authors = run_cmd(
        ["git", "log", "HEAD..%s" % pr_branch_name, "--pretty=format:%an <%ae>"]
    ).split("\n")
    distinct_authors = sorted(
        set(commit_authors), key=lambda x: commit_authors.count(x), reverse=True
    )
    primary_author = input(
        'Enter primary author in the format of "name <email>" [%s]: ' % distinct_authors[0]
    )
    if primary_author == "":
        primary_author = distinct_authors[0]
    else:
        # When primary author is specified manually, de-dup it from author list and
        # put it at the head of author list.
        distinct_authors = list(filter(lambda x: x != primary_author, distinct_authors))
        distinct_authors.insert(0, primary_author)

    merge_message_flags = []

    merge_message_flags += ["-m", title]
    if body is not None:
        # We remove @ symbols from the body to avoid triggering e-mails
        # to people every time someone creates a public fork of Spark.
        merge_message_flags += ["-m", body.replace("@", "")]

    committer_name = run_cmd("git config --get user.name").strip()
    committer_email = run_cmd("git config --get user.email").strip()

    if had_conflicts:
        message = "This patch had conflicts when merged, resolved by\nCommitter: %s <%s>" % (
            committer_name,
            committer_email,
        )
        merge_message_flags += ["-m", message]

    # The string "Closes #%s" string is required for GitHub to correctly close the PR
    merge_message_flags += ["-m", "Closes #%s from %s." % (pr_num, pr_repo_desc)]

    authors = "Authored-by:" if len(distinct_authors) == 1 else "Lead-authored-by:"
    authors += " %s" % (distinct_authors.pop(0))
    if len(distinct_authors) > 0:
        authors += "\n" + "\n".join(["Co-authored-by: %s" % a for a in distinct_authors])
    authors += "\n" + "Signed-off-by: %s <%s>" % (committer_name, committer_email)

    merge_message_flags += ["-m", authors]

    run_cmd(["git", "commit", '--author="%s"' % primary_author] + merge_message_flags)

    continue_maybe(
        "Merge complete (local ref %s). Push to %s?" % (target_branch_name, PUSH_REMOTE_NAME)
    )

    try:
        run_cmd("git push %s %s:%s" % (PUSH_REMOTE_NAME, target_branch_name, target_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    merge_hash = run_cmd("git rev-parse %s" % target_branch_name)[:8]
    clean_up()
    print("Pull request #%s merged!" % pr_num)
    print("Merge hash: %s" % merge_hash)
    return merge_hash


def cherry_pick(pr_num, merge_hash, default_branch):
    pick_ref = input("Enter a branch name [%s]: " % default_branch)
    if pick_ref == "":
        pick_ref = default_branch

    pick_branch_name = "%s_PICK_PR_%s_%s" % (BRANCH_PREFIX, pr_num, pick_ref.upper())

    run_cmd("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, pick_ref, pick_branch_name))
    run_cmd("git checkout %s" % pick_branch_name)

    try:
        run_cmd("git cherry-pick -sx %s" % merge_hash)
    except Exception as e:
        msg = "Error cherry-picking: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and finish the cherry-pick. Finished?"
        continue_maybe(msg)

    continue_maybe(
        "Pick complete (local ref %s). Push to %s?" % (pick_branch_name, PUSH_REMOTE_NAME)
    )

    try:
        run_cmd("git push %s %s:%s" % (PUSH_REMOTE_NAME, pick_branch_name, pick_ref))
    except Exception as e:
        clean_up()
        fail("Exception while pushing: %s" % e)

    pick_hash = run_cmd("git rev-parse %s" % pick_branch_name)[:8]
    clean_up()

    print("Pull request #%s picked into %s!" % (pr_num, pick_ref))
    print("Pick hash: %s" % pick_hash)
    return pick_ref


def fix_version_from_branch(branch, versions):
    # Note: Assumes this is a sorted (newest->oldest) list of un-released versions
    if branch == "master":
        return versions[0]
    else:
        branch_ver = branch.replace("branch-", "")
        return list(filter(lambda x: x.name.startswith(branch_ver), versions))[-1]


def resolve_jira_issue(merge_branches, comment, default_jira_id=""):
    asf_jira = jira.client.JIRA(
        {"server": JIRA_API_BASE}, basic_auth=(JIRA_USERNAME, JIRA_PASSWORD)
    )

    jira_id = input("Enter a JIRA id [%s]: " % default_jira_id)
    if jira_id == "":
        jira_id = default_jira_id

    try:
        issue = asf_jira.issue(jira_id)
    except Exception as e:
        fail("ASF JIRA could not find %s\n%s" % (jira_id, e))

    cur_status = issue.fields.status.name
    cur_summary = issue.fields.summary
    cur_assignee = issue.fields.assignee
    if cur_assignee is None:
        cur_assignee = choose_jira_assignee(issue, asf_jira)
    # Check again, we might not have chosen an assignee
    if cur_assignee is None:
        cur_assignee = "NOT ASSIGNED!!!"
    else:
        cur_assignee = cur_assignee.displayName

    if cur_status == "Resolved" or cur_status == "Closed":
        fail("JIRA issue %s already has status '%s'" % (jira_id, cur_status))
    print("=== JIRA %s ===" % jira_id)
    print(
        "summary\t\t%s\nassignee\t%s\nstatus\t\t%s\nurl\t\t%s/%s\n"
        % (cur_summary, cur_assignee, cur_status, JIRA_BASE, jira_id)
    )

    versions = asf_jira.project_versions("SPARK")
    versions = sorted(versions, key=lambda x: x.name, reverse=True)
    versions = list(filter(lambda x: x.raw["released"] is False, versions))
    # Consider only x.y.z versions
    versions = list(filter(lambda x: re.match(r"\d+\.\d+\.\d+", x.name), versions))

    default_fix_versions = list(
        map(lambda x: fix_version_from_branch(x, versions).name, merge_branches)
    )
    for v in default_fix_versions:
        # Handles the case where we have forked a release branch but not yet made the release.
        # In this case, if the PR is committed to the master branch and the release branch, we
        # only consider the release branch to be the fix version. E.g. it is not valid to have
        # both 1.1.0 and 1.0.0 as fix versions.
        (major, minor, patch) = v.split(".")
        if patch == "0":
            previous = "%s.%s.%s" % (major, int(minor) - 1, 0)
            if previous in default_fix_versions:
                default_fix_versions = list(filter(lambda x: x != v, default_fix_versions))
    default_fix_versions = ",".join(default_fix_versions)

    available_versions = set(list(map(lambda v: v.name, versions)))
    while True:
        try:
            fix_versions = input(
                "Enter comma-separated fix version(s) [%s]: " % default_fix_versions
            )
            if fix_versions == "":
                fix_versions = default_fix_versions
            fix_versions = fix_versions.replace(" ", "").split(",")
            if set(fix_versions).issubset(available_versions):
                break
            else:
                print(
                    "Specified version(s) [%s] not found in the available versions, try "
                    "again (or leave blank and fix manually)." % (", ".join(fix_versions))
                )
        except KeyboardInterrupt:
            raise
        except BaseException:
            traceback.print_exc()
            print("Error setting fix version(s), try again (or leave blank and fix manually)")

    def get_version_json(version_str):
        return list(filter(lambda v: v.name == version_str, versions))[0].raw

    jira_fix_versions = list(map(lambda v: get_version_json(v), fix_versions))

    resolve = list(filter(lambda a: a["name"] == "Resolve Issue", asf_jira.transitions(jira_id)))[0]
    resolution = list(filter(lambda r: r.raw["name"] == "Fixed", asf_jira.resolutions()))[0]
    asf_jira.transition_issue(
        jira_id,
        resolve["id"],
        fixVersions=jira_fix_versions,
        comment=comment,
        resolution={"id": resolution.raw["id"]},
    )

    print("Successfully resolved %s with fixVersions=%s!" % (jira_id, fix_versions))


def choose_jira_assignee(issue, asf_jira):
    """
    Prompt the user to choose who to assign the issue to in jira, given a list of candidates,
    including the original reporter and all commentors
    """
    while True:
        try:
            reporter = issue.fields.reporter
            commentors = list(map(lambda x: x.author, issue.fields.comment.comments))
            candidates = set(commentors)
            candidates.add(reporter)
            candidates = list(candidates)
            print("JIRA is unassigned, choose assignee")
            for idx, author in enumerate(candidates):
                if author.key == "apachespark":
                    continue
                annotations = ["Reporter"] if author == reporter else []
                if author in commentors:
                    annotations.append("Commentor")
                print("[%d] %s (%s)" % (idx, author.displayName, ",".join(annotations)))
            raw_assignee = input(
                "Enter number of user, or userid, to assign to (blank to leave unassigned):"
            )
            if raw_assignee == "":
                return None
            else:
                try:
                    id = int(raw_assignee)
                    assignee = candidates[id]
                except BaseException:
                    # assume it's a user id, and try to assign (might fail, we just prompt again)
                    assignee = asf_jira.user(raw_assignee)
                asf_jira.assign_issue(issue.key, assignee.name)
                return assignee
        except KeyboardInterrupt:
            raise
        except BaseException:
            traceback.print_exc()
            print("Error assigning JIRA, try again (or leave blank and fix manually)")


def resolve_jira_issues(title, merge_branches, comment):
    jira_ids = re.findall("SPARK-[0-9]{4,5}", title)

    if len(jira_ids) == 0:
        resolve_jira_issue(merge_branches, comment)
    for jira_id in jira_ids:
        resolve_jira_issue(merge_branches, comment, jira_id)


def standardize_jira_ref(text):
    """
    Standardize the [SPARK-XXXXX] [MODULE] prefix
    Converts "[SPARK-XXX][mllib] Issue", "[MLLib] SPARK-XXX. Issue" or "SPARK XXX [MLLIB]: Issue" to
    "[SPARK-XXX][MLLIB] Issue"

    >>> standardize_jira_ref(
    ...     "[SPARK-5821] [SQL] ParquetRelation2 CTAS should check if delete is successful")
    '[SPARK-5821][SQL] ParquetRelation2 CTAS should check if delete is successful'
    >>> standardize_jira_ref(
    ...     "[SPARK-4123][Project Infra][WIP]: Show new dependencies added in pull requests")
    '[SPARK-4123][PROJECT INFRA][WIP] Show new dependencies added in pull requests'
    >>> standardize_jira_ref("[MLlib] Spark  5954: Top by key")
    '[SPARK-5954][MLLIB] Top by key'
    >>> standardize_jira_ref("[SPARK-979] a LRU scheduler for load balancing in TaskSchedulerImpl")
    '[SPARK-979] a LRU scheduler for load balancing in TaskSchedulerImpl'
    >>> standardize_jira_ref(
    ...     "SPARK-1094 Support MiMa for reporting binary compatibility across versions.")
    '[SPARK-1094] Support MiMa for reporting binary compatibility across versions.'
    >>> standardize_jira_ref("[WIP]  [SPARK-1146] Vagrant support for Spark")
    '[SPARK-1146][WIP] Vagrant support for Spark'
    >>> standardize_jira_ref(
    ...     "SPARK-1032. If Yarn app fails before registering, app master stays aroun...")
    '[SPARK-1032] If Yarn app fails before registering, app master stays aroun...'
    >>> standardize_jira_ref(
    ...     "[SPARK-6250][SPARK-6146][SPARK-5911][SQL] Types are now reserved words in DDL parser.")
    '[SPARK-6250][SPARK-6146][SPARK-5911][SQL] Types are now reserved words in DDL parser.'
    >>> standardize_jira_ref("Additional information for users building from source code")
    'Additional information for users building from source code'
    """
    jira_refs = []
    components = []

    # If the string is compliant, no need to process any further
    if re.search(r"^\[SPARK-[0-9]{3,6}\](\[[A-Z0-9_\s,]+\] )+\S+", text):
        return text

    # Extract JIRA ref(s):
    pattern = re.compile(r"(SPARK[-\s]*[0-9]{3,6})+", re.IGNORECASE)
    for ref in pattern.findall(text):
        # Add brackets, replace spaces with a dash, & convert to uppercase
        jira_refs.append("[" + re.sub(r"\s+", "-", ref.upper()) + "]")
        text = text.replace(ref, "")

    # Extract spark component(s):
    # Look for alphanumeric chars, spaces, dashes, periods, and/or commas
    pattern = re.compile(r"(\[[\w\s,.-]+\])", re.IGNORECASE)
    for component in pattern.findall(text):
        components.append(component.upper())
        text = text.replace(component, "")

    # Cleanup any remaining symbols:
    pattern = re.compile(r"^\W+(.*)", re.IGNORECASE)
    if pattern.search(text) is not None:
        text = pattern.search(text).groups()[0]

    # Assemble full text (JIRA ref(s), module(s), remaining text)
    clean_text = "".join(jira_refs).strip() + "".join(components).strip() + " " + text.strip()

    # Replace multiple spaces with a single space, e.g. if no jira refs and/or components were
    # included
    clean_text = re.sub(r"\s+", " ", clean_text.strip())

    return clean_text


def get_current_ref():
    ref = run_cmd("git rev-parse --abbrev-ref HEAD").strip()
    if ref == "HEAD":
        # The current ref is a detached HEAD, so grab its SHA.
        return run_cmd("git rev-parse HEAD").strip()
    else:
        return ref


def main():
    global original_head

    os.chdir(SPARK_HOME)
    original_head = get_current_ref()

    # Check this up front to avoid failing the JIRA update at the very end
    if not JIRA_USERNAME or not JIRA_PASSWORD:
        continue_maybe("The env-vars JIRA_USERNAME and/or JIRA_PASSWORD are not set. Continue?")

    branches = get_json("%s/branches" % GITHUB_API_BASE)
    branch_names = list(filter(lambda x: x.startswith("branch-"), [x["name"] for x in branches]))
    # Assumes branch names can be sorted lexicographically
    latest_branch = sorted(branch_names, reverse=True)[0]

    pr_num = input("Which pull request would you like to merge? (e.g. 34): ")
    pr = get_json("%s/pulls/%s" % (GITHUB_API_BASE, pr_num))
    pr_events = get_json("%s/issues/%s/events" % (GITHUB_API_BASE, pr_num))

    url = pr["url"]

    # Warn if the PR is WIP
    if "[WIP]" in pr["title"]:
        msg = "The PR title has `[WIP]`:\n%s\nContinue?" % pr["title"]
        continue_maybe(msg)

    # Decide whether to use the modified title or not
    modified_title = standardize_jira_ref(pr["title"]).rstrip(".")
    if modified_title != pr["title"]:
        print("I've re-written the title as follows to match the standard format:")
        print("Original: %s" % pr["title"])
        print("Modified: %s" % modified_title)
        result = input("Would you like to use the modified title? (y/n): ")
        if result.lower() == "y":
            title = modified_title
            print("Using modified title:")
        else:
            title = pr["title"]
            print("Using original title:")
        print(title)
    else:
        title = pr["title"]

    modified_body = re.sub(re.compile(r"<!--[^>]*-->\n?", re.DOTALL), "", pr["body"]).lstrip()
    if modified_body != pr["body"]:
        print("=" * 80)
        print(modified_body)
        print("=" * 80)
        print("I've removed the comments from PR template like the above:")
        result = input("Would you like to use the modified body? (y/n): ")
        if result.lower() == "y":
            body = modified_body
            print("Using modified body:")
        else:
            body = pr["body"]
            print("Using original body:")
        print("=" * 80)
        print(body)
        print("=" * 80)
    else:
        body = pr["body"]
    target_ref = pr["base"]["ref"]
    user_login = pr["user"]["login"]
    base_ref = pr["head"]["ref"]
    pr_repo_desc = "%s/%s" % (user_login, base_ref)

    # Merged pull requests don't appear as merged in the GitHub API;
    # Instead, they're closed by asfgit.
    merge_commits = [
        e for e in pr_events if e["actor"]["login"] == "asfgit" and e["event"] == "closed"
    ]

    if merge_commits:
        merge_hash = merge_commits[0]["commit_id"]
        message = get_json("%s/commits/%s" % (GITHUB_API_BASE, merge_hash))["commit"]["message"]

        print("Pull request %s has already been merged, assuming you want to backport" % pr_num)
        commit_is_downloaded = (
            run_cmd(["git", "rev-parse", "--quiet", "--verify", "%s^{commit}" % merge_hash]).strip()
            != ""
        )
        if not commit_is_downloaded:
            fail("Couldn't find any merge commit for #%s, you may need to update HEAD." % pr_num)

        print("Found commit %s:\n%s" % (merge_hash, message))
        cherry_pick(pr_num, merge_hash, latest_branch)
        sys.exit(0)

    if not bool(pr["mergeable"]):
        msg = (
            "Pull request %s is not mergeable in its current form.\n" % pr_num
            + "Continue? (experts only!)"
        )
        continue_maybe(msg)

    print("\n=== Pull Request #%s ===" % pr_num)
    print("title\t%s\nsource\t%s\ntarget\t%s\nurl\t%s" % (title, pr_repo_desc, target_ref, url))
    continue_maybe("Proceed with merging pull request #%s?" % pr_num)

    merged_refs = [target_ref]

    merge_hash = merge_pr(pr_num, target_ref, title, body, pr_repo_desc)

    pick_prompt = "Would you like to pick %s into another branch?" % merge_hash
    while input("\n%s (y/n): " % pick_prompt).lower() == "y":
        merged_refs = merged_refs + [cherry_pick(pr_num, merge_hash, latest_branch)]

    if JIRA_IMPORTED:
        if JIRA_USERNAME and JIRA_PASSWORD:
            continue_maybe("Would you like to update an associated JIRA?")
            jira_comment = "Issue resolved by pull request %s\n[%s/%s]" % (
                pr_num,
                GITHUB_BASE,
                pr_num,
            )
            resolve_jira_issues(title, merged_refs, jira_comment)
        else:
            print("JIRA_USERNAME and JIRA_PASSWORD not set")
            print("Exiting without trying to close the associated JIRA.")
    else:
        print("Could not find jira-python library. Run 'sudo pip3 install jira' to install.")
        print("Exiting without trying to close the associated JIRA.")


if __name__ == "__main__":
    import doctest

    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        sys.exit(-1)
    try:
        main()
    except BaseException:
        clean_up()
        raise
