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
# Rolling integration branches branch-M.x (e.g. branch-4.x; later branch-5.x after 5.0.0)
# absorb work for the next minors on major M. Maintenance branches branch-M.N are cut from
# branch-M.x. Cherry-pick prompts sort by semver via semver_branch_rank (branch-M.x before branch-M.N).
#
# Fix Version defaults when resolving JIRA aim for the minimal set of versions that tells users
# which Spark releases will contain the commit, leveraging the Upstream-First backporting policy
# (cherry-picks flow master -> branch-M.x -> branch-M.N). master contributes the greatest
# unreleased N.0.0, but is omitted when any branch-M.x is in the merge set (a cherry-pick to
# branch-M.x has already landed on master). Each branch-M.x contributes that major's greatest
# unreleased minor.0, but is omitted when a sibling branch-M.N contributes M.N.0 (M.N.0 still
# unreleased means branch-M.N is still a subset of branch-M.x). Patch merges on branch-M.N use
# unreleased versions starting with M.N.
# This utility assumes you already have a local Spark git folder and that you
# have added remotes corresponding to both (i) the github apache Spark
# mirror and (ii) the apache git repo.

import argparse
import json
import os
import re
import subprocess
import sys
import traceback
from typing import Iterator, List, Optional
from urllib.error import HTTPError
from urllib.request import Request, urlopen

# Shared with dev/pr_merge_status.py so the two committer tools agree on where a PR landed.
# Importable because Python puts this script's own directory first on sys.path.
from spark_merge_footer import branches_with_merge_footer as _branches_with_merge_footer
from spark_merge_footer import has_merge_footer

try:
    import jira.client

    JIRA_IMPORTED = True
except ImportError:
    JIRA_IMPORTED = False

# Location of your Spark git development area
SPARK_HOME = os.environ.get("SPARK_HOME", os.getcwd())
# Remote name which points to the Github site
PR_REMOTE_NAME = os.environ.get("PR_REMOTE_NAME", "apache-github")
# Remote name which points to Apache git
PUSH_REMOTE_NAME = os.environ.get("PUSH_REMOTE_NAME", "apache")
# ASF JIRA username
JIRA_USERNAME = os.environ.get("JIRA_USERNAME", "")
# ASF JIRA password
JIRA_PASSWORD = os.environ.get("JIRA_PASSWORD", "")
# ASF JIRA access token
# If it is configured, username and password are dismissed
# Go to https://issues.apache.org/jira/secure/ViewProfile.jspa -> Personal Access Tokens for
# your own token management.
JIRA_ACCESS_TOKEN = os.environ.get("JIRA_ACCESS_TOKEN")
# Connection timeout (in seconds) for the JIRA client. Raise this if the default is too
# short for your network, e.g. when the TLS handshake goes through a slow proxy.
JIRA_CONNECT_TIMEOUT = float(os.environ.get("JIRA_CONNECT_TIMEOUT", "3.05"))
# Read timeout (in seconds) for a single JIRA request. Doubles as the budget for the startup
# liveness probe, so a server that is down or too slow to answer disables JIRA for this run
# rather than failing or stalling the merge.
JIRA_READ_TIMEOUT = float(os.environ.get("JIRA_READ_TIMEOUT", "30"))
# OAuth key used for issuing requests against the GitHub API. If this is not defined, then requests
# will be unauthenticated. You should only need to configure this if you find yourself regularly
# exceeding your IP's unauthenticated request rate limit. You can create an OAuth key at
# https://github.com/settings/tokens. This script only requires the "public_repo" scope.
GITHUB_OAUTH_KEY = os.environ.get("GITHUB_OAUTH_KEY")
# Setting the DRY_RUN env var to any non-empty value makes the --dry-run/-n flag default to on
# (see build_arg_parser); the flag is the primary interface and can also turn it on explicitly.
# Consistent with the SKIP_VERSION_CHECK env toggle above. In a dry run every read-only step and
# local git op still runs -- fetching the PR, JIRA lookup, project_versions, JIRA/GitHub token
# validation, and the local squash-merge and cherry-picks on the throwaway PR_TOOL_* branches so
# conflicts and the computed merge hash stay realistic -- but every outbound effect is routed
# through a DryRun* client (see Git/GitHub/Jira below) that logs a "DRY-RUN: would ..." line
# instead of executing it: the git push to PUSH_REMOTE_NAME, the GitHub PR close/comment, and all
# JIRA writes (component and fixVersion updates, assignment, and resolve/reopen transitions).
DRY_RUN_ENV = bool(os.environ.get("DRY_RUN"))


GITHUB_BASE = "https://github.com/apache/spark/pull"
GITHUB_COMMIT_BASE = "https://github.com/apache/spark/commit"
GITHUB_API_BASE = "https://api.github.com/repos/apache/spark"
JIRA_BASE = "https://issues.apache.org/jira/browse"
JIRA_API_BASE = "https://issues.apache.org/jira"
# Prefix added to temporary branches
BRANCH_PREFIX = "PR_TOOL"
# Set to a truthy value to skip the check that compares this script against apache/spark master.
# Intended for committers iterating on the merge script itself, where a local diff is expected.
SKIP_VERSION_CHECK = os.environ.get("SKIP_VERSION_CHECK", "")
# Path of this script relative to the repo root, used to fetch the canonical copy from master.
MERGE_SCRIPT_REPO_PATH = "dev/merge_spark_pr.py"


def semver_branch_rank(name):
    """
    Sort key for maintenance branches: higher merges / cherry-picks first.

    Goals:
      - Numeric Spark branches branch-M.N sort by (M, minor), not lexicographically
        (branch-4.11 is newer than branch-4.2).
      - Rolling integration branches branch-M.x rank before branch-M.N for the same M so they are
        suggested first among targets on major M.

    >>> semver_branch_rank("branch-5.x") > semver_branch_rank("branch-5.0") > semver_branch_rank("branch-4.11")
    True
    >>> semver_branch_rank("branch-5.x") > semver_branch_rank("branch-5.2")
    True
    >>> semver_branch_rank("branch-4.x") > semver_branch_rank("branch-4.2")
    True
    >>> semver_branch_rank("branch-4.2") > semver_branch_rank("branch-4.11")
    False
    """
    integration = re.match(r"^branch-(\d+)\.x$", name)
    if integration:
        return int(integration.group(1)), float("inf")
    matched = re.match(r"^branch-(\d+)\.(\d+)$", name)
    if matched:
        return int(matched.group(1)), int(matched.group(2))
    return (-1, -1)


def parse_version(name: str) -> Optional[tuple[int, int, int]]:
    """Parse a dotted ``x.y.z`` version name into an ``(int, int, int)`` tuple.

    Returns None for anything that is not a plain three-part numeric version, so the
    result can be compared directly and non-version names are skipped by callers.

    >>> parse_version("4.4.0")
    (4, 4, 0)
    >>> parse_version("4.10.2")
    (4, 10, 2)
    >>> parse_version("branch-4.x") is None
    True
    >>> parse_version("4.4") is None
    True
    """
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)$", name)
    return tuple(int(g) for g in m.groups()) if m else None


def _semver_max_version(names):
    """
    Highest dotted version by numeric semver (SPARK Fix Version naming).

    >>> _semver_max_version(["4.2.1", "4.3.0"])
    '4.3.0'
    >>> _semver_max_version(["5.0.0", "6.0.0"])
    '6.0.0'
    """
    parsed = [(parse_version(n), n) for n in names]
    parsed = [(t, n) for t, n in parsed if t is not None]
    if not parsed:
        return None
    return max(parsed)[1]


def _integration_major_from_branch(branch_name):
    """If branch-name is branch-M.x, return integer M; else None."""
    m = re.match(r"^branch-(\d+)\.x$", branch_name)
    return int(m.group(1)) if m else None


def _first_integration_major(merge_branches):
    """First branch-M.x on merge_branches in list order (deterministic).

    >>> _first_integration_major(["master", "branch-5.x", "branch-4.x"])
    5
    >>> _first_integration_major(["master", "branch-4.2"]) is None
    True
    """
    for ref in merge_branches:
        mj = _integration_major_from_branch(ref)
        if mj is not None:
            return mj
    return None


def _sort_version_names_lex_desc(names):
    """Same ordering as resolve_jira_issue (reverse lexicographic on Fix Version name)."""
    return sorted(names, reverse=True)


def compute_merge_default_fix_versions(merge_branches, unreleased_version_names):
    """
    Build suggested SPARK Fix Version names from merged git refs (no JIRA I/O).

    unreleased_version_names: x.y.z strings marked unreleased in JIRA.

    Returns (default_version_names, warning_messages).

    The output is the minimal set of Fix Versions that tells users which Spark releases will
    contain the commit, leveraging the Upstream-First backporting policy (cherry-picks flow
    master -> branch-M.x -> branch-M.N):
      - master contributes the greatest unreleased N.0.0;
      - branch-M.x contributes that major's greatest unreleased minor.0;
      - branch-M.N contributes its greatest unreleased M.N.patch.
    Redundant entries are then suppressed: master's N.0.0 is dropped when any branch-M.x is in
    the merge set (a cherry-pick to branch-M.x has already landed on master); branch-M.x's
    minor.0 is dropped when a branch-M.N for the same major contributes M.N.0 (M.N.0 still
    unreleased means branch-M.N is still a subset of branch-M.x).

    >>> compute_merge_default_fix_versions(["master"], ["4.3.0", "5.0.0", "6.0.0"])[0]
    ['6.0.0']

    >>> compute_merge_default_fix_versions(
    ...     ["master", "branch-4.x"], ["6.0.0", "5.0.0", "4.3.0", "4.2.1"])[0]
    ['4.3.0']

    >>> compute_merge_default_fix_versions(
    ...     ["master", "branch-5.x"],
    ...     ["7.0.0", "6.0.0", "5.2.0", "5.1.2", "5.1.1", "5.0.1"],
    ... )[0]
    ['5.2.0']

    >>> compute_merge_default_fix_versions(
    ...     ["master", "branch-3.5"],
    ...     ["6.0.0", "4.1.0", "3.5.2", "3.5.1", "3.5.0"],
    ... )[0]
    ['6.0.0', '3.5.2']

    >>> compute_merge_default_fix_versions(
    ...     ["master", "branch-1.1", "branch-1.0"],
    ...     ["2.0.0", "1.1.1", "1.1.0", "1.0.1", "1.0.0"],
    ... )[0]
    ['2.0.0', '1.1.1', '1.0.1']

    >>> d, w = compute_merge_default_fix_versions(["master"], ["4.3.0", "4.2.1"])
    >>> (d, len(w), "N.0.0" in w[0])
    ([], 1, True)

    >>> compute_merge_default_fix_versions(["branch-4.x"], ["4.3.0"])
    (['4.3.0'], [])

    >>> d, w = compute_merge_default_fix_versions(["branch-4.99"], ["4.3.0"])
    >>> d == [] and len(w) == 1 and "branch-4.99" in w[0]
    True

    >>> compute_merge_default_fix_versions(
    ...     ["master", "branch-5.x", "branch-4.x"], ["5.2.0", "4.3.0", "6.0.0"]
    ... )[0]
    ['5.2.0', '4.3.0']

    >>> compute_merge_default_fix_versions(
    ...     ["master", "branch-4.x", "branch-4.x"], ["6.0.0", "4.3.0"]
    ... )[0]
    ['4.3.0']

    >>> compute_merge_default_fix_versions(
    ...     ["master", "branch-4.x", "branch-4.2"], ["5.0.0", "4.3.0", "4.2.0"]
    ... )[0]
    ['4.2.0']

    >>> compute_merge_default_fix_versions(
    ...     ["master", "branch-4.x", "branch-4.2"], ["5.0.0", "4.3.0", "4.2.1"]
    ... )[0]
    ['4.3.0', '4.2.1']

    >>> compute_merge_default_fix_versions(
    ...     ["master", "branch-4.2"], ["5.0.0", "4.2.0"]
    ... )[0]
    ['5.0.0', '4.2.0']

    >>> compute_merge_default_fix_versions(
    ...     ["master", "branch-4.2"], ["5.0.0", "4.2.1"]
    ... )[0]
    ['5.0.0', '4.2.1']
    """
    names = _sort_version_names_lex_desc(list(unreleased_version_names))
    has_integration_branch = any(
        _integration_major_from_branch(b) is not None for b in merge_branches
    )

    # Collect each merge branch's candidate Fix Version. We track (branch, version) so we can
    # later suppress redundant entries (master, branch-M.x) based on which branches contributed.
    contributions = []
    warnings = []

    for b in merge_branches:
        if b == "master":
            majors = [n for n in names if re.match(r"^\d+\.0\.0$", n)]
            chosen = _semver_max_version(majors)
            if chosen:
                contributions.append((b, chosen))
            else:
                warnings.append(
                    "No unreleased N.0.0 Fix Version found in JIRA for master; "
                    "enter comma-separated Fix Version(s) manually when prompted."
                )
            continue
        line_major = _integration_major_from_branch(b)
        if line_major is not None:
            line_versions = [n for n in names if re.match(r"^%s\.\d+\.\d+$" % line_major, n)]
            chosen = _semver_max_version(line_versions)
            if chosen:
                contributions.append((b, chosen))
            else:
                warnings.append(
                    "Could not infer an unreleased Spark %s (minor.maintenance) fix version "
                    "for branch-%s.x merge; enter version(s) manually when prompted."
                    % (line_major, line_major)
                )
            continue
        prefix = b.replace("branch-", "") + "."
        found_versions = [n for n in names if n.startswith(prefix)]
        chosen = _semver_max_version(found_versions)
        if chosen:
            contributions.append((b, chosen))
        else:
            warnings.append(
                "Target version for %s is not found on JIRA, it may be archived or "
                "not created. Skipping it." % b
            )

    # Majors where a release branch's M.N.0 contribution makes the sibling branch-M.x's
    # minor.0 redundant: an unreleased M.N.0 means branch-M.N has not diverged from branch-M.x
    # yet, so anything that's on branch-M.N is also on branch-M.x.
    suppress_integration_majors = set()
    for b, v in contributions:
        if b == "master" or _integration_major_from_branch(b) is not None:
            continue
        m = re.match(r"^(\d+)\.\d+\.0$", v)
        if m:
            suppress_integration_majors.add(int(m.group(1)))

    def keep(item):
        b, _ = item
        if b == "master":
            return not has_integration_branch
        major = _integration_major_from_branch(b)
        if major is not None:
            return major not in suppress_integration_majors
        return True

    filtered = [item for item in contributions if keep(item)]
    return list(dict.fromkeys(v for _, v in filtered)), warnings


def compute_revert_fix_versions(merge_branches, unreleased_version_names, existing_version_names):
    """Suggest recorded Fix Versions to remove for the branches receiving a revert.

    Infer each branch separately so that redundant versions recorded by separate merge
    runs are included in the removal candidates.

    >>> versions = ["5.0.0", "4.3.0", "4.2.0", "3.5.2"]
    >>> compute_revert_fix_versions(
    ...     ["master", "branch-4.x", "branch-4.2"], versions, versions)
    (['5.0.0', '4.3.0', '4.2.0'], [])
    >>> compute_revert_fix_versions(["master"], versions, versions)
    (['5.0.0'], [])
    >>> compute_revert_fix_versions(["branch-4.2"], versions, ["4.3.0", "3.5.2"])
    ([], [])
    >>> compute_revert_fix_versions(["branch-4.x"], versions, ["4.3.0", "4.2.0"])
    (['4.3.0'], [])
    >>> compute_revert_fix_versions(
    ...     ["branch-4.2"], ["4.2.1", "4.20.0"], ["4.2.1", "4.20.0"])
    (['4.2.1'], [])
    """
    candidates = set()
    warnings = []
    for branch in merge_branches:
        versions, branch_warnings = compute_merge_default_fix_versions(
            [branch], unreleased_version_names
        )
        candidates.update(versions)
        warnings.extend(branch_warnings)
    return [v for v in existing_version_names if v in candidates], warnings


def additional_fix_versions(inferred_versions, existing_versions):
    """Return inferred Fix Versions not already present on a JIRA issue.

    Existing versions are preserved separately when the issue is updated, so this only
    identifies the additions needed after a later backport.

    >>> additional_fix_versions(["4.4.0"], ["5.0.0"])
    ['4.4.0']
    >>> additional_fix_versions(["4.4.0"], ["5.0.0", "4.4.0"])
    []
    >>> additional_fix_versions(["4.4.0", "4.3.4"], ["5.0.0", "4.4.0"])
    ['4.3.4']
    """
    existing = set(existing_versions)
    return [version for version in inferred_versions if version not in existing]


def fix_version_additions(inferred_versions, existing_versions):
    """Return (additions, all_inferred_present).

    An empty ``additions`` list is ambiguous on its own: either the issue already carries
    every inferred version, or nothing was inferred. Only the first means there is nothing
    to do; the second still needs the committer prompted.

    >>> fix_version_additions(["4.3.0"], ["5.0.0"])
    (['4.3.0'], False)
    >>> fix_version_additions(["5.0.0"], ["5.0.0"])
    ([], True)
    >>> fix_version_additions([], ["5.0.0"])
    ([], False)
    """
    additions = additional_fix_versions(inferred_versions, existing_versions)
    return additions, bool(inferred_versions) and not additions


def parse_version_list(raw: str) -> list[str]:
    """Split a comma-separated version string into trimmed, de-duplicated names.

    Surrounding whitespace on each entry is stripped and empty entries are dropped, so a
    blank or all-separator string yields no names. Input order is preserved. Shared by the
    Fix Version and Affects Version prompts so both parse committer input the same way.

    >>> parse_version_list("5.0.0, 4.3.0")
    ['5.0.0', '4.3.0']
    >>> parse_version_list(" 4.4.0 , , 4.4.0 ")
    ['4.4.0']
    >>> parse_version_list("   ")
    []
    """
    names = [segment.strip() for segment in raw.split(",")]
    return list(dict.fromkeys(n for n in names if n))


def fix_versions_from_input(raw_input, default_fix_versions):
    """Resolve the Fix Version prompt's raw input into a list of version names.

    Blank falls back to the inferred default. With no default to fall back on, blank means
    skip: the empty list, not [""], which no known version can match.

    >>> fix_versions_from_input("4.2.2", "")
    ['4.2.2']
    >>> fix_versions_from_input("", "5.0.0")
    ['5.0.0']
    >>> fix_versions_from_input("5.0.0, 4.3.0", "")
    ['5.0.0', '4.3.0']
    >>> fix_versions_from_input("", "")
    []
    >>> fix_versions_from_input("   ", "")
    []
    """
    if raw_input == "":
        raw_input = default_fix_versions
    return parse_version_list(raw_input)


def fix_precedes_affects(fix_version_names: list[str], affects_version_names: list[str]) -> bool:
    """Whether the earliest fix version is below the earliest recorded Affects Version.

    True means the affected floor sits above a fixed release, so the ticket omits a
    version the fix reaches -- either too-high on a fresh resolve (fixed 4.4.0, affects
    only 5.0.0) or a backport to an earlier line (affects 4.4.0, backport adds fix 4.3.x).
    Non-``x.y.z`` names and empty lists compare as absent, so they never trigger a prompt.

    >>> fix_precedes_affects(["4.4.0"], ["5.0.0"])
    True
    >>> fix_precedes_affects(["4.4.0"], ["4.3.0"])
    False
    >>> fix_precedes_affects(["4.4.0"], ["4.4.0"])
    False
    >>> fix_precedes_affects(["4.4.0", "4.3.1"], ["4.4.0"])
    True
    >>> fix_precedes_affects(["4.4.0"], ["4.10.0"])
    True
    >>> fix_precedes_affects([], ["5.0.0"])
    False
    """
    fix = [t for t in map(parse_version, fix_version_names) if t is not None]
    affects = [t for t in map(parse_version, affects_version_names) if t is not None]
    if not fix or not affects:
        return False
    return min(fix) < min(affects)


def suggest_affects_version(
    fix_version_names: list[str], unreleased_version_names: list[str]
) -> str:
    """Suggest the earliest fix version only if it is an unreleased major or minor release.

    An unreleased x.y.0 can identify affected development builds. A patch fix version
    does not identify the earlier release affected by the bug.

    >>> unreleased = ["5.0.0", "4.4.0", "4.2.1"]
    >>> suggest_affects_version(["4.4.0"], unreleased)
    '4.4.0'
    >>> suggest_affects_version(["5.0.0"], unreleased)
    '5.0.0'
    >>> suggest_affects_version(["5.0.0", "4.4.0"], unreleased)
    '4.4.0'
    >>> suggest_affects_version(["4.2.1"], unreleased)
    ''
    >>> suggest_affects_version(["4.4.0", "4.2.1"], unreleased)
    ''
    >>> suggest_affects_version(["4.3.0", "4.4.0"], unreleased)
    ''
    >>> suggest_affects_version(["4.10.0", "4.9.0"], ["4.10.0", "4.9.0"])
    '4.9.0'
    >>> suggest_affects_version([], unreleased)
    ''
    >>> suggest_affects_version(["unknown", "4.4.0"], unreleased)
    ''
    """
    versions = [(parse_version(name), name) for name in fix_version_names]
    if not versions or any(version is None for version, _ in versions):
        return ""
    version, name = min(versions)
    return name if version[2] == 0 and name in unreleased_version_names else ""


def red(text):
    return "\033[91m%s\033[0m" % text


def bold(text):
    return "\033[1m%s\033[0m" % text


def print_error(msg):
    print(red(msg))


def bold_input(prompt) -> str:
    return input("\033[1m%s\033[0m" % prompt)


def get_input(prompt, options, bold=True, ignore_case=True):
    """
    Get input from the user until a valid answer is provided.

    Args:
        prompt: The prompt to display to the user.
        options:
            * A dictionary of "option: accepted answer" to choose from.
            * A list of options.
            * A regex pattern.
        bold: Whether to use bold formatting for the prompt.
        ignore_case: Whether to ignore case when comparing the answer to the options.

    Returns:
        If options is provided - the valid answer from the user.
        If regex is provided
            * If no group is specified - the whole matched string.
            * If one group is specified - the first group.
            * If multiple groups are specified - a tuple of the groups.
    """

    # Normalize options
    if isinstance(options, (list, tuple)):
        options = {option: [option] for option in options}

    if isinstance(options, dict):
        for option, acceptable_answer in options.items():
            if ignore_case:
                options[option] = [answer.lower() for answer in acceptable_answer]

    while True:
        if bold:
            answer = bold_input(prompt)
        else:
            answer = input(prompt)

        answer = answer.strip()
        if ignore_case:
            answer = answer.lower()

        if isinstance(options, str):
            if (m := re.match(options, answer)) is not None:
                groups = m.groups()
                if len(groups) == 0:
                    return m.group(0)
                if len(groups) == 1:
                    return groups[0]
                else:
                    return groups
        else:
            for option, acceptable_answer in options.items():
                if answer in acceptable_answer:
                    return option


def get_json(url):
    try:
        request = Request(url)
        if GITHUB_OAUTH_KEY:
            request.add_header("Authorization", "token %s" % GITHUB_OAUTH_KEY)
        return json.load(urlopen(request))
    except HTTPError as e:
        if "X-RateLimit-Remaining" in e.headers and e.headers["X-RateLimit-Remaining"] == "0":
            print_error(
                "Exceeded the GitHub API rate limit; see the instructions in "
                + "dev/merge_spark_pr.py to configure an OAuth token for making authenticated "
                + "GitHub requests."
            )
        elif e.code == 401:
            print_error(
                "GITHUB_OAUTH_KEY is invalid or expired. Please regenerate a new one with "
                + "at least the 'public_repo' scope on https://github.com/settings/tokens and "
                + "update your local settings before you try again."
            )
        else:
            print_error("Unable to fetch URL, exiting: %s" % url)
        sys.exit(-1)


def merge_commit_candidates(pr_events):
    """Split `pr_events` into (closed_commits, referenced_commits), each oldest-first.

    Ordered by time so that a PR reopened and merged again yields its latest merge last.

    >>> merge_commit_candidates([{"event": "closed", "commit_id": "a", "created_at": "t2"},
    ...                          {"event": "referenced", "commit_id": "b", "created_at": "t1"}])
    (['a'], ['b'])
    >>> merge_commit_candidates([{"event": "closed", "commit_id": None, "created_at": "t1"}])
    ([], [])
    >>> merge_commit_candidates([{"event": "referenced", "commit_id": "c", "created_at": "t2"},
    ...                          {"event": "referenced", "commit_id": "b", "created_at": "t1"}])
    ([], ['b', 'c'])
    """

    def commits_of(event_name):
        matched = [e for e in pr_events if e["event"] == event_name and e["commit_id"] is not None]
        return [e["commit_id"] for e in sorted(matched, key=lambda x: x["created_at"])]

    return commits_of("closed"), commits_of("referenced")


def find_merge_commit(pr_num, pr_events):
    """Return (hash, message) of the commit that merged `pr_num`, or (None, None).

    GitHub attributes the merge commit to the `closed` event only when that commit lands
    on the default branch (master), because the "Closes #N" keyword in the commit message
    is what closes the PR and the keyword is honored only there. A PR merged into any
    other branch -- e.g. one opened against a rolling branch-M.x -- is instead closed by
    this script through the API, and that `closed` event carries no commit, so the merge
    survives only as a `referenced` event. Prefer the `closed` commit, which GitHub itself
    linked; otherwise fall back to `referenced` events, which are also raised by any commit
    merely mentioning the PR, so confirm each against the merge footer `merge_pr` generates.
    """

    def message_of(commit_hash):
        return get_json("%s/commits/%s" % (GITHUB_API_BASE, commit_hash))["commit"]["message"]

    closed_commits, referenced_commits = merge_commit_candidates(pr_events)
    if closed_commits:
        return closed_commits[-1], message_of(closed_commits[-1])

    for commit_hash in reversed(referenced_commits):
        message = message_of(commit_hash)
        if has_merge_footer(message, pr_num):
            return commit_hash, message
    return None, None


class GitHub:
    """GitHub REST writes used by the merge script -- the single seam for PR mutations.

    Reads go through the module-level get_json(); only the two mutations (closing the PR and
    posting a comment) live here, so DryRunGitHub can override them without touching any read
    path. main() constructs GitHub() normally and DryRunGitHub() for a dry run.
    """

    def close_pr(self, pr_num):
        url = "%s/pulls/%s" % (GITHUB_API_BASE, pr_num)
        data = json.dumps({"state": "closed"}).encode("utf-8")
        request = Request(url, data=data, method="PATCH")
        request.add_header("Content-Type", "application/json")
        request.add_header("Accept", "application/vnd.github+json")
        if GITHUB_OAUTH_KEY:
            request.add_header("Authorization", "token %s" % GITHUB_OAUTH_KEY)
        try:
            return json.load(urlopen(request))
        except HTTPError as e:
            print_error("Failed to close PR #%s: HTTP %s %s" % (pr_num, e.code, e.reason))
            return None

    def comment(self, pr_num, body):
        # Posting a comment is a write and needs auth; without a token, skip rather than 401.
        if not GITHUB_OAUTH_KEY:
            print_error("GITHUB_OAUTH_KEY is not set; skipping the comment.")
            return None
        url = "%s/issues/%s/comments" % (GITHUB_API_BASE, pr_num)
        data = json.dumps({"body": body}).encode("utf-8")
        request = Request(url, data=data, method="POST")
        request.add_header("Content-Type", "application/json")
        request.add_header("Accept", "application/vnd.github+json")
        request.add_header("Authorization", "token %s" % GITHUB_OAUTH_KEY)
        try:
            return json.load(urlopen(request))
        except HTTPError as e:
            print_error("Failed to comment on PR #%s: HTTP %s %s" % (pr_num, e.code, e.reason))
            return None


class DryRunGitHub(GitHub):
    """Logs the intended PR close/comment instead of calling the GitHub API."""

    def close_pr(self, pr_num):
        print("DRY-RUN: would close PR #%s via the GitHub API." % pr_num)
        return None

    def comment(self, pr_num, body):
        print("DRY-RUN: would post the following comment on PR #%s:\n%s" % (pr_num, body))
        return None


# Set in main(): GitHub() normally, DryRunGitHub() for a dry run. Module-level default so the
# top-level except handler and any early failure path have a usable client.
github = GitHub()


def post_merge_comment(pr_num, merged_commits):
    """Post a comment on the PR recording every branch the change landed on and a
    link to the resulting commit, so the merge is traceable from the PR page.

    ``merged_commits`` is an ordered list of (branch, commit_hash) pairs, the merge
    sink first followed by each cherry-pick target.
    """
    if not merged_commits:
        return
    lines = [
        "- merged into %s %s/%s" % (ref, GITHUB_COMMIT_BASE, commit_hash)
        for ref, commit_hash in merged_commits
    ]
    summary = "**Merge Summary:**\n" + "\n".join(lines)
    attribution = "*Posted by `merge_spark_pr.py`*"
    body = "%s\n\n%s" % (summary, attribution)
    print(
        "\n%s\n\n%s\n%s"
        % (bold("Posting merge comment on PR #%s:" % pr_num), bold(summary), attribution)
    )
    github.comment(pr_num, body)


def fail(msg):
    print_error(msg)
    clean_up()
    sys.exit(-1)


class Git:
    """Runs the merge-flow git commands for the merge script.

    This is the seam for the merge/backport flow, not every git invocation:
    check_script_up_to_date() shells out to git merge-base directly. ``run`` executes read-only
    and local commands (fetch, checkout, merge, commit, cherry-pick, rev-parse, config, branch
    bookkeeping); those are safe even in a dry run because the merge and cherry-picks happen on
    the throwaway PR_TOOL_* branches that clean_up removes. ``push`` is the only command that
    mutates the shared repo at PUSH_REMOTE_NAME, so it is the one method DryRunGit overrides;
    everything else it inherits and runs for real, keeping conflict detection and the computed
    merge hash realistic. main() constructs Git() normally and DryRunGit() for a dry run.
    """

    def run(self, cmd):
        print(cmd)
        if isinstance(cmd, list):
            return subprocess.check_output(cmd).decode("utf-8")
        else:
            return subprocess.check_output(cmd.split(" ")).decode("utf-8")

    def push(self, remote, local_ref, remote_ref):
        return self.run("git push %s %s:%s" % (remote, local_ref, remote_ref))


class DryRunGit(Git):
    """Logs the intended push instead of updating the remote; all other git runs for real."""

    def push(self, remote, local_ref, remote_ref):
        print("DRY-RUN: would push %s to %s:%s" % (local_ref, remote, remote_ref))
        return ""


# Set in main(): Git() normally, DryRunGit() for a dry run. Module-level default so clean_up and
# the top-level except handler have a usable client even if main() fails early.
git = Git()


class SkipCherryPick(Exception):
    """Signals that the committer declined to resolve a conflicting cherry-pick.

    A backport conflict is routine, and by the time one is hit the merge into the target
    branch (and any earlier cherry-picks) has already been pushed. Declining it must skip
    only that one branch -- letting the caller offer another branch and, crucially, still
    resolve the JIRA -- instead of aborting the whole merge the way a hard `fail()` would.
    """


def continue_maybe(prompt, cherry=False):
    if get_input(f"{prompt} (y/N): ", ["y", "n", ""]) != "y":
        if cherry:
            try:
                git.run("git cherry-pick --abort")
            except Exception:
                print_error("Unable to abort and get back to the state before cherry-pick")
            print("Skipping this cherry-pick; the merge continues.")
            clean_up()
            raise SkipCherryPick()
        fail("Okay, exiting")


def clean_up():
    if "original_head" in globals():
        print("Restoring head pointer to %s" % original_head)
        git.run("git checkout %s" % original_head)

        branches = git.run("git branch").replace(" ", "").split("\n")

        for branch in list(filter(lambda x: x.startswith(BRANCH_PREFIX), branches)):
            print("Deleting local branch %s" % branch)
            git.run("git branch -D %s" % branch)


# merge the requested PR and return the merge hash
def merge_pr(pr_num, target_ref, title, body, pr_repo_desc, pr_author, co_authors):
    pr_branch_name = "%s_MERGE_PR_%s" % (BRANCH_PREFIX, pr_num)
    target_branch_name = "%s_MERGE_PR_%s_%s" % (BRANCH_PREFIX, pr_num, target_ref.upper())
    git.run("git fetch %s pull/%s/head:%s" % (PR_REMOTE_NAME, pr_num, pr_branch_name))
    git.run("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, target_ref, target_branch_name))
    git.run("git checkout %s" % target_branch_name)

    had_conflicts = False
    try:
        git.run(["git", "merge", pr_branch_name, "--squash"])
    except Exception as e:
        msg = "Error merging: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg)
        msg = "Okay, please fix any conflicts and 'git add' conflicting files... Finished?"
        continue_maybe(msg)
        had_conflicts = True

    # Use PR author as the primary author for proper GitHub attribution
    primary_author = bold_input(
        'Enter primary author in the format of "name <email>" [%s]: ' % pr_author
    )
    if primary_author == "":
        primary_author = pr_author

    merge_message_flags = []

    merge_message_flags += ["-m", title]
    if body is not None:
        # We remove @ symbols from the body to avoid triggering e-mails
        # to people every time someone creates a public fork of Spark.
        merge_message_flags += ["-m", body.replace("@", "")]

    committer_name = git.run("git config --get user.name").strip()
    committer_email = git.run("git config --get user.email").strip()

    if had_conflicts:
        message = "This patch had conflicts when merged, resolved by\nCommitter: %s <%s>" % (
            committer_name,
            committer_email,
        )
        merge_message_flags += ["-m", message]

    # The string "Closes #%s" string is required for GitHub to correctly close the PR
    merge_message_flags += ["-m", "Closes #%s from %s." % (pr_num, pr_repo_desc)]

    authors = "Authored-by:" if len(co_authors) == 0 else "Lead-authored-by:"
    authors += " %s" % primary_author
    if len(co_authors) > 0:
        authors += "\n" + "\n".join(["Co-authored-by: %s" % a for a in co_authors])
    authors += "\n" + "Signed-off-by: %s <%s>" % (committer_name, committer_email)

    merge_message_flags += ["-m", authors]

    git.run(["git", "commit", '--author="%s"' % primary_author] + merge_message_flags)

    continue_maybe(
        "Merge complete (local ref %s). Push to %s?" % (target_branch_name, PUSH_REMOTE_NAME)
    )

    try:
        git.push(PUSH_REMOTE_NAME, target_branch_name, target_ref)
    except Exception as e:
        clean_up()
        print_error("Exception while pushing: %s" % e)

    merge_hash = git.run("git rev-parse %s" % target_branch_name).strip()
    clean_up()
    print("Pull request #%s merged!" % pr_num)
    print("Merge hash: %s" % merge_hash)
    return merge_hash


def _do_cherry_pick(pr_num, merge_hash, pick_ref):
    """Cherry-pick `merge_hash` onto `pick_ref` and push.

    Returns the (pushed ref, pushed commit hash) pair. Raises `SkipCherryPick` if the
    cherry-pick conflicts and the committer declines to resolve it, after attempting to
    abort the cherry-pick and restore the working tree.
    """
    pick_branch_name = "%s_PICK_PR_%s_%s" % (BRANCH_PREFIX, pr_num, pick_ref.upper())

    git.run("git fetch %s %s:%s" % (PUSH_REMOTE_NAME, pick_ref, pick_branch_name))
    git.run("git checkout %s" % pick_branch_name)
    pick_head = git.run("git rev-parse HEAD").strip()

    try:
        git.run(
            [
                "git",
                "-c",
                "commit.cleanup=scissors",
                "cherry-pick",
                "-sx",
                merge_hash,
            ]
        )
    except Exception as e:
        msg = "Error cherry-picking: %s\nWould you like to manually fix-up this merge?" % e
        continue_maybe(msg, True)
        msg = "Okay, please fix any conflicts and 'git add' conflicting files... Finished?"
        continue_maybe(msg, True)
        if git.run("git rev-parse HEAD").strip() == pick_head:
            # Important to use `scissors` and `--edit` otherwise git will strip lines starting
            # with `#` when calling `--continue`. See: https://github.com/apache/spark/pull/58214
            git.run(
                [
                    "git",
                    "-c",
                    "commit.cleanup=scissors",
                    "cherry-pick",
                    "--continue",
                    "--edit",
                ]
            )
        else:
            print("Cherry-pick already completed manually; continuing with the backport.")

    continue_maybe(
        "Pick complete (local ref %s). Push to %s?" % (pick_branch_name, PUSH_REMOTE_NAME)
    )

    try:
        git.push(PUSH_REMOTE_NAME, pick_branch_name, pick_ref)
    except Exception as e:
        fail("Exception while pushing: %s" % e)

    pick_hash = git.run("git rev-parse %s" % pick_branch_name).strip()
    clean_up()

    print("Pull request #%s picked into %s!" % (pr_num, pick_ref))
    print("Pick hash: %s" % pick_hash)
    return pick_ref, pick_hash


def branches_with_merge_footer(pr_num, branch_names):
    """Release branches from `branch_names` that already carry `pr_num`'s merge footer.

    Thin wrapper over the shared reader in `spark_merge_footer`, adding this script's own
    policy: a git failure here must not abort a merge that may already have pushed, so it
    warns and reports nothing rather than exiting. Per that module's refresh policy no fetch
    is issued, so a backport not yet fetched into PUSH_REMOTE_NAME's tracking refs is simply
    not reported -- the committer is still prompted and can type any branch.
    """
    try:
        landed = _branches_with_merge_footer(
            pr_num, PUSH_REMOTE_NAME, lambda args: git.run(["git"] + args)
        )
    except Exception as e:
        print_error("Could not scan for existing backports of #%s (%s)." % (pr_num, e))
        return []
    # Keep branch_names' newest-first order, and drop anything not a known release branch.
    return [b for b in branch_names if b in landed]


def default_pick_branch(branch_names, already_picked):
    """Highest-ranked release branch that has not already received the change, or None.

    `branch_names` is ordered newest-first (see `semver_branch_rank`) and `already_picked`
    holds the branches the change is known to be on, so the prompt never defaults to a
    branch where the cherry-pick would come up empty. Returns None when every known branch
    already has it, so callers can say so instead of offering an empty pick.

    >>> default_pick_branch(["branch-4.x", "branch-4.3", "branch-4.2"], ("branch-4.x",))
    'branch-4.3'
    >>> default_pick_branch(["branch-4.x", "branch-4.3"], ())
    'branch-4.x'
    >>> default_pick_branch(["branch-4.x"], ("branch-4.x",)) is None
    True
    """
    remaining = [b for b in branch_names if b not in already_picked]
    return remaining[0] if remaining else None


def _upstream_first_sibling(target_ref, pick_ref, branch_names, already_picked):
    """Return the sibling branch-M.x if Upstream-First should prompt, else None.

    The policy only applies when the PR was merged into master: that's the only case
    where the committer can type branch-M.N at the cherry-pick prompt and bypass the
    rolling branch-M.x. When the PR was opened against branch-M.x the merge itself
    lands there (nothing to bypass), and when it was opened against branch-M.N the
    author already chose per-branch scope.

    >>> _upstream_first_sibling("master", "branch-4.2", ["branch-4.x", "branch-4.2"], ())
    'branch-4.x'
    >>> _upstream_first_sibling("master", "branch-4.2", ["branch-4.x", "branch-4.2"],
    ...                         ("branch-4.x",))
    >>> _upstream_first_sibling("master", "branch-4.x", ["branch-4.x"], ())
    >>> _upstream_first_sibling("master", "branch-4.99", ["branch-4.2"], ())
    >>> _upstream_first_sibling("branch-4.x", "branch-4.2", ["branch-4.x", "branch-4.2"], ())
    >>> _upstream_first_sibling("branch-4.2", "branch-3.5", ["branch-4.x", "branch-3.5"], ())
    """
    if target_ref != "master":
        return None
    m = re.match(r"^branch-(\d+)\.(\d+)$", pick_ref)
    if not m:
        return None
    candidate = "branch-%s.x" % m.group(1)
    if candidate in branch_names and candidate not in already_picked:
        return candidate
    return None


def cherry_pick(pr_num, merge_hash, default_branch, branch_names, target_ref, already_picked=()):
    """Prompt for a target branch and cherry-pick `merge_hash` onto it.

    Enforces the Upstream-First policy (see header comment) via
    `_upstream_first_sibling`: when the PR was merged into master and the committer
    types a branch-M.N target while branch-M.x is also a known release branch AND
    has not already received this commit, prompt to confirm whether to pick into
    BOTH (the policy-compliant default) or branch-M.N only (treated as a
    maintenance-only bugfix). Returns the list of (ref, commit_hash) pairs actually
    picked into, so the main loop can advance its remaining-branches list correctly
    and record each backport commit for the merge comment. The list is empty (or, for the
    Upstream-First two-branch path, holds only what landed) when the committer declines to
    resolve a conflict, so the caller simply offers the next branch and still resolves JIRA.
    """
    while True:
        pick_ref = bold_input(f"Enter a branch name [{default_branch}]: ")
        if pick_ref == "":
            pick_ref = default_branch
        if pick_ref in branch_names:
            break
        valid_branches = ", ".join(branch_names)
        print_error(
            f"'{pick_ref}' is not a known release branch. "
            f"Valid branches: {valid_branches}. Please try again."
        )

    sibling_x = _upstream_first_sibling(target_ref, pick_ref, branch_names, already_picked)
    if sibling_x is not None:
        print()
        print("=" * 80)
        print(
            "Upstream-First policy: non-bugfix commits on %s should also land on %s."
            % (pick_ref, sibling_x)
        )
        print(
            "If this is a %s-only maintenance bugfix, you may pick %s alone." % (pick_ref, pick_ref)
        )
        print("Otherwise, pick both (%s first, then %s)." % (sibling_x, pick_ref))
        print("=" * 80)
        choice = get_input(
            f"Pick into [b]oth {sibling_x} + {pick_ref} / [o]nly {pick_ref} / [a]bort (default: both): ",
            {"b": ["b", "both", ""], "o": ["o", "only"], "a": ["a", "abort"]},
        )
        if choice == "b":
            # Preserve any pick that was already pushed: if the branch-M.N pick is skipped after
            # branch-M.x landed, still return branch-M.x so it's recorded and JIRA/comment
            # reflect it.
            picked = []
            try:
                picked.append(_do_cherry_pick(pr_num, merge_hash, sibling_x))
                picked.append(_do_cherry_pick(pr_num, merge_hash, pick_ref))
            except SkipCherryPick:
                pass
            return picked
        elif choice == "o":
            try:
                return [_do_cherry_pick(pr_num, merge_hash, pick_ref)]
            except SkipCherryPick:
                return []
        elif choice == "a":
            fail("Aborted by user at Upstream-First policy prompt.")
        else:
            fail("Unrecognized choice %r; aborting." % choice)

    try:
        return [_do_cherry_pick(pr_num, merge_hash, pick_ref)]
    except SkipCherryPick:
        return []


# Common words carry no signal when comparing a PR title to a JIRA summary, so they are
# dropped before scoring. Kept deliberately small: over-aggressive stopword removal makes
# unrelated titles look similar. Component tags ([SQL], [CORE], ...) are stripped separately.
_SIMILARITY_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "the",
        "to",
        "for",
        "of",
        "in",
        "on",
        "and",
        "or",
        "with",
        "is",
        "are",
        "be",
        "when",
        "should",
        "make",
        "add",
        "fix",
        "fixes",
        "support",
        "enable",
        "use",
        "using",
    }
)
_SIMILARITY_WORD_RE = re.compile(r"[a-z0-9]+")
# Below this Jaccard word-overlap score, warn that the PR title and JIRA summary look
# unrelated. Tuned to flag clear mismatches (an unrelated ticket scores ~0) while leaving
# paraphrases and reworded summaries (which still share key nouns) above the line.
SIMILARITY_WARN_THRESHOLD = 0.2


def title_similarity(pr_title, summary):
    """Jaccard word-overlap between a PR title and a JIRA summary, in [0.0, 1.0].

    Component/version tags ([SQL], [4.x], ...) and the leading SPARK id are stripped,
    words are lowercased, and common stopwords are dropped, so the score reflects the
    substantive words the two share. 1.0 means identical word sets; 0.0 means none in
    common (or an empty side).

    >>> title_similarity("[SPARK-1][SQL] Compute stable checksum", "Compute a stable checksum")
    1.0
    >>> title_similarity("[SPARK-1][SQL] Compute stable checksum", "Refactor the logging backend")
    0.0
    >>> round(title_similarity("[SPARK-1] Ceil and floor overflow", "Handle floor overflow"), 2)
    0.5

    A follow-up title usually describes the fix it adds, not the original ticket, so
    it scores low against the JIRA summary even though it references the right ticket
    (the caller skips the low-similarity warning for [FOLLOWUP] PRs -- see
    format_jira_verification):

    >>> title_similarity("[SPARK-1][FOLLOWUP] Fix a typo", "Add a cache")
    0.0

    A Revert PR keeps its title verbatim (the "Revert" prefix and quotes are not
    tags), so the extra word lowers the score but a genuine match still scores high:

    >>> round(title_similarity('Revert "[SPARK-1] Add a cache"', "Add a cache"), 2)
    0.5
    """

    def tokens(text):
        text = re.sub(r"\[[^\]]*\]", " ", text)
        words = _SIMILARITY_WORD_RE.findall(text.lower())
        return {w for w in words if w not in _SIMILARITY_STOPWORDS}

    a = tokens(pr_title)
    b = tokens(summary)
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def format_jira_verification(
    pr_num,
    pr_title,
    jira_id,
    summary,
    status,
    issuetype,
    resolution=None,
    use_color=False,
    is_followup=False,
):
    """Render the JIRA-vs-PR match block shown before merging.

    Places the PR title next to the linked ticket's summary so the committer can
    eyeball whether they match, flags a ticket that is already Resolved/Closed
    (a fresh merge should target an open ticket), and scores how much the PR title
    and the JIRA summary overlap -- a low score suggests the wrong ticket. Pure
    formatting so it is covered by the inline doctests. ``use_color`` wraps the
    warnings in the same red as ``print_error``; it is left off in the doctests so
    the expected output stays plain text.

    ``is_followup`` suppresses the low-similarity warning: a [FOLLOWUP] PR title
    describes the fix it adds, not the original ticket, so it legitimately scores
    low against the JIRA summary. The score is still shown for reference, but with
    a note instead of a warning so the expected divergence isn't flagged as a
    likely wrong ticket. The Resolved/Closed warning still applies.

    >>> print(format_jira_verification(
    ...     42,
    ...     "[SPARK-2222][SQL] Compute stable checksum",
    ...     "SPARK-1111",
    ...     "Refactor the logging backend",
    ...     "Resolved",
    ...     "Improvement",
    ... ))
    === Verify JIRA matches PR #42 ===
    PR title:  [SPARK-2222][SQL] Compute stable checksum
    JIRA SPARK-1111: Refactor the logging backend
      Status:  Resolved   <-- WARNING: already Resolved/Closed
      Type:    Improvement
      Match:   0.00   <-- WARNING: low title similarity, wrong ticket?

    >>> print(format_jira_verification(
    ...     42,
    ...     "[SPARK-2222][SQL] Compute stable checksum",
    ...     "SPARK-2222",
    ...     "Compute a stable checksum",
    ...     "In Progress",
    ...     "Bug",
    ... ))
    === Verify JIRA matches PR #42 ===
    PR title:  [SPARK-2222][SQL] Compute stable checksum
    JIRA SPARK-2222: Compute a stable checksum
      Status:  In Progress
      Type:    Bug
      Match:   1.00

    A [FOLLOWUP] title scores low but is not warned about (only noted):

    >>> print(format_jira_verification(
    ...     42,
    ...     "[SPARK-1][FOLLOWUP] Fix a typo",
    ...     "SPARK-1",
    ...     "Add a cache",
    ...     "In Progress",
    ...     "Bug",
    ...     is_followup=True,
    ... ))
    === Verify JIRA matches PR #42 ===
    PR title:  [SPARK-1][FOLLOWUP] Fix a typo
    JIRA SPARK-1: Add a cache
      Status:  In Progress
      Type:    Bug
      Match:   0.00   (FOLLOWUP: title intentionally differs, not checked)
    """
    status_warning = ""
    resolution_suffix = " (%s)" % resolution if resolution else ""
    if status in ("Resolved", "Closed"):
        status_warning = "   <-- WARNING: already Resolved/Closed"
        if use_color:
            status_warning = red(status_warning)
    score = title_similarity(pr_title, summary)
    if is_followup:
        # A follow-up title describes its own fix, not the original ticket, so a low
        # score is expected and not a wrong-ticket signal. Note it, don't warn.
        match_suffix = "   (FOLLOWUP: title intentionally differs, not checked)"
    elif score < SIMILARITY_WARN_THRESHOLD:
        match_suffix = "   <-- WARNING: low title similarity, wrong ticket?"
        if use_color:
            match_suffix = red(match_suffix)
    else:
        match_suffix = ""
    return "\n".join(
        [
            "=== Verify JIRA matches PR #%s ===" % pr_num,
            "PR title:  %s" % pr_title,
            "JIRA %s: %s" % (jira_id, summary),
            "  Status:  %s%s%s" % (status, resolution_suffix, status_warning),
            "  Type:    %s" % issuetype,
            "  Match:   %.2f%s" % (score, match_suffix),
        ]
    )


def print_jira_issue_summary(issue):
    summary = "Summary\t\t%s\n" % issue.fields.summary
    assignee = issue.fields.assignee
    if assignee is not None:
        assignee = assignee.displayName
    assignee = "Assignee\t%s\n" % assignee
    status = "Status\t\t%s\n" % issue.fields.status.name
    resolution = ""
    if issue.fields.resolution is not None:
        resolution = "Resolution\t%s\n" % issue.fields.resolution.name
    components = "Components\t%s\n" % [x.name for x in issue.fields.components]
    url = "Url\t\t%s/%s\n" % (JIRA_BASE, issue.key)
    target_versions = "Affected\t%s\n" % [x.name for x in issue.fields.versions]
    fix_versions = ""
    if len(issue.fields.fixVersions) > 0:
        fix_versions = "Fixed\t\t%s\n" % [x.name for x in issue.fields.fixVersions]
    print("=== JIRA %s ===" % issue.key)
    print(
        "%s%s%s%s%s%s%s%s"
        % (summary, assignee, status, resolution, components, url, target_versions, fix_versions)
    )


def check_linked_jira_issues(pr_num, title):
    """Check the JIRA tickets that open ``title`` before merging, failing on a mismatch.

    Only the leading ids count (see :func:`leading_jira_ids`), so a ticket that
    the title body mentions is neither fetched nor checked.  Requires
    ``asf_jira`` to be set unless JIRA was disabled for this run, in which case
    both checks are skipped (see :func:`jira_ids_to_verify`).
    """
    jira_ids = jira_ids_to_verify(title)
    # Epic / Umbrella tickets group related work and must not be resolved by a single PR.
    # Collect every offender so the committer sees the full list in one shot rather than
    # discovering them one-by-one across repeated merge attempts.
    blocking_issue_types = {"Epic", "Umbrella"}
    blockers = []
    linked_issues = []
    for jira_id in jira_ids:
        try:
            issue = asf_jira.issue(jira_id)
        except Exception:
            print_error("Unable to fetch summary of %s" % jira_id)
            continue
        print_jira_issue_summary(issue)
        linked_issues.append((jira_id, issue))
        issue_type = issue.fields.issuetype.name
        if issue_type in blocking_issue_types:
            blockers.append((jira_id, issue_type))
    if blockers:
        ids_str = ", ".join("%s (%s)" % (jid, t) for jid, t in blockers)
        fail(
            "Cannot merge PR #%s. Linked JIRA(s) %s are Umbrella or Epic "
            "tickets and MUST not be resolved by a single PR. File "
            "Sub-task(s) under %s and update the PR title to reference "
            "the Sub-task(s) instead." % (pr_num, ids_str, ids_str)
        )

    # Confirm each linked JIRA actually matches this PR before merging. A committer can
    # reference the wrong ticket -- e.g. mistaking a GitHub PR number in the body
    # ("Closes #NNNNN") for a SPARK JIRA id -- which resolves an unrelated ticket on merge.
    # Show the PR title next to each ticket's summary, warn when a ticket is already
    # Resolved/Closed (a fresh merge should target an open one), and require confirmation.
    # For [FOLLOWUP] PRs the title describes the follow-up fix rather than the ticket, so
    # its low similarity to the JIRA summary is expected and the warning is suppressed.
    # The tag is read inside Revert/Reapply wrappers too.
    is_followup = "FOLLOWUP" in leading_title_tags(title)
    for jira_id, issue in linked_issues:
        print()
        print(
            format_jira_verification(
                pr_num,
                title,
                jira_id,
                issue.fields.summary,
                issue.fields.status.name,
                issue.fields.issuetype.name,
                resolution=(
                    issue.fields.resolution.name if issue.fields.resolution is not None else None
                ),
                use_color=True,
                is_followup=is_followup,
            )
        )
        if get_input("Does %s match this PR? (y/N): " % jira_id, ["y", "n", ""]) != "y":
            fail(
                "Aborting: %s does not match PR #%s. Fix the PR title to reference the "
                "correct JIRA ticket and retry." % (jira_id, pr_num)
            )


def jira_components_from_title_tags(tags):
    """Canonical SPARK JIRA component names implied by PR-title component tags.

    Each tag is resolved through the component registry; a tag that maps to a
    JIRA component contributes that component's canonical name, whether primary
    or not (e.g. [SQL] -> "SQL", [TEST] -> "Tests"). Tags that are not JIRA
    components (status markers like [FOLLOWUP]/[MINOR], version tags like [4.X],
    or unknown tags) contribute nothing. Aliases normalize to the canonical
    name. The result preserves input order and is de-duplicated.

    >>> jira_components_from_title_tags(["SQL", "CORE"])
    ['SQL', 'Spark Core']
    >>> jira_components_from_title_tags(["PYTHON", "DOCS"])
    ['PySpark', 'Documentation']
    >>> jira_components_from_title_tags(["SQL", "TEST"])
    ['SQL', 'Tests']
    >>> jira_components_from_title_tags(["UDF"])
    ['UDF']
    >>> jira_components_from_title_tags(["SQL", "FOLLOWUP", "4.X", "BOGUS"])
    ['SQL']
    >>> jira_components_from_title_tags(["SQL", "SQL"])
    ['SQL']
    """
    names = []
    for tag in tags:
        c = Component.find(tag)
        if c is not None and c.jira_name:
            names.append(c.jira_name)
    return list(dict.fromkeys(names))


def reconcile_jira_components(issue, title_components):
    """Prompt to sync JIRA components when they differ from the PR title.

    ``title_components`` is the list of normalized PR-title component tags (e.g.
    ["SQL", "TEST"]). Every tag that maps to a JIRA component -- primary or not,
    e.g. [SQL] -> "SQL" and [TEST] -> "Tests" -- is reconciled; tags with no JIRA
    component ([MINOR], [FOLLOWUP], version tags, unknown tags) are dropped. The
    mapped names are compared, as a set, against the issue's current components.
    On a mismatch, offer to overwrite JIRA with the PR title's components, append
    them to the existing ones, or keep JIRA unchanged (the default).
    """
    title_jira_components = jira_components_from_title_tags(title_components)
    if not title_jira_components:
        return
    current = [c.name for c in issue.fields.components]
    if set(current) == set(title_jira_components):
        return

    print()
    print("=" * 80)
    print("PR title components differ from JIRA %s:" % issue.key)
    print("  PR title: %s" % ", ".join(title_jira_components))
    print("  JIRA:     %s" % (", ".join(current) if current else "(none)"))
    print("=" * 80)
    choice = get_input(
        "[o]verwrite JIRA with PR title / [a]ppend PR title to JIRA / [k]eep JIRA as is "
        "(default: keep): ",
        {"o": ["o", "overwrite"], "a": ["a", "append"], "k": ["k", "keep", ""]},
    )
    if choice == "k":
        print("Keeping JIRA %s components unchanged." % issue.key)
        return
    if choice == "o":
        new_names = list(title_jira_components)
    elif choice == "a":
        # Append the PR title's components, keeping the existing ones first.
        new_names = list(dict.fromkeys(current + title_jira_components))

    jira_ops.update_components(issue, new_names)


def reconcile_jira_affects_versions(
    issue,
    fix_version_names: list[str],
    affects_available: set[str],
    unreleased_version_names: list[str],
) -> None:
    """Prompt the committer to correct Affects Version/s newer than the earliest fix.

    Blank input accepts the suggested version, or skips when none is available. Versions
    are validated against ``affects_available`` before the committer chooses to append
    (default), overwrite, or keep. Entering 'skip' or [k]eep leaves the field untouched.
    Writes go through ``jira_ops`` so a dry run only logs them.
    """
    current_names = [v.name for v in issue.fields.versions]
    default_affects_version = suggest_affects_version(fix_version_names, unreleased_version_names)
    if default_affects_version:
        prompt_suffix = f"[{default_affects_version}] (or 'skip')"
        skip_hint = "enter 'skip'"
    else:
        prompt_suffix = "(blank to skip)"
        skip_hint = "leave blank to skip"
    print()
    print("=" * 80)
    print(
        f"JIRA {issue.key} Affects Version/s {current_names if current_names else '(none)'} "
        f"vs Fix Version/s {fix_version_names}: at least one Affects Version must be at or before "
        f"the earliest Fix Version, so the recorded affected version is likely wrong."
    )
    print("=" * 80)
    while True:
        try:
            raw = bold_input(f"Enter comma-separated affects version(s) {prompt_suffix}: ").strip()
            if raw.lower() == "skip" or (not raw and not default_affects_version):
                print(f"Affects Version/s left unchanged; update {issue.key} manually.")
                return
            new_names = parse_version_list(raw or default_affects_version)
            if new_names and set(new_names).issubset(affects_available):
                break
            print(
                f"Specified version(s) [{', '.join(new_names)}] not found in the available "
                f"versions, try again (or {skip_hint})."
            )
        except KeyboardInterrupt:
            raise
        except BaseException:
            traceback.print_exc()
            print(f"Error setting affects version(s), try again (or {skip_hint}).")

    if current_names:
        choice = get_input(
            f"[a]ppend to / [o]verwrite existing {current_names} / [k]eep as is "
            "(default: append): ",
            {"a": ["a", "append", ""], "o": ["o", "overwrite"], "k": ["k", "keep"]},
        )
        if choice == "k":
            print(f"Keeping JIRA {issue.key} Affects Version/s unchanged.")
            return
        if choice == "a":
            # Keep the existing versions first, then add the entered ones.
            new_names = list(dict.fromkeys(current_names + new_names))

    jira_ops.update_affects_versions(issue, new_names)


def maybe_reconcile_jira_affects_versions(
    issue, fix_version_names, affects_available, unreleased_version_names
):
    """Reconcile the Affects Version/s only when they fail to precede the fix version(s)."""
    if fix_precedes_affects(fix_version_names, [v.name for v in issue.fields.versions]):
        reconcile_jira_affects_versions(
            issue, fix_version_names, affects_available, unreleased_version_names
        )


def get_jira_issue(prompt, default_jira_id=""):
    jira_id = bold_input("%s [%s]: " % (prompt, default_jira_id))
    if jira_id == "":
        jira_id = default_jira_id
        if jira_id == "":
            print("JIRA ID not found, skipping.")
            return None
    try:
        issue = asf_jira.issue(jira_id)
        print_jira_issue_summary(issue)
        status = issue.fields.status.name
        if status == "Resolved" or status == "Closed":
            resolution = issue.fields.resolution
            resolution_name = resolution.name if resolution is not None else None
            print("JIRA issue %s already has status '%s' (%s)" % (jira_id, status, resolution_name))
            # Only Fixed resolutions permit updates to resolved or closed tickets.
            if resolution_name != "Fixed":
                return None
        if get_input("Check if the JIRA information is as expected (y/N): ", ["y", "n", ""]) == "y":
            return issue
        else:
            return get_jira_issue(
                "Enter the revised JIRA ID again or leave blank to skip",
            )
    except Exception as e:
        print_error("ASF JIRA could not find %s: %s" % (jira_id, e))
        return get_jira_issue(
            "Enter the revised JIRA ID again or leave blank to skip",
        )


def resolve_jira_issue(
    merge_branches,
    comment,
    default_jira_id="",
    title_components=(),
):
    issue = get_jira_issue("Enter a JIRA id", default_jira_id)
    if issue is None:
        return

    status = issue.fields.status.name
    is_resolved = status == "Resolved" or status == "Closed"

    if not is_resolved:
        if issue.fields.assignee is None:
            choose_jira_assignee(issue)

        reconcile_jira_components(issue, title_components)

    all_versions = asf_jira.project_versions("SPARK")
    # Consider only x.y.z, unreleased, unarchived versions
    versions = [
        x
        for x in all_versions
        if not x.raw["released"] and not x.raw["archived"] and parse_version(x.name) is not None
    ]
    versions = sorted(versions, key=lambda x: x.name, reverse=True)
    # Affects Version/s may name an already-released version, so validate the affects prompt
    # against all unarchived x.y.z versions, not just the unreleased fix candidates.
    affects_available = {
        x.name for x in all_versions if not x.raw["archived"] and parse_version(x.name) is not None
    }

    unreleased_names = [v.name for v in versions]
    default_fix_list, infer_warnings = compute_merge_default_fix_versions(
        merge_branches, unreleased_names
    )
    for w in infer_warnings:
        print_error(w)

    existing_fix_versions = list(issue.fields.fixVersions) if is_resolved else []
    existing_fix_version_names = [v.name for v in existing_fix_versions]
    if is_resolved:
        # A later backport run must preserve the versions recorded by the original merge and
        # only add versions inferred from the newly discovered branches.
        default_fix_list, all_inferred_present = fix_version_additions(
            default_fix_list, existing_fix_version_names
        )
        if all_inferred_present:
            print(
                "JIRA issue %s already contains all inferred fix versions; no fix version "
                "update needed." % issue.key
            )
            # A re-run may still have Affects Version/s sitting above the unchanged fix set.
            maybe_reconcile_jira_affects_versions(
                issue, existing_fix_version_names, affects_available, unreleased_names
            )
            return
        if default_fix_list:
            print(
                "JIRA issue %s has fix version(s) %s; inferred addition(s): %s"
                % (issue.key, existing_fix_version_names, default_fix_list)
            )
            if get_input("Add these fix version(s)? (y/N): ", ["y", "n", ""]) != "y":
                # Declining the addition still leaves any Affects Version/s that sit above
                # the already-recorded fix version(s) to reconcile.
                maybe_reconcile_jira_affects_versions(
                    issue, existing_fix_version_names, affects_available, unreleased_names
                )
                return
        else:
            # Nothing inferred, so there is nothing to confirm; fall through to the prompt.
            print(
                "JIRA issue %s has fix version(s) %s; no additional fix version could be "
                "inferred." % (issue.key, existing_fix_version_names)
            )
    default_fix_versions = ",".join(default_fix_list)

    available_versions = set(list(map(lambda v: v.name, versions)))
    while True:
        try:
            prompt = "Enter comma-separated fix version(s) [%s]: "
            if is_resolved:
                prompt = "Enter comma-separated additional fix version(s) [%s]: "
            fix_versions = fix_versions_from_input(
                bold_input(prompt % default_fix_versions), default_fix_versions
            )
            if not fix_versions:
                print("No fix version entered; update %s manually." % issue.key)
                return
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

    # On a fresh resolve, offer to update the Affects Version/s when they sit above the fix
    # version(s) just chosen; the already-resolved paths handle their own cases.
    if not is_resolved:
        maybe_reconcile_jira_affects_versions(
            issue, fix_versions, affects_available, unreleased_names
        )

    def get_version_json(version_str):
        return list(filter(lambda v: v.name == version_str, versions))[0].raw

    jira_fix_versions = list(map(lambda v: get_version_json(v), fix_versions))

    if is_resolved:
        existing_names = set(existing_fix_version_names)
        jira_fix_versions = [v for v in jira_fix_versions if v["name"] not in existing_names]
        if not jira_fix_versions:
            print("No new fix versions selected for JIRA issue %s; no update needed." % issue.key)
            maybe_reconcile_jira_affects_versions(
                issue, existing_fix_version_names, affects_available, unreleased_names
            )
            return
        # A backport adds an earlier fix line, which usually means that line is affected too;
        # offer to extend the Affects Version/s down when they miss the full fix set.
        full_fix_names = existing_fix_version_names + [v["name"] for v in jira_fix_versions]
        maybe_reconcile_jira_affects_versions(
            issue, full_fix_names, affects_available, unreleased_names
        )
        jira_ops.add_fix_versions(issue, existing_fix_versions, jira_fix_versions)
        return

    resolve = list(filter(lambda a: a["name"] == "Resolve Issue", asf_jira.transitions(issue.key)))[
        0
    ]
    resolution = list(filter(lambda r: r.raw["name"] == "Fixed", asf_jira.resolutions()))[0]
    jira_ops.resolve_issue(
        issue, resolve["id"], jira_fix_versions, comment, resolution.raw["id"], fix_versions
    )


def revert_jira_issue(merge_branches, default_jira_id=""):
    issue = get_jira_issue("Enter the JIRA id for the reverted change", default_jira_id)
    if issue is None:
        return

    remove_jira_fix_versions(issue, merge_branches)
    if issue.fields.status.name not in ("Resolved", "Closed"):
        return

    prompt = "Reopen JIRA issue %s so the change can be reworked? (y/N): " % issue.key
    if get_input(prompt, ["y", "n", ""]) != "y":
        return
    try:
        reopen = next(
            (t for t in asf_jira.transitions(issue.key) if t["name"] == "Reopen Issue"), None
        )
        if reopen is None:
            print_error(
                "No Reopen Issue transition available for %s; reopen it manually." % issue.key
            )
            return
        jira_ops.reopen_issue(issue, reopen["id"])
    except Exception as e:
        print_error("Failed to reopen %s; reopen it manually: %s" % (issue.key, e))


def remove_jira_fix_versions(issue, merge_branches):
    versions = asf_jira.project_versions("SPARK")
    unreleased_names = [
        v.name
        for v in versions
        if not v.raw["released"] and not v.raw["archived"] and parse_version(v.name) is not None
    ]
    existing_names = [v.name for v in issue.fields.fixVersions]
    available_names = set(existing_names).intersection(unreleased_names)
    if not available_names:
        print("JIRA issue %s has no unreleased fix versions to remove." % issue.key)
        return

    defaults, warnings = compute_revert_fix_versions(
        merge_branches, unreleased_names, existing_names
    )
    for warning in warnings:
        print_error(warning)
    print("Revert merged into: %s" % ", ".join(merge_branches))
    print("JIRA issue %s has fix version(s): %s" % (issue.key, existing_names))
    print("Available unreleased fix version(s) to remove: %s" % sorted(available_names))
    while True:
        raw = bold_input(
            "Enter comma-separated fix version(s) to remove [%s], or 'skip': " % ",".join(defaults)
        )
        if raw.strip().lower() == "skip":
            return
        removals = fix_versions_from_input(raw, ",".join(defaults))
        if not removals:
            print("No fix version selected; update %s manually if needed." % issue.key)
            return
        if set(removals).issubset(available_names):
            break
        print_error("Select only unreleased fix versions currently recorded on this JIRA.")

    prompt = "Remove fix version(s) %s from %s? (y/N): " % (removals, issue.key)
    if get_input(prompt, ["y", "n", ""]) == "y":
        jira_ops.remove_fix_versions(issue, removals)


def choose_jira_assignee(issue):
    """
    Prompt the user to choose who to assign the issue to in jira, given a list of candidates,
    including the original reporter and all commentators
    """
    while True:
        try:
            reporter = issue.fields.reporter
            commentators = list(map(lambda x: x.author, issue.fields.comment.comments))
            candidates = set(commentators)
            candidates.add(reporter)
            candidates = list(candidates)
            print("JIRA is unassigned, choose assignee")
            for idx, author in enumerate(candidates):
                if author.key == "apachespark":
                    continue
                annotations = ["Reporter"] if author == reporter else []
                if author in commentators:
                    annotations.append("Commentator")
                print("[%d] %s (%s)" % (idx, author.displayName, ",".join(annotations)))
            raw_assignee = bold_input(
                "Enter number of user, or userid, to assign to (blank to leave unassigned): "
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
                try:
                    jira_ops.assign(issue.key, assignee.name)
                except Exception as e:
                    if (
                        e.__class__.__name__ == "JIRAError"
                        and ("'%s' cannot be assigned" % assignee.name)
                        in getattr(e, "response").text
                    ):
                        continue_maybe(
                            "User '%s' cannot be assigned, add to contributors role and try again?"
                            % assignee.name
                        )
                        jira_ops.grant_contributor(assignee.name)
                        jira_ops.assign(issue.key, assignee.name)
                    else:
                        raise e
                return assignee
        except KeyboardInterrupt:
            raise
        except BaseException:
            traceback.print_exc()
            print("Error assigning JIRA, try again (or leave blank and fix manually)")


class Jira:
    """ASF JIRA writes used by the merge script -- the single seam for JIRA mutations.

    The pre-write JIRA lookups stay on the module-level asf_jira client; the writes (components,
    fix versions, workflow transitions, assignment, and the contributor-role grant) go through
    here, so DryRunJira can log them. Two Production writes (add_fix_versions, resolve_issue) also
    read the issue back afterward to print the updated summary; DryRunJira skips the write and that
    read-back alike. main() builds Jira(asf_jira) normally and DryRunJira(asf_jira) for a dry run,
    after initialize_jira() sets asf_jira.
    """

    def __init__(self, client):
        self._client = client

    def update_components(self, issue, new_names):
        try:
            issue.update(fields={"components": [{"name": n} for n in new_names]})
            print("Updated JIRA %s components to: %s" % (issue.key, ", ".join(new_names)))
        except Exception as e:
            print_error("Failed to update components on JIRA %s: %s" % (issue.key, e))

    def update_affects_versions(self, issue, new_names: list[str]) -> None:
        try:
            issue.update(fields={"versions": [{"name": n} for n in new_names]})
            print(f"Updated JIRA {issue.key} Affects Version/s to: {', '.join(new_names)}")
        except Exception as e:
            print_error(f"Failed to update Affects Version/s on JIRA {issue.key}: {e}")

    def add_fix_versions(self, issue, existing_fix_versions, new_version_jsons):
        issue.update(
            fields={"fixVersions": [v.raw for v in existing_fix_versions] + new_version_jsons}
        )
        try:
            print_jira_issue_summary(self._client.issue(issue.key))
        except Exception:
            print("Unable to fetch JIRA issue %s after updating fix versions" % issue.key)
        print(
            "Successfully updated %s with additional fixVersions=%s!"
            % (issue.key, [v["name"] for v in new_version_jsons])
        )

    def remove_fix_versions(self, issue, version_names):
        issue.update(update={"fixVersions": [{"remove": {"name": name}} for name in version_names]})
        print("Successfully removed fixVersions=%s from %s!" % (version_names, issue.key))

    def resolve_issue(
        self, issue, resolve_id, fix_version_jsons, comment, resolution_id, fix_version_names
    ):
        self._client.transition_issue(
            issue.key,
            resolve_id,
            fixVersions=fix_version_jsons,
            comment=comment,
            resolution={"id": resolution_id},
        )
        try:
            print_jira_issue_summary(self._client.issue(issue.key))
        except Exception:
            print("Unable to fetch JIRA issue %s after resolving" % issue.key)
        print("Successfully resolved %s with fixVersions=%s!" % (issue.key, fix_version_names))

    def reopen_issue(self, issue, reopen_id):
        # Spark's Reopen Issue workflow clears the resolution.
        self._client.transition_issue(issue.key, reopen_id)
        print("Successfully reopened %s!" % issue.key)

    def assign(self, issue_key, assignee):
        # Shorthand for jira.client.JIRA.assign_issue. The library's own assign_issue re-searches
        # users and blindly picks the first of 20 candidates when unmatched; here the assignee is
        # already resolved, so PUT it directly.
        url = getattr(self._client, "_get_latest_url")(f"issue/{issue_key}/assignee")
        getattr(self._client, "_session").put(url, data=json.dumps({"name": assignee}))
        return True

    def grant_contributor(self, user):
        role = self._client.project_role("SPARK", 10010)
        role.add_user(user)
        print("Successfully added user '%s' to contributors role" % user)


class DryRunJira(Jira):
    """Logs the intended JIRA writes instead of calling the API; reads still go through."""

    def update_components(self, issue, new_names):
        print("DRY-RUN: would set JIRA %s components to: %s" % (issue.key, ", ".join(new_names)))

    def update_affects_versions(self, issue, new_names: list[str]) -> None:
        print(f"DRY-RUN: would set JIRA {issue.key} Affects Version/s to: {', '.join(new_names)}")

    def add_fix_versions(self, issue, existing_fix_versions, new_version_jsons):
        print(
            "DRY-RUN: would add fixVersions=%s to JIRA %s."
            % ([v["name"] for v in new_version_jsons], issue.key)
        )

    def remove_fix_versions(self, issue, version_names):
        print("DRY-RUN: would remove fixVersions=%s from JIRA %s." % (version_names, issue.key))

    def resolve_issue(
        self, issue, resolve_id, fix_version_jsons, comment, resolution_id, fix_version_names
    ):
        print(
            "DRY-RUN: would resolve JIRA %s as Fixed with fixVersions=%s and add comment:\n%s"
            % (issue.key, fix_version_names, comment)
        )

    def reopen_issue(self, issue, reopen_id):
        print("DRY-RUN: would reopen JIRA %s." % issue.key)

    def assign(self, issue_key, assignee):
        print("DRY-RUN: would assign JIRA %s to '%s'." % (issue_key, assignee))
        return True

    def grant_contributor(self, user):
        print("DRY-RUN: would add user '%s' to the SPARK contributors role." % user)


# Set in main() after initialize_jira(): Jira(asf_jira) normally, DryRunJira(asf_jira) for a dry
# run. None until then; only the JIRA-write flow, well after construction, uses it.
jira_ops = None

# Set True by initialize_jira() when the liveness probe fails, leaving asf_jira None and every JIRA
# step skipped. A module-level default (unlike asf_jira, which only exists after initialize_jira())
# so callers can short-circuit on it without JIRA being set up -- which the doctests rely on.
jira_disabled = False


def resolve_jira_issues(jira_ids, merge_branches, comment, title_components=()):
    if len(jira_ids) == 0:
        resolve_jira_issue(
            merge_branches,
            comment,
            title_components=title_components,
        )
    for jira_id in jira_ids:
        resolve_jira_issue(
            merge_branches,
            comment,
            jira_id,
            title_components=title_components,
        )


def update_jira_for_pr(pr_num, title, merge_branches, title_components, is_revert=False):
    if jira_disabled:
        # JIRA was disabled at startup (unreachable); nothing to update.
        print()
        print_error(
            f"JIRA is disabled; skipping JIRA update for PR #{pr_num}. "
            f"Reconcile the ticket(s) manually."
        )
        return

    skip_jira_title_tags = ("MINOR", "TRIVIAL", "FOLLOWUP")
    jira_ids = leading_jira_ids(title)
    # Revert/Reapply PRs keep their title verbatim, so the tags are read inside the wrappers,
    # without Title.parse, which rejects some older titles such as [SPARK-N][FOLLOWUP][MINOR].
    tags = leading_title_tags(title)
    if jira_ids:
        # Title.parse rejects an id next to [MINOR] or [TRIVIAL], and prompt_for_components()
        # rejects both markers, so only a subject that predates the rule or was not merged with
        # this script has both. Such a commit was normally merged as the fix for that id, so
        # the id is updated.
        tags -= _LEADING_TAGS
    skipped = [tag for tag in skip_jira_title_tags if tag in tags]
    if skipped:
        print()
        print_error(
            "Skipping JIRA operations for PR #%s because title has %s."
            % (pr_num, ", ".join("[%s]" % tag for tag in skipped))
        )
        return

    # asf_jira is set here: the jira_disabled guard above returns when JIRA is unreachable, and
    # initialize_jira() fails fast on a missing library/credentials when JIRA is live.
    print()
    continue_maybe("Would you like to update an associated JIRA?")
    if is_revert:
        for jira_id in jira_ids or [""]:
            revert_jira_issue(merge_branches, jira_id)
        return

    jira_comment = "Issue resolved by pull request %s\n[%s/%s]" % (
        pr_num,
        GITHUB_BASE,
        pr_num,
    )
    resolve_jira_issues(
        jira_ids,
        merge_branches,
        jira_comment,
        title_components,
    )


def jira_ids_to_verify(title):
    """Leading JIRA ids of the PR title, for check_linked_jira_issues().

    Empty -- after a printed reminder to reconcile by hand -- when JIRA was disabled for this
    run, so both checks are skipped instead of failing on asf_jira being None.
    """
    jira_ids = leading_jira_ids(title)
    if jira_disabled:
        print_error(
            f"JIRA is disabled; skipping the Epic/Umbrella check and ticket verification. "
            f"Confirm {', '.join(jira_ids) or 'the linked ticket(s)'} manually."
        )
        return []
    return jira_ids


class Component:
    """A Spark PR-title tag, paired with its canonical JIRA component name.

    ``jira_name`` is the canonical name of the SPARK JIRA component (e.g.
    "Documentation"); empty for status markers like [MINOR] that are not
    JIRA components but are still recognized in PR titles.

    ``tag`` is the preferred PR-title abbreviation (uppercase, no brackets,
    e.g. "DOC"). ``aliases`` lists other accepted spellings that resolve to
    the same component (e.g. "DOCS", "DOCUMENTATION" -> "DOC").

    ``primary`` marks components whose presence alone satisfies the merge-time
    requirement. Non-primary JIRA components (e.g. [TEST], [SHUFFLE], [DEPLOY])
    remain recognized — they normalize and pass through validation — but
    they must be paired with a primary tag (e.g. [SQL][TEST]). Status
    markers are never primary. [WIP] is intentionally absent from the
    registry: a WIP PR should be aborted at the earlier WIP warning, not
    merged.
    """

    def __init__(self, tag, aliases=(), primary=False, jira_name=""):
        self.tag = tag
        self.aliases = frozenset(aliases)
        self.primary = primary
        self.jira_name = jira_name

    def matches(self, token):
        return token == self.tag or token in self.aliases

    @classmethod
    def find(cls, token):
        """Return the Component matching ``token`` (case-insensitive), or None."""
        if token is None:
            return None
        token = token.strip().upper()
        for c in COMPONENTS:
            if c.matches(token):
                return c
        return None

    @classmethod
    def normalize(cls, token):
        """Return the registered tag for ``token``, or the stripped, upper-cased token if none.

        >>> Component.normalize(" follow-up "), Component.normalize(" bogus ")
        ('FOLLOWUP', 'BOGUS')
        """
        c = cls.find(token)
        return c.tag if c is not None else token.strip().upper()


# Full SPARK JIRA component list (sorted alphabetically by tag), followed
# by status markers. Keep in sync with the components in JIRA — fetch the
# current list with:
#   curl -s https://issues.apache.org/jira/rest/api/2/project/SPARK/components
# A `primary=True` marker indicates the tag alone satisfies the merge-time
# component requirement; non-primary JIRA components must be paired with a
# primary one (e.g. [SQL][TEST], [CORE][SHUFFLE]). Status
# markers leave `jira_name` empty.
COMPONENTS = (
    Component("BLOCK_MANAGER", jira_name="Block Manager"),
    Component("BUILD", primary=True, jira_name="Build"),
    Component("CONNECT", primary=True, jira_name="Connect"),
    Component("CORE", ("SPARK_CORE",), primary=True, jira_name="Spark Core"),
    Component("DEPLOY", jira_name="Deploy"),
    Component("DOC", ("DOCS", "DOCUMENTATION"), primary=True, jira_name="Documentation"),
    Component("DOCKER", primary=True, jira_name="Spark Docker"),
    Component("EC2", jira_name="EC2"),
    Component("EXAMPLES", ("EXAMPLE",), primary=True, jira_name="Examples"),
    Component("GRAPHX", primary=True, jira_name="GraphX"),
    Component("INFRA", ("PROJECT_INFRA",), primary=True, jira_name="Project Infra"),
    Component("IO", jira_name="Input/Output"),
    Component("JAVA", ("JAVA_API", "JAVAAPI"), jira_name="Java API"),
    Component("K8S", ("KUBERNETES",), primary=True, jira_name="Kubernetes"),
    Component("MESOS", jira_name="Mesos"),
    Component("ML", primary=True, jira_name="ML"),
    Component("MLLIB", primary=True, jira_name="MLlib"),
    Component("OPTIMIZER", jira_name="Optimizer"),
    Component("PROTOBUF", jira_name="Protobuf"),
    Component("PS", primary=True, jira_name="Pandas API on Spark"),
    Component("PYTHON", ("PYSPARK",), primary=True, jira_name="PySpark"),
    Component("R", ("SPARKR",), primary=True, jira_name="R"),
    Component("REPL", ("SHELL", "SPARK_SHELL"), jira_name="Spark Shell"),
    Component("SCHEDULER", jira_name="Scheduler"),
    Component("SDP", ("PIPELINES",), primary=True, jira_name="Declarative Pipelines"),
    Component("SECURITY", primary=True, jira_name="Security"),
    Component("SHUFFLE", jira_name="Shuffle"),
    Component("SQL", primary=True, jira_name="SQL"),
    Component("SS", primary=True, jira_name="Structured Streaming"),
    Component("STREAMING", ("DSTREAM", "DSTREAMS"), primary=True, jira_name="DStreams"),
    Component("SUBMIT", jira_name="Spark Submit"),
    Component("TEST", ("TESTS", "TEST-ONLY", "TESTS-ONLY"), jira_name="Tests"),
    Component("UDF", primary=True, jira_name="UDF"),
    Component("UI", ("WEBUI", "WEB_UI"), primary=True, jira_name="Web UI"),
    Component("WINDOWS", primary=True, jira_name="Windows"),
    Component("YARN", primary=True, jira_name="YARN"),
    # Status markers — recognized in PR titles, but not JIRA components.
    Component("FOLLOWUP", ("FOLLOW-UP",)),
    Component("MINOR"),
    Component("TRIVIAL"),
)


_BRACKET_TAG_RE = re.compile(r"\[\s*([A-Za-z0-9._-]+)\s*\]")
_SPARK_ID_RE = re.compile(r"^SPARK-\d+$", re.IGNORECASE)
_VERSION_TAG_RE = re.compile(r"^\d+\.(\d+|X)$")
_LEADING_TAGS = frozenset({"MINOR", "TRIVIAL"})
# A SPARK id that opens a title without brackets, e.g. the old subject "SPARK-49004".
_BARE_SPARK_ID_RE = re.compile(r"SPARK-[0-9]+\b", re.IGNORECASE | re.ASCII)
# A Revert "..." / Reapply "..." wrapper, which main() keeps verbatim in the PR title.
_WRAPPER_RE = re.compile(r'(?:Revert|Reapply) "(.*)"', re.DOTALL)


def _iter_bracket_tags(raw: str) -> Iterator[tuple[str, int]]:
    """Yield ``(TAG, end)`` for each tag in the leading run of bracket tags of ``raw``.

    ``TAG`` is upper-cased, and ``end`` is the position just past the tag and
    the spaces that follow it.  Scanning stops at the first position where no
    complete bracket tag starts.

    >>> list(_iter_bracket_tags("[SPARK-1] [sql]Fix [X]"))
    [('SPARK-1', 10), ('SQL', 15)]
    """
    pos = 0
    while m := _BRACKET_TAG_RE.match(raw, pos):
        pos = m.end()
        while pos < len(raw) and raw[pos] == " ":
            pos += 1
        yield m.group(1).upper(), pos


class Title:
    """Structured PR title: SPARK refs, component tags, and body.

    ``leading``    — SPARK-NNNNN IDs and [MINOR]/[TRIVIAL] markers, in order.
    ``components`` — all other bracket tags, in order.
    ``text``       — body text following the bracket sequence.

    >>> t = Title.parse("[SPARK-1234][SQL] Fix something")
    >>> t.leading, t.components, t.text
    (['SPARK-1234'], ['SQL'], 'Fix something')
    >>> str(t)
    '[SPARK-1234][SQL] Fix something'
    >>> t = Title.parse("[SPARK-1234][SQL][FOLLOWUP] Fix something")
    >>> t.leading, t.components, t.text
    (['SPARK-1234'], ['SQL', 'FOLLOWUP'], 'Fix something')
    >>> str(t)
    '[SPARK-1234][SQL][FOLLOWUP] Fix something'
    >>> t = Title.parse("[SPARK-1234]")
    >>> t.leading, t.components, t.text
    (['SPARK-1234'], [], '')
    >>> str(t)
    '[SPARK-1234]'
    """

    def __init__(
        self,
        leading: List[str],
        components: List[str],
        text: str,
    ) -> None:
        self.leading = leading
        self.components = components
        self.text = text

    @classmethod
    def parse(cls, raw: str) -> "Title":
        """Parse a PR title string into a :class:`Title`.

        A title must open with a leading tag ([SPARK-NNNNN], [MINOR], or
        [TRIVIAL]); otherwise :exc:`ValueError` is raised.  Subsequent bracket
        tokens (spaces trimmed, separated by optional whitespace) go to
        ``components``.  The remainder is ``text``.

        >>> t = Title.parse("[SPARK-1234][SQL][TESTS] Fix something")
        >>> t.leading, t.components, t.text
        (['SPARK-1234'], ['SQL', 'TESTS'], 'Fix something')
        >>> t = Title.parse("  [ SPARK-1234 ]  [ SQL ] [  TESTS  ]   Fix something")
        >>> t.leading, t.components, t.text
        (['SPARK-1234'], ['SQL', 'TESTS'], 'Fix something')
        >>> t = Title.parse("[SPARK-1234 ][ sql ][ followup ] Fix")
        >>> t.leading, t.components, t.text
        (['SPARK-1234'], ['SQL', 'FOLLOWUP'], 'Fix')
        >>> str(t)
        '[SPARK-1234][SQL][FOLLOWUP] Fix'
        >>> Title.parse("[MINOR] Fix typo").leading
        ['MINOR']
        >>> t = Title.parse("[spark-1234][sql][followup] Fix")
        >>> t.leading, t.components
        (['SPARK-1234'], ['SQL', 'FOLLOWUP'])
        >>> Title.parse("[SPARK-1234][SPARK-5678][SQL] Fix").leading
        ['SPARK-1234', 'SPARK-5678']
        >>> Title.parse("[SPARK-1234][4.X][SQL] Fix").components
        ['4.X', 'SQL']
        >>> Title.parse("[SPARK-1234][SQL][4.2] Fix").components
        ['SQL', '4.2']
        >>> Title.parse("[SQL] Fix")
        Traceback (most recent call last):
            ...
        ValueError: title must start with [SPARK-NNNNN], [MINOR], or [TRIVIAL]: '[SQL] Fix'
        >>> Title.parse("No brackets")
        Traceback (most recent call last):
            ...
        ValueError: title must start with [SPARK-NNNNN], [MINOR], or [TRIVIAL]: 'No brackets'
        >>> Title.parse("[SPARK-1234][SQL][SPARK-123] Fix")
        Traceback (most recent call last):
            ...
        ValueError: [SPARK-NNNNN] tags must all appear before other tags: '[SPARK-1234][SQL][SPARK-123] Fix'
        >>> Title.parse("[SPARK-1234][MINOR][SQL] Fix")
        Traceback (most recent call last):
            ...
        ValueError: [SPARK-NNNNN], [MINOR], and [TRIVIAL] cannot coexist
        >>> Title.parse("[MINOR][TRIVIAL][SQL] Fix")
        Traceback (most recent call last):
            ...
        ValueError: [SPARK-NNNNN], [MINOR], and [TRIVIAL] cannot coexist
        """
        leading: List[str] = []
        components: List[str] = []

        raw = raw.strip()
        m0 = _BRACKET_TAG_RE.match(raw)
        first = m0.group(1).upper() if m0 else ""
        if not (_SPARK_ID_RE.match(first) or first in _LEADING_TAGS):
            raise ValueError("title must start with [SPARK-NNNNN], [MINOR], or [TRIVIAL]: %r" % raw)

        past_leading = False
        end = 0
        for tag, end in _iter_bracket_tags(raw):
            if _SPARK_ID_RE.match(tag):
                if past_leading:
                    raise ValueError(
                        "[SPARK-NNNNN] tags must all appear before other tags: %r" % raw
                    )
                leading.append(tag)
            elif tag in _LEADING_TAGS:
                leading.append(tag)
            else:
                components.append(tag)
                past_leading = True

        text = raw[end:].lstrip()
        markers = [t for t in leading if t in _LEADING_TAGS]
        if len(markers) > 1 or (markers and len(leading) > len(markers)):
            raise ValueError("[SPARK-NNNNN], [MINOR], and [TRIVIAL] cannot coexist")
        return cls(leading, components, text)

    def __str__(self) -> str:
        parts = "".join("[%s]" % t for t in self.leading)
        parts += "".join("[%s]" % c for c in self.components)
        if not self.text:
            return parts
        return parts + (" " if parts else "") + self.text


def _unwrap_title(title: str) -> str:
    """Strip ``Revert "..."`` / ``Reapply "..."`` wrappers from a PR title.

    ``main()`` keeps Revert and Reapply PR titles verbatim, so the JIRA id,
    leading markers, and components all live inside the quotes.  Unwrapping here
    lets the id-extraction and skip-tag logic see them.  Wrappers are stripped
    repeatedly, so a revert-of-revert (``Revert "Revert "...""``), including
    mixed ``Revert``/``Reapply`` nesting, is reduced to its innermost title.
    Surrounding whitespace is stripped from the title and from each unwrapped
    layer.  A title without such a wrapper is otherwise returned unchanged.

    >>> _unwrap_title('Revert "[SPARK-1234][SQL] Fix"')
    '[SPARK-1234][SQL] Fix'
    >>> _unwrap_title('Reapply "[SPARK-1234][SQL] Fix"')
    '[SPARK-1234][SQL] Fix'
    >>> _unwrap_title('Revert "Revert "[SPARK-1234][SQL] Fix""')
    '[SPARK-1234][SQL] Fix'
    >>> _unwrap_title('Revert " Reapply " [SPARK-1234][SQL] Fix""')
    '[SPARK-1234][SQL] Fix'
    >>> _unwrap_title('[SPARK-1234][SQL] Fix')
    '[SPARK-1234][SQL] Fix'
    >>> _unwrap_title('Revert "')
    'Revert "'
    """
    title = title.strip()
    while m := _WRAPPER_RE.fullmatch(title):
        title = m.group(1).strip()
    return title


def leading_jira_ids(title: str) -> list[str]:
    """Return the JIRA ids that open ``title``, in order, upper-cased, without duplicates.

    ``Revert "..."`` / ``Reapply "..."`` wrappers, including nested ones, are
    stripped first (see :func:`_unwrap_title`), so those PRs still yield the
    wrapped id.  The ids are the run of ``[SPARK-N]`` bracket tags at the start
    of the title.  Scanning stops at the first other tag, so a SPARK id later in
    the title, such as a reference to a related ticket in the body, is not
    collected.  The tags after the ids are not validated, so a title that
    :meth:`Title.parse` rejects, such as ``[SPARK-1234][SQL][MINOR] Fix``, still
    yields its id.  A title that opens with a bare id instead of a bracket, such
    as the old subject ``SPARK-49004``, yields only that first id.  Any other
    title, including one whose id follows another tag, yields an empty list.

    >>> leading_jira_ids("[SPARK-1234][SPARK-5678][SQL] Follow up to SPARK-9999")
    ['SPARK-1234', 'SPARK-5678']
    >>> leading_jira_ids('Reapply "[SPARK-1234][SQL] Fix"')
    ['SPARK-1234']
    >>> leading_jira_ids('Revert "Revert "[SPARK-1234][SQL] Fix""')
    ['SPARK-1234']
    >>> leading_jira_ids("[SPARK-1234][spark-1234] Fix")
    ['SPARK-1234']
    >>> leading_jira_ids("[SPARK-123456][SQL] Fix")
    ['SPARK-123456']
    >>> leading_jira_ids("[SPARK-1234][SQL][MINOR] Fix")
    ['SPARK-1234']
    >>> leading_jira_ids('Revert " [SPARK-1234][SQL][MINOR] Fix"')
    ['SPARK-1234']
    >>> leading_jira_ids('Revert "SPARK-49004"')
    ['SPARK-49004']
    >>> leading_jira_ids('Revert "SPARK-9545, SPARK-9547: Fix"')
    ['SPARK-9545']
    >>> leading_jira_ids("[MINOR][SQL] Fix typo")
    []
    >>> leading_jira_ids("[WIP][SPARK-1234] Fix")
    []
    >>> leading_jira_ids("No leading tag, mentions SPARK-5678")
    []
    """
    title = _unwrap_title(title)
    m = _BARE_SPARK_ID_RE.match(title)
    if m:
        return [m.group(0).upper()]
    ids = []
    for tag, _ in _iter_bracket_tags(title):
        if not _SPARK_ID_RE.match(tag):
            break
        ids.append(tag)
    return list(dict.fromkeys(ids))


def leading_title_tags(title: str) -> set[str]:
    """Return the tags in the leading run of bracket tags of ``title``, normalized.

    ``Revert "..."`` / ``Reapply "..."`` wrappers are stripped first (see
    :func:`_unwrap_title`), and aliases are normalized (see
    :meth:`Component.normalize`).  Like :func:`leading_jira_ids`, the run is not
    validated, so a title that :meth:`Title.parse` rejects still yields its tags.

    >>> sorted(leading_title_tags('Revert "[SPARK-1234][FOLLOW-UP][SQL][MINOR] Fix [X]"'))
    ['FOLLOWUP', 'MINOR', 'SPARK-1234', 'SQL']
    >>> leading_title_tags("SPARK-49004")
    set()
    """
    return {Component.normalize(tag) for tag, _ in _iter_bracket_tags(_unwrap_title(title))}


def prompt_for_components():
    """
    Prompt the committer for component(s) when the PR title lacks a primary
    component. Each entered token is normalized via Component.normalize
    (e.g. "DOCS" -> "DOC", "PYSPARK" -> "PYTHON"); unrecognized tokens are only
    upper-cased. Re-prompts until at least one entered token resolves to a
    primary Component (one with primary=True) and every token is one that the
    bracket-tag scan reads back from the title as a component. So a token with
    other characters, such as "SPARK CORE", is rejected. A SPARK id is rejected
    too: it belongs in the PR title. So are [MINOR] and [TRIVIAL]: they mark a PR
    without a JIRA ticket and belong at the start of its title, where Title.parse
    checks them. Returns an uppercase list of tags in insertion order.
    """
    print("PR title is missing a primary [COMPONENT] tag.")
    print("Primary components (one of these is required):")
    primary = [c for c in COMPONENTS if c.primary]
    width = max(len(c.tag) for c in primary)
    for c in primary:
        print("  [%s]%s - %s" % (c.tag, " " * (width - len(c.tag)), c.jira_name))
    while True:
        raw = bold_input(
            "Enter comma-separated component(s) to insert into the title (e.g. CORE,SQL): "
        )
        components = []
        has_primary = False
        for token in raw.split(","):
            t = token.strip().upper()
            if t:
                c = Component.find(t)
                if c is not None and c.primary:
                    has_primary = True
                components.append(Component.normalize(t))
        if not components:
            print_error("Component(s) cannot be empty. Please enter at least one.")
            continue
        # A tag that the bracket scan cannot read would hide the tags after it, such as
        # FOLLOWUP, from the JIRA skip check, at this merge and at a later revert. A SPARK id
        # would become an extra linked ticket or an id after a component.
        invalid = [
            t
            for t in components
            if not _BRACKET_TAG_RE.fullmatch("[%s]" % t) or _SPARK_ID_RE.match(t)
        ]
        if invalid:
            print_error(
                "Invalid component(s): %s. Use letters, digits, '.', '_', or '-', and no SPARK id."
                % ", ".join(invalid)
            )
            continue
        markers = [t for t in components if t in _LEADING_TAGS]
        if markers:
            print_error(
                "%s cannot be entered here. A PR without a JIRA ticket puts [MINOR] or "
                "[TRIVIAL] at the start of its title instead; a PR with one cannot be marked so."
                % ", ".join("[%s]" % t for t in markers)
            )
            continue
        if not has_primary:
            print_error(
                "At least one component must be a primary tag (see list above). "
                "Got: %s" % ", ".join(components)
            )
            continue
        return components


def get_current_ref():
    ref = git.run("git rev-parse --abbrev-ref HEAD").strip()
    if ref == "HEAD":
        # The current ref is a detached HEAD, so grab its SHA.
        return git.run("git rev-parse HEAD").strip()
    else:
        return ref


def jira_is_live():
    """Quick liveness probe for the SPARK JIRA backend, to decide whether to touch JIRA at all.

    Returns (True, None) when the backend looks reachable, and (False, reason) -- so the merge
    proceeds with JIRA disabled rather than failing or hanging -- when JIRA is down, errors out,
    or takes longer than JIRA_READ_TIMEOUT to answer. The reason names what the probe hit (an
    HTTP status or the raised exception) so the disable message can tell the committer why.
    The ASF JIRA instance goes down for infra maintenance often enough to be worth tolerating.
    """
    # issues.apache.org is a reverse proxy in front of several backends, so the front door can be
    # up while the backend serving SPARK returns 503. Probe a SPARK-specific endpoint so the result
    # reflects whether SPARK operations will actually work. A 4xx still means a backend answered
    # (up); a 5xx (e.g. that 503) or a connection/timeout error means it is effectively dead.
    url = f"{JIRA_API_BASE}/rest/api/2/project/SPARK"
    try:
        with urlopen(Request(url), timeout=JIRA_READ_TIMEOUT):
            return True, None
    except HTTPError as e:
        if e.code < 500:
            return True, None
        return False, f"server answered HTTP {e.code}"
    except Exception as e:
        return False, f"{e.__class__.__name__}: {e}"


def initialize_jira():
    # A successful merge should resolve its ticket, so JIRA access is normally mandatory and we
    # bail out early -- before any git work -- on a missing library or credentials. But the ASF
    # JIRA instance is not always up; probe it first and, if it is unreachable or too slow, disable
    # JIRA for this run (asf_jira stays None) and let the merge proceed, reconciling by hand later.
    global asf_jira, jira_disabled
    asf_jira = None
    jira_disabled = False
    jira_server = {"server": JIRA_API_BASE}

    live, reason = jira_is_live()
    if not live:
        jira_disabled = True
        print_error(
            f"JIRA is unreachable or unhealthy ({reason}); disabling all JIRA steps for this "
            f"merge. Resolve the associated ticket(s) manually."
        )
        return

    if not JIRA_IMPORTED:
        fail("ERROR finding jira library. Run 'pip3 install jira' to install, then retry.")
    elif JIRA_ACCESS_TOKEN:
        client = jira.client.JIRA(
            jira_server,
            token_auth=JIRA_ACCESS_TOKEN,
            timeout=(JIRA_CONNECT_TIMEOUT, JIRA_READ_TIMEOUT),
        )
        try:
            # Eagerly check if the token is valid to align with the behavior of username/password
            # authn
            client.current_user()
            asf_jira = client
        except Exception as e:
            if e.__class__.__name__ == "JIRAError" and getattr(e, "status_code", None) == 401:
                msg = (
                    "ASF JIRA could not authenticate with the invalid or expired token '%s'"
                    % JIRA_ACCESS_TOKEN
                )
                fail(msg)
            else:
                raise e
    elif JIRA_USERNAME and JIRA_PASSWORD:
        print("You can use JIRA_ACCESS_TOKEN instead of JIRA_USERNAME/JIRA_PASSWORD.")
        print("Visit https://issues.apache.org/jira/secure/ViewProfile.jspa ")
        print("and click 'Personal Access Tokens' menu to manage your own tokens.")
        asf_jira = jira.client.JIRA(
            jira_server,
            basic_auth=(JIRA_USERNAME, JIRA_PASSWORD),
            timeout=(JIRA_CONNECT_TIMEOUT, JIRA_READ_TIMEOUT),
        )
    else:
        fail(
            "Neither JIRA_ACCESS_TOKEN nor JIRA_USERNAME/JIRA_PASSWORD are set. Configure JIRA "
            "credentials (a personal access token is recommended) and retry. See "
            "https://issues.apache.org/jira/secure/ViewProfile.jspa -> Personal Access Tokens."
        )


def check_script_up_to_date():
    # Running a stale merge script can silently reintroduce already-fixed behavior (e.g. skipping
    # JIRA resolution). Rather than download the whole file, ask GitHub only for the latest commit
    # on master that touched this script (a small response) and fail if that commit is not already
    # in the local history -- i.e. the committer is missing an upstream update. Being ahead of
    # master (branched from it) is fine and does not trip the check. Best-effort: if master cannot
    # be reached we warn and proceed rather than block merges on a transient network/rate error.
    if SKIP_VERSION_CHECK:
        print("SKIP_VERSION_CHECK is set; not verifying this script against master.")
        return

    url = "%s/commits?path=%s&sha=master&per_page=1" % (GITHUB_API_BASE, MERGE_SCRIPT_REPO_PATH)
    try:
        request = Request(url)
        if GITHUB_OAUTH_KEY:
            request.add_header("Authorization", "token %s" % GITHUB_OAUTH_KEY)
        commits = json.load(urlopen(request))
    except Exception as e:
        print_error(
            "Could not check %s against master (%s). Proceeding with the local copy."
            % (MERGE_SCRIPT_REPO_PATH, e)
        )
        return
    if not commits:
        return
    latest_sha = commits[0]["sha"]

    # `git merge-base --is-ancestor` exits 0 when latest_sha is reachable from HEAD (we have it),
    # nonzero when it is missing (stale) or unknown locally (never fetched).
    try:
        subprocess.check_call(
            ["git", "merge-base", "--is-ancestor", latest_sha, "HEAD"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return
    except subprocess.CalledProcessError:
        pass

    fail(
        "Your local %s is out of date: apache/spark master has a newer revision (%s) that is not "
        "in your local history. Update it before merging, e.g.:\n"
        "    git fetch %s master && git checkout %s/master -- %s\n"
        "then re-run. Set SKIP_VERSION_CHECK=1 to bypass (e.g. when changing this script itself)."
        % (
            MERGE_SCRIPT_REPO_PATH,
            latest_sha[:12],
            PUSH_REMOTE_NAME,
            PUSH_REMOTE_NAME,
            MERGE_SCRIPT_REPO_PATH,
        )
    )


def build_arg_parser():
    """CLI parser: an optional PR number plus the --dry-run/-n flag.

    --dry-run defaults to on when the DRY_RUN env var is set to any non-empty value (see the
    DRY_RUN_ENV note near the top of this file), so either the flag or the env var enables a
    dry run; passing the flag forces it on regardless.
    """
    parser = argparse.ArgumentParser(description="Merge a Spark pull request.")
    parser.add_argument(
        "pr_num",
        nargs="?",
        help="Pull request number to merge; prompted for interactively if omitted.",
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        default=DRY_RUN_ENV,
        help="Run every read-only step for real but suppress all outbound effects (git push, "
        "GitHub PR close/comment, JIRA writes), logging each as a 'DRY-RUN: would ...' line. "
        "Also enabled by setting the DRY_RUN environment variable.",
    )
    return parser


def main():
    global git, github, jira_ops, original_head
    args = build_arg_parser().parse_args()
    if args.dry_run:
        # Swap the outbound clients for their logging-only variants; everything else is unchanged.
        git = DryRunGit()
        github = DryRunGitHub()
        print(
            bold(
                "=== DRY-RUN: read-only steps run for real, but the git push to %s, the GitHub "
                "PR close/comment, and all JIRA writes are suppressed and only logged. ==="
                % PUSH_REMOTE_NAME
            )
        )

    check_script_up_to_date()
    initialize_jira()
    jira_ops = DryRunJira(asf_jira) if args.dry_run else Jira(asf_jira)

    os.chdir(SPARK_HOME)
    original_head = get_current_ref()

    branches = get_json("%s/branches" % GITHUB_API_BASE)
    branch_names = list(filter(lambda x: x.startswith("branch-"), [x["name"] for x in branches]))
    branch_names = sorted(branch_names, key=semver_branch_rank, reverse=True)

    if args.pr_num is None:
        pr_num = get_input("Which pull request would you like to merge? (e.g. 34): ", r"^\d+$")
    else:
        pr_num = args.pr_num
        print("Start to merge pull request #%s" % (pr_num))
    pr = get_json("%s/pulls/%s" % (GITHUB_API_BASE, pr_num))
    pr_events = get_json("%s/issues/%s/events" % (GITHUB_API_BASE, pr_num))

    url = pr["url"]
    title = pr["title"]

    # Fail hard on draft, WIP, or DO-NOT-MERGE PRs to prevent accidental merges.
    if pr.get("draft", False) or "[WIP]" in title or "[DO-NOT-MERGE]" in title:
        fail("Cannot merge a draft PR #%s: %s" % (pr_num, title))

    # e.g. 'Revert "[SPARK-56357][BUILD] Upgrade sbt to 1.12.8"'
    is_revert_pr = title.startswith('Revert "') and title.endswith('"')
    # e.g. 'Reapply "[SPARK-56357][BUILD] Upgrade sbt to 1.12.8"'
    is_reapply_pr = title.startswith('Reapply "') and title.endswith('"')

    # Normalized PR-title component tags, used later to reconcile JIRA components. Empty for
    # Revert/Reapply PRs, whose titles are kept verbatim and not parsed for components.
    title_components: List[str] = []

    # Revert and Reapply PRs keep their title verbatim.
    if not (is_revert_pr or is_reapply_pr):
        # Parse; fail on a malformed title.
        try:
            parsed = Title.parse(title)
        except ValueError as e:
            fail("Malformed PR title: %s" % e)

        # Normalize component tags via the registry and track primary.
        components = []
        has_primary = False
        for tag in parsed.components:
            c = Component.find(tag)
            if c is not None and c.primary:
                has_primary = True
            components.append(Component.normalize(tag))
        if not has_primary:
            new_tags = prompt_for_components()
            components = new_tags + components

        # Deduplicate tags in insertion order.
        components = list(dict.fromkeys(components))

        # Move version tags (e.g. [4.X], [4.2]) to the head of components.
        versions = [t for t in components if _VERSION_TAG_RE.match(t)]
        if versions:
            others = [t for t in components if not _VERSION_TAG_RE.match(t)]
            components = versions + others

        # Move FOLLOWUP to the last tag.
        non_followup = [t for t in components if t != "FOLLOWUP"]
        if len(non_followup) < len(components):
            components = non_followup + ["FOLLOWUP"]

        # Warn about tags that are neither known components nor version tags.
        unknown = [
            t for t in components if Component.find(t) is None and not _VERSION_TAG_RE.match(t)
        ]
        if unknown:
            print_error("Title has unknown tag(s): %s" % ", ".join("[%s]" % t for t in unknown))

        parsed.components = components
        title_components = list(parsed.components)
        title = str(parsed)
        if title != pr["title"]:
            print("Normalized title: %s" % title)

    body = pr["body"]
    if body is None:
        body = ""
    modified_body = re.sub(re.compile(r"<!--[^>]*-->\n?", re.DOTALL), "", body).lstrip()
    if modified_body != body:
        print("=" * 80)
        print(modified_body)
        print("=" * 80)
        print("I've removed the comments from PR template like the above:")
        if get_input("Would you like to use the modified body? (y/N): ", ["y", "n", ""]) == "y":
            body = modified_body
            print("Using modified body:")
        else:
            print("Using original body:")
        print("=" * 80)
        print(body)
        print("=" * 80)
    target_ref = pr["base"]["ref"]
    user_login = pr["user"]["login"]
    base_ref = pr["head"]["ref"]
    pr_repo_desc = "%s/%s" % (user_login, base_ref)

    # Fetch PR author's GitHub profile and PR commits for commit attribution
    pr_author_info = get_json("https://api.github.com/users/%s" % user_login)
    pr_author_name = pr_author_info.get("name") or user_login
    pr_author_email = pr_author_info.get("email")
    pr_commits = get_json("%s/pulls/%s/commits" % (GITHUB_API_BASE, pr_num))
    if not pr_author_email:
        # If the GitHub profile has no public email, find the author's email from the
        # PR's git commits. Only use it if GitHub links the commit to the PR author's
        # account (meaning the email is verified on their GitHub account).
        for c in pr_commits:
            commit_author = c.get("author")
            if commit_author and commit_author.get("login") == user_login:
                pr_author_email = c["commit"]["author"]["email"]
                break
    if not pr_author_email:
        pr_author_email = "%s+%s@users.noreply.github.com" % (pr_author_info["id"], user_login)
    pr_author = "%s <%s>" % (pr_author_name, pr_author_email)

    # Collect co-authors: commit authors whose GitHub login differs from the PR author.
    # This deduplicates the PR author (who may use multiple emails across commits) by login.
    co_authors = []
    seen = set()
    for c in pr_commits:
        gh_author = c.get("author")
        if gh_author and gh_author.get("login") == user_login:
            continue
        raw = "%s <%s>" % (c["commit"]["author"]["name"], c["commit"]["author"]["email"])
        if raw not in seen:
            seen.add(raw)
            co_authors.append(raw)

    # Merged pull requests don't appear as merged in the GitHub API;
    # Instead, they're closed by committers.
    # A PR might have multiple merge commits, if it's reopened and merged again. We shall
    # cherry-pick PRs in closed state with the latest merge hash.
    # If the PR is still open (reopened), we shall not cherry-pick it but perform the normal
    # merge as it could have been reverted earlier.
    merge_hash, message = (None, None)
    if pr["state"] == "closed":
        merge_hash, message = find_merge_commit(pr_num, pr_events)

    if merge_hash is not None:
        print("Pull request %s has already been merged, assuming you want to backport" % pr_num)
        commit_is_downloaded = (
            git.run(["git", "rev-parse", "--quiet", "--verify", "%s^{commit}" % merge_hash]).strip()
            != ""
        )
        if not commit_is_downloaded:
            fail("Couldn't find any merge commit for #%s, you may need to update HEAD." % pr_num)

        print("Found commit %s:\n%s" % (merge_hash, message))
        # The change is already on target_ref and on any branch a previous run backported it
        # to, so exclude all of them: defaulting to one would cherry-pick an empty commit.
        picked_refs = [target_ref] + [
            b for b in branches_with_merge_footer(pr_num, branch_names) if b != target_ref
        ]
        if len(picked_refs) > 1:
            print("Already backported to: %s" % ", ".join(picked_refs[1:]))
        # Loop so one invocation can reach several maintenance branches, as the merge path does.
        picked_commits = []
        try:
            while True:
                default = default_pick_branch(branch_names, tuple(picked_refs))
                if default is None:
                    print(
                        "Every known release branch already contains #%s; nothing to pick." % pr_num
                    )
                    break
                picked = cherry_pick(
                    pr_num,
                    merge_hash,
                    default,
                    branch_names,
                    target_ref,
                    already_picked=tuple(picked_refs),
                )
                picked_refs = picked_refs + [ref for ref, _ in picked]
                picked_commits = picked_commits + picked
                prompt = "Would you like to pick %s into another branch?" % merge_hash
                if get_input(f"\n{prompt} (y/N): ", ["y", "n", ""]) != "y":
                    break
        finally:
            # Report whatever was pushed even if a later pick is aborted, since the earlier
            # pushes have already landed.
            if picked_commits:
                post_merge_comment(pr_num, picked_commits)
            # Reconcile JIRA with every branch that contains the merge or revert.
            update_jira_for_pr(pr_num, title, picked_refs, title_components, is_revert=is_revert_pr)
        sys.exit(0)

    if not bool(pr["mergeable"]):
        msg = (
            "Pull request %s is not mergeable in its current form.\n" % pr_num
            + "Continue? (experts only!)"
        )
        continue_maybe(msg)

    check_linked_jira_issues(pr_num, title)

    print("\n=== Pull Request #%s ===" % pr_num)
    print("title\t%s\nsource\t%s\ntarget\t%s\nurl\t%s" % (title, pr_repo_desc, target_ref, url))
    continue_maybe("Proceed with merging pull request #%s?" % pr_num)

    merged_refs = [target_ref]

    merge_hash = merge_pr(pr_num, target_ref, title, body, pr_repo_desc, pr_author, co_authors)

    # Ordered (branch, commit_hash) pairs for the merge comment: the merge sink first,
    # then each cherry-pick target as it is picked.
    merged_commits = [(target_ref, merge_hash)]

    # merged_refs drives both the next prompt default and the already_picked set passed to
    # cherry_pick, so each grows with every cherry-pick -- including branches consumed by the
    # Upstream-First two-branch path inside cherry_pick (e.g. picking branch-M.x + branch-M.N
    # in a single prompt). It starts with target_ref, the merge sink, never to be re-picked.
    pick_prompt = "Would you like to pick %s into another branch?" % merge_hash
    # Always record the merge summary for what actually landed, even if a later
    # cherry-pick is aborted or cancelled: the merge into the target branch has
    # already been pushed, so cancelling a backport must not drop that line.
    try:
        while get_input(f"\n{pick_prompt} (y/N): ", ["y", "n", ""]) == "y":
            default = default_pick_branch(branch_names, tuple(merged_refs))
            if default is None:
                print("Every known release branch already contains #%s; nothing to pick." % pr_num)
                break
            picked = cherry_pick(
                pr_num,
                merge_hash,
                default,
                branch_names,
                target_ref,
                already_picked=tuple(merged_refs),
            )
            merged_refs = merged_refs + [ref for ref, _ in picked]
            merged_commits = merged_commits + picked
    finally:
        if merged_commits:
            # The "Closes #N" keyword in the commit message only auto-closes the PR when the
            # commit lands on the default branch. For merges into other branches (e.g.
            # branch-X.Y backport PRs), GitHub leaves the PR open, so close it through the API.
            pr_state = get_json("%s/pulls/%s" % (GITHUB_API_BASE, pr_num)).get("state")
            if pr_state != "closed":
                close_message = "PR #%s is still open after push; closing it explicitly." % pr_num
                print("\n%s\n" % bold(close_message))
                github.close_pr(pr_num)
            # Record every branch that successfully received the change on the PR.
            post_merge_comment(pr_num, merged_commits)
        # This is deliberately in the finally block: once the target branch has been pushed,
        # cancelling a later cherry-pick must not bypass the JIRA update decision.
        update_jira_for_pr(pr_num, title, merged_refs, title_components, is_revert=is_revert_pr)


__test__ = {
    "revert_jira_issue": """
    A revert removes the versions for its branches while preserving released, archived,
    and unrelated versions. JIRA receives individual removals so concurrent additions survive.

    >>> from contextlib import redirect_stdout
    >>> from io import StringIO
    >>> from types import SimpleNamespace
    >>> from unittest.mock import Mock, call, patch
    >>> def version(name, released=False, archived=False):
    ...     return SimpleNamespace(
    ...         name=name, raw={"name": name, "released": released, "archived": archived})
    >>> versions = [version("5.0.0"), version("4.3.0"), version("4.2.1", released=True),
    ...             version("4.2.0", archived=True), version("3.5.2")]
    >>> issue = Mock(key="SPARK-1234")
    >>> issue.fields.fixVersions = versions
    >>> issue.fields.status.name = "Open"
    >>> client = Mock()
    >>> client.project_versions.return_value = versions
    >>> writer = Jira(client)
    >>> def run_revert(answers):
    ...     issue.reset_mock()
    ...     client.reset_mock()
    ...     output = StringIO()
    ...     with (
    ...         patch.dict(revert_jira_issue.__globals__, asf_jira=client,
    ...                    jira_ops=writer, get_jira_issue=Mock(return_value=issue)),
    ...         patch("builtins.input", side_effect=answers),
    ...         redirect_stdout(output),
    ...     ):
    ...         revert_jira_issue(["master", "branch-4.x"], "SPARK-1234")
    ...     return output.getvalue()
    >>> output = run_revert(["", "y"])
    >>> issue.update.assert_called_once_with(update={"fixVersions": [
    ...     {"remove": {"name": "5.0.0"}}, {"remove": {"name": "4.3.0"}}]})
    >>> client.mock_calls
    [call.project_versions('SPARK')]

    The committer can narrow the selection, decline confirmation, or skip the update.

    >>> output = run_revert(["5.0.0", "y"])
    >>> issue.update.assert_called_once_with(
    ...     update={"fixVersions": [{"remove": {"name": "5.0.0"}}]})
    >>> for answers in (["", "n"], ["", ""], ["skip"]):
    ...     output = run_revert(answers)
    ...     issue.update.assert_not_called()

    Released, archived, and unrecorded versions are rejected even when entered manually.

    >>> output = run_revert(["4.2.1", "4.2.0", "9.9.9", "", "y"])
    >>> output.count("Select only unreleased fix versions currently recorded on this JIRA.")
    3
    >>> issue.update.assert_called_once_with(update={"fixVersions": [
    ...     {"remove": {"name": "5.0.0"}}, {"remove": {"name": "4.3.0"}}]})

    Dry runs log the removal without writing to JIRA.

    >>> writer = DryRunJira(client)
    >>> output = run_revert(["", "y"])
    >>> "DRY-RUN: would remove fixVersions=['5.0.0', '4.3.0'] from JIRA SPARK-1234." in output
    True
    >>> issue.update.assert_not_called()
    >>> client.mock_calls
    [call.project_versions('SPARK')]

    Removing the last fix versions is allowed. A rerun with no matching versions is a no-op.

    >>> writer = Jira(client)
    >>> issue.fields.fixVersions = versions[:2]
    >>> output = run_revert(["", "y"])
    >>> issue.update.assert_called_once_with(update={"fixVersions": [
    ...     {"remove": {"name": "5.0.0"}}, {"remove": {"name": "4.3.0"}}]})
    >>> issue.fields.fixVersions = versions[2:]
    >>> output = run_revert([""])
    >>> issue.update.assert_not_called()
    >>> issue.fields.fixVersions = []
    >>> output = run_revert([])
    >>> "no unreleased fix versions to remove" in output
    True
    >>> issue.update.assert_not_called()

    Resolved and closed tickets can be reopened even when no fix versions remain.

    >>> client.transitions.return_value = [
    ...     {"id": "701", "name": "Close Issue"}, {"id": "3", "name": "Reopen Issue"}]
    >>> for status in ("Resolved", "Closed"):
    ...     issue.fields.status.name = status
    ...     output = run_revert(["y"])
    ...     client.transitions.assert_called_once_with("SPARK-1234")
    ...     client.transition_issue.assert_called_once_with("SPARK-1234", "3")
    ...     issue.update.assert_not_called()

    Reopening is optional and independent of fix-version removal.

    >>> for answer in ("n", ""):
    ...     output = run_revert([answer])
    ...     client.transitions.assert_not_called()
    ...     client.transition_issue.assert_not_called()
    >>> issue.fields.fixVersions = versions
    >>> output = run_revert(["", "y", "y"])
    >>> issue.update.assert_called_once_with(update={"fixVersions": [
    ...     {"remove": {"name": "5.0.0"}}, {"remove": {"name": "4.3.0"}}]})
    >>> client.transition_issue.assert_called_once_with("SPARK-1234", "3")
    >>> for answers in (["skip", "y"], ["", "n", "y"]):
    ...     output = run_revert(answers)
    ...     issue.update.assert_not_called()
    ...     client.transition_issue.assert_called_once_with("SPARK-1234", "3")
    >>> output = run_revert(["", "y", "n"])
    >>> issue.update.assert_called_once()
    >>> client.transition_issue.assert_not_called()

    Dry runs log both updates and leave JIRA untouched.

    >>> writer = DryRunJira(client)
    >>> output = run_revert(["", "y", "y"])
    >>> "DRY-RUN: would remove fixVersions=" in output
    True
    >>> "DRY-RUN: would reopen JIRA SPARK-1234." in output
    True
    >>> issue.update.assert_not_called()
    >>> client.transition_issue.assert_not_called()

    Unavailable transitions report that manual action is needed.

    >>> writer = Jira(client)
    >>> client.transitions.return_value = [{"id": "701", "name": "Close Issue"}]
    >>> output = run_revert(["skip", "y"])
    >>> "No Reopen Issue transition available for SPARK-1234; reopen it manually." in output
    True
    >>> client.transition_issue.assert_not_called()

    Lookup and transition failures are nonfatal and request manual reopening.

    >>> client.transitions.side_effect = RuntimeError("lookup failed")
    >>> output = run_revert(["skip", "y"])
    >>> "Failed to reopen SPARK-1234; reopen it manually: lookup failed" in output
    True
    >>> client.transition_issue.assert_not_called()
    >>> client.transitions.side_effect = None
    >>> client.transitions.return_value = [{"id": "3", "name": "Reopen Issue"}]
    >>> client.transition_issue.side_effect = RuntimeError("permission denied")
    >>> output = run_revert(["skip", "y"])
    >>> "Failed to reopen SPARK-1234; reopen it manually: permission denied" in output
    True
    >>> client.transition_issue.assert_called_once_with("SPARK-1234", "3")
    >>> client.transition_issue.side_effect = None

    Active tickets require no reopening prompt or transition lookup.

    >>> issue.fields.fixVersions = []
    >>> for status in ("Open", "In Progress", "Reopened"):
    ...     issue.fields.status.name = status
    ...     output = run_revert([])
    ...     client.transitions.assert_not_called()
    ...     client.transition_issue.assert_not_called()
    """,
    "update_jira_for_revert": """
    Reverts update each leading JIRA id once and bypass the resolve/add path.

    >>> from contextlib import redirect_stdout
    >>> from io import StringIO
    >>> from unittest.mock import Mock, call, patch
    >>> reverter, resolver, confirm = Mock(), Mock(), Mock()
    >>> def update(title, branches, is_revert=False, components=()):
    ...     reverter.reset_mock()
    ...     resolver.reset_mock()
    ...     confirm.reset_mock()
    ...     with (
    ...         patch.dict(update_jira_for_pr.__globals__, revert_jira_issue=reverter,
    ...                    resolve_jira_issue=resolver, continue_maybe=confirm),
    ...         redirect_stdout(StringIO()),
    ...     ):
    ...         update_jira_for_pr("123", title, branches, list(components), is_revert=is_revert)
    >>> title = 'Revert "[SPARK-1234][SPARK-5678][SQL] Fix mentions SPARK-9999"'
    >>> update(title, ["master", "branch-4.x"], is_revert=True)
    >>> assert reverter.call_args_list == [
    ...     call(['master', 'branch-4.x'], 'SPARK-1234'),
    ...     call(['master', 'branch-4.x'], 'SPARK-5678')]
    >>> resolver.assert_not_called()

    Backport-only runs use the same revert path. A missing JIRA id is entered interactively.

    >>> update('Revert "[SPARK-1234][SQL] Fix"', ["branch-4.2"], is_revert=True)
    >>> reverter.assert_called_once_with(["branch-4.2"], "SPARK-1234")
    >>> update('Revert "Fix"', ["master"], is_revert=True)
    >>> reverter.assert_called_once_with(["master"], "")

    A bare leading id, as in the old subject SPARK-49004, still reaches the revert path.

    >>> update('Revert "SPARK-49004"', ["master"], is_revert=True)
    >>> reverter.assert_called_once_with(["master"], "SPARK-49004")

    Wrapped skip tags follow the same policy as direct merges for FOLLOWUP, including the
    FOLLOW-UP alias, and for MINOR and TRIVIAL when the title has no leading id. The tags are
    read even from a wrapped title that Title.parse rejects.

    >>> for title in ('Revert "[MINOR][SQL] Fix"', 'Revert "[TRIVIAL][SQL] Fix"',
    ...               'Revert "[SPARK-1234][SQL][FOLLOWUP] Fix"',
    ...               'Revert "[SPARK-1234][SQL][FOLLOW-UP] Fix"',
    ...               'Revert "[SPARK-1234][SQL][FOLLOWUP][MINOR] Fix"',
    ...               'Revert "[MINOR][TRIVIAL][SQL] Fix"', 'Revert "[SQL][MINOR] Fix"',
    ...               'Revert "[MINOR][SPARK-1234] Fix"'):
    ...     update(title, ["master"], is_revert=True)
    ...     reverter.assert_not_called()
    ...     resolver.assert_not_called()
    ...     confirm.assert_not_called()

    Title.parse rejects an id next to MINOR or TRIVIAL, and so does the component prompt, so
    only an older subject opens with an id and has one. Such a commit was normally merged as
    the fix for its id, so the id is updated.

    >>> update('Revert "[SPARK-1234][SQL][MINOR] Fix"', ["master"], is_revert=True)
    >>> reverter.assert_called_once_with(["master"], "SPARK-1234")

    The skip tags come from the title alone, so direct merges skip through it.

    >>> for title in ("[MINOR][SQL] Fix", "[TRIVIAL][SQL] Fix", "[SPARK-1234][SQL][FOLLOWUP] Fix"):
    ...     update(title, ["master"], components=["SQL"])
    ...     reverter.assert_not_called()
    ...     resolver.assert_not_called()
    ...     confirm.assert_not_called()

    Reapply titles are unwrapped too, so their skip tags apply as well.

    >>> for title in ('Reapply "[MINOR][SQL] Fix"', 'Reapply "[SPARK-1234][SQL][FOLLOWUP] Fix"',
    ...               'Reapply "[SPARK-1234][SQL][FOLLOW-UP] Fix"',
    ...               'Reapply "[SPARK-1234][SQL][FOLLOWUP][MINOR] Fix"'):
    ...     update(title, ["master"])
    ...     reverter.assert_not_called()
    ...     resolver.assert_not_called()
    ...     confirm.assert_not_called()

    Nested wrappers are unwrapped before the skip-tag and id checks, so an inner
    skip tag is still honored and an inner id still reaches the revert path. A
    revert of a reapply is used here because it is a revert both by its outermost
    wrapper, which is how main() sets is_revert today, and by wrapper nesting.

    >>> update('Revert "Reapply "[MINOR][SQL] Fix""', ["master"], is_revert=True)
    >>> reverter.assert_not_called()
    >>> resolver.assert_not_called()
    >>> confirm.assert_not_called()
    >>> update('Revert "Reapply "[SPARK-1234][SQL] Fix""', ["master"], is_revert=True)
    >>> reverter.assert_called_once_with(["master"], "SPARK-1234")

    Ordinary merges and reapplies resolve the leading id, not an id in the title body.

    >>> for title in ('[SPARK-1234][SQL] Fix mentions SPARK-9999',
    ...               'Reapply "[SPARK-1234][SQL] Fix"',
    ...               'Reapply "[SPARK-1234][SQL][TRIVIAL] Fix"'):
    ...     update(title, ["master"])
    ...     reverter.assert_not_called()
    ...     resolver.assert_called_once_with(
    ...         ["master"],
    ...         "Issue resolved by pull request 123\\n[https://github.com/apache/spark/pull/123]",
    ...         "SPARK-1234", title_components=[])
    >>> update("[SPARK-1234][SPARK-5678][SQL] Fix", ["master"])
    >>> [c.args[2] for c in resolver.call_args_list]
    ['SPARK-1234', 'SPARK-5678']

    A title without an id still offers to resolve one that the committer enters.

    >>> update('Reapply "Fix"', ["master"])
    >>> resolver.assert_called_once_with(
    ...     ["master"],
    ...     "Issue resolved by pull request 123\\n[https://github.com/apache/spark/pull/123]",
    ...     title_components=[])

    When JIRA was disabled at startup (unreachable), every shape of update is skipped: no
    prompt, no resolve, no revert. The disabled message is the one printed even for titles
    carrying skip tags, so the committer is told JIRA was the reason.

    >>> def update_disabled(title, is_revert=False):
    ...     reverter.reset_mock()
    ...     resolver.reset_mock()
    ...     confirm.reset_mock()
    ...     output = StringIO()
    ...     with (
    ...         patch.dict(update_jira_for_pr.__globals__, revert_jira_issue=reverter,
    ...                    resolve_jira_issue=resolver, continue_maybe=confirm,
    ...                    jira_disabled=True),
    ...         redirect_stdout(output),
    ...     ):
    ...         update_jira_for_pr("123", title, ["master"], [], is_revert=is_revert)
    ...     assert not reverter.called
    ...     assert not resolver.called
    ...     assert not confirm.called
    ...     return output.getvalue()
    >>> disabled_outputs = [
    ...     update_disabled(t, is_revert=t.startswith('Revert'))
    ...     for t in ('[SPARK-1234][SQL] Fix', 'Revert "[SPARK-1234][SQL] Fix"',
    ...               'Revert "[MINOR][SQL] Fix"', '[SPARK-1234][SQL][FOLLOWUP] Fix')]
    >>> all("JIRA is disabled; skipping JIRA update for PR #123" in o for o in disabled_outputs)
    True
    >>> all("because title has" not in o for o in disabled_outputs)
    True
    """,
    "prompt_for_components": """
    The prompt rejects MINOR and TRIVIAL, which belong at the start of the title, tokens that
    the bracket-tag scan cannot read back, and SPARK ids, and asks again until the components
    include a primary one.

    >>> from contextlib import redirect_stdout
    >>> from io import StringIO
    >>> from unittest.mock import patch
    >>> output = StringIO()
    >>> answers = ["sql,minor", " Trivial ", "sql, spark core", "sql,[tests]", "sql,spark-2",
    ...            "sql][minor", "tests", "sql, follow-up"]
    >>> with (
    ...     patch.dict(prompt_for_components.__globals__, bold_input=lambda _: answers.pop(0)),
    ...     redirect_stdout(output),
    ... ):
    ...     components = prompt_for_components()
    >>> components
    ['SQL', 'FOLLOWUP']
    >>> answers
    []
    >>> "[MINOR] cannot be entered here" in output.getvalue()
    True
    >>> "[TRIVIAL] cannot be entered here" in output.getvalue()
    True
    >>> for bad in ("SPARK CORE", "[TESTS]", "SPARK-2", "SQL][MINOR"):
    ...     assert "Invalid component(s): %s." % bad in output.getvalue(), bad
    """,
    "check_linked_jira_issues": """
    Only the leading ids are fetched and checked, so an Epic that the title body mentions
    neither blocks the merge nor asks for confirmation.

    >>> from contextlib import redirect_stdout
    >>> from io import StringIO
    >>> from unittest.mock import Mock, call, patch
    >>> def issue(issue_type):
    ...     i = Mock()
    ...     i.fields.issuetype.name = issue_type
    ...     return i
    >>> client = Mock()
    >>> client.issue.side_effect = {
    ...     "SPARK-60328": issue("Sub-task"), "SPARK-60329": issue("Bug"),
    ...     "SPARK-34261": issue("Epic"), "SPARK-34262": issue("Umbrella")}.__getitem__
    >>> verify = Mock(return_value="")
    >>> stop = Mock(side_effect=SystemExit)
    >>> def check(title, answers=("y",)):
    ...     for m in (client, verify, stop):
    ...         m.reset_mock()
    ...     with (
    ...         patch.dict(check_linked_jira_issues.__globals__, asf_jira=client,
    ...                    print_jira_issue_summary=Mock(), format_jira_verification=verify,
    ...                    fail=stop),
    ...         patch("builtins.input", side_effect=answers),
    ...         redirect_stdout(StringIO()),
    ...     ):
    ...         check_linked_jira_issues("123", title)
    >>> check("[SPARK-60328][SQL][TESTS] Use a unique jar copy in the HiveDDLSuite "
    ...       "SPARK-34261 test")
    >>> client.issue.call_args_list
    [call('SPARK-60328')]
    >>> stop.assert_not_called()
    >>> verify.call_args.args[2]
    'SPARK-60328'

    Each leading id is confirmed, and declining one aborts the merge.

    >>> check("[SPARK-60328][SPARK-60329][SQL] Fix", answers=("y", "y"))
    >>> [c.args[2] for c in verify.call_args_list]
    ['SPARK-60328', 'SPARK-60329']
    >>> stop.assert_not_called()
    >>> try:
    ...     check("[SPARK-60328][SQL] Fix", answers=("n",))
    ... except SystemExit:
    ...     pass
    >>> stop.call_args.args[0].startswith("Aborting: SPARK-60328 does not match PR #123.")
    True
    >>> try:
    ...     check("[SPARK-60328][SQL] Fix", answers=("",))
    ... except SystemExit:
    ...     pass
    >>> stop.call_count
    1

    A ticket that cannot be fetched is skipped.

    >>> check("[SPARK-1][SQL] Fix")
    >>> client.issue.call_args_list
    [call('SPARK-1')]
    >>> verify.assert_not_called()
    >>> stop.assert_not_called()
    >>> try:
    ...     check("[SPARK-1][SPARK-34261][SQL] Fix")
    ... except SystemExit:
    ...     pass
    >>> "SPARK-34261 (Epic)" in stop.call_args.args[0]
    True

    When JIRA was disabled for this run, nothing is fetched or confirmed.

    >>> client.reset_mock()
    >>> output = StringIO()
    >>> with (
    ...     patch.dict(check_linked_jira_issues.__globals__, asf_jira=client, jira_disabled=True),
    ...     redirect_stdout(output),
    ... ):
    ...     check_linked_jira_issues("123", "[SPARK-60328][SQL] Fix")
    >>> client.issue.assert_not_called()
    >>> "skipping the Epic/Umbrella check" in output.getvalue()
    True

    Leading Epic and Umbrella tickets block the merge, all listed at once, before any
    confirmation.

    >>> try:
    ...     check("[SPARK-34261][SPARK-60328][SPARK-34262][SQL] Fix")
    ... except SystemExit:
    ...     pass
    >>> msg = stop.call_args.args[0]
    >>> "SPARK-34261 (Epic), SPARK-34262 (Umbrella)" in msg
    True
    >>> verify.assert_not_called()

    A FOLLOWUP tag suppresses the similarity check inside Revert/Reapply wrappers and
    under the FOLLOW-UP alias too, while a title without the tag is still checked.

    >>> for title in ('[SPARK-60328][SQL][FOLLOWUP] Fix', 'Revert "[SPARK-60328][SQL] Fix"',
    ...               '[SPARK-60328][SQL] Fix FOLLOWUP handling',
    ...               'Revert "[SPARK-60328][SQL][FOLLOW-UP] Fix"',
    ...               'Reapply "[SPARK-60328][FOLLOWUP][SQL][MINOR] Fix"'):
    ...     check(title)
    ...     print(verify.call_args.kwargs["is_followup"])
    True
    False
    False
    True
    True
    """,
    "reconcile_jira_affects_versions": """
    Blank input accepts the suggested version and appends it to the existing versions:

    >>> from contextlib import redirect_stdout
    >>> from io import StringIO
    >>> from types import SimpleNamespace
    >>> from unittest.mock import Mock, patch
    >>> issue = SimpleNamespace(
    ...     key="SPARK-1", fields=SimpleNamespace(versions=[SimpleNamespace(name="5.0.0")]))
    >>> available = {"5.0.0", "4.4.0"}
    >>> writer = Mock(spec=Jira)
    >>> with (
    ...     patch.dict(reconcile_jira_affects_versions.__globals__, jira_ops=writer),
    ...     patch("builtins.input", side_effect=["", "", KeyboardInterrupt]) as user_input,
    ...     redirect_stdout(StringIO()),
    ... ):
    ...     reconcile_jira_affects_versions(issue, ["4.4.0"], available, ["4.4.0"])
    >>> writer.update_affects_versions.assert_called_once_with(issue, ["5.0.0", "4.4.0"])
    >>> user_input.call_count
    2

    Explicitly skipping an eligible suggestion leaves the JIRA writer untouched:

    >>> writer.reset_mock()
    >>> with (
    ...     patch.dict(reconcile_jira_affects_versions.__globals__, jira_ops=writer),
    ...     patch("builtins.input", side_effect=["skip", KeyboardInterrupt]) as user_input,
    ...     redirect_stdout(StringIO()),
    ... ):
    ...     reconcile_jira_affects_versions(issue, ["4.4.0"], available, ["4.4.0"])
    >>> writer.mock_calls
    []
    >>> user_input.call_count
    1
    """,
    "jira_is_live": """
    The probe classifies what urlopen hit: a response -- even a 4xx -- means live, a 5xx or
    any non-HTTP error (DNS failure, connection refused, TLS error) means dead, and the
    reason names it so the committer can tell an ASF outage from a local network problem.

    >>> from unittest.mock import MagicMock, Mock, patch
    >>> from urllib.error import HTTPError, URLError
    >>> import socket
    >>> def probe(error=None):
    ...     opener = MagicMock() if error is None else Mock(side_effect=error)
    ...     with patch.dict(jira_is_live.__globals__, urlopen=opener):
    ...         return jira_is_live()
    >>> probe()
    (True, None)
    >>> probe(HTTPError("https://issues.apache.org/jira", 404, "Not Found", {}, None))
    (True, None)
    >>> probe(HTTPError("https://issues.apache.org/jira", 503, "Service Unavailable", {}, None))
    (False, 'server answered HTTP 503')
    >>> probe(URLError("connection refused"))
    (False, 'URLError: <urlopen error connection refused>')
    >>> live, reason = probe(socket.timeout("timed out"))
    >>> live, "timed out" in reason
    (False, True)
    """,
    "initialize_jira": """
    An unreachable or unhealthy JIRA disables this run's JIRA steps without touching the
    client library or credentials, which are only checked once the probe says JIRA is live.

    >>> from contextlib import redirect_stdout
    >>> from io import StringIO
    >>> from unittest.mock import Mock, patch
    >>> def initialize_with_probe(result):
    ...     output = StringIO()
    ...     with (
    ...         patch.dict(initialize_jira.__globals__, jira_is_live=Mock(return_value=result)),
    ...         redirect_stdout(output),
    ...     ):
    ...         initialize_jira()
    ...         g = initialize_jira.__globals__
    ...         return (g["jira_disabled"], g["asf_jira"]), output.getvalue()
    >>> state, output = initialize_with_probe((False, "server answered HTTP 503"))
    >>> state
    (True, None)
    >>> "JIRA is unreachable or unhealthy (server answered HTTP 503)" in output
    True
    >>> "disabling all JIRA steps for this merge" in output
    True
    """,
    "jira_ids_to_verify": """
    The leading ids of the title feed the Epic/Umbrella and match checks; with JIRA disabled
    they are dropped -- with a reminder naming them, or the generic fallback -- so the checks
    are skipped rather than failing on asf_jira being None.

    >>> from contextlib import redirect_stdout
    >>> from io import StringIO
    >>> from unittest.mock import patch
    >>> def ids_for(title, disabled):
    ...     output = StringIO()
    ...     with (
    ...         patch.dict(jira_ids_to_verify.__globals__, jira_disabled=disabled),
    ...         redirect_stdout(output),
    ...     ):
    ...         return jira_ids_to_verify(title), output.getvalue()
    >>> ids_for("[SPARK-1234][SPARK-5678][SQL] Fix", False)
    (['SPARK-1234', 'SPARK-5678'], '')
    >>> ids, output = ids_for("[SPARK-1234][SQL] Fix mentions SPARK-9999", True)
    >>> ids, "Confirm SPARK-1234 manually" in output
    ([], True)
    >>> ids, output = ids_for("[SQL] Fix", True)
    >>> ids, "the linked ticket(s)" in output
    ([], True)
    """,
}


if __name__ == "__main__":
    import doctest

    failure_count, test_count = doctest.testmod()
    if failure_count:
        sys.exit(-1)
    try:
        main()
    except BaseException:
        clean_up()
        raise
