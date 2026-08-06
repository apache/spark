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

"""
Reports which apache/spark branches a pull request was merged to, so the caller does
not have to reason about it by hand (see the "Checking PR Merge Status" section of
AGENTS.md for why this is non-obvious):

    $ dev/pr_merge_status.py 56356
    PR #56356 (base master): [SPARK-57295][SQL] Make database location validation ...
    merged: yes
      master       9357bc9ae05
      branch-4.x   72edddb358e

Spark merges with dev/merge_spark_pr.py rather than the GitHub button, so a merged PR
shows up as "Closed" (not "Merged") with empty merge metadata, and backports are plain
cherry-pick pushes. This script does not rely on that metadata. The merge script writes
a closing trailer "Closes #<pr> from <author>/<branch>" into the master commit, and
every cherry-pick of it to a maintenance branch retains that trailer; the script finds
the commits carrying it and reports each branch that contains one. It reports the
mechanical facts only -- whether a change *should* have been backported is a judgement
call and is NOT made here.

It first asks GitHub (via the `gh` CLI) whether the PR is open: an open PR cannot have
merged anywhere, so it returns immediately without touching git. Otherwise it fetches
`master` and the `branch-*` refs from the apache/spark remote in a single pass and reads
branch containment from local git. So it needs `gh` (authenticated) and a non-shallow
checkout.

It lists `master` and the latest major's release branches the commit reached (e.g.
branch-4.x, branch-4.2); the header repeats the base branch for context. Other majors'
branches are omitted to keep the focus current, except the PR's own base-branch major, so
a PR opened against an older release line (a backport) still shows its merge there. Pass
--all-branches to instead list every branch the commit reached, including omitted older
majors.

Caveat: it reports where the PR's commits landed, not whether one was later reverted on
a branch (rare; visible in the PR conversation).

Usage: dev/pr_merge_status.py <pr-number> [--all-branches]
"""

import re
import subprocess
import sys

REPO = "apache/spark"


def gh(path, jq=None, allow_404=False):
    """Calls `gh api <path>` and returns its stdout (filtered by `jq` if given). Returns
    None on a 404 when `allow_404`. Exits with a clear message if `gh` is missing,
    unauthenticated, or otherwise fails."""
    args = ["gh", "api", path]
    if jq is not None:
        args += ["--jq", jq]
    try:
        result = subprocess.run(args, capture_output=True, text=True, encoding="utf-8")
    except FileNotFoundError:
        sys.exit(
            "error: the GitHub CLI `gh` is required but was not found.\n"
            "    Install it from https://cli.github.com/ and run `gh auth login`."
        )
    if result.returncode != 0:
        if allow_404 and "(HTTP 404)" in result.stderr:
            return None
        sys.stderr.write(result.stderr)
        sys.exit("error: `gh api %s` failed. Is `gh` authenticated? Check `gh auth status`." % path)
    return result.stdout


def git(*args, check=True):
    """Runs a git command. Returns stdout; on failure, exits (check=True) or returns None
    (check=False, used for the best-effort fetch)."""
    result = subprocess.run(["git", *args], capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        if not check:
            return None
        sys.stderr.write(result.stderr)
        sys.exit("error: command failed: git " + " ".join(args))
    return result.stdout


def detect_remote():
    """Returns the name of the git remote pointing to apache/spark, or None. A clone may
    have several aliases for the same upstream; any one works (the script fetches it
    before reading, so its refs are current), and picking just one keeps the branch
    lookups fast. Preference order: the remote named `upstream` (the AGENTS.md convention)
    first; then SSH over HTTPS (key auth, no credential prompts); then declaration order
    in `git remote -v`."""
    matches = []  # (name, url) for each fetch remote whose URL is apache/spark
    seen = set()
    for line in git("remote", "-v").splitlines():
        parts = line.split()
        if (
            len(parts) >= 3
            and parts[2] == "(fetch)"
            and re.search(r"[:/]apache/spark(\.git)?$", parts[1])
            and parts[0] not in seen
        ):
            seen.add(parts[0])
            matches.append((parts[0], parts[1]))
    if not matches:
        return None

    def rank(name_url):
        name, url = name_url
        is_https = url.startswith(("http://", "https://"))
        return (name != "upstream", is_https)

    # sorted() is stable, so ties keep `git remote -v` order.
    return sorted(matches, key=rank)[0][0]


def branch_major(name):
    """Returns the major version of a `branch-<major>.<minor>` / `branch-<major>.x` name,
    or None for `master` and anything that isn't a release branch."""
    match = re.fullmatch(r"branch-(\d+)\.(?:x|\d+)", name)
    return int(match.group(1)) if match else None


def latest_major(remote):
    """Returns the highest release-branch major version present in `remote`'s tracking refs,
    or None if there is none. Call after fetch_branches: the incremental `branch-*` fetch
    brings in a brand-new major's branch too, so reading local refs here reflects the remote
    without a second network round-trip."""
    out = git("for-each-ref", "--format=%(refname:short)", "refs/remotes/%s/branch-*" % remote)
    names = [ref.split("/", 1)[1] for ref in out.splitlines() if "/" in ref]
    majors = [m for m in (branch_major(n) for n in names) if m is not None]
    return max(majors) if majors else None


def is_relevant(branch, majors):
    """A branch is in scope when it is `master` or a release branch whose major is in
    `majors` (the latest major, plus the PR's own base-branch major)."""
    return branch == "master" or branch_major(branch) in majors


def fetch_branches(remote):
    """Updates the remote-tracking refs for `master` and every `branch-*` from `remote`, so
    containment below reflects the latest state. Fetching all release branches in one pass --
    rather than first asking the remote for the latest major and fetching only that -- is
    intentional: an incremental fetch transfers no objects for unchanged (end-of-life)
    branches, so it costs the same as a targeted fetch while saving a second network
    round-trip, and it still brings in a brand-new major's branch. Best-effort: a fetch
    failure (e.g. offline) warns and falls back to whatever refs are already local."""
    if (
        git(
            "fetch",
            "--quiet",
            remote,
            "+refs/heads/master:refs/remotes/%s/master" % remote,
            "+refs/heads/branch-*:refs/remotes/%s/branch-*" % remote,
            check=False,
        )
        is None
    ):
        sys.stderr.write(
            "warning: `git fetch %s` failed; using already-fetched branches, which may be "
            "stale (a recently merged PR could look unmerged).\n" % remote
        )


def commits_with_trailer(trailer, remote):
    """Returns the full SHAs of commits on `remote`'s branches whose message contains
    `trailer`. Scoping to the one remote (rather than `--all`) keeps fork refs and tags
    from adding noise or walk cost."""
    out = git("log", "--remotes=%s" % remote, "--fixed-strings", "--grep", trailer, "--format=%H")
    return list(dict.fromkeys(out.split()))


def official_branches_containing(commit, remote):
    """Returns the `remote` branch names (e.g. 'master', 'branch-4.x') that contain
    `commit`, ignoring the remote's HEAD alias and any non-branch refs."""
    out = git(
        "for-each-ref",
        "--contains",
        commit,
        "--format=%(refname:short)",
        "refs/remotes/%s/" % remote,
    )
    prefix = remote + "/"
    branches = set()
    for ref in out.splitlines():
        # Real branches are "<remote>/<branch>"; the remote's HEAD symref shortens to the
        # bare remote name (e.g. "upstream") -- skip anything without the "<remote>/" prefix,
        # and the explicit "<remote>/HEAD" form for good measure.
        if not ref.startswith(prefix):
            continue
        name = ref[len(prefix) :]
        if name != "HEAD":
            branches.add(name)
    return branches


def display_key(name):
    """Sorts `master` first, then branch-<major>.<minor> ascending, with branch-<N>.x
    (the active dev line for the next feature release) after its numeric siblings."""
    if name == "master":
        return (0, 0, 0)
    match = re.fullmatch(r"branch-(\d+)\.(x|\d+)", name)
    if match is None:
        return (2, 0, name)
    minor = float("inf") if match.group(2) == "x" else int(match.group(2))
    return (1, int(match.group(1)), minor)


def main():
    args = sys.argv[1:]
    all_branches = "--all-branches" in args
    rest = [a for a in args if a != "--all-branches"]
    if len(rest) != 1 or rest[0].startswith("-"):
        sys.exit("Usage: dev/pr_merge_status.py <pr-number> [--all-branches]")
    pr = rest[0].lstrip("#")
    if not pr.isdigit():
        sys.exit("error: <pr-number> must be a number, got %r" % rest[0])

    info = gh(
        "repos/%s/pulls/%s" % (REPO, pr), jq="[.state, .base.ref, .title] | @tsv", allow_404=True
    )
    if info is None:
        sys.exit("error: PR #%s not found in %s" % (pr, REPO))
    fields = info.rstrip("\n").split("\t")
    state, base, title = fields[0], fields[1], fields[2] if len(fields) > 2 else ""
    print("PR #%s (base %s): %s" % (pr, base, title))

    # An open PR cannot have merged anywhere -- short-circuit before touching git.
    if state == "open":
        print("open -- not merged yet.")
        return

    remote = detect_remote()
    if remote is None:
        sys.exit(
            "error: no git remote points to apache/spark. Add one and retry:\n"
            "    git remote add upstream https://github.com/apache/spark.git"
        )
    fetch_branches(remote)
    # In scope: master, the latest major's branches, and the PR's own base-branch major --
    # the last so a PR opened against an older line (e.g. a branch-3.5 backport) still shows
    # its merge there, since a merge always lands on the base branch.
    majors = {m for m in (latest_major(remote), branch_major(base)) if m is not None}

    trailer = "Closes #%s from " % pr
    landed = {}
    for commit in commits_with_trailer(trailer, remote):
        for branch in official_branches_containing(commit, remote):
            if all_branches or is_relevant(branch, majors):
                landed[branch] = commit[:11]

    if landed:
        print("merged: yes")
        for branch in sorted(landed, key=display_key):
            print("  %-12s %s" % (branch, landed[branch]))
    else:
        print('closed without merging -- no "%s" commit found (rejected or superseded).' % trailer)


if __name__ == "__main__":
    main()
