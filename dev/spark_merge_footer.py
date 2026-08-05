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
Shared reader for the merge footer that `dev/merge_spark_pr.py` writes into every commit
it creates, so the committer tools cannot disagree about where a pull request landed.

`merge_pr` ends each message it generates with

    Closes #<pr> from <author>/<branch>.

    Authored-by: A <a@example.org>
    Signed-off-by: C <c@example.org>

and `git cherry-pick -x` copies that footer verbatim into every backport, appending its own
provenance lines after it. The footer is therefore the signal that identifies both a merge
and its backports -- `git ... --contains <merge_hash>` cannot, because a cherry-pick is a
new commit that no other branch contains.

Two properties make reading it reliable, and both are easy to get wrong:

- A PR body is passed through as its own `git commit -m` paragraph, so it may quote another
  commit's footer in full, structure included. Only *position* distinguishes the generated
  footer: `merge_pr` appends it last, so the generated one is the final "Closes" paragraph.
- `git log --grep` matches its pattern anywhere in a message, so it can only narrow the
  walk; every candidate it returns must still be validated with `has_merge_footer`.

When imported, nothing here exits, prints, or runs git: callers pass a `run_git` callable and
so keep their own error-handling policy -- `dev/pr_merge_status.py` exits on a git failure,
while `dev/merge_spark_pr.py` must not abort a merge in progress. (Running this file directly
executes its doctests and exits nonzero if any fail.)

Refresh policy: `branches_with_merge_footer` reads local remote-tracking refs only and
never fetches. A caller that needs current data fetches first (as `pr_merge_status.py`
does, best-effort); a caller that must not touch the network mid-run simply accepts that a
branch not yet fetched goes unreported.
"""

import re

# The generated footer: a "Closes #<pr> from <ref>" line alone on its paragraph, followed by
# the authors paragraph. `\s*$` tolerates trailing whitespace. Requiring the blank line and
# the authors line rejects prose that merely mentions a PR; taking the *last* match (see
# `merge_footer_pr`) rejects a body that quotes a real footer.
_MERGE_FOOTER_RE = re.compile(
    r"^Closes #(\d+) from \S+\s*$\n\n(?:Lead-authored-by|Authored-by):",
    re.MULTILINE,
)


def merge_footer_trailer(pr_num):
    """The literal fragment to pass to `git log --fixed-strings --grep`.

    Only a prefilter to narrow the walk: it matches anywhere in a message, so callers
    validate each candidate with `has_merge_footer`.

    >>> merge_footer_trailer(1)
    'Closes #1 from '
    """
    return "Closes #%s from " % pr_num


def merge_footer_pr(message):
    """The PR number in `message`'s generated merge footer, or None if it has none.

    Reads the *last* "Closes" paragraph, since a PR body copied into the message may quote
    an earlier one. Cherry-pick provenance lines may follow the footer, but no later
    "Closes" paragraph can.

    >>> footer = "Closes #1 from a/b.\\n\\nAuthored-by: A <a@e.org>\\nSigned-off-by: C <c@e.org>"
    >>> merge_footer_pr("[SPARK-1][SQL] Title\\n\\nSome body.\\n\\n" + footer)
    1
    >>> merge_footer_pr("[SPARK-1][SQL] Title\\n\\n" + footer.replace("Authored", "Lead-authored"))
    1
    >>> merge_footer_pr("[SPARK-1][SQL] Title\\n\\nNo footer here.") is None
    True

    A cherry-pick keeps the footer, with `-x` provenance appended after it:

    >>> pick = footer + "\\n(cherry picked from commit abc123)\\nSigned-off-by: C <c@e.org>"
    >>> merge_footer_pr("[SPARK-1][SQL] Title\\n\\n" + pick)
    1

    A body quoting another PR's complete footer does not shadow the real one:

    >>> quoted = "Reverting:\\n\\n" + footer + "\\n\\nSee above."
    >>> own = footer.replace("#1", "#2")
    >>> merge_footer_pr("[SPARK-2][SQL] Later\\n\\n%s\\n\\n%s" % (quoted, own))
    2
    """
    matches = _MERGE_FOOTER_RE.findall(message)
    return int(matches[-1]) if matches else None


def has_merge_footer(message, pr_num):
    """Whether `message`'s generated merge footer closes `pr_num`. See `merge_footer_pr`.

    `pr_num` may be an int or a string of digits: callers get the PR number from argv or from
    the GitHub API, and comparing those two forms directly would silently never match.

    >>> footer = "Closes #1 from a/b.\\n\\nAuthored-by: A <a@e.org>\\nSigned-off-by: C <c@e.org>"
    >>> has_merge_footer("[SPARK-1][SQL] Title\\n\\n" + footer, 1)
    True
    >>> has_merge_footer("[SPARK-1][SQL] Title\\n\\n" + footer, "1")
    True
    >>> has_merge_footer("[SPARK-1][SQL] Title\\n\\n" + footer, 2)
    False

    A commit whose body quotes another PR's full footer is not taken for that PR's merge:

    >>> quoted = "Reverting:\\n\\n" + footer + "\\n\\nSee above."
    >>> later = "[SPARK-2][SQL] Later\\n\\n%s\\n\\n%s" % (quoted, footer.replace("#1", "#2"))
    >>> has_merge_footer(later, 1)
    False
    >>> has_merge_footer(later, 2)
    True
    """
    return merge_footer_pr(message) == int(pr_num)


def parse_commit_records(out):
    """Parse `git log --format='%H %B%x00'` output into (commit_hash, message) pairs.

    A commit message spans lines, so records are NUL-delimited rather than newline-delimited.

    >>> parse_commit_records("abc first\\nline two\\x00def second\\x00")
    [('abc', 'first\\nline two'), ('def', 'second')]
    >>> parse_commit_records("")
    []
    """
    records = []
    for record in out.split("\0"):
        record = record.strip("\n")
        if not record:
            continue
        commit_hash, _, message = record.partition(" ")
        records.append((commit_hash, message))
    return records


def branch_names_from_refs(out, remote):
    """Branch names in `git for-each-ref --format='%(refname:short)'` output for `remote`.

    Real branches are "<remote>/<branch>"; the remote's HEAD symref shortens to the bare
    remote name, so anything without the "<remote>/" prefix is skipped, as is the explicit
    "<remote>/HEAD" form.

    >>> sorted(branch_names_from_refs("up/master\\nup/branch-4.x\\nup\\nup/HEAD\\n", "up"))
    ['branch-4.x', 'master']
    """
    prefix = remote + "/"
    names = set()
    for ref in out.splitlines():
        if not ref.startswith(prefix):
            continue
        name = ref[len(prefix) :]
        if name != "HEAD":
            names.add(name)
    return names


def branches_with_merge_footer(pr_num, remote, run_git):
    """Map each `remote` branch carrying `pr_num`'s merge footer to the commit that has it.

    `run_git(args)` runs `git` with `args` and returns its stdout; the caller supplies it so
    this module imposes no error-handling or exit policy of its own. Reads local
    remote-tracking refs only -- see this module's refresh policy.

    Scoping the walk to `--remotes=<remote>` keeps fork refs and tags from adding noise or
    cost. Every commit `--grep` returns is validated before its branches count, so a commit
    that merely quotes the trailer cannot make a branch look like it has the change.
    """
    out = run_git(
        [
            "log",
            "--remotes=%s" % remote,
            "--fixed-strings",
            "--grep",
            merge_footer_trailer(pr_num),
            "--format=%H %B%x00",
        ]
    )
    landed = {}
    for commit_hash, message in parse_commit_records(out):
        if not has_merge_footer(message, pr_num):
            continue
        refs = run_git(
            [
                "for-each-ref",
                "--contains",
                commit_hash,
                "--format=%(refname:short)",
                "refs/remotes/%s/" % remote,
            ]
        )
        for branch in branch_names_from_refs(refs, remote):
            landed[branch] = commit_hash
    return landed


if __name__ == "__main__":
    import doctest
    import sys

    failure_count, test_count = doctest.testmod()
    if failure_count:
        sys.exit(-1)
