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
Prints the two candidate first-release versions for a change, so the caller can pick
one per the "Versioning and Branch Policy" section of AGENTS.md (numbers below are
illustrative; the actual versions advance as branches are cut):

    master       5.0.0        <- use for master-only changes
    branch-4.x   4.3.0        <- use for normally-backported changes (most PRs)

Choosing between them ("is this change master-only?") is a judgement call and is NOT
made here -- this script only reports the mechanical facts.

Takes no arguments. Reads from a local git remote pointing at apache/spark (the
`upstream` the AGENTS.md pre-flight has you configure); errors out if none exists,
rather than fetching full histories over the network into your working repo.

Usage: dev/next_version_candidates.py
"""

import re
import subprocess
import sys


def git(*args):
    """Runs a git command, exiting with its stderr on failure."""
    result = subprocess.run(["git", *args], capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        sys.exit("error: command failed: git " + " ".join(args))
    return result.stdout


def detect_remote():
    """Returns the name of the git remote pointing to apache/spark, or None."""
    for line in git("remote", "-v").splitlines():
        parts = line.split()
        if (
            len(parts) >= 3
            and parts[2] == "(fetch)"
            and re.search(r"[:/]apache/spark(\.git)?$", parts[1])
        ):
            return parts[0]
    return None


def latest_maintenance_branch(remote):
    """Returns the highest branch-<N>.x on the remote (e.g. branch-10.x > branch-4.x)."""
    branches = []
    for line in git("ls-remote", "--heads", remote, "refs/heads/branch-*.x").splitlines():
        name = line.rsplit("/", 1)[-1]
        match = re.fullmatch(r"branch-(\d+)\.x", name)
        if match:
            branches.append((int(match.group(1)), name))
    return max(branches)[1] if branches else None


def pom_version(remote, ref):
    """Fetches `ref` and returns its pom.xml project version with -SNAPSHOT stripped."""
    git("fetch", "--quiet", remote, ref)
    pom = git("show", "FETCH_HEAD:pom.xml")
    match = re.search(r"<version>([^<]+)-SNAPSHOT</version>", pom)
    if match is None:
        sys.exit(f"error: no -SNAPSHOT project version found in {ref}:pom.xml")
    return match.group(1)


def main():
    remote = detect_remote()
    if remote is None:
        sys.exit(
            "error: no git remote points to apache/spark. Add one and retry:\n"
            "    git remote add upstream https://github.com/apache/spark.git"
        )

    branch = latest_maintenance_branch(remote)
    if branch is None:
        sys.exit(f"error: no branch-<N>.x maintenance branch found on remote '{remote}'")

    print(f"{'master':<12} {pom_version(remote, 'master')}")
    print(f"{branch:<12} {pom_version(remote, branch)}")


if __name__ == "__main__":
    main()
