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
Checks the repository's agent instruction files (AGENTS.md / CLAUDE.md, and the detail files
in .agents/ directories) for the properties that coding agents such as Claude Code and Codex
depend on:

  pairing   Each directory with agent instructions has both names, one a symlink to the
            other, as the root AGENTS.md asks. Codex reads AGENTS.md and ignores CLAUDE.md
            by default; Claude Code reads CLAUDE.md. Two real files let the two tools follow
            different instructions, and a missing name can leave one tool without them.
  codex-cap Codex concatenates AGENTS.md from the repo root down to its working directory
            into one budget of project_doc_max_bytes (32768 by default), filled root first,
            and silently truncates past it. Every root-to-directory chain must fit.
  budget    Soft size targets that keep always-loaded instructions small: the root file
            and nested files. Detail belongs in a sibling .agents/ directory behind a
            pointer.
  pointers  Every `.agents/...`, `AGENTS.md` or `CLAUDE.md` path quoted in an instruction
            file, including the files under .agents/, resolves to an existing file.

Report-only by default; pass --strict to exit non-zero on errors (not on budget warnings).

    $ dev/agent_docs_lint.py
    $ dev/agent_docs_lint.py --strict --only codex-cap
"""

import argparse
import os
import re
import subprocess
import sys
from collections import defaultdict

CODEX_CAP = 32768
ROOT_BUDGET = 8 * 1024
NESTED_BUDGET = 4 * 1024
CHECKS = ("pairing", "codex-cap", "budget", "pointers")
NAMES = ("AGENTS.md", "CLAUDE.md", "AGENTS.override.md")
POINTER_RE = re.compile(r"`((?:[\w.-]+/)*(?:\.agents/[\w./-]+\.md|AGENTS\.md|CLAUDE\.md))`")


def repo_root():
    return subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()


def tracked_files():
    # Tracked plus untracked-but-not-ignored files, so a new AGENTS.md is checked before it is
    # staged. Skip tracked files deleted in the working tree.
    out = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], text=True
    )
    return sorted({p for p in out.split("\0") if p and os.path.lexists(p)})


def is_detail_file(path):
    """A Markdown file under a .agents/ directory."""
    return path.endswith(".md") and ".agents" in path.split("/")[:-1]


def codex_selected(directory):
    """The one file Codex loads in `directory`, if any (override > AGENTS.md)."""
    for name in ("AGENTS.override.md", "AGENTS.md"):
        path = os.path.join(directory, name)
        if os.path.isfile(path):
            return path
    return None


def size(path):
    with open(path, "rb") as f:
        return len(f.read())


def check_pairing(dirs):
    errors = []
    for d, names in sorted(dirs.items()):
        shown = d or "."
        agents = os.path.join(d, "AGENTS.md")
        claude = os.path.join(d, "CLAUDE.md")
        if "AGENTS.md" not in names:
            errors.append(
                f"{shown}: no AGENTS.md (found {', '.join(sorted(names))}). Make AGENTS.md the "
                "real file and CLAUDE.md a symlink to it"
            )
            continue
        if not os.path.isfile(agents):
            errors.append(f"{shown}: AGENTS.md is a dangling symlink")
            continue
        if "CLAUDE.md" not in names:
            errors.append(
                f"{shown}: only AGENTS.md; add a CLAUDE.md symlink to it "
                "(ln -s AGENTS.md CLAUDE.md)"
            )
            continue
        if os.path.realpath(claude) != os.path.realpath(agents):
            errors.append(
                f"{shown}: AGENTS.md and CLAUDE.md are different files; Claude Code reads "
                "CLAUDE.md and Codex reads AGENTS.md. Keep one real file and make the other a "
                "symlink to it"
            )
    return errors, []


def check_codex_cap(dirs):
    # Group by the file where truncation starts: one oversized ancestor cuts every chain
    # below it, and listing each of those chains separately hides the cause.
    cuts = defaultdict(list)
    for d in sorted(dirs):
        parts = d.split("/") if d else []
        chain = []
        for i in range(len(parts) + 1):
            f = codex_selected("/".join(parts[:i]))
            if f:
                chain.append(f)
        total = sum(size(f) for f in chain)
        if total > CODEX_CAP:
            used, cut_in = 0, None
            for f in chain:
                if used + size(f) > CODEX_CAP:
                    cut_in = f
                    break
                used += size(f)
            cuts[cut_in].append((d or ".", total))
    errors = []
    for cut_in, chains in sorted(cuts.items()):
        worst = max(chains, key=lambda c: c[1])
        errors.append(
            f"{cut_in}: Codex truncates here in {len(chains)} chain(s); worst is "
            f"{worst[0]} at {worst[1]} bytes, {worst[1] - CODEX_CAP} bytes never seen"
        )
    return errors, []


def check_budget(files):
    warnings = []
    for f in files:
        if os.path.islink(f):
            continue
        budget = ROOT_BUDGET if os.path.dirname(f) == "" else NESTED_BUDGET
        n = size(f)
        if n > budget:
            warnings.append(
                f"{f}: {n} bytes > {budget} target; move branch-only detail into .agents/ "
                "behind a pointer"
            )
    return [], warnings


def check_pointers(files):
    errors = []
    for f in files:
        if os.path.islink(f):
            continue
        # Pointers may be written relative to the file or to any ancestor directory (e.g. a
        # nested AGENTS.md naming `.agents/x.md` beside it, or a file under .agents/ naming
        # a sibling as `.agents/y.md`).
        parts = os.path.dirname(f).split("/") if os.path.dirname(f) else []
        bases = ["/".join(parts[:i]) for i in range(len(parts), -1, -1)]
        with open(f, encoding="utf-8", errors="replace") as fh:
            for lineno, line in enumerate(fh, 1):
                for target in POINTER_RE.findall(line):
                    candidates = [os.path.normpath(os.path.join(b, target)) for b in bases]
                    if not any(os.path.exists(c) for c in candidates):
                        errors.append(f"{f}:{lineno}: pointer to missing file `{target}`")
    return errors, []


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--strict", action="store_true", help="exit 1 if any check reports an error")
    ap.add_argument(
        "--only", choices=CHECKS, action="append", help="run only this check (repeatable)"
    )
    args = ap.parse_args()
    os.chdir(repo_root())

    all_files = tracked_files()
    files = [p for p in all_files if os.path.basename(p) in NAMES]
    details = [p for p in all_files if is_detail_file(p)]
    dirs = defaultdict(set)
    for f in files:
        dirs[os.path.dirname(f)].add(os.path.basename(f))

    runners = {
        "pairing": lambda: check_pairing(dirs),
        "codex-cap": lambda: check_codex_cap(dirs),
        "budget": lambda: check_budget(files),
        "pointers": lambda: check_pointers(files + details),
    }
    total_errors = 0
    for name in args.only or CHECKS:
        errors, warnings = runners[name]()
        total_errors += len(errors)
        print(f"== {name}: {len(errors)} error(s), {len(warnings)} warning(s)")
        for e in errors:
            print(f"  ERROR {e}")
        for w in warnings:
            print(f"  WARN  {w}")
    print(f"{len(dirs)} directories, {len(files) + len(details)} files checked")
    return 1 if args.strict and total_errors else 0


if __name__ == "__main__":
    sys.exit(main())
