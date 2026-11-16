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
            by default. Claude Code reads AGENTS.md only when no CLAUDE.md exists in the
            working directory or above it, so once the root has a CLAUDE.md, a nested
            directory with only an AGENTS.md is skipped. Two real files let the two tools
            follow different instructions.
  codex-cap Codex concatenates AGENTS.md from the repo root down to its working directory
            into one budget of project_doc_max_bytes (32768 by default), filled root first,
            and silently truncates past it. Every root-to-directory chain must fit.
  budget    Soft size targets that keep always-loaded instructions small: the root file
            and nested files. Detail belongs in a sibling .agents/ directory behind a
            pointer. Warnings only.
  pointers  Every `.agents/...`, `AGENTS.md` or `CLAUDE.md` path quoted in an instruction
            file, including the files under .agents/, resolves to an existing file.

Known failures that cannot be fixed yet (e.g. vendored third-party docs) can be exempted in
an optional exceptions file, dev/agent_docs_lint_exceptions.txt by default. Each line is
`<check> <subject>`, where the subject is what the error names: the directory for pairing,
the file where truncation starts for codex-cap, and `<file> <target>` for pointers. `#`
starts a comment. Exempted errors are still reported but do not fail --strict. An entry that
no longer matches any error is itself an error, so the list only shrinks as files are fixed.

Report-only by default; pass --strict to exit non-zero on errors (not on budget warnings).
CI runs it with --strict.

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
EXEMPTIBLE = ("pairing", "codex-cap", "pointers")
EXCEPTIONS_FILE = "dev/agent_docs_lint_exceptions.txt"
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


# Each check returns (errors, warnings). An error is a (subject, message) pair; the subject is
# what an exceptions-file entry names to exempt it.


def check_pairing(dirs):
    errors = []
    for d, names in sorted(dirs.items()):
        shown = d or "."
        agents = os.path.join(d, "AGENTS.md")
        claude = os.path.join(d, "CLAUDE.md")
        if "AGENTS.md" not in names:
            msg = (
                f"no AGENTS.md (found {', '.join(sorted(names))}). Make AGENTS.md the real "
                "file and CLAUDE.md a symlink to it"
            )
        elif not os.path.isfile(agents):
            msg = "AGENTS.md is a dangling symlink"
        elif "CLAUDE.md" not in names:
            msg = (
                "only AGENTS.md; add a CLAUDE.md symlink to it (ln -s AGENTS.md CLAUDE.md). "
                "By default Claude Code skips it when a CLAUDE.md exists in or above its "
                "working directory"
            )
        elif os.path.realpath(claude) != os.path.realpath(agents):
            msg = (
                "AGENTS.md and CLAUDE.md are different files, so Claude Code and Codex can "
                "follow different instructions. Keep one real file and make the other a "
                "symlink to it"
            )
        else:
            continue
        errors.append((shown, f"{shown}: {msg}"))
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
        msg = (
            f"{cut_in}: Codex truncates here in {len(chains)} chain(s); worst is "
            f"{worst[0]} at {worst[1]} bytes, {worst[1] - CODEX_CAP} bytes never seen"
        )
        errors.append((cut_in, msg))
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
                    if "..." in target.split("/"):
                        continue  # An elided path such as `a/.../AGENTS.md`, not a pointer.
                    candidates = [os.path.normpath(os.path.join(b, target)) for b in bases]
                    if not any(os.path.exists(c) for c in candidates):
                        msg = f"{f}:{lineno}: pointer to missing file `{target}`"
                        errors.append((f"{f} {target}", msg))
    return errors, []


def load_exceptions(path):
    """Returns ({check: {subject: lineno}}, errors) for the exceptions file, if it exists."""
    exceptions = defaultdict(dict)
    errors = []
    if not os.path.isfile(path):
        return exceptions, errors
    with open(path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            check, _, subject = line.partition(" ")
            subject = " ".join(subject.split())
            if check not in EXEMPTIBLE or not subject:
                errors.append(
                    f"{path}:{lineno}: expected `<check> <subject>` with <check> one of "
                    f"{', '.join(EXEMPTIBLE)}"
                )
                continue
            exceptions[check][subject] = lineno
    return exceptions, errors


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--strict", action="store_true", help="exit 1 if any check reports an error")
    ap.add_argument(
        "--only", choices=CHECKS, action="append", help="run only this check (repeatable)"
    )
    ap.add_argument(
        "--exceptions",
        default=EXCEPTIONS_FILE,
        help=f"exceptions file, relative to the repo root (default: {EXCEPTIONS_FILE})",
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
    exceptions, malformed = load_exceptions(args.exceptions)
    total_errors = len(malformed)
    if malformed:
        print(f"== exceptions: {len(malformed)} error(s)")
        for e in malformed:
            print(f"  ERROR {e}")
    for name in args.only or CHECKS:
        errors, warnings = runners[name]()
        exempt = exceptions.get(name, {})
        failing = [(s, m) for s, m in errors if s not in exempt]
        exempted = [m for s, m in errors if s in exempt]
        matched = {s for s, _ in errors}
        stale = [
            f"{args.exceptions}:{lineno}: stale exception `{name} {s}`; it matches no error, "
            "so remove it"
            for s, lineno in sorted(exempt.items(), key=lambda e: e[1])
            if s not in matched
        ]
        total_errors += len(failing) + len(stale)
        print(
            f"== {name}: {len(failing) + len(stale)} error(s), {len(exempted)} exempted, "
            f"{len(warnings)} warning(s)"
        )
        for s, m in failing:
            print(f"  ERROR {m}")
            print(f"        (to exempt a known failure, add `{name} {s}` to {args.exceptions})")
        for m in stale:
            print(f"  ERROR {m}")
        for m in exempted:
            print(f"  EXEMPT {m}")
        for w in warnings:
            print(f"  WARN  {w}")
    print(f"{len(dirs)} directories, {len(files) + len(details)} files checked")
    return 1 if args.strict and total_errors else 0


if __name__ == "__main__":
    sys.exit(main())
