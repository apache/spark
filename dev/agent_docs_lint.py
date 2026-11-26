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
Checks the repository's agent instruction files (AGENTS.md / CLAUDE.md) and their detail
files for the properties that coding agents such as Claude Code and Codex depend on. A detail
file is a Markdown file directly in a .agents/ directory; subdirectories such as
.agents/skills/ are loaded by the tools themselves and are not checked here.

  pairing   Each directory with agent instructions has both names, one a symlink to the
            other, as `.agents/agent-docs.md` asks. Codex reads AGENTS.md (or AGENTS.override.md
            when present) and ignores CLAUDE.md by default. Claude Code reads AGENTS.md only
            when no CLAUDE.md exists in the working directory or above it, so once the root
            has a CLAUDE.md, a nested directory with only an AGENTS.md is skipped. Two
            different files let the two tools follow different instructions.
  codex-cap Codex concatenates AGENTS.md from the repo root down to its working directory
            into one budget of project_doc_max_bytes (32768 by default), filled root first,
            and stops past it without warning. Every root-to-directory chain must fit.
  budget    Soft size targets that keep always-loaded instructions small: the root file
            and nested files. Detail belongs in a sibling .agents/ directory behind a
            pointer. Warnings only.
  pointers  Every `.agents/...`, `AGENTS.md` or `CLAUDE.md` path quoted in an instruction or
            detail file resolves to an existing file. A pointer that starts with .agents/
            names the .agents/ directory beside its index; other paths may be relative to
            the file's directory or any ancestor, e.g. a path from the repo root.
  orphans   Every detail file is reachable through pointers from an instruction file. An
            agent reads a detail file only by following a pointer to it.

Known failures that cannot be fixed yet (e.g. vendored third-party docs) can be exempted in
an optional exceptions file, dev/agent_docs_lint_exceptions.txt by default. Each line is
`<check> <subject>`, where the subject is what the error names: the directory for pairing,
the file where truncation starts for codex-cap, `<file> <target>` for pointers, and the
detail file for orphans. `#` starts a comment. Exempted errors are still reported but do not
fail --strict. An entry that no longer matches any error is itself an error, so the list only
shrinks as files are fixed.

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
CHECKS = ("pairing", "codex-cap", "budget", "pointers", "orphans")
EXEMPTIBLE = ("pairing", "codex-cap", "pointers", "orphans")
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
    """A Markdown file directly in a .agents/ directory."""
    return path.endswith(".md") and os.path.basename(os.path.dirname(path)) == ".agents"


def codex_selected(directory):
    """The one file Codex loads in `directory`, if any (override > AGENTS.md)."""
    for name in ("AGENTS.override.md", "AGENTS.md"):
        path = os.path.join(directory, name)
        if os.path.isfile(path):
            return path
    return None


def real(path):
    """`path` with symlinks resolved, relative to the repo root."""
    return os.path.relpath(os.path.realpath(path))


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
        elif not os.path.isfile(claude):
            msg = "CLAUDE.md is a dangling symlink; make it a symlink to AGENTS.md"
        elif real(claude) != real(agents):
            msg = (
                "AGENTS.md and CLAUDE.md are different files, so Claude Code and Codex can "
                "follow different instructions. Keep one real file and make the other a "
                "symlink to it"
            )
        elif real(claude) != real(codex_selected(d)):
            msg = (
                "AGENTS.override.md is a different file, so Codex reads it instead of "
                "AGENTS.md while Claude Code reads AGENTS.md. Move its content into AGENTS.md "
                "and delete it"
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
                chain.append((f, os.path.getsize(f)))
        total = sum(n for _, n in chain)
        if total > CODEX_CAP:
            used = 0
            for f, n in chain:
                if used + n > CODEX_CAP:
                    cuts[f].append((d or ".", total))
                    break
                used += n
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
        n = os.path.getsize(f)
        if n > budget:
            warnings.append(
                f"{f}: {n} bytes > {budget} target; move branch-only detail into .agents/ "
                "behind a pointer"
            )
    return [], warnings


def resolve(f, target):
    """The file that the pointer `target` in `f` names (see real()), or None if missing."""
    if target.startswith(".agents/"):
        # The .agents/ directory beside the index: the file's own directory, or for a detail
        # file, the directory that holds its .agents/.
        home = os.path.dirname(f)
        if is_detail_file(f):
            home = os.path.dirname(home)
        bases = [home]
    else:
        parts = os.path.dirname(f).split("/") if os.path.dirname(f) else []
        bases = ["/".join(parts[:i]) for i in range(len(parts), -1, -1)]
    for base in bases:
        candidate = os.path.normpath(os.path.join(base, target))
        if os.path.exists(candidate):
            return real(candidate)
    return None


def scan_pointers(files):
    """Returns [(file, lineno, target, resolved)] for every pointer in `files`."""
    found = []
    for f in files:
        if os.path.islink(f):
            continue
        with open(f, encoding="utf-8", errors="replace") as fh:
            for lineno, line in enumerate(fh, 1):
                for target in POINTER_RE.findall(line):
                    if "..." in target.split("/"):
                        continue  # An elided path such as `a/.../AGENTS.md`, not a pointer.
                    found.append((f, lineno, target, resolve(f, target)))
    return found


def check_pointers(scan):
    errors = [
        (f"{f} {target}", f"{f}:{lineno}: pointer to missing file `{target}`")
        for f, lineno, target, resolved in scan
        if resolved is None
    ]
    return errors, []


def check_orphans(scan, files, details):
    edges = defaultdict(set)
    for f, _, _, resolved in scan:
        if resolved:
            edges[real(f)].add(resolved)
    reached = set()
    todo = [real(f) for f in files if os.path.exists(f)]
    while todo:
        f = todo.pop()
        if f not in reached:
            reached.add(f)
            todo.extend(edges[f])
    errors = [
        (d, f"{d}: no pointer reaches it from an AGENTS.md, so agents never read it")
        for d in details
        if real(d) not in reached
    ]
    return errors, []


def load_exceptions(path, required):
    """Returns ({check: {subject: lineno}}, errors) for the exceptions file."""
    exceptions = defaultdict(dict)
    if not os.path.isfile(path):
        return exceptions, [f"{path}: exceptions file not found"] if required else []
    errors = []
    with open(path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            parts = line.split(None, 1)
            if len(parts) != 2 or parts[0] not in EXEMPTIBLE:
                errors.append(
                    f"{path}:{lineno}: expected `<check> <subject>` with <check> one of "
                    f"{', '.join(EXEMPTIBLE)}"
                )
                continue
            check, subject = parts[0], " ".join(parts[1].split())
            if subject in exceptions[check]:
                errors.append(
                    f"{path}:{lineno}: duplicate of line {exceptions[check][subject]}; remove it"
                )
                continue
            exceptions[check][subject] = lineno
    return exceptions, errors


def main(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--strict", action="store_true", help="exit 1 if any check reports an error")
    ap.add_argument(
        "--only", choices=CHECKS, action="append", help="run only this check (repeatable)"
    )
    ap.add_argument(
        "--exceptions",
        help=f"exceptions file, which must exist (default: {EXCEPTIONS_FILE} in the repo root, "
        "if it exists)",
    )
    args = ap.parse_args(argv)
    # An explicit path is relative to the current directory, so resolve it before the chdir.
    exceptions_path = os.path.abspath(args.exceptions) if args.exceptions else None
    os.chdir(repo_root())
    if exceptions_path is None:
        shown_path, exceptions_path, required = EXCEPTIONS_FILE, EXCEPTIONS_FILE, False
    else:
        shown_path, required = args.exceptions, True

    all_files = tracked_files()
    files = [p for p in all_files if os.path.basename(p) in NAMES]
    details = [p for p in all_files if is_detail_file(p)]
    dirs = defaultdict(set)
    for f in files:
        dirs[os.path.dirname(f)].add(os.path.basename(f))
    scan = scan_pointers(files + details)

    runners = {
        "pairing": lambda: check_pairing(dirs),
        "codex-cap": lambda: check_codex_cap(dirs),
        "budget": lambda: check_budget(files),
        "pointers": lambda: check_pointers(scan),
        "orphans": lambda: check_orphans(scan, files, details),
    }
    exceptions, malformed = load_exceptions(exceptions_path, required)
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
            f"{shown_path}:{lineno}: stale exception `{name} {s}`; it matches no error, "
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
            print(f"        (to exempt a known failure, add `{name} {s}` to {shown_path})")
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
