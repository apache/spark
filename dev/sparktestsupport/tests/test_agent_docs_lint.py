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

import contextlib
import io
import os
import subprocess
import tempfile
import unittest

import agent_docs_lint as lint


class AgentDocsLintTestBase(unittest.TestCase):
    """Runs each test in a fresh git repository, which the lint reads from the working dir."""

    def setUp(self):
        self.old_cwd = os.getcwd()
        self.tmp = tempfile.TemporaryDirectory()
        os.chdir(self.tmp.name)
        subprocess.run(["git", "init", "-q"], check=True)

    def tearDown(self):
        os.chdir(self.old_cwd)
        self.tmp.cleanup()

    def write(self, path, text="x\n"):
        if os.path.dirname(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

    def link(self, path, target):
        if os.path.dirname(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
        os.symlink(target, path)

    def pair(self, directory="", text="x\n"):
        """An AGENTS.md with a CLAUDE.md symlink beside it."""
        self.write(os.path.join(directory, "AGENTS.md"), text)
        self.link(os.path.join(directory, "CLAUDE.md"), "AGENTS.md")

    def run_main(self, *args):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = lint.main(list(args))
        return code, out.getvalue()

    def subjects(self, check):
        files = [p for p in lint.tracked_files() if os.path.basename(p) in lint.NAMES]
        details = [p for p in lint.tracked_files() if lint.is_detail_file(p)]
        dirs = {}
        for f in files:
            dirs.setdefault(os.path.dirname(f), set()).add(os.path.basename(f))
        scan = lint.scan_pointers(files + details)
        errors, _ = {
            "pairing": lambda: lint.check_pairing(dirs),
            "codex-cap": lambda: lint.check_codex_cap(dirs),
            "pointers": lambda: lint.check_pointers(scan),
            "orphans": lambda: lint.check_orphans(scan, files, details),
        }[check]()
        return [s for s, _ in errors]


class PairingTests(AgentDocsLintTestBase):
    def test_symlinked_pair_passes(self):
        self.pair()
        self.pair("a")
        self.assertEqual(self.subjects("pairing"), [])

    def test_missing_or_divergent_files(self):
        self.pair()
        self.write("claude_only/CLAUDE.md")
        self.write("agents_only/AGENTS.md")
        self.write("two_files/AGENTS.md", "a\n")
        self.write("two_files/CLAUDE.md", "b\n")
        self.link("dangling_agents/AGENTS.md", "missing.md")
        self.link("dangling_agents/CLAUDE.md", "AGENTS.md")
        self.write("dangling_claude/AGENTS.md")
        self.link("dangling_claude/CLAUDE.md", "missing.md")
        self.pair("override")
        self.write("override/AGENTS.override.md", "different\n")
        self.assertEqual(
            self.subjects("pairing"),
            [
                "agents_only",
                "claude_only",
                "dangling_agents",
                "dangling_claude",
                "override",
                "two_files",
            ],
        )


class CodexCapTests(AgentDocsLintTestBase):
    def test_truncation_is_reported_where_it_starts(self):
        self.pair(text="r" * 20000)
        self.pair("a", "a" * 13000)
        self.pair("a/b")
        self.pair("small", "s" * 100)
        self.assertEqual(self.subjects("codex-cap"), ["a/AGENTS.md"])

    def test_override_counts_instead_of_agents_md(self):
        self.pair(text="r" * 30000)
        self.pair("a", "a" * 100)
        self.write("a/AGENTS.override.md", "o" * 5000)
        self.assertEqual(self.subjects("codex-cap"), ["a/AGENTS.override.md"])


class PointerTests(AgentDocsLintTestBase):
    def test_missing_targets(self):
        self.pair(text="`.agents/here.md` `.agents/gone.md` `a/AGENTS.md` `b/AGENTS.md`\n")
        self.write(".agents/here.md", "`.agents/also-gone.md`\n")
        self.pair("a")
        self.assertEqual(
            self.subjects("pointers"),
            [
                "AGENTS.md .agents/gone.md",
                "AGENTS.md b/AGENTS.md",
                ".agents/here.md .agents/also-gone.md",
            ],
        )

    def test_agents_dir_pointer_resolves_beside_its_index(self):
        # `.agents/x.md` in a/AGENTS.md means a/.agents/x.md, even though .agents/x.md exists.
        self.pair(text="`.agents/x.md`\n")
        self.write(".agents/x.md")
        self.pair("a", "`.agents/x.md` `.agents/y.md`\n")
        self.write("a/.agents/y.md", "`.agents/z.md`\n")
        self.write("a/.agents/z.md")
        self.assertEqual(self.subjects("pointers"), ["a/AGENTS.md .agents/x.md"])

    def test_other_paths_resolve_against_ancestors(self):
        self.pair()
        self.pair("a/b", "`c/AGENTS.md` `a/c/AGENTS.md` `a/.../AGENTS.md`\n")
        self.pair("a/c")
        self.assertEqual(self.subjects("pointers"), [])


class OrphanTests(AgentDocsLintTestBase):
    def test_unreachable_detail_files(self):
        self.pair(text="`.agents/linked.md`\n")
        self.write(".agents/linked.md", "`.agents/chained.md`\n")
        self.write(".agents/chained.md", "`.agents/chained.md`\n")
        self.write(".agents/unlinked.md", "`.agents/only-from-unlinked.md`\n")
        self.write(".agents/only-from-unlinked.md")
        self.write(".agents/skills/foo/SKILL.md")  # Not a detail file: loaded by the tool.
        self.assertEqual(
            sorted(self.subjects("orphans")),
            [".agents/only-from-unlinked.md", ".agents/unlinked.md"],
        )


class MainTests(AgentDocsLintTestBase):
    def setUp(self):
        super().setUp()
        self.pair(text="`.agents/gone.md`\n" + "b" * 9000)

    def test_strict_fails_on_errors_but_not_on_warnings(self):
        self.assertEqual(self.run_main()[0], 0)
        self.assertEqual(self.run_main("--strict")[0], 1)
        code, out = self.run_main("--strict", "--only", "budget")
        self.assertEqual(code, 0)
        self.assertIn("WARN  AGENTS.md", out)

    def test_exceptions_exempt_known_failures(self):
        self.write(lint.EXCEPTIONS_FILE, "# comment\npointers\tAGENTS.md   .agents/gone.md\n")
        code, out = self.run_main("--strict")
        self.assertEqual(code, 0, out)
        self.assertIn("EXEMPT AGENTS.md:1: pointer to missing file", out)

    def test_stale_duplicate_and_malformed_exceptions_fail(self):
        self.write(
            lint.EXCEPTIONS_FILE,
            "pointers AGENTS.md .agents/gone.md\n"
            "pointers AGENTS.md .agents/gone.md\n"
            "orphans .agents/fixed.md\n"
            "unknown-check x\n",
        )
        code, out = self.run_main("--strict")
        self.assertEqual(code, 1)
        self.assertIn(":2: duplicate of line 1", out)
        self.assertIn(":3: stale exception `orphans .agents/fixed.md`", out)
        self.assertIn(":4: expected `<check> <subject>`", out)

    def test_explicit_exceptions_path_is_relative_to_cwd_and_must_exist(self):
        self.write("dev/known.txt", "pointers AGENTS.md .agents/gone.md\n")
        os.chdir("dev")
        self.assertEqual(self.run_main("--strict", "--exceptions", "known.txt")[0], 0)
        os.chdir("dev")  # main() moved to the repo root.
        code, out = self.run_main("--strict", "--exceptions", "missing.txt")
        self.assertEqual(code, 1)
        self.assertIn("missing.txt: exceptions file not found", out)


if __name__ == "__main__":
    unittest.main()
