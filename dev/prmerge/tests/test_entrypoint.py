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
Tests for the `__main__` block of `dev/merge_spark_pr.py` and for `run_prmerge_tests`.
"""

import ast
import doctest
import io
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import merge_spark_pr


def _entrypoint():
    source = ast.parse(Path(merge_spark_pr.__file__).read_bytes())
    return compile(
        ast.Module(body=[source.body[-1]], type_ignores=[]),
        merge_spark_pr.__file__,
        "exec",
    )


def _commands(git_run):
    commands = []
    for call in git_run.call_args_list:
        arg = call.args[0]
        text = arg if isinstance(arg, str) else " ".join(arg)
        commands.append(text.split()[1])
    return commands


class EntrypointTests(unittest.TestCase):
    def setUp(self):
        self.code = _entrypoint()
        self.refused = subprocess.CalledProcessError(1, "git checkout")

    def _exec(self, main, *, leave_flag_on=False, suite_passes=True):
        overrides = {"original_head": "master"}
        if not leave_flag_on:
            overrides["preserve_local_state"] = False
        with (
            patch.object(doctest, "testmod", return_value=(0, 0)),
            patch.dict(merge_spark_pr.clean_up.__globals__, overrides),
            patch.object(merge_spark_pr.git, "run", side_effect=self.refused) as git_run,
            patch("builtins.input", side_effect=["a"]),
            redirect_stdout(io.StringIO()),
        ):
            namespace = dict(
                vars(merge_spark_pr),
                __name__="__main__",
                main=main,
                # The block under test also runs the suite. Stub it so this exec does not
                # re-enter the tests while git and input are patched.
                run_prmerge_tests=lambda: suite_passes,
            )
            if leave_flag_on:
                # The compiled block reads this name from the globals it is exec'd with.
                namespace["preserve_local_state"] = True
            try:
                exec(self.code, namespace)
            except BaseException as exc:
                ended = getattr(exc, "code", type(exc).__name__)
            else:
                self.fail("entrypoint returned")
        return _commands(git_run), ended

    def test_abort_routes_run_no_further_git(self):
        def abort_then_fail():
            try:
                merge_spark_pr._restore_original_head()
            finally:
                merge_spark_pr.fail("Okay, exiting")

        def ordinary_exit():
            raise SystemExit(-1)

        cases = (
            (merge_spark_pr._restore_original_head, ["checkout"], -1),
            (abort_then_fail, ["checkout"], -1),
            (ordinary_exit, ["checkout"], "CalledProcessError"),
        )
        for entry, commands, ended in cases:
            with self.subTest(entry=entry.__name__):
                self.assertEqual(self._exec(entry), (commands, ended))

    def test_abort_flag_is_off_again_afterwards(self):
        self._exec(merge_spark_pr._restore_original_head)
        self.assertFalse(merge_spark_pr.preserve_local_state)

    def test_flag_left_on_stops_before_main(self):
        ran = []
        commands, ended = self._exec(lambda: ran.append(1), leave_flag_on=True)
        self.assertEqual(ended, -1)
        self.assertEqual(ran, [])
        self.assertEqual(commands, [])

    def test_failing_prmerge_tests_stop_before_main(self):
        ran = []
        commands, ended = self._exec(lambda: ran.append(1), suite_passes=False)
        self.assertEqual(ended, -1)
        self.assertEqual(ran, [])
        self.assertEqual(commands, [])


class RunPrmergeTestsTests(unittest.TestCase):
    def _run(self, tests=None, *, symlink=False):
        """
        Run `run_prmerge_tests` for a script in a temp dir whose `prmerge/tests` holds a
        single test module with the given source, or no such directory when it is `None`.
        """
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        script = tmp / "real" / "merge_spark_pr.py"
        if tests is None:
            script.parent.mkdir()
        else:
            tests_dir = script.parent / "prmerge" / "tests"
            tests_dir.mkdir(parents=True)
            (tests_dir.parent / "__init__.py").touch()
            (tests_dir / "__init__.py").touch()
            (tests_dir / "test_x.py").write_text(tests)
        script.touch()
        if symlink:
            link = tmp / "link" / "merge_spark_pr.py"
            link.parent.mkdir()
            link.symlink_to(script)
            script = link
        output, errors = io.StringIO(), io.StringIO()
        with (
            patch.object(merge_spark_pr, "__file__", str(script)),
            # Discovery records its top-level dir on the loader. Keep that off the shared one.
            patch.object(unittest, "defaultTestLoader", unittest.TestLoader()),
            # Discovery imports `prmerge.tests` under the same name as the real package.
            patch.dict(sys.modules),
            patch.object(sys, "path", list(sys.path)),
            redirect_stdout(output),
            redirect_stderr(errors),
        ):
            for name in [m for m in sys.modules if m.split(".")[0] == "prmerge"]:
                del sys.modules[name]
            passed = merge_spark_pr.run_prmerge_tests()
        self.assertEqual(output.getvalue(), "")
        return passed, errors.getvalue()

    def test_anything_but_a_passing_suite_refuses(self):
        header = "import unittest\nclass T(unittest.TestCase):\n    def test(self): "
        passing, failing = header + "pass\n", header + "self.fail()\n"
        for name, tests in (("missing", None), ("failing", failing), ("zero tests", "")):
            with self.subTest(name):
                passed, errors = self._run(tests)
                self.assertFalse(passed)
                self.assertIn("If dev/prmerge/ is behind master", errors)
        self.assertEqual(self._run(passing), (True, ""))
        self.assertEqual(self._run(passing, symlink=True), (True, ""))
