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
Tests for the `__main__` block of `dev/merge_spark_pr.py`.
"""

import ast
import doctest
import io
import subprocess
import unittest
from contextlib import redirect_stdout
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

    def _exec(self, main, *, leave_flag_on=False):
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
                run_prmerge_tests=lambda: True,
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
