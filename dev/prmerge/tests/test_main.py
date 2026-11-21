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
End-to-end tests of a merge or backport run, with GitHub, git, and JIRA stubbed out.
"""

import io
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

import merge_spark_pr

_FOOTER = "\n\nBody\n\nCloses #1 from x/h.\n\nAuthored-by: X <x@x>"


def _pr(title, merged):
    return {
        "url": "u",
        "title": title,
        "draft": False,
        "body": "b",
        "base": {"ref": "master"},
        "user": {"login": "x"},
        "head": {"ref": "h"},
        "state": "closed" if merged else "open",
        "mergeable": True,
    }


class MainRecordsTests(unittest.TestCase):
    def _run(self, calls, message=None):
        pr = _pr("[SPARK-1][SQL] Fix", message is not None)

        def get_json(url):
            if url.endswith("/branches"):
                return [{"name": b} for b in ("master", "branch-4.x", "branch-4.2")]
            if url.endswith("/pulls/1"):
                return pr
            if url.endswith("/users/x"):
                return {"name": "X", "email": "x@x", "id": 1}
            return []

        remaining = list(calls)

        def ask(prompt, *args, **kwargs):
            return "y" if remaining and "into another branch" in prompt else "n"

        def pick(*args, record, **kwargs):
            for pair in remaining.pop(0):
                if pair is None:
                    sys.exit(-1)
                record(pair)

        found = (None, None) if message is None else ("abc", message)
        comment, update = Mock(), Mock()
        with (
            patch.object(sys, "argv", ["merge_spark_pr.py", "1"]),
            patch("os.chdir"),
            patch.dict(
                merge_spark_pr.main.__globals__,
                DRY_RUN_ENV=False,
                check_script_up_to_date=Mock(),
                initialize_jira=Mock(),
                asf_jira=Mock(),
                jira_disabled=False,
                clean_up=Mock(),
                get_current_ref=Mock(return_value="master"),
                get_json=get_json,
                get_json_pages=get_json,
                find_merge_commit=Mock(return_value=found),
                git=Mock(),
                github=Mock(),
                check_linked_jira_issues=Mock(),
                continue_maybe=Mock(),
                get_input=ask,
                merge_pr=Mock(return_value="abc"),
                post_merge_comment=comment,
                prompt_for_components=Mock(return_value=["CORE"]),
                branches_with_merge_footer=Mock(return_value=[]),
                default_pick_branch=Mock(return_value="branch-4.x"),
                cherry_pick=pick,
                update_jira_for_pr=update,
                print=Mock(),
            ),
        ):
            code = None
            try:
                merge_spark_pr.main()
            except SystemExit as exc:
                code = exc.code
        reported = comment.call_args.args[1] if comment.called else None
        return reported, update.call_args.args[2], code

    def test_merge_records_the_target_then_each_pick(self):
        self.assertEqual(
            self._run([[("branch-4.x", "d")], [("branch-4.2", "e")]]),
            (
                [("master", "abc"), ("branch-4.x", "d"), ("branch-4.2", "e")],
                ["master", "branch-4.x", "branch-4.2"],
                None,
            ),
        )

    def test_a_pick_recorded_before_a_later_one_ends_the_run_is_kept(self):
        self.assertEqual(
            self._run([[("branch-4.x", "d")], [None]]),
            ([("master", "abc"), ("branch-4.x", "d")], ["master", "branch-4.x"], -1),
        )
        self.assertEqual(self._run([]), ([("master", "abc")], ["master"], None))

    def test_backport_comment_holds_only_the_picks(self):
        message = "[SPARK-1][SQL] Fix" + _FOOTER
        self.assertEqual(
            self._run([[("branch-4.x", "d")], [("branch-4.2", "e")]], message),
            (
                [("branch-4.x", "d"), ("branch-4.2", "e")],
                ["master", "branch-4.x", "branch-4.2"],
                0,
            ),
        )
        self.assertEqual(
            self._run([[("branch-4.x", "d")], [None]], message),
            ([("branch-4.x", "d")], ["master", "branch-4.x"], -1),
        )


class MainTitlesTests(unittest.TestCase):
    def _run(self, pr_title, message=None, answer="n"):
        pr = _pr(pr_title, message is not None)
        requested = []

        def get_json(url):
            requested.append(url)
            if url.endswith("/branches"):
                return [{"name": "master"}, {"name": "branch-4.x"}]
            if url.endswith("/pulls/1"):
                return pr
            if url.endswith("/users/x"):
                return {"name": "X", "email": "x@x", "id": 1}
            return []

        def ask(prompt, *args, **kwargs):
            if prompt.startswith("Update JIRA from the current PR title"):
                print("asked: use the current PR title?")
                return answer
            if "into another branch" in prompt and message is not None:
                print("asked: pick another?")
            return "n"

        prompt = Mock(return_value=["CORE"])
        update = Mock(side_effect=lambda *args: print("JIRA update"))
        merge, check = Mock(return_value="abc"), Mock()
        pick = Mock(side_effect=lambda *a, **k: print("picked") or [("branch-4.x", "d")])
        found = (None, None) if message is None else ("abc", message)
        pick_default = Mock(return_value="branch-4.x")
        output = io.StringIO()

        def report(msg):
            print(msg.splitlines()[0])

        with (
            patch.object(sys, "argv", ["merge_spark_pr.py", "1"]),
            patch("os.chdir"),
            patch.dict(
                merge_spark_pr.main.__globals__,
                DRY_RUN_ENV=False,
                check_script_up_to_date=Mock(),
                initialize_jira=Mock(),
                asf_jira=Mock(),
                jira_disabled=False,
                clean_up=Mock(),
                get_current_ref=Mock(return_value="master"),
                get_json=get_json,
                get_json_pages=get_json,
                find_merge_commit=Mock(return_value=found),
                git=Mock(),
                github=Mock(),
                check_linked_jira_issues=check,
                continue_maybe=Mock(),
                get_input=ask,
                merge_pr=merge,
                post_merge_comment=Mock(),
                prompt_for_components=prompt,
                branches_with_merge_footer=Mock(return_value=[]),
                default_pick_branch=pick_default,
                cherry_pick=pick,
                update_jira_for_pr=update,
                print=Mock(),
                print_error=report,
            ),
            redirect_stdout(output),
        ):
            code = None
            try:
                merge_spark_pr.main()
            except SystemExit as exc:
                code = exc.code
        committed = merge.call_args.args[2] if merge.called else None
        checked = check.call_args.args[1] if check.called else None
        self.assertEqual(committed, checked)
        self.assertTrue(all(call.args[0] == ["branch-4.x"] for call in pick_default.call_args_list))
        unused = ("/issues/1/events",) if message is None else ("/pulls/1/commits", "/users/x")
        self.assertFalse([url for url in requested if url.endswith(unused)], requested)
        lines = [line for line in output.getvalue().splitlines() if line]
        return update.call_args.args[1], committed, prompt.called, code, lines

    def test_merge_uses_the_prepared_title(self):
        self.assertEqual(
            self._run("[SPARK-1] Fix"),
            ("[SPARK-1][CORE] Fix", "[SPARK-1][CORE] Fix", True, None, ["JIRA update"]),
        )

    def test_merge_runs_the_wrapper_check(self):
        title = 'Revert "Revert to using X"'
        self.assertEqual(
            self._run(title),
            (
                title,
                title,
                False,
                None,
                [
                    'The innermost title of Revert "Revert to using X" still starts with '
                    "Revert or Reapply.",
                    "JIRA update",
                ],
            ),
        )

    def test_backport_of_our_merge_keeps_the_committed_subject(self):
        cases = (
            ("[SPARK-1][SQL] Fix", "[SPARK-1][SQL] Fix" + _FOOTER, "[SPARK-1][SQL] Fix"),
            (
                "[SPARK-1][CORE][TESTS] Fix",
                "[SPARK-1][CORE][TEST] Fix" + _FOOTER,
                "[SPARK-1][CORE][TEST] Fix",
            ),
            (
                "[SPARK-1][TESTS] Fix",
                "[SPARK-1][CORE][TEST] Fix" + _FOOTER,
                "[SPARK-1][CORE][TEST] Fix",
            ),
        )
        for pr_title, message, subject in cases:
            with self.subTest(pr_title=pr_title):
                self.assertEqual(
                    self._run(pr_title, message),
                    (subject, None, False, 0, ["picked", "asked: pick another?", "JIRA update"]),
                )

    def test_backport_can_take_the_current_pr_title_after_the_picks(self):
        message = "[SPARK-1][SQL] Fix" + _FOOTER
        lines = [
            "picked",
            "asked: pick another?",
            "The merge commit subject and the current PR title would update JIRA differently:",
            "asked: use the current PR title?",
            "JIRA update",
        ]
        self.assertEqual(
            self._run("[SPARK-2][SQL] Fix", message),
            ("[SPARK-1][SQL] Fix", None, False, 0, lines),
        )
        self.assertEqual(
            self._run("[SPARK-2][SQL] Fix", message, answer="y"),
            ("[SPARK-2][SQL] Fix", None, False, 0, lines),
        )

    def test_backport_does_not_run_the_wrapper_check(self):
        wrapped = 'Revert "Revert to using comma-delimited split"'
        lines = ["picked", "asked: pick another?", "JIRA update"]
        self.assertEqual(
            self._run(wrapped, wrapped + _FOOTER),
            (wrapped, None, False, 0, lines),
        )
        other = "[SPARK-3][SQL] Other" + _FOOTER.replace("#1", "#3")
        self.assertEqual(self._run(wrapped, other), (wrapped, None, False, 0, lines))

    def test_backport_without_our_footer_prepares_the_current_title(self):
        message = "[SPARK-3][SQL] Other" + _FOOTER.replace("#1", "#3")
        self.assertEqual(
            self._run("[SPARK-2] Fix", message),
            (
                "[SPARK-2][CORE] Fix",
                None,
                True,
                0,
                ["picked", "asked: pick another?", "JIRA update"],
            ),
        )
