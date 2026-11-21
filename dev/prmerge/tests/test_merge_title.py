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
Tests for how the merge script prepares and checks a PR title before merging it.
"""

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

import merge_spark_pr as script


class PromptForComponentsTests(unittest.TestCase):
    def test_rejects_markers_ids_and_unreadable_tokens_until_a_primary(self):
        answers = [
            "sql,minor",
            " Trivial ",
            "sql, spark core",
            "sql,[tests]",
            "sql,spark-2",
            "sql][minor",
            "tests",
            "sql, follow-up",
        ]
        output = io.StringIO()
        with (
            patch.dict(
                script.prompt_for_components.__globals__, bold_input=lambda _: answers.pop(0)
            ),
            redirect_stdout(output),
        ):
            components = script.prompt_for_components()

        self.assertEqual(components, ["SQL", "FOLLOWUP"])
        self.assertEqual(answers, [])
        text = output.getvalue()
        self.assertIn("[MINOR] cannot be entered here", text)
        self.assertIn("[TRIVIAL] cannot be entered here", text)
        for bad in ("SPARK CORE", "[TESTS]", "SPARK-2", "SQL][MINOR"):
            self.assertIn(f"Invalid component(s): {bad}.", text)


class PrepareMergeTitleTests(unittest.TestCase):
    def _prepare(self, title):
        stop = Mock(side_effect=SystemExit)
        prompt = Mock(return_value=["CORE"])
        errors = []
        output = io.StringIO()
        with (
            patch.dict(
                script.prepare_merge_title.__globals__,
                fail=stop,
                prompt_for_components=prompt,
                print_error=errors.append,
            ),
            redirect_stdout(output),
        ):
            try:
                result = script.prepare_merge_title(title)
            except SystemExit:
                result = stop.call_args.args[0]
        return result, prompt, errors, output.getvalue()

    def test_wrapped_titles_skip_the_component_prompt(self):
        titles = (
            'Revert "[SPARK-1234][SQL] Fix"',
            'Revert "Revert "[SPARK-1234][SQL] Fix""',
            'Reapply "[SPARK-1234][SQL] Fix"',
            'Revert "Revert to using comma-delimited split"',
        )
        for title in titles:
            with self.subTest(title=title):
                result, prompt, errors, output = self._prepare(title)
                self.assertEqual(result, title)
                self.assertFalse(prompt.called)
                self.assertEqual(errors, [])
                self.assertEqual(output, "")

    def test_an_unchanged_title_is_not_reprinted(self):
        result, prompt, errors, output = self._prepare("[SPARK-1234][SQL] Fix")
        self.assertEqual(result, "[SPARK-1234][SQL] Fix")
        self.assertFalse(prompt.called)
        self.assertEqual(errors, [])
        self.assertEqual(output, "")

    def test_component_case_and_followup_are_normalized(self):
        result, prompt, errors, output = self._prepare("[SPARK-1234][FOLLOWUP][sql] Fix")
        self.assertEqual(result, "[SPARK-1234][SQL][FOLLOWUP] Fix")
        self.assertFalse(prompt.called)
        self.assertEqual(errors, [])
        self.assertEqual(output, "Normalized title: [SPARK-1234][SQL][FOLLOWUP] Fix\n")

    def test_version_tags_move_ahead_of_components(self):
        result, prompt, errors, output = self._prepare("[SPARK-1234][SQL][4.2][sql] Fix")
        self.assertEqual(result, "[SPARK-1234][4.2][SQL] Fix")
        self.assertFalse(prompt.called)
        # A version tag is not an unknown tag, so there is no warning.
        self.assertEqual(errors, [])
        self.assertEqual(output, "Normalized title: [SPARK-1234][4.2][SQL] Fix\n")

    def test_surrounding_whitespace_is_stripped_and_printed(self):
        wrapped = ' Revert "[SPARK-1234][SQL] Fix" '
        result, prompt, errors, output = self._prepare(wrapped)
        self.assertEqual(result, 'Revert "[SPARK-1234][SQL] Fix"')
        self.assertFalse(prompt.called)
        self.assertEqual(errors, [])
        self.assertEqual(output, 'Normalized title: Revert "[SPARK-1234][SQL] Fix"\n')
        result, prompt, errors, output = self._prepare(" [SPARK-1234][SQL] Fix ")
        self.assertEqual(result, "[SPARK-1234][SQL] Fix")
        self.assertFalse(prompt.called)
        self.assertEqual(errors, [])
        self.assertEqual(output, "Normalized title: [SPARK-1234][SQL] Fix\n")

    def test_missing_primary_component_is_prompted_and_printed(self):
        result, prompt, errors, output = self._prepare("[SPARK-1234] Fix")
        self.assertEqual(result, "[SPARK-1234][CORE] Fix")
        prompt.assert_called_once_with()
        self.assertEqual(errors, [])
        self.assertEqual(output, "Normalized title: [SPARK-1234][CORE] Fix\n")

    def test_unknown_tag_is_kept_and_reported(self):
        result, prompt, errors, output = self._prepare("[SPARK-1234][SQL][BOGUS] Fix")
        self.assertEqual(result, "[SPARK-1234][SQL][BOGUS] Fix")
        self.assertEqual(errors, ["Title has unknown tag(s): [BOGUS]"])
        self.assertEqual(output, "")
        self.assertFalse(prompt.called)

    def test_malformed_title_fails(self):
        result, prompt, errors, output = self._prepare("[SQL] Fix")
        self.assertEqual(
            result,
            "Malformed PR title: title must start with [SPARK-NNNNN], [MINOR], "
            "or [TRIVIAL]: '[SQL] Fix'",
        )
        self.assertFalse(prompt.called)
        self.assertEqual(errors, [])
        self.assertEqual(output, "")

    def test_bare_revert_quote_is_parsed_as_an_ordinary_title(self):
        for title in ('Revert "', 'Reapply "'):
            with self.subTest(title=title):
                result, _, errors, output = self._prepare(title)
                self.assertEqual(
                    result,
                    "Malformed PR title: title must start with [SPARK-NNNNN], [MINOR], "
                    f"or [TRIVIAL]: '{title}'",
                )
                self.assertEqual(errors, [])
                self.assertEqual(output, "")


def _squashed(text):
    return " ".join(text.split())


def _wrapped(title):
    """
    Run check_wrapped_title and summarize how it stopped.

    A failure is `fail:` plus the message. A prompt is `confirm:` plus the JIRA update
    direction the warning names (a revert or a re-land, decided by the outer wrappers) and,
    in parentheses, which warning was shown (word, quote, or one word). Anything else is the
    printed text, or `pass` when there is none.
    """
    stop = Mock(side_effect=SystemExit)
    confirm = Mock()
    output = io.StringIO()
    with (
        patch.dict(script.check_wrapped_title.__globals__, fail=stop, continue_maybe=confirm),
        redirect_stdout(output),
    ):
        try:
            script.check_wrapped_title(title)
        except SystemExit:
            return "fail: " + stop.call_args.args[0]
    if not confirm.called:
        return output.getvalue() or "pass"
    confirm.assert_called_once_with("Merge with this title?")
    said = output.getvalue()
    if "still starts with Revert or Reapply" in said:
        shape = "word"
    elif "starts with a quote." in said:
        shape = "quote"
    else:
        shape = "one word"
    line = said.splitlines()[1]
    if not line.startswith("  JIRA update") or said.count("JIRA update") != 1:
        raise AssertionError(said)
    if "a revert, which offers to remove fix versions" in line:
        taken = "a revert"
    elif "a re-land, which resolves the ticket or adds fix versions" in line:
        taken = "a re-land"
    else:
        taken = line
    return f"confirm: {taken} ({shape})"


class CheckWrappedTitleTests(unittest.TestCase):
    def test_inner_wrapper_word_asks_for_confirmation(self):
        cases = (
            ('Revert "Revert "[SPARK-1234][SQL] Fix"', "confirm: a revert (word)"),
            ('Reapply "Revert "[SPARK-1234][SQL] Fix"', "confirm: a re-land (word)"),
            ('Revert "Revert [SPARK-1234][SQL] Fix""', "confirm: a revert (word)"),
            ('Revert "revert [SPARK-1234][SQL] Fix"', "confirm: a revert (word)"),
            ('Revert "Reverted [SPARK-1234][SQL] Fix"', "confirm: a revert (word)"),
            ('Revert "Reapply "[SPARK-1234][SQL] Fix"', "confirm: a revert (word)"),
            ('Revert "Reapplied [SPARK-1234][SQL] Fix"', "confirm: a revert (word)"),
            ('Revert "Re-apply [SPARK-1234][SQL] Fix"', "confirm: a revert (word)"),
            ('Revert "Revert to using comma-delimited split"', "confirm: a revert (word)"),
            ('Reapply "Revert to using comma-delimited split"', "confirm: a re-land (word)"),
            ('Revert "Revert "Revert "[SPARK-1234][SQL] Fix""', "confirm: a re-land (word)"),
        )
        for title, expected in cases:
            with self.subTest(title=title):
                self.assertEqual(_wrapped(title), expected)

    def test_inner_quote_asks_unless_the_quote_is_later_in_the_subject(self):
        cases = (
            ('Revert ""[SPARK-1234][SQL] Fix""', "confirm: a revert (quote)"),
            ('Revert "Reaply "[SPARK-1234][SQL] Fix""', "confirm: a revert (one word)"),
            ('Reapply "Revet "[SPARK-1234][SQL] Fix""', "confirm: a re-land (one word)"),
            ('Revert "Reaply"[SPARK-1234][SQL] Fix""', "confirm: a revert (one word)"),
            (
                'Revert ""launch" scripts is more accurate terminology"',
                "confirm: a revert (quote)",
            ),
            ('Revert "Rename "jobs" to "applications""', "confirm: a revert (one word)"),
            ('Revert "[SPARK-1234][SQL] Support "foo" in X"', "pass"),
            ('Revert "[SPARK-1234][SQL] Fix "foo""', "pass"),
        )
        for title, expected in cases:
            with self.subTest(title=title):
                self.assertEqual(_wrapped(title), expected)

    def test_misspelled_wrapper_warning_names_the_fix(self):
        said = []
        title = 'Revert "Reaply "[SPARK-1234][SQL] Fix""'
        with patch.dict(
            script.check_wrapped_title.__globals__,
            print_error=lambda msg: said.append(msg) or print(msg),
        ):
            self.assertEqual(_wrapped(title), "confirm: a revert (one word)")
        self.assertEqual(
            _squashed(said[0]),
            _squashed(
                """
                The innermost title of Revert "Reaply "[SPARK-1234][SQL] Fix"" starts with one
                word and a quote.
                  JIRA update: a revert, which offers to remove fix versions (decided by the
                  outer wrappers alone; no JIRA id or skip tag is read from the innermost title).
                  If a quote is doubled or a wrapper misspelled, e.g. Revert ""..."" or
                  Reaply "...", fix it so that each layer is Revert "..." or Reapply "...", e.g.
                  Reapply "[SPARK-N] ..." for a revert of a revert.
                  If the subject only quotes something, as in Rename "jobs" to ... or "launch"
                  ..., keep the title, and enter the JIRA id at the prompt after the merge, or
                  leave it blank to skip.
                """
            ),
        )

    def test_disabled_jira_says_to_reconcile_by_hand(self):
        said = []
        title = 'Revert "Revert to using comma-delimited split"'
        with patch.dict(
            script.check_wrapped_title.__globals__,
            print_error=lambda msg: said.append(msg) or print(msg),
            jira_disabled=True,
        ):
            self.assertEqual(_wrapped(title), "confirm: a revert (word)")
        self.assertEqual(
            _squashed(said[0]),
            _squashed(
                """
                The innermost title of Revert "Revert to using comma-delimited split" still
                starts with Revert or Reapply.
                  JIRA update (disabled for this run): would be a revert, which offers to remove
                  fix versions (decided by the outer wrappers alone; no JIRA id or skip tag is
                  read from the innermost title).
                  If a quote was dropped, or the innermost title is the subject of a revert
                  commit, e.g. Revert [SPARK-N] ..., retitle the PR so that each layer is
                  Revert "..." or Reapply "...", e.g. Reapply "[SPARK-N] ..." for a revert of a
                  revert.
                  If it only starts with the word, as in Revert to using ..., keep the title,
                  and reconcile the ticket by hand after the merge.
                """
            ),
        )

    def test_empty_inner_title_fails(self):
        titles = ('Revert ""', 'Revert " "', 'Reapply ""', 'Revert "Revert """')
        for title in titles:
            with self.subTest(title=title):
                self.assertEqual(
                    _wrapped(title),
                    "fail: Empty title inside the Revert/Reapply wrappers of the PR title: "
                    f"{title}",
                )
        for title in ('Revert """"', 'Revert """', 'Revert "Revert """"', 'Revert "" ""'):
            with self.subTest(title=title):
                self.assertEqual(_wrapped(title).split(" inside ")[0], "fail: Empty title")

    def test_balanced_wrappers_and_ordinary_titles_pass(self):
        titles = (
            'Revert "[SPARK-1234][SQL] Fix"',
            'Revert "Reapply "[SPARK-1234] Fix""',
            'Revert "SPARK-49004"',
            'Revert "Fix"',
            "[SPARK-1234][SQL] Revert X",
            'Revert "[SPARK-1234][SQL] Revert X"',
        )
        for title in titles:
            with self.subTest(title=title):
                self.assertEqual(_wrapped(title), "pass")
