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
Tests for how the merge script checks and updates JIRA tickets.
"""

import io
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, call, patch
from urllib.error import HTTPError, URLError

import merge_spark_pr as script
from merge_spark_pr import DryRunJira, Jira

_BRANCHES = ["master", "branch-4.x"]
_COMMENT = "Issue resolved by pull request 123\n[https://github.com/apache/spark/pull/123]"
_REMOVE_BOTH = [{"remove": {"name": "5.0.0"}}, {"remove": {"name": "4.3.0"}}]
_REOPEN = [{"id": "701", "name": "Close Issue"}, {"id": "3", "name": "Reopen Issue"}]


def _version(name, released=False, archived=False):
    return SimpleNamespace(
        name=name, raw={"name": name, "released": released, "archived": archived}
    )


def _catalog():
    return [
        _version("5.0.0"),
        _version("4.3.0"),
        _version("4.2.1", released=True),
        _version("4.2.0", archived=True),
        _version("3.5.2"),
    ]


class RevertJiraIssueTests(unittest.TestCase):
    def _revert(
        self,
        answers,
        fix_versions,
        status="Open",
        dry_run=False,
        transitions=None,
        transition_error=None,
    ):
        catalog = _catalog()
        issue = Mock(key="SPARK-1234")
        issue.fields.fixVersions = list(fix_versions)
        issue.fields.status.name = status
        client = Mock()
        client.project_versions.return_value = catalog
        if transition_error == "lookup":
            client.transitions.side_effect = RuntimeError("lookup failed")
        else:
            client.transitions.return_value = transitions
        if transition_error == "permission":
            client.transition_issue.side_effect = RuntimeError("permission denied")
        writer = DryRunJira(client) if dry_run else Jira(client)
        output = io.StringIO()
        with (
            patch.dict(
                script.revert_jira_issue.__globals__,
                asf_jira=client,
                jira_ops=writer,
                get_jira_issue=Mock(return_value=issue),
            ),
            patch("builtins.input", side_effect=answers),
            redirect_stdout(output),
        ):
            script.revert_jira_issue(_BRANCHES, "SPARK-1234")
        return issue, client, output.getvalue()

    def test_removes_branch_versions_and_keeps_the_rest(self):
        catalog = _catalog()
        issue, client, _ = self._revert(["", "y"], catalog)
        issue.update.assert_called_once_with(update={"fixVersions": _REMOVE_BOTH})
        self.assertEqual(client.mock_calls, [call.project_versions("SPARK")])

    def test_committer_can_narrow_decline_or_skip(self):
        catalog = _catalog()
        issue, _, _ = self._revert(["5.0.0", "y"], catalog)
        issue.update.assert_called_once_with(
            update={"fixVersions": [{"remove": {"name": "5.0.0"}}]}
        )
        for answers in (["", "n"], ["", ""], ["skip"]):
            issue, _, _ = self._revert(answers, catalog)
            issue.update.assert_not_called()

    def test_released_archived_and_unknown_versions_are_rejected(self):
        issue, _, output = self._revert(["4.2.1", "4.2.0", "9.9.9", "", "y"], _catalog())
        self.assertEqual(
            output.count("Select only unreleased fix versions currently recorded on this JIRA."),
            3,
        )
        issue.update.assert_called_once_with(update={"fixVersions": _REMOVE_BOTH})

    def test_dry_run_logs_the_removal(self):
        issue, client, output = self._revert(["", "y"], _catalog(), dry_run=True)
        self.assertIn(
            "DRY-RUN: would remove fixVersions=['5.0.0', '4.3.0'] from JIRA SPARK-1234.",
            output,
        )
        issue.update.assert_not_called()
        self.assertEqual(client.mock_calls, [call.project_versions("SPARK")])

    def test_last_versions_can_be_removed_and_a_miss_is_a_no_op(self):
        catalog = _catalog()
        issue, _, _ = self._revert(["", "y"], catalog[:2])
        issue.update.assert_called_once_with(update={"fixVersions": _REMOVE_BOTH})

        issue, _, _ = self._revert([""], catalog[2:])
        issue.update.assert_not_called()

        issue, _, output = self._revert([], [])
        self.assertIn("no unreleased fix versions to remove", output)
        issue.update.assert_not_called()

    def test_resolved_and_closed_tickets_can_be_reopened_with_nothing_left(self):
        for status in ("Resolved", "Closed"):
            issue, client, _ = self._revert(["y"], [], status=status, transitions=_REOPEN)
            client.transitions.assert_called_once_with("SPARK-1234")
            client.transition_issue.assert_called_once_with("SPARK-1234", "3")
            issue.update.assert_not_called()

    def test_reopening_is_optional_and_independent_of_removal(self):
        for answer in ("n", ""):
            _, client, _ = self._revert([answer], [], status="Closed", transitions=_REOPEN)
            client.transitions.assert_not_called()
            client.transition_issue.assert_not_called()

        issue, client, _ = self._revert(
            ["", "y", "y"], _catalog(), status="Closed", transitions=_REOPEN
        )
        issue.update.assert_called_once_with(update={"fixVersions": _REMOVE_BOTH})
        client.transition_issue.assert_called_once_with("SPARK-1234", "3")

        for answers in (["skip", "y"], ["", "n", "y"]):
            issue, client, _ = self._revert(
                answers, _catalog(), status="Closed", transitions=_REOPEN
            )
            issue.update.assert_not_called()
            client.transition_issue.assert_called_once_with("SPARK-1234", "3")

        issue, client, _ = self._revert(
            ["", "y", "n"], _catalog(), status="Closed", transitions=_REOPEN
        )
        issue.update.assert_called_once()
        client.transition_issue.assert_not_called()

    def test_dry_run_logs_removal_and_reopen(self):
        issue, client, output = self._revert(
            ["", "y", "y"], _catalog(), status="Closed", transitions=_REOPEN, dry_run=True
        )
        self.assertIn("DRY-RUN: would remove fixVersions=", output)
        self.assertIn("DRY-RUN: would reopen JIRA SPARK-1234.", output)
        issue.update.assert_not_called()
        client.transition_issue.assert_not_called()

    def test_missing_reopen_transition_asks_for_a_manual_reopen(self):
        _, _, output = self._revert(
            ["skip", "y"],
            _catalog(),
            status="Resolved",
            transitions=[{"id": "701", "name": "Close Issue"}],
        )
        self.assertIn(
            "No Reopen Issue transition available for SPARK-1234; reopen it manually.",
            output,
        )

    def test_reopen_lookup_and_transition_failures_are_nonfatal(self):
        _, client, output = self._revert(
            ["skip", "y"], _catalog(), status="Resolved", transition_error="lookup"
        )
        self.assertIn("Failed to reopen SPARK-1234; reopen it manually: lookup failed", output)
        client.transition_issue.assert_not_called()

        _, client, output = self._revert(
            ["skip", "y"],
            _catalog(),
            status="Resolved",
            transitions=[{"id": "3", "name": "Reopen Issue"}],
            transition_error="permission",
        )
        self.assertIn("Failed to reopen SPARK-1234; reopen it manually: permission denied", output)
        client.transition_issue.assert_called_once_with("SPARK-1234", "3")

    def test_active_tickets_are_not_reopened(self):
        for status in ("Open", "In Progress", "Reopened"):
            _, client, _ = self._revert([], [], status=status, transitions=_REOPEN)
            client.transitions.assert_not_called()
            client.transition_issue.assert_not_called()


class UpdateJiraForPrTests(unittest.TestCase):
    def setUp(self):
        self.reverter = Mock()
        self.resolver = Mock()
        self.confirm = Mock()

    def _update(self, title, branches, disabled=False):
        self.reverter.reset_mock()
        self.resolver.reset_mock()
        self.confirm.reset_mock()
        output = io.StringIO()
        globals_ = dict(
            revert_jira_issue=self.reverter,
            resolve_jira_issue=self.resolver,
            continue_maybe=self.confirm,
        )
        if disabled:
            globals_["jira_disabled"] = True
        with (
            patch.dict(script.update_jira_for_pr.__globals__, globals_),
            redirect_stdout(output),
        ):
            script.update_jira_for_pr("123", title, branches)
        return output.getvalue()

    def _assert_idle(self):
        self.reverter.assert_not_called()
        self.resolver.assert_not_called()
        self.confirm.assert_not_called()

    def test_revert_updates_each_leading_id_and_skips_resolve(self):
        title = 'Revert "[SPARK-1234][SPARK-5678][SQL] Fix mentions SPARK-9999"'
        self._update(title, _BRANCHES)
        self.assertEqual(
            self.reverter.call_args_list,
            [
                call(_BRANCHES, "SPARK-1234"),
                call(_BRANCHES, "SPARK-5678"),
            ],
        )
        self.resolver.assert_not_called()

    def test_backport_and_titles_without_a_bracketed_id_still_revert(self):
        self._update('Revert "[SPARK-1234][SQL] Fix"', ["branch-4.2"])
        self.reverter.assert_called_once_with(["branch-4.2"], "SPARK-1234")
        self._update('Revert "Fix"', ["master"])
        self.reverter.assert_called_once_with(["master"], "")
        self._update('Revert "SPARK-49004"', ["master"])
        self.reverter.assert_called_once_with(["master"], "SPARK-49004")

    def test_skip_tags_bypass_revert_and_resolve(self):
        titles = (
            'Revert "[MINOR][SQL] Fix"',
            'Revert "[TRIVIAL][SQL] Fix"',
            'Revert "[SPARK-1234][SQL][FOLLOWUP] Fix"',
            'Revert "[SPARK-1234][SQL][FOLLOW-UP] Fix"',
            'Revert "[SPARK-1234][SQL][FOLLOWUP][MINOR] Fix"',
            'Revert "[MINOR][TRIVIAL][SQL] Fix"',
            'Revert "[SQL][MINOR] Fix"',
            'Revert "[MINOR][SPARK-1234] Fix"',
            "[MINOR][SQL] Fix",
            "[TRIVIAL][SQL] Fix",
            "[SPARK-1234][SQL][FOLLOWUP] Fix",
            'Reapply "[MINOR][SQL] Fix"',
            'Reapply "[SPARK-1234][SQL][FOLLOWUP] Fix"',
            'Reapply "[SPARK-1234][SQL][FOLLOW-UP] Fix"',
            'Reapply "[SPARK-1234][SQL][FOLLOWUP][MINOR] Fix"',
            'Revert "Reapply "[MINOR][SQL] Fix""',
        )
        for title in titles:
            with self.subTest(title=title):
                self._update(title, ["master"])
                self._assert_idle()

    def test_id_beside_minor_still_reverts(self):
        self._update('Revert "[SPARK-1234][SQL][MINOR] Fix"', ["master"])
        self.reverter.assert_called_once_with(["master"], "SPARK-1234")

    def test_nested_reapply_of_a_real_title_reverts(self):
        self._update('Revert "Reapply "[SPARK-1234][SQL] Fix""', ["master"])
        self.reverter.assert_called_once_with(["master"], "SPARK-1234")

    def test_merges_and_reapplies_resolve_leading_ids_and_their_components(self):
        cases = (
            ("[SPARK-1234][SQL] Fix mentions SPARK-9999", ["SQL"]),
            ("[SPARK-1234][sql][DOCS] Fix", ["SQL", "DOC"]),
            ('Reapply "[SPARK-1234][SQL] Fix"', []),
            ('Reapply "[SPARK-1234][SQL][TRIVIAL] Fix"', []),
            ("[SPARK-1234][SQL][MINOR] Fix", []),
        )
        for title, components in cases:
            with self.subTest(title=title):
                self._update(title, ["master"])
                self.reverter.assert_not_called()
                self.resolver.assert_called_once_with(
                    ["master"], _COMMENT, "SPARK-1234", title_components=components
                )
        self._update("[SPARK-1234][SPARK-5678][SQL] Fix", ["master"])
        self.assertEqual(
            [call.args[2] for call in self.resolver.call_args_list],
            ["SPARK-1234", "SPARK-5678"],
        )

    def test_wrapper_nesting_decides_revert_or_reland(self):
        self._update('Revert "Revert "[SPARK-1234][SQL] Fix""', ["master"])
        self.reverter.assert_not_called()
        self.resolver.assert_called_once_with(
            ["master"], _COMMENT, "SPARK-1234", title_components=[]
        )
        self._update('Reapply "Revert "[SPARK-1234][SQL] Fix""', ["master"])
        self.reverter.assert_called_once_with(["master"], "SPARK-1234")
        self.resolver.assert_not_called()

    def test_title_without_an_id_still_offers_resolve(self):
        self._update('Reapply "Fix"', ["master"])
        self.resolver.assert_called_once_with(["master"], _COMMENT, title_components=[])

    def test_disabled_jira_skips_every_shape_without_the_title_reason(self):
        titles = (
            "[SPARK-1234][SQL] Fix",
            'Revert "[SPARK-1234][SQL] Fix"',
            'Revert "[MINOR][SQL] Fix"',
            "[SPARK-1234][SQL][FOLLOWUP] Fix",
        )
        for title in titles:
            with self.subTest(title=title):
                output = self._update(title, ["master"], disabled=True)
                self._assert_idle()
                self.assertIn("JIRA is disabled; skipping JIRA update for PR #123", output)
                self.assertNotIn("because title has", output)


def _issue(issue_type):
    issue = Mock()
    issue.fields.issuetype.name = issue_type
    return issue


class CheckLinkedJiraIssuesTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.client.issue.side_effect = {
            "SPARK-60328": _issue("Sub-task"),
            "SPARK-60329": _issue("Bug"),
            "SPARK-34261": _issue("Epic"),
            "SPARK-34262": _issue("Umbrella"),
        }.__getitem__
        self.verify = Mock(return_value="")
        self.stop = Mock(side_effect=SystemExit)

    def _check(self, title, answers=("y",)):
        for mock in (self.client, self.verify, self.stop):
            mock.reset_mock()
        with (
            patch.dict(
                script.check_linked_jira_issues.__globals__,
                asf_jira=self.client,
                print_jira_issue_summary=Mock(),
                format_jira_verification=self.verify,
                fail=self.stop,
            ),
            patch("builtins.input", side_effect=answers),
            redirect_stdout(io.StringIO()),
        ):
            try:
                script.check_linked_jira_issues("123", title)
            except SystemExit:
                return "stopped"
        return None

    def test_only_leading_ids_are_fetched(self):
        title = (
            "[SPARK-60328][SQL][TESTS] Use a unique jar copy in the HiveDDLSuite SPARK-34261 test"
        )
        self.assertIsNone(self._check(title))
        self.assertEqual(self.client.issue.call_args_list, [call("SPARK-60328")])
        self.stop.assert_not_called()
        self.assertEqual(self.verify.call_args.args[2], "SPARK-60328")

    def test_each_leading_id_is_confirmed_and_declining_aborts(self):
        self.assertIsNone(self._check("[SPARK-60328][SPARK-60329][SQL] Fix", answers=("y", "y")))
        self.assertEqual(
            [call.args[2] for call in self.verify.call_args_list],
            ["SPARK-60328", "SPARK-60329"],
        )
        self.stop.assert_not_called()

        self.assertEqual(self._check("[SPARK-60328][SQL] Fix", answers=("n",)), "stopped")
        self.assertTrue(
            self.stop.call_args.args[0].startswith("Aborting: SPARK-60328 does not match PR #123.")
        )
        self.assertEqual(self._check("[SPARK-60328][SQL] Fix", answers=("",)), "stopped")
        self.assertEqual(self.stop.call_count, 1)

    def test_a_ticket_that_cannot_be_fetched_is_skipped(self):
        self.assertIsNone(self._check("[SPARK-1][SQL] Fix"))
        self.assertEqual(self.client.issue.call_args_list, [call("SPARK-1")])
        self.verify.assert_not_called()
        self.stop.assert_not_called()

        self.assertEqual(self._check("[SPARK-1][SPARK-34261][SQL] Fix"), "stopped")
        self.assertIn("SPARK-34261 (Epic)", self.stop.call_args.args[0])

    def test_disabled_jira_fetches_nothing(self):
        output = io.StringIO()
        with (
            patch.dict(
                script.check_linked_jira_issues.__globals__,
                asf_jira=self.client,
                jira_disabled=True,
            ),
            redirect_stdout(output),
        ):
            script.check_linked_jira_issues("123", "[SPARK-60328][SQL] Fix")
        self.client.issue.assert_not_called()
        self.assertIn("skipping the Epic/Umbrella check", output.getvalue())

    def test_leading_epic_and_umbrella_block_before_confirmation(self):
        self.assertEqual(self._check("[SPARK-34261][SPARK-60328][SPARK-34262][SQL] Fix"), "stopped")
        message = self.stop.call_args.args[0]
        self.assertIn("SPARK-34261 (Epic), SPARK-34262 (Umbrella)", message)
        self.verify.assert_not_called()

    def test_followup_suppresses_the_similarity_check_inside_wrappers_too(self):
        titles = (
            ("[SPARK-60328][SQL][FOLLOWUP] Fix", True),
            ('Revert "[SPARK-60328][SQL] Fix"', False),
            ("[SPARK-60328][SQL] Fix FOLLOWUP handling", False),
            ('Revert "[SPARK-60328][SQL][FOLLOW-UP] Fix"', True),
            ('Reapply "[SPARK-60328][FOLLOWUP][SQL][MINOR] Fix"', True),
        )
        for title, followup in titles:
            with self.subTest(title=title):
                self.assertIsNone(self._check(title))
                self.assertEqual(self.verify.call_args.kwargs["is_followup"], followup)


class ReconcileAffectsVersionsTests(unittest.TestCase):
    def test_blank_input_appends_the_suggestion(self):
        issue = SimpleNamespace(
            key="SPARK-1",
            fields=SimpleNamespace(versions=[SimpleNamespace(name="5.0.0")]),
        )
        writer = Mock(spec=Jira)
        with (
            patch.dict(script.reconcile_jira_affects_versions.__globals__, jira_ops=writer),
            patch("builtins.input", side_effect=["", "", KeyboardInterrupt]) as user_input,
            redirect_stdout(io.StringIO()),
        ):
            script.reconcile_jira_affects_versions(issue, ["4.4.0"], {"5.0.0", "4.4.0"}, ["4.4.0"])
        writer.update_affects_versions.assert_called_once_with(issue, ["5.0.0", "4.4.0"])
        self.assertEqual(user_input.call_count, 2)

    def test_skip_leaves_the_writer_untouched(self):
        issue = SimpleNamespace(
            key="SPARK-1",
            fields=SimpleNamespace(versions=[SimpleNamespace(name="5.0.0")]),
        )
        writer = Mock(spec=Jira)
        with (
            patch.dict(script.reconcile_jira_affects_versions.__globals__, jira_ops=writer),
            patch("builtins.input", side_effect=["skip", KeyboardInterrupt]) as user_input,
            redirect_stdout(io.StringIO()),
        ):
            script.reconcile_jira_affects_versions(issue, ["4.4.0"], {"5.0.0", "4.4.0"}, ["4.4.0"])
        self.assertEqual(writer.mock_calls, [])
        self.assertEqual(user_input.call_count, 1)


class JiraProbeTests(unittest.TestCase):
    def _probe(self, error=None):
        if error is None:
            # urlopen is used as a context manager when the probe gets a response.
            opener = MagicMock()
        else:
            opener = Mock(side_effect=error)
        with patch.dict(script.jira_is_live.__globals__, urlopen=opener):
            return script.jira_is_live()

    def test_response_and_client_error_are_live(self):
        self.assertEqual(self._probe(), (True, None))
        self.assertEqual(
            self._probe(HTTPError("https://issues.apache.org/jira", 404, "Not Found", {}, None)),
            (True, None),
        )

    def test_server_and_network_errors_are_dead(self):
        dead = self._probe(
            HTTPError("https://issues.apache.org/jira", 503, "Service Unavailable", {}, None)
        )
        self.assertEqual(dead, (False, "server answered HTTP 503"))
        refused = self._probe(URLError("connection refused"))
        self.assertEqual(refused, (False, "URLError: <urlopen error connection refused>"))
        live, reason = self._probe(TimeoutError("timed out"))
        self.assertFalse(live)
        self.assertIn("timed out", reason)

    def test_unreachable_probe_disables_jira_without_a_client(self):
        output = io.StringIO()
        with (
            patch.dict(
                script.initialize_jira.__globals__,
                jira_is_live=Mock(return_value=(False, "server answered HTTP 503")),
            ),
            redirect_stdout(output),
        ):
            script.initialize_jira()
            state = (script.jira_disabled, script.asf_jira)
        self.assertEqual(state, (True, None))
        text = output.getvalue()
        self.assertIn("JIRA is unreachable or unhealthy (server answered HTTP 503)", text)
        self.assertIn("disabling all JIRA steps for this merge", text)


class JiraIdsToVerifyTests(unittest.TestCase):
    def _ids(self, title, disabled):
        output = io.StringIO()
        with (
            patch.dict(script.jira_ids_to_verify.__globals__, jira_disabled=disabled),
            redirect_stdout(output),
        ):
            return script.jira_ids_to_verify(title), output.getvalue()

    def test_enabled_run_returns_the_leading_ids(self):
        ids, output = self._ids("[SPARK-1234][SPARK-5678][SQL] Fix", False)
        self.assertEqual(ids, ["SPARK-1234", "SPARK-5678"])
        self.assertEqual(output, "")

    def test_disabled_run_drops_ids_and_names_them(self):
        ids, output = self._ids("[SPARK-1234][SQL] Fix mentions SPARK-9999", True)
        self.assertEqual(ids, [])
        self.assertIn("Confirm SPARK-1234 manually", output)

        ids, output = self._ids("[SQL] Fix", True)
        self.assertEqual(ids, [])
        self.assertIn("the linked ticket(s)", output)
