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

import gzip
import importlib.util
import tempfile
import unittest
from datetime import datetime, timezone
from http.client import IncompleteRead
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError

spec = importlib.util.spec_from_file_location(
    "ci_dashboard_history", Path(__file__).resolve().parents[1] / "ci_dashboard_history.py"
)
history = importlib.util.module_from_spec(spec)
spec.loader.exec_module(history)


START = "2026-11-01T00:00:00Z"
END = "2026-11-01T01:00:00Z"
NOW = datetime(2026, 11, 11, tzinfo=timezone.utc)


def job(name="build", **kwargs):
    return {
        "name": name,
        "started_at": START,
        "completed_at": END,
        "conclusion": "success",
        **kwargs,
    }


def run(number, **kwargs):
    return {
        "id": number,
        "run_started_at": START,
        "head_branch": "master",
        "conclusion": "success",
        "run_attempt": 1,
        "event": "push",
        "status": "completed",
        "created_at": START,
        "updated_at": START,
        **kwargs,
    }


class FakeAPI:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, path, **params):
        self.calls.append((path, params))
        value = self.responses[path]
        return value(params) if callable(value) else value


class HistoryTests(unittest.TestCase):
    def test_job_time_excludes_queue_wait_and_counts_parallel_jobs(self):
        origin = history.timestamp("2026-10-31T23:00:00Z")
        values = history.intervals([job(), job(), job(conclusion="skipped")], origin)
        self.assertEqual(values, [[3600, 3600], [3600, 3600]])
        with self.assertRaisesRegex(ValueError, "unfinished"):
            history.intervals([job(completed_at=None)], origin)
        with self.assertRaisesRegex(ValueError, "precedes"):
            history.intervals([job(completed_at="2026-10-31T23:59:59Z")], origin)

    def test_branch_matrix_is_partitioned_without_repeating_whole_run_cost(self):
        api = FakeAPI(
            {
                "actions/workflows/publish.yml/runs": {
                    "workflow_runs": [run(1), run(2, run_attempt=2)]
                },
                "actions/runs/1/jobs": {
                    "total_count": 2,
                    "jobs": [job("publish (master)"), job("publish (branch-4.x)")],
                },
            }
        )
        workloads = [
            {
                "file": "publish.yml",
                "scheduler": "publish.yml",
                "branch": branch,
                "matrixBranch": True,
            }
            for branch in ["master", "branch-4.x"]
        ]
        values = history.profiles(api, workloads, NOW, NOW, 3)
        self.assertEqual(values[("publish.yml", "master")]["runnerHours"], 1)
        self.assertEqual(values[("publish.yml", "branch-4.x")]["runnerHours"], 1)
        self.assertEqual(values[("publish.yml", "master")]["sampleCount"], 1)
        self.assertEqual(api.calls[0][1]["branch"], "master")
        self.assertEqual(len(api.calls), 2)

    def test_push_cost_includes_failures_and_all_job_attempts(self):
        api = FakeAPI(
            {
                "actions/runs": {
                    "total_count": 2,
                    "workflow_runs": [
                        run(1),
                        run(2, conclusion="failure", head_branch="branch-4.x"),
                    ],
                },
                "actions/runs/1/jobs": {"total_count": 2, "jobs": [job(), job()]},
                "actions/runs/2/jobs": {"total_count": 1, "jobs": [job(conclusion="failure")]},
            }
        )
        value = history.observed_usage(
            api, [], history.timestamp(START), history.timestamp(END), history.JobTimings(api)
        )["push"]
        self.assertEqual(value["runnerHours"], 3)
        self.assertEqual(value["jobCount"], 3)
        self.assertEqual(value["failedRuns"], 1)
        self.assertEqual(value["runCount"], 2)
        self.assertNotIn("event", api.calls[0][1])
        self.assertTrue(all(p["filter"] == "all" for path, p in api.calls if path.endswith("jobs")))

    def test_skipped_push_run_without_start_time_has_no_runner_cost(self):
        api = FakeAPI(
            {
                "actions/runs": {
                    "total_count": 1,
                    "workflow_runs": [
                        run(1, run_started_at=None, created_at=START, conclusion="skipped")
                    ],
                },
                "actions/runs/1/jobs": {
                    "total_count": 1,
                    "jobs": [job(conclusion="skipped", started_at=None, completed_at=None)],
                },
            }
        )
        value = history.observed_usage(
            api, [], history.timestamp(START), history.timestamp(END), history.JobTimings(api)
        )["push"]
        self.assertEqual(value["runnerHours"], 0)
        self.assertEqual(value["jobCount"], 0)
        self.assertEqual(value["runCount"], 1)

    def test_actual_usage_includes_all_triggers_spillover_and_running_jobs(self):
        runs = [
            run(1, conclusion="failure"),
            run(2, event="schedule", path=".github/workflows/publish.yml"),
            run(3, event="workflow_dispatch", head_branch="branch-4.2"),
            run(4, event="workflow_dispatch", status="in_progress"),
            run(5, path=".github/workflows/update_build_status.yml"),
        ]
        api = FakeAPI(
            {
                "actions/runs": {"total_count": 5, "workflow_runs": runs},
                "actions/runs/1/jobs": {
                    "total_count": 1,
                    "jobs": [
                        job(started_at="2026-10-31T23:30:00Z", completed_at="2026-11-01T00:30:00Z")
                    ],
                },
                "actions/runs/2/jobs": {"total_count": 1, "jobs": [job("publish (branch-4.x)")]},
                "actions/runs/3/jobs": {
                    "total_count": 1,
                    "jobs": [job(completed_at="2026-11-01T00:15:00Z")],
                },
                "actions/runs/4/jobs": {
                    "total_count": 1,
                    "jobs": [job(started_at="2026-11-01T00:30:00Z", completed_at=None)],
                },
            }
        )
        value = history.observed_usage(
            api,
            [{"file": "publish.yml", "branch": "branch-4.x", "matrixBranch": True}],
            history.timestamp(START),
            history.timestamp(END),
            history.JobTimings(api),
        )
        day = value["days"]["2026-11-01"]
        self.assertEqual(day["hours"][0], 2.25)
        self.assertEqual(day["pushHours"][0], 0.5)
        self.assertEqual(day["byBranch"]["branch-4.x"][0], 1)
        self.assertEqual(value["push"]["runnerHours"], 0.5)
        self.assertFalse(any(path.endswith("/5/jobs") for path, _ in api.calls))

    def test_waiting_and_unassigned_jobs_do_not_count_as_runner_usage(self):
        waiting = job(
            status="waiting", runner_id=0, started_at="2026-10-01T00:00:00Z", completed_at=None
        )
        cancelled = job(status="completed", runner_id=0, conclusion="cancelled")
        queued = job(status="queued", runner_id=42)
        self.assertEqual(
            history.intervals([waiting, cancelled, queued], history.timestamp(START)), []
        )
        api = FakeAPI(
            {
                "actions/runs": {"total_count": 1, "workflow_runs": [run(1)]},
                "actions/runs/1/jobs": {
                    "total_count": 4,
                    "jobs": [waiting, cancelled, queued, job(runner_id=42)],
                },
            }
        )
        value = history.observed_usage(
            api, [], history.timestamp(START), history.timestamp(END), history.JobTimings(api)
        )
        self.assertEqual(value["days"]["2026-11-01"]["hours"][0], 1)
        self.assertEqual(value["jobCount"], 1)

    def test_completed_job_cache_invalidates_reruns_and_does_not_cache_running_jobs(self):
        api = FakeAPI({"actions/runs/1/jobs": {"total_count": 1, "jobs": [job()]}})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "history.json"
            store = history.JobTimings(api, path)
            store.get(run(1))
            store.get(run(1))
            self.assertEqual(len(api.calls), 1)
            store.get(run(1, updated_at=END, run_attempt=2))
            self.assertEqual(len(api.calls), 2)
            store.get(run(1, status="in_progress", updated_at=END))
            store.get(run(1, status="in_progress", updated_at=END))
            self.assertEqual(len(api.calls), 4)
            store.state["days"] = {"2026-01-01": {}, "2026-11-10": {}}
            store.save(NOW)
            restored = history.JobTimings(api, path)
            self.assertEqual(list(restored.state["days"]), ["2026-11-10"])
            restored.get(run(1, updated_at=END))
            self.assertEqual(len(api.calls), 4)

    def test_job_pagination_and_push_search_limit(self):
        api = FakeAPI(
            {
                "actions/runs/1/jobs": lambda p: {
                    "total_count": 101,
                    "jobs": [job()] * (100 if p["page"] == 1 else 1),
                },
                "actions/runs": {"total_count": 1001, "workflow_runs": []},
            }
        )
        self.assertEqual(len(history.jobs(api, run(1))), 101)
        with self.assertRaisesRegex(ValueError, "1,000-run"):
            history.observed_usage(
                api, [], history.timestamp(START), history.timestamp(END), history.JobTimings(api)
            )["push"]

    def test_interrupted_gzip_response_is_retried_and_credentials_are_not_reported(self):
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.headers = {"Content-Encoding": "gzip"}
        response.read.return_value = gzip.compress(b'{"ok": true}')
        api = history.GitHub("test-secret")
        with mock.patch.object(history, "urlopen", side_effect=[IncompleteRead(b"x", 1), response]):
            with mock.patch.object(history, "sleep") as sleep:
                self.assertEqual(api.get("actions/runs"), {"ok": True})
                sleep.assert_called_once_with(1)
        failure = HTTPError("https://api.github.com/", 403, "forbidden", {}, None)
        with mock.patch.object(history, "urlopen", side_effect=failure):
            with self.assertRaisesRegex(RuntimeError, "HTTP 403") as error:
                api.get("actions/runs")
            self.assertNotIn("test-secret", str(error.exception))

    def test_failed_collection_is_marked_unavailable(self):
        class Unavailable:
            def get(self, *args, **kwargs):
                raise RuntimeError("GitHub Actions API returned HTTP 403")

        data = {"workloads": [{"file": "build.yml", "branch": "master", "scheduler": "build.yml"}]}
        history.collect_history(data, api=Unavailable(), now=NOW)
        self.assertIsNone(data["workloads"][0]["history"])
        self.assertEqual(len(data["history"]["errors"]), 1)
        self.assertIn("403", data["history"]["pushError"])


if __name__ == "__main__":
    unittest.main()
