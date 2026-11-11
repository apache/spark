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

import importlib.util
import unittest
from pathlib import Path

import yaml

spec = importlib.util.spec_from_file_location(
    "ci_dashboard", Path(__file__).resolve().parents[2] / "dev/generate-ci-dashboard.py"
)
dashboard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dashboard)


class CIDashboardTests(unittest.TestCase):
    def test_anchored_dispatch(self):
        workflow = yaml.load(
            """
name: Scheduler
on:
  schedule:
    - cron: &build_cron '17 6 1-28/3 * *'
jobs:
  schedule:
    env:
      BUILD_CRON: *build_cron
    steps:
      - name: Maven
        if: github.event.schedule == env.BUILD_CRON
        run: ./dev/dispatch-if-changed.sh build_maven.yml branch-4.x
""",
            yaml.BaseLoader,
        )
        data = dashboard.snapshot({"branch4x_scheduler.yml": workflow}, "abc")
        self.assertEqual(len(data["workloads"]), 1)
        target = data["workloads"][0]
        self.assertEqual(target["branch"], "branch-4.x")
        self.assertEqual(target["file"], "build_maven.yml")
        self.assertEqual(target["crons"], ["17 6 1-28/3 * *"])
        self.assertTrue(target["skipIfUnchanged"])
        self.assertEqual(data["workflows"][0]["events"], ["schedule"])

    def test_literal_dispatch_and_unmapped_cron(self):
        workflow = {
            "on": {"schedule": [{"cron": "0 1 * * *"}]},
            "jobs": {
                "schedule": {
                    "steps": [
                        {
                            "if": "github.event.schedule == '0 1 * * *'",
                            "run": "gh workflow run build.yml --repo apache/spark --ref branch-4.x",
                        }
                    ]
                }
            },
        }
        self.assertEqual(dashboard.dispatches(workflow, ["0 1 * * *"])[0][1], "branch-4.x")
        second = dict(workflow["jobs"]["schedule"]["steps"][0])
        second["run"] = "gh workflow run other.yml --repo apache/spark --ref branch-4.x"
        workflow["jobs"]["schedule"]["steps"].append(second)
        self.assertEqual(len(dashboard.dispatches(workflow, ["0 1 * * *"])), 2)
        with self.assertRaisesRegex(ValueError, "at least one"):
            dashboard.dispatches(workflow, ["0 1 * * *", "0 2 * * *"])
        workflow["jobs"]["schedule"]["steps"][0]["if"] = "unknown"
        with self.assertRaisesRegex(ValueError, "Cannot resolve"):
            dashboard.dispatches(workflow, ["0 1 * * *"])

    def test_branch_matrix_defaults(self):
        workflow = {
            "on": {"schedule": [{"cron": "0 1 * * *"}]},
            "jobs": {
                "publish": {
                    "strategy": {
                        "matrix": {
                            "branch": "${{ fromJSON( inputs.branch || "
                            '\'["master", "branch-4.x"]\' ) }}'
                        }
                    }
                }
            },
        }
        data = dashboard.snapshot({"publish.yml": workflow}, "abc")
        self.assertEqual([w["branch"] for w in data["workloads"]], ["branch-4.x", "master"])
        workflow["jobs"]["publish"]["strategy"]["matrix"]["branch"] = "${{ unknown }}"
        with self.assertRaisesRegex(ValueError, "Cannot resolve"):
            dashboard.branch_targets(workflow)

    def test_inventory_includes_non_scheduled_workflows(self):
        data = dashboard.snapshot(
            {
                "push.yml": {"on": ["push", "workflow_dispatch"]},
                "reusable.yml": {
                    "on": "workflow_call",
                    "jobs": {
                        "build": {"needs": "prepare", "uses": "./.github/workflows/reusable.yml"}
                    },
                },
            },
            "abc",
        )
        self.assertEqual(data["workloads"], [])
        self.assertEqual(len(data["workflows"]), 2)
        self.assertEqual(data["workflows"][1]["jobCount"], 1)
        self.assertEqual(data["workflows"][1]["calls"], ["reusable.yml"])

    def test_unrecognized_scheduler_and_timezone(self):
        workflow = {"on": {"schedule": [{"cron": "0 0 * * *"}]}}
        with self.assertRaisesRegex(ValueError, "Unrecognized scheduler"):
            dashboard.snapshot({"branch_scheduler.yml": workflow}, "abc")
        workflow["on"]["schedule"][0]["timezone"] = "America/New_York"
        with self.assertRaisesRegex(ValueError, "UTC"):
            dashboard.snapshot({"build.yml": workflow}, "abc")


if __name__ == "__main__":
    unittest.main()
