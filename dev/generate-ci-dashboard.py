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

"""Generate the static CI dashboard snapshot; never execute workflow expressions or steps."""

import argparse
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import yaml


def git(root, *args):
    return subprocess.check_output(
        ["git", "-C", str(root), *args], text=True, stderr=subprocess.PIPE
    ).strip()


def read_workflows(root):
    # BaseLoader does not evaluate expressions or construct custom YAML objects.
    return {
        path.name: yaml.load(path.read_text(), yaml.BaseLoader)
        for path in sorted((root / ".github/workflows").iterdir())
        if path.suffix in (".yml", ".yaml")
    }


def triggers(workflow):
    # BaseLoader preserves GitHub's `on` key, which YAML 1.1 treats as a boolean.
    value = workflow.get("on", {})
    if isinstance(value, str):
        return {value: None}
    if isinstance(value, list):
        return dict.fromkeys(value)
    return value


def branch_targets(workflow):
    """Resolve a literal branch matrix, including workflow_dispatch's fromJSON default."""
    branches = set()
    for job in workflow.get("jobs", {}).values():
        matrix = job.get("strategy", {}).get("matrix", {})
        if not isinstance(matrix, dict) or "branch" not in matrix:
            continue
        value = matrix["branch"]
        if isinstance(value, str):
            match = re.fullmatch(
                r"\$\{\{\s*fromJSON\(\s*inputs\.branch\s*\|\|\s*'([^']+)'\s*\)\s*}}",
                value,
            )
            if not match:
                raise ValueError("Cannot resolve scheduled branch matrix: " + value)
            value = json.loads(match[1])
        if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
            raise ValueError("Expected a list of scheduled branches")
        branches.update(value)
    return sorted(branches) or ["master"]


def dispatches(workflow, crons):
    """Resolve the supported scheduler patterns, failing when a dispatch is ambiguous."""
    result = []
    for job in workflow.get("jobs", {}).values():
        for step in job.get("steps", []):
            run = step.get("run", "")
            match = re.search(r"dispatch-if-changed\.sh\s+([\w.-]+\.ya?ml)\s+([\w./-]+)", run)
            if not match:
                match = re.search(
                    r"gh workflow run\s+([\w.-]+\.ya?ml)[^\n]*?--ref\s+([\w./-]+)", run
                )
            if not match:
                continue
            condition = step.get("if", "")
            env = {**workflow.get("env", {}), **job.get("env", {}), **step.get("env", {})}
            selected = re.findall(r"github\.event\.schedule\s*==\s*'([^']+)'", condition)
            selected += re.findall(r'github\.event\.schedule\s*==\s*"([^"]+)"', condition)
            for variable in re.findall(r"github\.event\.schedule\s*==\s*env\.([\w]+)", condition):
                selected.append(env.get(variable))
            if not selected or any(cron not in crons for cron in selected):
                raise ValueError("Cannot resolve scheduled dispatch: " + step.get("name", run))
            result.append((match[1], match[2], sorted(set(selected)), "dispatch-if-changed" in run))
    if result and any(not any(cron in entry[2] for entry in result) for cron in crons):
        raise ValueError("Each scheduler cron must select at least one target")
    return result


def snapshot(workflows, revision):
    inventory = []
    workloads = []
    for filename, workflow in sorted(workflows.items()):
        events = triggers(workflow)
        jobs = workflow.get("jobs", {})
        inventory.append(
            {
                "file": filename,
                "name": workflow.get("name", filename),
                "events": sorted(events),
                "jobCount": len(jobs),
                "calls": sorted(
                    {
                        Path(job["uses"]).name
                        for job in jobs.values()
                        if job.get("uses", "").startswith("./.github/workflows/")
                    }
                ),
            }
        )
        schedules = events.get("schedule") or []
        if any(s.get("timezone", "UTC") != "UTC" for s in schedules):
            raise ValueError(filename + ": the dashboard currently supports UTC schedules only")
        crons = [entry["cron"] for entry in schedules]
        if not crons:
            continue
        targets = dispatches(workflow, crons)
        matrix_branch = not targets and any(
            isinstance(job.get("strategy", {}).get("matrix"), dict)
            and "branch" in job["strategy"]["matrix"]
            for job in jobs.values()
        )
        if not targets:
            if filename.endswith("_scheduler.yml"):
                raise ValueError("Unrecognized scheduler: " + filename)
            targets = [(filename, branch, crons, False) for branch in branch_targets(workflow)]
        for target, branch, selected, skip in targets:
            workloads.append(
                {
                    "file": target,
                    "name": workflows.get(target, {}).get("name", target),
                    "branch": branch,
                    "scheduler": filename,
                    "crons": selected,
                    "skipIfUnchanged": skip,
                    "matrixBranch": matrix_branch,
                }
            )
    ids = [(w["file"], w["branch"]) for w in workloads]
    if len(set(ids)) != len(ids):
        raise ValueError("Multiple schedule sources for a target; merge them before visualization")
    return {
        "revision": revision,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "workflows": inventory,
        "workloads": workloads,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--history-cache", type=Path, help="Cache public completed-job timings")
    parser.add_argument("--history", action="store_true", help="Sample public Actions job timings")
    args = parser.parse_args()
    try:
        revision = git(args.repo, "rev-parse", "HEAD")
    except (FileNotFoundError, subprocess.CalledProcessError):
        revision = "master"
    data = snapshot(read_workflows(args.repo), revision)
    if args.history:
        from ci_dashboard_history import collect_history

        collect_history(data, cache=args.history_cache)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, indent=2) + "\n")


if __name__ == "__main__":
    main()
