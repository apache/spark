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

"""Read Actions job timings for scheduled-load profiles and observed upstream push usage."""

import gzip
import json
import os
import re
import zlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.client import HTTPException
from time import sleep
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

REPOSITORY = "apache/spark"


def timestamp(value):
    if not value:
        raise ValueError("Missing GitHub Actions timestamp")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class GitHub:
    def __init__(self, token):
        self.token = token

    def get(self, path, **params):
        url = f"https://api.github.com/repos/{REPOSITORY}/{path}?{urlencode(params)}"
        request = Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Accept-Encoding": "gzip",
                "Authorization": "Bearer " + self.token,
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "spark-ci-dashboard",
            },
        )
        for attempt in range(3):
            try:
                with urlopen(request, timeout=30) as response:
                    body = response.read()
                    if response.headers.get("Content-Encoding") == "gzip":
                        body = gzip.decompress(body)
                    return json.loads(body)
            except HTTPError as error:
                if error.code not in (429, 500, 502, 503, 504) or attempt == 2:
                    # Include the status, never response headers or the authentication token.
                    raise RuntimeError(f"GitHub Actions API returned HTTP {error.code}") from None
            except (HTTPException, OSError, ValueError, EOFError, zlib.error) as error:
                if attempt == 2:
                    raise RuntimeError(
                        f"GitHub request failed after three attempts: {type(error).__name__}"
                    ) from None
            sleep(attempt + 1)


def jobs(api, run):
    result = []
    for page in range(1, 101):
        data = api.get(f"actions/runs/{run['id']}/jobs", filter="all", per_page=100, page=page)
        result.extend(data["jobs"])
        if len(result) >= data["total_count"]:
            return result
    raise ValueError("Workflow job pagination exceeded 10,000 jobs")


def uses_runner(job):
    return job.get("runner_id") != 0 and job.get("status") in (None, "in_progress", "completed")


def intervals(run_jobs, origin):
    result = []
    for job in run_jobs:
        if job.get("conclusion") == "skipped" or not job.get("started_at") or not uses_runner(job):
            continue
        if not job.get("completed_at"):
            raise ValueError("Completed run contains an unfinished job")
        start, end = timestamp(job["started_at"]), timestamp(job["completed_at"])
        if end < start:
            raise ValueError("Job completion precedes its start")
        if end > start:
            result.append([(start - origin).total_seconds(), (end - start).total_seconds()])
    return result


def profiles(api, workloads, since, until, sample_limit, timings=None):
    first = workloads[0]
    matrix = first.get("matrixBranch", False)
    branch = "master" if matrix else first["branch"]
    event = "schedule" if first["file"] == first["scheduler"] else "workflow_dispatch"
    data = api.get(
        f"actions/workflows/{quote(first['file'], safe='')}/runs",
        branch=branch,
        event=event,
        status="success",
        created=f"{since.isoformat()}..{until.isoformat()}",
        per_page=20,
    )
    # Successful first attempts represent one execution, without multi-day gaps before reruns.
    runs = [run for run in data["workflow_runs"] if run.get("run_attempt", 1) == 1][:sample_limit]
    samples = defaultdict(list)
    for run in runs:
        run_jobs = timings.get(run) if timings else jobs(api, run)
        origin = timestamp(run["run_started_at"])
        for workload in workloads:
            selected = run_jobs
            if matrix:
                pattern = rf"(?<![\w./-]){re.escape(workload['branch'])}(?![\w./-])"
                selected = [job for job in selected if re.search(pattern, job["name"])]
            profile = intervals(selected, origin)
            if profile:
                samples[(workload["file"], workload["branch"])].append(
                    (run["id"], run["run_started_at"], profile)
                )
    result = {}
    for key, values in samples.items():
        flattened = [interval for _, _, profile in values for interval in profile]
        result[key] = {
            "sampleCount": len(values),
            "runIds": [run_id for run_id, _, _ in values],
            "firstSampleAt": min(start for _, start, _ in values),
            "lastSampleAt": max(start for _, start, _ in values),
            "runnerHours": sum(duration for _, duration in flattened) / len(values) / 3600,
            "intervals": flattened,
        }
    return result


class JobTimings:
    """Cache public job timestamps for completed runs, invalidating entries after reruns."""

    def __init__(self, api, path=None):
        self.api = api
        self.path = path
        self.state = {"version": 2, "jobs": {}, "days": {}}
        if path and path.exists():
            try:
                value = json.loads(path.read_text())
                if value.get("version") == 2:
                    self.state = value
            except (OSError, ValueError):
                pass

    def get(self, run):
        key = str(run["id"])
        cached = self.state["jobs"].get(key)
        stamp = run.get("updated_at")
        completed = run.get("status", "completed") == "completed"
        if completed and stamp and cached and cached["updatedAt"] == stamp:
            return cached["jobs"]
        values = jobs(self.api, run)
        if completed and stamp:
            fields = (
                "id",
                "name",
                "status",
                "runner_id",
                "conclusion",
                "started_at",
                "completed_at",
            )
            self.state["jobs"][key] = {
                "updatedAt": stamp,
                "jobs": [{field: value.get(field) for field in fields} for value in values],
            }
        return values

    def save(self, now):
        cutoff = (now - timedelta(days=28)).date().isoformat()
        self.state["days"] = {d: v for d, v in self.state["days"].items() if d >= cutoff}
        self.state["jobs"] = {
            key: value
            for key, value in self.state["jobs"].items()
            if value["updatedAt"][:10] >= (now - timedelta(days=35)).date().isoformat()
        }
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(".tmp")
            temporary.write_text(json.dumps(self.state))
            temporary.replace(self.path)


def recent_runs(api, since, until):
    """Query one creation date at a time to stay below GitHub's 1,000-run search limit."""
    cursor = since
    result = {}
    while cursor < until:
        end = min(cursor + timedelta(days=1), until)
        found = 0
        for page in range(1, 11):
            data = api.get(
                "actions/runs",
                created=f"{cursor.isoformat()}..{end.isoformat()}",
                per_page=100,
                page=page,
            )
            if data["total_count"] > 1000:
                raise ValueError("One day exceeds GitHub's 1,000-run search limit")
            for run in data["workflow_runs"]:
                result[run["id"]] = run
            found += len(data["workflow_runs"])
            if found >= data["total_count"]:
                break
        cursor = end
    # Running older runs may still be consuming runners in the measured window.
    for page in range(1, 11):
        data = api.get("actions/runs", status="in_progress", per_page=100, page=page)
        for run in data["workflow_runs"]:
            result[run["id"]] = run
        if page * 100 >= data["total_count"]:
            break
    # Completed runs last updated before the measured window cannot contribute execution.
    values = [
        run
        for run in result.values()
        if not (
            run.get("status") == "completed"
            and run.get("updated_at")
            and timestamp(run["updated_at"]) < since + timedelta(days=1)
        )
    ]
    return sorted(values, key=lambda run: run.get("created_at", ""), reverse=True)


def observed_usage(api, workloads, since, until, timings):
    # Include the preceding creation day for jobs that spill into the first measured day.
    runs = recent_runs(api, since - timedelta(days=1), until)
    days = {}
    cursor = since
    while cursor < until:
        day = cursor.date().isoformat()
        days[day] = {
            "hours": [0.0] * 24,
            "pushHours": [0.0] * 24,
            "byBranch": {},
            "pushByBranch": {},
            "policy": {"runnerHours": 0, "peakJobs": 0},
            "through": min(cursor + timedelta(days=1), until).isoformat(),
        }
        cursor += timedelta(days=1)
    matrices = defaultdict(list)
    for w in workloads:
        if w.get("matrixBranch"):
            matrices[w["file"]].append(w["branch"])
    push_branches = defaultdict(lambda: {"runs": 0, "runnerHours": 0, "jobs": 0})
    run_count, job_count, failed_pushes = 0, 0, 0

    def measure(run):
        return run, timings.get(run)

    with ThreadPoolExecutor(max_workers=4) as executor:
        for run, run_jobs in executor.map(measure, runs):
            push = run.get("event") == "push"
            file = run.get("path", "").split("@")[0].split("/")[-1]
            polling = file == "update_build_status.yml"
            policy_events = defaultdict(list)
            touched = False
            run_hours, run_job_count = 0, 0
            for job in run_jobs:
                if (
                    job.get("conclusion") == "skipped"
                    or not job.get("started_at")
                    or not uses_runner(job)
                ):
                    continue
                start = max(since, timestamp(job["started_at"]))
                # Active jobs contribute their measured execution so far.
                finish = min(
                    until, timestamp(job["completed_at"]) if job.get("completed_at") else until
                )
                if finish <= start:
                    continue
                # Policy budgets include every upstream workflow, including status polling.
                day_start = start.replace(hour=0, minute=0, second=0, microsecond=0)
                while day_start < finish:
                    end = min(day_start + timedelta(days=1), finish)
                    begin = max(day_start, start)
                    day = day_start.date().isoformat()
                    days[day]["policy"]["runnerHours"] += (end - begin).total_seconds() / 3600
                    policy_events[day].extend([(begin, 1), (end, -1)])
                    day_start += timedelta(days=1)
                if polling:
                    continue
                touched = True
                job_count += 1
                run_job_count += 1
                run_hours += (finish - start).total_seconds() / 3600
                branch = run.get("head_branch") or "unknown"
                for candidate in matrices[file]:
                    if re.search(rf"(?<![\w./-]){re.escape(candidate)}(?![\w./-])", job["name"]):
                        branch = candidate
                        break
                slot = start.replace(minute=0, second=0, microsecond=0)
                while slot < finish:
                    end = slot + timedelta(hours=1)
                    value = (min(end, finish) - max(slot, start)).total_seconds() / 3600
                    row = days[slot.date().isoformat()]
                    hour = slot.hour
                    row["hours"][hour] += value
                    row["byBranch"].setdefault(branch, [0.0] * 24)[hour] += value
                    if push:
                        row["pushHours"][hour] += value
                        row["pushByBranch"].setdefault(branch, [0.0] * 24)[hour] += value
                    slot = end
            for day, events in policy_events.items():
                active, peak = 0, 0
                # Ends sort before starts so adjacent jobs do not count as concurrent.
                for _, change in sorted(events):
                    active += change
                    peak = max(peak, active)
                policy = days[day]["policy"]
                if peak > policy["peakJobs"]:
                    policy.update(peakJobs=peak, peakRunId=run["id"], peakWorkflow=file)
            created = run.get("created_at") or run.get("run_started_at")
            if not polling and (touched or (created and since <= timestamp(created) < until)):
                run_count += 1
                if push:
                    row = push_branches[run.get("head_branch") or "unknown"]
                    row["runs"] += 1
                    row["runnerHours"] += run_hours
                    row["jobs"] += run_job_count
                    failed_pushes += run.get("conclusion") in ("failure", "timed_out")
    push = {
        "since": since.isoformat(),
        "until": until.isoformat(),
        "runCount": sum(row["runs"] for row in push_branches.values()),
        "jobCount": sum(row["jobs"] for row in push_branches.values()),
        "runnerHours": sum(row["runnerHours"] for row in push_branches.values()),
        "failedRuns": failed_pushes,
        "branches": [{"branch": branch, **row} for branch, row in sorted(push_branches.items())],
    }
    return {"days": days, "push": push, "runCount": run_count, "jobCount": job_count}


def collect_history(data, api=None, now=None, sample_limit=3, cache=None):
    if api is None:
        token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
        if not token:
            raise ValueError("--history requires GH_TOKEN or GITHUB_TOKEN with Actions read access")
        api = GitHub(token)
    now = now or datetime.now(timezone.utc)
    since = now - timedelta(days=28)
    timings = JobTimings(api, cache)
    history = {
        "since": since.isoformat(),
        "until": now.isoformat(),
        "sampleLimit": sample_limit,
        "errors": [],
    }
    # Prioritize actual measurements; profiles can reuse the same cached job records.
    # Bootstrap a shorter window under the Actions token quota; warm caches extend it.
    actual_days = 7 if timings.state["jobs"] or timings.state["days"] else 3
    actual_since = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        days=actual_days
    )
    try:
        actual = observed_usage(api, data["workloads"], actual_since, now, timings)
        timings.state["days"].update(actual["days"])
        timings.state["push"] = actual["push"]
    except (RuntimeError, OSError, ValueError, KeyError) as error:
        history["actualError"] = str(error)
    groups = defaultdict(list)
    for workload in data["workloads"]:
        branch = "master" if workload.get("matrixBranch") else workload["branch"]
        groups[(workload["file"], branch)].append(workload)

    def sample(group):
        try:
            return profiles(api, group, since, now, sample_limit, timings), None
        except (RuntimeError, OSError, ValueError, KeyError) as error:
            return {}, {"file": group[0]["file"], "branch": group[0]["branch"], "error": str(error)}

    collected = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        for result, error in executor.map(sample, groups.values()):
            collected.update(result)
            if error:
                history["errors"].append(error)
    for workload in data["workloads"]:
        workload["history"] = collected.get((workload["file"], workload["branch"]))
    timings.save(now)
    history["observed"] = {"days": timings.state["days"]}
    if "push" in timings.state:
        history["push"] = timings.state["push"]
    else:
        history["pushError"] = history.get("actualError", "Actual timings are unavailable")
    data["history"] = history
