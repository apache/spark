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


def intervals(run_jobs, origin):
    result = []
    for job in run_jobs:
        if job.get("conclusion") == "skipped" or not job.get("started_at"):
            continue
        if not job.get("completed_at"):
            raise ValueError("Completed run contains an unfinished job")
        start, end = timestamp(job["started_at"]), timestamp(job["completed_at"])
        if end < start:
            raise ValueError("Job completion precedes its start")
        if end > start:
            result.append([(start - origin).total_seconds(), (end - start).total_seconds()])
    return result


def profiles(api, workloads, since, until, sample_limit):
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
        run_jobs = jobs(api, run)
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


def observed_push(api, since, until):
    runs = []
    for page in range(1, 11):
        data = api.get(
            "actions/runs",
            event="push",
            status="completed",
            created=f"{since.isoformat()}..{until.isoformat()}",
            per_page=100,
            page=page,
        )
        if data["total_count"] > 1000:
            raise ValueError("Push history exceeds GitHub's 1,000-run search limit")
        runs.extend(data["workflow_runs"])
        if len(runs) >= data["total_count"]:
            break
    totals = defaultdict(lambda: {"runs": 0, "runnerHours": 0, "jobs": 0})

    def measure(run):
        origin = timestamp(run.get("run_started_at") or run.get("created_at"))
        profile = intervals(jobs(api, run), origin)
        return run, sum(duration for _, duration in profile) / 3600, len(profile)

    with ThreadPoolExecutor(max_workers=4) as executor:
        for run, hours, count in executor.map(measure, runs):
            row = totals[run["head_branch"]]
            row["runs"] += 1
            row["runnerHours"] += hours
            row["jobs"] += count
    return {
        "since": since.isoformat(),
        "until": until.isoformat(),
        "runCount": len(runs),
        "failedRuns": sum(run["conclusion"] == "failure" for run in runs),
        "runnerHours": sum(row["runnerHours"] for row in totals.values()),
        "jobCount": sum(row["jobs"] for row in totals.values()),
        "branches": [{"branch": branch, **row} for branch, row in sorted(totals.items())],
    }


def collect_history(data, api=None, now=None, sample_limit=3):
    if api is None:
        token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
        if not token:
            raise ValueError("--history requires GH_TOKEN or GITHUB_TOKEN with Actions read access")
        api = GitHub(token)
    now = now or datetime.now(timezone.utc)
    since = now - timedelta(days=28)
    groups = defaultdict(list)
    for workload in data["workloads"]:
        branch = "master" if workload.get("matrixBranch") else workload["branch"]
        groups[(workload["file"], branch)].append(workload)
    errors = []

    def sample(group):
        try:
            return profiles(api, group, since, now, sample_limit), None
        except (RuntimeError, OSError, ValueError, KeyError) as error:
            return {}, {"file": group[0]["file"], "branch": group[0]["branch"], "error": str(error)}

    collected = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        for result, error in executor.map(sample, groups.values()):
            collected.update(result)
            if error:
                errors.append(error)
    for workload in data["workloads"]:
        workload["history"] = collected.get((workload["file"], workload["branch"]))
    history = {
        "since": since.isoformat(),
        "until": now.isoformat(),
        "sampleLimit": sample_limit,
        "errors": errors,
    }
    try:
        history["push"] = observed_push(api, now - timedelta(days=7), now)
    except (RuntimeError, OSError, ValueError, KeyError) as error:
        history["pushError"] = str(error)
    data["history"] = history
