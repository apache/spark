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

"""
Render the schedule of the scheduled CI workflows as an SVG timeline.

The schedules are read from the cron entries in .github/workflows: workflows scheduled
directly run on master, and the branch CI schedulers (branch*_scheduler.yml) dispatch
workflows on maintenance branches through dev/dispatch-if-changed.sh. Each run is drawn
from its cron start time for its estimated duration, with its estimated number of runners,
both from ESTIMATES below. There is one panel per cadence (daily, every N days, weekly),
followed by a heatmap of the combined runner load.

Every scheduled workflow must be listed in either ESTIMATES or EXCLUDED_WORKFLOWS, so adding
a scheduled workflow requires deciding whether it belongs in the image.

Usage:
    dev/visualize_ci_schedule.py           Regenerate the image.
    dev/visualize_ci_schedule.py --check   Exit with an error if the image is out of date.
    dev/visualize_ci_schedule.py --sample  Print estimates measured from recent runs.

The image only depends on the workflow files and this script, and the script only uses the
Python standard library. --sample additionally needs an authenticated GitHub CLI (gh).
"""

import argparse
import concurrent.futures
import datetime
import glob
import json
import math
import os
import re
import statistics
import subprocess
import sys
import textwrap
from collections import namedtuple
from xml.sax.saxutils import escape

SPARK_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOW_DIR = os.path.join(SPARK_HOME, ".github", "workflows")
DEFAULT_OUTPUT = os.path.join(WORKFLOW_DIR, "images", "scheduled-ci.svg")
SCRIPT_NAME = "dev/visualize_ci_schedule.py"


# ---------------------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------------------

# Estimated cost of one run of each scheduled workflow, keyed by the workflow file that does
# the work (for branch builds, the dispatched workflow):
#   hours:   wall-clock time from the start of the first job to the end of the last one.
#   runners: average number of runners in use during that time, i.e. the job-hours of a run
#            divided by `hours`. ASF limits GitHub Actions usage in full-time runners.
# The values are medians of recent apache/spark runs printed by `--sample` (last sampled on
# 2026-11-11), with the range of the sampled runs in the comments. They are rough; refresh
# them when a workflow becomes notably faster, slower, or wider.
Estimate = namedtuple("Estimate", ["hours", "runners"])

ESTIMATES = {
    "build_codegen_jdk.yml": Estimate(3.5, 8),  # 10 runs: 2.4-9.7h, 3-11 runners
    "build_coverage.yml": Estimate(2, 5),  # 10 runs: 1.8-11.3h, 1-5 runners
    "build_java17.yml": Estimate(2.5, 7),  # 10 runs: 1.8-4.2h, 4-8 runners
    "build_java21.yml": Estimate(2.5, 8),  # 10 runs: 2.1-3.8h, 6-9 runners
    "build_java25.yml": Estimate(2.5, 8),  # 10 runs: 2.1-4.3h, 5-9 runners
    "build_maven.yml": Estimate(2.5, 4),  # 10 runs: 2.1-3.0h, 3-5 runners
    "build_maven_java21.yml": Estimate(3, 4),  # 10 runs: 2.3-3.9h, 3-5 runners
    "build_maven_java21_arm.yml": Estimate(2.5, 4),  # 10 runs: 2.4-3.0h, 4-5 runners
    "build_maven_java21_macos26.yml": Estimate(4.5, 4),  # 10 runs: 4.1-5.9h, 4-4 runners
    "build_maven_java25.yml": Estimate(2.5, 4),  # 10 runs: 2.2-4.0h, 3-5 runners
    "build_non_ansi.yml": Estimate(2.5, 9),  # 10 runs: 1.6-2.9h, 7-10 runners
    "build_python_3.11.yml": Estimate(2, 4),  # 10 runs: 1.3-4.0h, 2-5 runners
    "build_python_3.12_arm.yml": Estimate(1.5, 6),  # 10 runs: 1.3-1.5h, 5-6 runners
    "build_python_3.12_classic_only.yml": Estimate(1.5, 3),  # 10 runs: 1.3-3.8h, 1-4 runners
    "build_python_3.12_macos26.yml": Estimate(2, 5),  # 10 runs: 1.7-2.1h, 5-5 runners
    "build_python_3.12_pandas_3.yml": Estimate(0.5, 2),  # 10 runs: 0.3-2.8h, 1-5 runners
    "build_python_3.13.yml": Estimate(1.5, 5),  # 10 runs: 1.3-3.1h, 2-5 runners
    "build_python_3.14.yml": Estimate(1.5, 5),  # 10 runs: 1.3-2.3h, 3-6 runners
    "build_python_3.14_nogil.yml": Estimate(1.5, 3),  # 10 runs: 1.3-5.8h, 1-4 runners
    "build_python_3.15.yml": Estimate(1, 2),  # 10 runs: 0.5-1.6h, 1-2 runners
    "build_python_3.9.yml": Estimate(4.5, 4),  # 10 runs: 3.5-8.4h, 2-4 runners
    "build_python_connect.yml": Estimate(1, 1),  # 10 runs: 1.0-1.2h, 1-1 runners
    "build_python_connect40.yml": Estimate(1, 1),  # 10 runs: 0.7-1.1h, 1-1 runners
    "build_python_minimum.yml": Estimate(2, 4),  # 10 runs: 1.4-8.9h, 1-6 runners
    "build_python_pypy3.10.yml": Estimate(1, 2),  # 10 runs: 0.7-2.8h, 1-2 runners
    "build_rockdb_as_ui_backend.yml": Estimate(2, 7),  # 10 runs: 1.9-3.4h, 5-8 runners
    "build_scala213.yml": Estimate(2, 6),  # 10 runs: 1.7-6.5h, 2-7 runners
    "build_sparkr_window.yml": Estimate(1.5, 1),  # 10 runs: 1.0-1.4h, 1-1 runners
    "build_uds.yml": Estimate(2, 8),  # 10 runs: 2.0-2.6h, 7-9 runners
    "publish_snapshot.yml": Estimate(1.5, 5),  # 10 runs: 1.0-1.5h, 4-6 runners
    "release.yml": Estimate(2, 1),  # 10 runs: 1.4-2.5h, 1-1 runners
}

# Scheduled workflows that are not drawn, because they usually finish within an hour and do
# not run tests, with what they do.
EXCLUDED_WORKFLOWS = {
    "build_infra_images_cache.yml": "refreshes the CI image cache",
    "labeler.yml": "labels PRs",
    "pages.yml": "deploys the documentation site",
    "stale.yml": "closes stale PRs",
    "update_build_status.yml": "syncs PR build status",
}

# Branch that workflows scheduled directly (not through a branch scheduler) run on.
DEFAULT_BRANCH = "master"

# ASF limits on the GitHub Actions usage of apache/spark, as averages in full-time runners over
# all of its workflows: over a calendar week, and over any five consecutive days. See
# https://infra.apache.org/github-actions-policy.html.
ASF_WEEKLY_RUNNERS = 25
ASF_FIVE_DAY_RUNNERS = 30


# ---------------------------------------------------------------------------------------
# Workflow parsing
# ---------------------------------------------------------------------------------------

# A scheduled run of `workflow` on `branch`, triggered by `cron` in the `source` workflow.
Schedule = namedtuple("Schedule", ["workflow", "branch", "cron", "source"])

# A schedule placed on a timeline repeating every `period` days. `starts` are the start
# minutes within the period, `duration` is in minutes, `runners` is the average number of
# runners in use, and `days` are the days of the month for day-of-month cadences (None
# otherwise).
Timeline = namedtuple("Timeline", ["schedule", "period", "starts", "duration", "runners", "days"])

CRON_LINE_RE = re.compile(r"""^\s*-\s*cron:\s*(?:&([\w-]+)\s+)?(["'])([^"']+)\2\s*(?:#.*)?$""")
ENV_ALIAS_RE = re.compile(r"^\s*(\w+):\s*\*([\w-]+)\s*$", re.MULTILINE)
STEP_RE = re.compile(r"^\s*- name:", re.MULTILINE)
DISPATCH_RE = re.compile(r"dispatch-if-changed\.sh\s+(\S+)\s+(\S+)")
SCHEDULE_CONDITION_RE = re.compile(r"github\.event\.schedule\s*==\s*env\.(\w+)")

WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]


class ScheduleError(Exception):
    pass


def read_crons(path, text):
    """Returns the (anchor, cron) pairs of the cron entries in a workflow file."""
    crons = []
    for lineno, line in enumerate(text.splitlines(), 1):
        if not re.match(r"^\s*-?\s*cron:", line):
            continue
        m = CRON_LINE_RE.match(line)
        if not m:
            raise ScheduleError("%s:%d: cannot parse the cron entry %r" % (path, lineno, line))
        crons.append((m.group(1), m.group(3).strip()))
    return crons


def parse_scheduler(name, text, crons):
    """Returns the schedules of a branch scheduler that dispatches workflows on branches."""
    anchors = {}
    for anchor, cron in crons:
        if anchor is None:
            raise ScheduleError("%s: cron '%s' needs a YAML anchor" % (name, cron))
        anchors[anchor] = cron
    env_to_anchor = dict(ENV_ALIAS_RE.findall(text))
    used = set()
    schedules = []
    for step in STEP_RE.split(text)[1:]:
        dispatch = DISPATCH_RE.search(step)
        if not dispatch:
            continue
        conditions = SCHEDULE_CONDITION_RE.findall(step)
        if len(conditions) != 1:
            raise ScheduleError(
                "%s: the step dispatching %s on %s must check exactly one cron, found %s"
                % (name, dispatch.group(1), dispatch.group(2), conditions)
            )
        anchor = env_to_anchor.get(conditions[0])
        if anchor not in anchors:
            raise ScheduleError(
                "%s: env.%s does not refer to a cron anchor" % (name, conditions[0])
            )
        used.add(anchor)
        schedules.append(Schedule(dispatch.group(1), dispatch.group(2), anchors[anchor], name))
    unused = sorted(set(anchors) - used)
    if unused:
        raise ScheduleError("%s: crons not used by any dispatch step: %s" % (name, unused))
    return schedules


def collect_schedules(workflow_dir):
    schedules = []
    paths = glob.glob(os.path.join(workflow_dir, "*.yml"))
    paths += glob.glob(os.path.join(workflow_dir, "*.yaml"))
    for path in sorted(paths):
        name = os.path.basename(path)
        with open(path, encoding="utf-8") as f:
            text = f.read()
        crons = read_crons(name, text)
        if not crons:
            continue
        if DISPATCH_RE.search(text):
            schedules.extend(parse_scheduler(name, text, crons))
        else:
            schedules.extend(Schedule(name, DEFAULT_BRANCH, cron, name) for _, cron in crons)
    return schedules


# ---------------------------------------------------------------------------------------
# Cron handling
# ---------------------------------------------------------------------------------------


def parse_cron_field(field, lo, hi):
    """Expands a cron field such as '*', '5', '1-28/3', '*/10' or '1,3' into its values."""
    values = set()
    for part in field.split(","):
        m = re.match(r"^(\*|\d+(?:-\d+)?)(?:/(\d+))?$", part)
        if not m:
            raise ScheduleError("unsupported cron field '%s'" % field)
        step = int(m.group(2) or 1)
        if m.group(1) == "*":
            start, end = lo, hi
        elif "-" in m.group(1):
            start, end = (int(v) for v in m.group(1).split("-"))
        else:
            start = int(m.group(1))
            end = hi if m.group(2) else start
        if not lo <= start <= end <= hi or step < 1:
            raise ScheduleError("cron field '%s' is out of range [%d, %d]" % (field, lo, hi))
        values.update(range(start, end + 1, step))
    return sorted(values)


def to_timeline(schedule, estimate):
    """Places a schedule on a repeating timeline according to its cron expression.

    Supported cadences are daily ('* *' day of month and week), weekly (a day-of-week
    list) and every N days (an arithmetic day-of-month progression such as '1-28/3').
    """
    fields = schedule.cron.split()
    if len(fields) != 5:
        raise ScheduleError("%s: invalid cron '%s'" % (schedule.source, schedule.cron))
    minute, hour, dom, month, dow = fields
    try:
        minutes = parse_cron_field(minute, 0, 59)
        hours = parse_cron_field(hour, 0, 23)
        if month != "*":
            raise ScheduleError("month restrictions are not supported")
        if dom == "*" and dow == "*":
            period, day_offsets, days = 1, [0], None
        elif dom == "*":
            period, days = 7, None
            day_offsets = sorted({d % 7 for d in parse_cron_field(dow, 0, 7)})
        elif dow == "*":
            days = parse_cron_field(dom, 1, 31)
            period = days[1] - days[0] if len(days) > 1 else 0
            if period < 2 or any(b - a != period for a, b in zip(days, days[1:])):
                raise ScheduleError("the day of month must be a range with a step, e.g. 1-28/3")
            day_offsets = [(days[0] - 1) % period]
        else:
            raise ScheduleError("restricting both day of month and day of week is unsupported")
    except ScheduleError as e:
        raise ScheduleError("%s: cron '%s': %s" % (schedule.source, schedule.cron, e))
    starts = sorted(d * 1440 + h * 60 + m for d in day_offsets for h in hours for m in minutes)
    duration = int(round(estimate.hours * 60))
    return Timeline(schedule, period, starts, duration, estimate.runners, days)


def build_timelines(schedules):
    scheduled = {s.workflow for s in schedules}
    errors = []
    for workflow in sorted(scheduled):
        if workflow not in ESTIMATES and workflow not in EXCLUDED_WORKFLOWS:
            errors.append(
                "%s is scheduled but has no estimate. Add it to ESTIMATES (see --sample), or "
                "to EXCLUDED_WORKFLOWS if it usually finishes within an hour and does not run "
                "tests, in %s." % (workflow, SCRIPT_NAME)
            )
        if workflow in ESTIMATES and workflow in EXCLUDED_WORKFLOWS:
            errors.append("%s is in both ESTIMATES and EXCLUDED_WORKFLOWS." % workflow)
    for table, entries in (
        ("ESTIMATES", ESTIMATES),
        ("EXCLUDED_WORKFLOWS", EXCLUDED_WORKFLOWS),
    ):
        for workflow in sorted(set(entries) - scheduled):
            errors.append(
                "%s is no longer scheduled. Remove it from %s in %s."
                % (workflow, table, SCRIPT_NAME)
            )
    if errors:
        raise ScheduleError("\n".join(errors))
    timelines = [
        to_timeline(s, ESTIMATES[s.workflow])
        for s in schedules
        if s.workflow not in EXCLUDED_WORKFLOWS
    ]
    excluded = [s for s in schedules if s.workflow in EXCLUDED_WORKFLOWS]
    return timelines, excluded


def phase_name(offset):
    return chr(ord("A") + offset)


def describe_start(period, start):
    """Formats a start minute within a period, e.g. 'Sat 15:17' or 'B 06:17'."""
    day, minute = divmod(start, 1440)
    clock = "%02d:%02d" % divmod(minute, 60)
    if period == 7:
        return "%s %s" % (WEEKDAYS[day], clock)
    if period > 1:
        return "%s %s" % (phase_name(day), clock)
    return clock


def describe_cadence(timeline):
    period = timeline.period
    if period == 1:
        return "daily"
    if period == 7:
        return "weekly"
    return "every %d days" % period


def concurrency(intervals, length):
    """Returns the total weight of the running intervals at each minute of a cyclic timeline.

    `intervals` are (start, duration, weight) triples, in minutes; they wrap around at
    `length`.
    """
    diff = [0] * (length + 1)
    for start, duration, weight in intervals:
        remaining = duration
        start %= length
        while remaining > 0:
            end = min(length, start + remaining)
            diff[start] += weight
            diff[end] -= weight
            remaining -= end - start
            start = 0
    counts, running = [], 0
    for value in diff[:length]:
        running += value
        counts.append(running)
    return counts


def lcm(a, b):
    return a * b // math.gcd(a, b)


# ---------------------------------------------------------------------------------------
# Sampling recent runs
# ---------------------------------------------------------------------------------------

SAMPLE_REPO = "apache/spark"
# Only first attempts of scheduled or dispatched runs that ran to the end are sampled, since
# re-running failed jobs keeps the timestamps of the jobs that passed. Successful runs are
# preferred because failed runs may have stopped early.
SAMPLE_EVENTS = ("schedule", "workflow_dispatch")
SAMPLE_CONCLUSIONS = ("success", "failure")
SAMPLE_PARALLELISM = 8
JOB_TIMES_JQ = (
    '.jobs[] | select(.conclusion != "skipped" and .started_at != null'
    ' and .completed_at != null) | "\\(.started_at) \\(.completed_at)"'
)


def gh(args):
    try:
        result = subprocess.run(
            ["gh"] + args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
    except OSError:
        raise ScheduleError("--sample needs the GitHub CLI (gh).")
    if result.returncode != 0:
        raise ScheduleError("gh %s failed: %s" % (" ".join(args), result.stderr.strip()))
    return result.stdout


def parse_time(text):
    return datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ")


def sample_run(run_id):
    """Returns the (wall-clock hours, job-hours) of a workflow run, or None if it has no jobs."""
    path = "repos/%s/actions/runs/%d/jobs?per_page=100" % (SAMPLE_REPO, run_id)
    jobs = []
    for line in gh(["api", "--paginate", path, "--jq", JOB_TIMES_JQ]).splitlines():
        if line.strip():
            started, completed = (parse_time(t) for t in line.split())
            jobs.append((started, max(started, completed)))
    if not jobs:
        return None
    wall = (max(c for _, c in jobs) - min(s for s, _ in jobs)).total_seconds() / 3600.0
    busy = sum((c - s).total_seconds() for s, c in jobs) / 3600.0
    return (wall, busy) if wall > 0 else None


def sample_workflow(workflow, runs):
    """Returns the (wall-clock hours, job-hours) of the latest `runs` runs of a workflow."""
    fields = "databaseId,event,status,conclusion,attempt"
    listing = json.loads(
        gh(
            ["run", "list", "--repo", SAMPLE_REPO, "--workflow", workflow, "--limit", "100"]
            + ["--json", fields]
        )
    )
    candidates = [
        run
        for run in listing
        if run["event"] in SAMPLE_EVENTS
        and run["status"] == "completed"
        and run["conclusion"] in SAMPLE_CONCLUSIONS
        and run["attempt"] == 1
    ]
    # Latest successful runs first, then the latest failed runs if there are not enough.
    candidates.sort(key=lambda run: run["conclusion"] != "success")
    samples = []
    for run in candidates:
        if len(samples) == runs:
            break
        sample = sample_run(run["databaseId"])
        if sample:
            samples.append(sample)
    return samples


def sample_estimates(runs):
    """Prints an ESTIMATES table measured from the latest runs of every scheduled workflow."""
    workflows = sorted({s.workflow for s in collect_schedules(WORKFLOW_DIR)})
    print("Sampling %d workflows in %s ..." % (len(workflows), SAMPLE_REPO), file=sys.stderr)
    with concurrent.futures.ThreadPoolExecutor(SAMPLE_PARALLELISM) as pool:
        all_samples = list(pool.map(lambda w: sample_workflow(w, runs), workflows))
    lines = []
    for workflow, samples in zip(workflows, all_samples):
        if not samples:
            lines.append("    # %s: no completed runs found" % workflow)
            continue
        walls = [w for w, _ in samples]
        widths = [b / w for w, b in samples]
        hours = max(0.5, round(statistics.median(walls) * 2) / 2.0)
        runners = max(1, int(round(statistics.median(widths))))
        lines.append(
            '    "%s": Estimate(%g, %d),  # %d runs: %.1f-%.1fh, %.0f-%.0f runners'
            % (
                workflow,
                hours,
                runners,
                len(samples),
                min(walls),
                max(walls),
                min(widths),
                max(widths),
            )
        )
    print("\n".join(lines))


# ---------------------------------------------------------------------------------------
# SVG rendering
# ---------------------------------------------------------------------------------------

WIDTH = 1240
MARGIN = 20
LEFT = 372  # x where the plots start; the row labels live to the left of it.
PLOT_WIDTH = WIDTH - LEFT - 36
ROW_HEIGHT = 18
# Bar thickness grows with the number of runners, up to the widest workflow.
MIN_BAR_HEIGHT = 4
MAX_BAR_HEIGHT = 14

FONT_SANS = 'system-ui, -apple-system, "Segoe UI", Helvetica, Arial, sans-serif'
FONT_MONO = 'ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace'

SURFACE = "#fcfcfb"
ZEBRA = "#f4f3ef"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
# Categorical colors assigned to branches in a fixed order: master, then newest first.
BRANCH_COLORS = [
    "#2a78d6",
    "#eb6834",
    "#1baf7a",
    "#eda100",
    "#e87ba4",
    "#008300",
    "#6250d6",
    "#e34948",
]
OTHER_BRANCH_COLOR = "#898781"
# Sequential ramp for the load heatmap, from no load to the peak.
HEAT_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
HEAT_ZERO = "#f4f3ef"


def branch_sort_key(branch):
    if branch == DEFAULT_BRANCH:
        return (0, ())
    version = re.findall(r"\d+|x", branch)
    return (1, tuple(-(10**6) if v == "x" else -int(v) for v in version), branch)


def fmt(value):
    """Formats a coordinate deterministically with at most one decimal."""
    text = "%.1f" % value
    return text[:-2] if text.endswith(".0") else text


def fmt_hours(minutes):
    return "%gh" % (minutes / 60.0)


class Svg:
    def __init__(self):
        self.parts = []

    def add(self, element):
        self.parts.append(element)

    def text(
        self, x, y, content, size=12, fill=INK, anchor="start", weight=None, mono=False, title=None
    ):
        attrs = 'x="%s" y="%s" font-size="%s" fill="%s"' % (fmt(x), fmt(y), size, fill)
        if anchor != "start":
            attrs += ' text-anchor="%s"' % anchor
        if weight:
            attrs += ' font-weight="%s"' % weight
        if mono:
            attrs += " font-family='%s'" % FONT_MONO
        body = escape(content)
        if title:
            body = "<title>%s</title>%s" % (escape(title), body)
        self.add("<text %s>%s</text>" % (attrs, body))

    def rect(self, x, y, w, h, fill, rx=0, title=None, extra=""):
        attrs = 'x="%s" y="%s" width="%s" height="%s" fill="%s"' % (
            fmt(x),
            fmt(y),
            fmt(w),
            fmt(h),
            fill,
        )
        if rx:
            attrs += ' rx="%s"' % rx
        if extra:
            attrs += " " + extra
        if title:
            self.add("<rect %s><title>%s</title></rect>" % (attrs, escape(title)))
        else:
            self.add("<rect %s/>" % attrs)

    def line(self, x1, y1, x2, y2, stroke=GRID, width=1, dash=None):
        attrs = 'x1="%s" y1="%s" x2="%s" y2="%s" stroke="%s" stroke-width="%s"' % (
            fmt(x1),
            fmt(y1),
            fmt(x2),
            fmt(y2),
            stroke,
            width,
        )
        if dash:
            attrs += ' stroke-dasharray="%s"' % dash
        self.add("<line %s/>" % attrs)


def wrap(text, width=175):
    return textwrap.wrap(text, width) or [""]


class Renderer:
    def __init__(self, timelines, excluded):
        self.timelines = timelines
        self.excluded = excluded
        branches = sorted({t.schedule.branch for t in timelines}, key=branch_sort_key)
        self.branch_colors = {
            b: BRANCH_COLORS[i] if i < len(BRANCH_COLORS) else OTHER_BRANCH_COLOR
            for i, b in enumerate(branches)
        }
        self.branches = branches
        self.periods = sorted({t.period for t in timelines})
        self.cycle = 1
        for period in self.periods:
            self.cycle = lcm(self.cycle, period)
        if self.cycle > 84:
            raise ScheduleError(
                "The combined cycle of the cadences %s is %d days, too long to draw."
                % (self.periods, self.cycle)
            )
        self.max_runners = max(t.runners for t in timelines)
        self.load = self.combined_load()
        # Runner-hours in use on each day of the cycle.
        self.used_per_day = [
            sum(self.load[d * 1440 : (d + 1) * 1440]) / 60.0 for d in range(self.cycle)
        ]
        self.svg = Svg()
        self.y = MARGIN

    # -- Layout helpers -------------------------------------------------------------

    def x_of(self, minute, length):
        return LEFT + PLOT_WIDTH * minute / float(length)

    def cycle_day_label(self, day):
        parts = []
        for period in self.periods:
            if period == 7:
                parts.append(WEEKDAYS[day % 7])
            elif period > 1:
                parts.append(phase_name(day % period))
        return " ".join(parts) if parts else "Every day"

    def combined_load(self):
        length = self.cycle * 1440
        intervals = []
        for t in self.timelines:
            for repeat in range(self.cycle // t.period):
                for start in t.starts:
                    intervals.append((repeat * t.period * 1440 + start, t.duration, t.runners))
        return concurrency(intervals, length)

    def busiest_window(self, days):
        """Returns the most runner-hours used in `days` consecutive days of the cycle."""
        n = self.cycle
        return max(sum(self.used_per_day[(d + i) % n] for i in range(days)) for d in range(n))

    # -- Sections -------------------------------------------------------------------

    def header(self):
        svg = self.svg
        svg.text(
            MARGIN, self.y + 22, "Apache Spark scheduled CI workflows (UTC)", size=22, weight="bold"
        )
        self.y += 34
        runs_per_day = sum(len(t.starts) / float(t.period) for t in self.timelines)
        runner_hours_per_day = sum(
            len(t.starts) * t.duration * t.runners / 60.0 / t.period for t in self.timelines
        )
        load = self.load
        peak = max(load) if load else 0
        peak_hours = sorted({m // 60 for m, v in enumerate(load) if v == peak})
        weekly_share = self.busiest_window(7) / (ASF_WEEKLY_RUNNERS * 7 * 24.0)
        five_day_share = self.busiest_window(5) / (ASF_FIVE_DAY_RUNNERS * 5 * 24.0)
        cadences = []
        for period in self.periods:
            group = [t for t in self.timelines if t.period == period]
            cadences.append("%d %s" % (len(group), describe_cadence(group[0])))
        first = peak_hours[0] if peak_hours else 0
        peak_desc = "%s %02d:00" % (self.cycle_day_label(first // 24), first % 24)
        lines = [
            "%d schedules (%s) start %.1f runs per day, using an estimated %.0f runner-hours per "
            "day, i.e. %.1f runners busy on average."
            % (
                len(self.timelines),
                ", ".join(cadences),
                runs_per_day,
                runner_hours_per_day,
                runner_hours_per_day / 24.0,
            ),
            "Peak: %d runners in use at once, reached in %d of the %d hours of the %d-day cycle "
            "below, first on %s." % (peak, len(peak_hours), self.cycle * 24, self.cycle, peak_desc),
            "ASF limits, which also cover all other apache/spark workflows: the busiest 7 days use "
            "%.0f%% of the weekly limit of %d runners, and the busiest 5 days use %.0f%% of the "
            "five-day limit of %d runners."
            % (weekly_share * 100, ASF_WEEKLY_RUNNERS, five_day_share * 100, ASF_FIVE_DAY_RUNNERS),
            "Generated by %s from .github/workflows. Do not edit by hand; regenerate it "
            "after changing a schedule." % SCRIPT_NAME,
        ]
        for line in [w for line in lines for w in wrap(line, 160)]:
            svg.text(MARGIN, self.y + 14, line, size=13, fill=INK_SECONDARY)
            self.y += 19
        self.y += 6
        # Branch legend.
        x = MARGIN
        svg.text(x, self.y + 12, "Branch:", size=12, fill=INK_SECONDARY)
        x += 56
        for branch in self.branches:
            svg.rect(x, self.y + 2, 12, 12, self.branch_colors[branch], rx=2)
            svg.text(x + 17, self.y + 12, branch, size=12, mono=True)
            x += 17 + 7.4 * len(branch) + 18
        self.y += 30

    def hour_axis(self, length_hours, y_top, y_bottom, day_labels):
        """Draws the hour grid, day separators and labels of a timeline panel."""
        svg = self.svg
        length = length_hours * 60
        label_step = next(
            s for s in (1, 2, 3, 6, 12, 24) if PLOT_WIDTH * s / float(length_hours) >= 34
        )
        for hour in range(0, length_hours + 1, label_step):
            x = self.x_of(hour * 60, length)
            day_boundary = hour % 24 == 0
            svg.line(x, y_top, x, y_bottom, stroke=AXIS if day_boundary else GRID)
            if hour < length_hours:
                svg.text(x + 2, y_top - 4, "%02d" % (hour % 24), size=10, fill=INK_MUTED, mono=True)
        if day_labels:
            for day, label in enumerate(day_labels):
                x = self.x_of((day + 0.5) * 1440, length)
                svg.text(
                    x,
                    y_top - 18,
                    label,
                    size=12,
                    anchor="middle",
                    weight="bold",
                    fill=INK_SECONDARY,
                )

    def panel(self, period):
        svg = self.svg
        group = sorted(
            (t for t in self.timelines if t.period == period),
            key=lambda t: (t.starts[0], t.schedule.workflow, branch_sort_key(t.schedule.branch)),
        )
        length = period * 1440
        runs = sum(len(t.starts) for t in group)
        if period == 1:
            title = "Daily"
        elif period == 7:
            title = "Weekly"
        else:
            title = "Every %d days" % period
        svg.text(
            MARGIN,
            self.y + 18,
            "%s: %d runs per %s" % (title, runs, "day" if period == 1 else "%d days" % period),
            size=17,
            weight="bold",
        )
        self.y += 26
        if period == 7:
            note = "Day of the week from the cron expression."
            day_labels = WEEKDAYS
        elif period > 1:
            phases = []
            for offset in range(period):
                variants = sorted(
                    {tuple(t.days) for t in group if t.days and (t.days[0] - 1) % period == offset}
                )
                desc = " or ".join("%d, %d, ..., %d" % (v[0], v[1], v[-1]) for v in variants)
                phases.append("%s = days %s" % (phase_name(offset), desc or "-"))
            note = (
                "Day-of-month phases: %s. Phases restart every month, so the interval "
                "varies around month ends." % "; ".join(phases)
            )
            day_labels = ["Phase %s" % phase_name(o) for o in range(period)]
        else:
            note = "Every day."
            day_labels = None
        svg.text(MARGIN, self.y + 14, note, size=12, fill=INK_SECONDARY)
        self.y += 22
        axis_top = self.y + (40 if day_labels else 20)
        rows_top = axis_top + 4
        rows_bottom = rows_top + ROW_HEIGHT * len(group)
        load_top = rows_bottom + 10
        load_height = 50
        load_bottom = load_top + load_height

        for i, t in enumerate(group):
            if i % 2 == 1:
                svg.rect(MARGIN, rows_top + i * ROW_HEIGHT, WIDTH - 2 * MARGIN, ROW_HEIGHT, ZEBRA)
        self.hour_axis(period * 24, axis_top, load_bottom, day_labels)

        for i, t in enumerate(group):
            s = t.schedule
            row_y = rows_top + i * ROW_HEIGHT
            text_y = row_y + 13
            color = self.branch_colors[s.branch]
            svg.text(MARGIN + 4, text_y, s.workflow, size=12, mono=True)
            svg.text(LEFT - 82, text_y, s.branch, size=12, mono=True, fill=INK_SECONDARY)
            tooltip = "%s on %s: cron '%s' in %s, about %s with %d runners" % (
                s.workflow,
                s.branch,
                s.cron,
                s.source,
                fmt_hours(t.duration),
                t.runners,
            )
            bar_height = MIN_BAR_HEIGHT + (MAX_BAR_HEIGHT - MIN_BAR_HEIGHT) * (
                t.runners - 1
            ) / float(max(self.max_runners - 1, 1))
            bar_y = row_y + (ROW_HEIGHT - bar_height) / 2.0
            for start in t.starts:
                end = start + t.duration
                pieces = [(start, min(end, length))]
                if end > length:
                    pieces.append((0, end - length))
                for a, b in pieces:
                    x1, x2 = self.x_of(a, length), self.x_of(b, length)
                    svg.rect(x1, bar_y, max(x2 - x1, 2), bar_height, color, rx=2, title=tooltip)
                label = "%s, %s x %d" % (
                    describe_start(period, start),
                    fmt_hours(t.duration),
                    t.runners,
                )
                label_x = self.x_of(min(end, length), length) + 5
                anchor = "start"
                if end > length or label_x + 6.6 * len(label) > LEFT + PLOT_WIDTH:
                    label_x = self.x_of(start, length) - 5
                    anchor = "end"
                svg.text(
                    label_x,
                    text_y - 1,
                    label,
                    size=11,
                    fill=INK_SECONDARY,
                    anchor=anchor,
                    mono=True,
                )

        # Load of this panel alone.
        counts = concurrency(
            [(start, t.duration, t.runners) for t in group for start in t.starts], length
        )
        peak = max(counts) if counts else 0
        svg.text(MARGIN + 4, load_top + 18, "Runners in use", size=12, weight="bold")
        svg.text(
            MARGIN + 4,
            load_top + 32,
            "this panel only, peak %d" % peak,
            size=11,
            fill=INK_SECONDARY,
        )
        svg.text(
            MARGIN + 4,
            load_top + 46,
            "dashed: ASF weekly average limit",
            size=11,
            fill=INK_SECONDARY,
        )
        svg.line(LEFT, load_bottom, LEFT + PLOT_WIDTH, load_bottom, stroke=AXIS)
        scale = (load_height - 6) / float(max(peak, ASF_WEEKLY_RUNNERS))
        limit_y = load_bottom - ASF_WEEKLY_RUNNERS * scale
        svg.line(LEFT, limit_y, LEFT + PLOT_WIDTH, limit_y, stroke=INK_MUTED, dash="4 3")
        svg.text(
            LEFT - 7, limit_y + 4, str(ASF_WEEKLY_RUNNERS), size=10, anchor="end", fill=INK_MUTED
        )
        if peak:
            points = ["%s,%s" % (fmt(LEFT), fmt(load_bottom))]
            previous = None
            for minute, value in enumerate(counts + [0]):
                if value != previous:
                    x = fmt(self.x_of(minute, length))
                    if previous is not None:
                        points.append("%s,%s" % (x, fmt(load_bottom - previous * scale)))
                    points.append("%s,%s" % (x, fmt(load_bottom - value * scale)))
                    previous = value
            svg.add(
                '<polygon points="%s" fill="#2a78d6" fill-opacity="0.25" '
                'stroke="#2a78d6" stroke-width="1.5" stroke-linejoin="round"/>' % " ".join(points)
            )
            # Label the peak unless it would overlap the label of the ASF limit.
            ticks = [peak] if abs(peak - ASF_WEEKLY_RUNNERS) * scale >= 10 else []
            for value in ticks:
                y = load_bottom - value * scale
                svg.line(LEFT - 4, y, LEFT, y, stroke=AXIS)
                svg.text(LEFT - 7, y + 4, str(value), size=10, anchor="end", fill=INK_MUTED)
        self.y = load_bottom + 34

    def heatmap(self):
        svg = self.svg
        load = self.load
        peak = max(load) if load else 0
        daily_budget = ASF_WEEKLY_RUNNERS * 24.0
        cell_values = [
            [max(load[(d * 24 + h) * 60 : (d * 24 + h + 1) * 60]) for h in range(24)]
            for d in range(self.cycle)
        ]
        starts_per_day = [0] * self.cycle
        for t in self.timelines:
            for repeat in range(self.cycle // t.period):
                for start in t.starts:
                    starts_per_day[(repeat * t.period * 1440 + start) // 1440] += 1

        svg.text(
            MARGIN,
            self.y + 18,
            "Combined load: peak runners in use per hour",
            size=17,
            weight="bold",
        )
        self.y += 26
        if self.cycle > 1:
            note = (
                "All panels together over %d consecutive days, which covers every "
                "combination of %s once. Days are labeled by %s."
                % (
                    self.cycle,
                    " and ".join(
                        "weekday" if p == 7 else "%d-day phase" % p for p in self.periods if p > 1
                    ),
                    " and ".join("weekday" if p == 7 else "phase" for p in self.periods if p > 1),
                )
            )
        else:
            note = "All panels together over one day."
        svg.text(MARGIN, self.y + 14, note, size=12, fill=INK_SECONDARY)
        self.y += 22

        summary_width = 250
        grid_width = PLOT_WIDTH - summary_width
        cell_w = grid_width / 24.0
        cell_h = 18
        top = self.y + 20
        for h in range(24):
            svg.text(
                LEFT + (h + 0.5) * cell_w,
                top - 6,
                "%02d" % h,
                size=10,
                anchor="middle",
                fill=INK_MUTED,
                mono=True,
            )
        sx = LEFT + grid_width + 12
        bar_x, bar_w = sx + 108, 84
        svg.text(sx + 36, top - 6, "starts", size=10, anchor="end", fill=INK_MUTED)
        svg.text(sx + 96, top - 6, "runner-h", size=10, anchor="end", fill=INK_MUTED)
        svg.text(bar_x, top - 6, "of ASF daily budget", size=10, fill=INK_MUTED)

        def side_columns(y, starts, used, weight=None, title=None):
            svg.text(
                sx + 36,
                y + 13,
                starts,
                size=11,
                anchor="end",
                mono=True,
                weight=weight,
                title=title,
            )
            svg.text(
                sx + 96,
                y + 13,
                "%.0f" % used,
                size=11,
                anchor="end",
                mono=True,
                weight=weight,
                title=title,
            )
            share = used / daily_budget
            svg.rect(bar_x, y + 4, bar_w, cell_h - 8, ZEBRA, rx=2)
            svg.rect(
                bar_x,
                y + 4,
                max(bar_w * min(share, 1.0), 1),
                cell_h - 8,
                HEAT_RAMP[3],
                rx=2,
                title="%.0f of %.0f runner-hours" % (used, daily_budget),
            )
            svg.text(
                bar_x + bar_w + 34,
                y + 13,
                "%.0f%%" % (share * 100),
                size=11,
                anchor="end",
                mono=True,
                weight=weight,
                title=title,
            )

        def heat_color(value):
            if value == 0:
                return HEAT_ZERO, INK_MUTED
            index = int(math.ceil(value * len(HEAT_RAMP) / float(peak))) - 1
            index = min(max(index, 0), len(HEAT_RAMP) - 1)
            return HEAT_RAMP[index], ("#ffffff" if index >= 3 else INK)

        rows = [
            (self.cycle_day_label(d), cell_values[d], starts_per_day[d]) for d in range(self.cycle)
        ]
        # The last row has the peak of each hour, and the averages per day on the side.
        hourly_max = [max(cell_values[d][h] for d in range(self.cycle)) for h in range(24)]
        rows.append(("Max over days", hourly_max, None))
        for i, (label, values, starts) in enumerate(rows):
            y = top + i * cell_h + (8 if starts is None else 0)
            weight = "bold" if starts is None else None
            day_number = "" if starts is None else "Day %d" % (i + 1)
            svg.text(MARGIN + 4, y + 13, day_number, size=12, fill=INK_MUTED, mono=True)
            svg.text(LEFT - 10, y + 13, label, size=12, anchor="end", weight=weight, mono=True)
            for h, value in enumerate(values):
                fill, ink = heat_color(value)
                svg.rect(
                    LEFT + h * cell_w + 1,
                    y + 1,
                    cell_w - 2,
                    cell_h - 2,
                    fill,
                    rx=2,
                    title="%s %02d:00-%02d:59 UTC: up to %d runners in use" % (label, h, h, value),
                )
                if value:
                    svg.text(
                        LEFT + (h + 0.5) * cell_w,
                        y + 13,
                        str(value),
                        size=11,
                        anchor="middle",
                        fill=ink,
                        weight=weight,
                    )
            if starts is not None:
                side_columns(y, str(starts), self.used_per_day[i])
            else:
                side_columns(
                    y,
                    "%.1f" % (sum(starts_per_day) / float(self.cycle)),
                    sum(self.used_per_day) / self.cycle,
                    weight="bold",
                    title="average per day",
                )
        self.y = top + len(rows) * cell_h + 8 + 14

        # Color scale legend: one swatch per color with the range of values it stands for.
        x = LEFT
        svg.text(MARGIN + 4, self.y + 12, "Runners in use", size=12, fill=INK_SECONDARY)
        bins = [("0", 0)]
        for i in range(len(HEAT_RAMP)):
            lo = peak * i // len(HEAT_RAMP) + 1
            hi = peak * (i + 1) // len(HEAT_RAMP)
            if lo <= hi:
                bins.append((str(lo) if lo == hi else "%d-%d" % (lo, hi), hi))
        for text, value in bins:
            fill, ink = heat_color(value)
            svg.rect(x, self.y, 44, 16, fill, rx=2)
            svg.text(x + 22, self.y + 12, text, size=11, anchor="middle", fill=ink)
            x += 48
        self.y += 26
        side_note = (
            "On the right: the runs that start each day, the runner-hours used each day, and "
            "their share of the ASF daily budget, i.e. the weekly limit of %d runners x 24h = "
            "%.0f runner-hours. The bottom row shows the peak of each hour and the daily "
            "averages." % (ASF_WEEKLY_RUNNERS, daily_budget)
        )
        for line in wrap(side_note):
            svg.text(MARGIN + 4, self.y + 12, line, size=11, fill=INK_MUTED)
            self.y += 16
        self.y += 18

    def footer(self):
        svg = self.svg
        svg.text(MARGIN, self.y + 16, "Notes", size=15, weight="bold")
        self.y += 24
        excluded = []
        for s in sorted(self.excluded):
            excluded.append("%s ('%s', %s)" % (s.workflow, s.cron, EXCLUDED_WORKFLOWS[s.workflow]))
        notes = [
            "Bars start at the cron time and last for the estimated duration. Their thickness and "
            "the 'x N' in their labels show the estimated average number of runners a run keeps "
            "busy. Both estimates are medians of recent apache/spark runs, in ESTIMATES of %s; "
            "runs can take longer, use more runners at their busiest, or start late." % SCRIPT_NAME,
            "Branch builds are dispatched by the branch*_scheduler.yml workflows through "
            "dev/dispatch-if-changed.sh, which skips a run when the branch has not changed "
            "since its last successful run, so the real load on maintenance branches is "
            "usually lower.",
            "Workflows scheduled directly run on %s. When this SVG is opened on its own, "
            "hovering over a bar shows its cron entry and the file that defines it."
            % DEFAULT_BRANCH,
            "Not shown because they usually finish within an hour and do not run tests: %s."
            % "; ".join(excluded),
            "ASF limits apache/spark to %d full-time runners on average per calendar week and %d "
            "over any five consecutive days, for all of its workflows including the builds of "
            "pushes, and to 20 concurrent jobs per workflow; see "
            "https://infra.apache.org/github-actions-policy.html and "
            ".github/workflows/README.md for the scheduling guidelines."
            % (ASF_WEEKLY_RUNNERS, ASF_FIVE_DAY_RUNNERS),
        ]
        for note in notes:
            for i, line in enumerate(wrap(note)):
                svg.text(MARGIN + 12, self.y + 13, line, size=12, fill=INK_SECONDARY)
                if i == 0:
                    svg.text(MARGIN, self.y + 13, "-", size=12, fill=INK_SECONDARY)
                self.y += 17
            self.y += 3

    def render(self):
        self.header()
        for period in self.periods:
            self.panel(period)
        self.heatmap()
        self.footer()
        height = int(self.y + MARGIN)
        head = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            LICENSE_HEADER,
            "<!-- Generated by %s. Do not edit by hand. -->" % SCRIPT_NAME,
            '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" '
            "viewBox=\"0 0 %d %d\" font-family='%s'>" % (WIDTH, height, WIDTH, height, FONT_SANS),
            '<rect width="100%%" height="100%%" fill="%s"/>' % SURFACE,
        ]
        return "\n".join(head + self.svg.parts + ["</svg>"]) + "\n"


LICENSE_HEADER = """<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->"""


def generate(workflow_dir=WORKFLOW_DIR):
    schedules = collect_schedules(workflow_dir)
    timelines, excluded = build_timelines(schedules)
    if not timelines:
        raise ScheduleError("No scheduled workflows to draw found in %s." % workflow_dir)
    return Renderer(timelines, excluded).render()


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="check that the image is up to date instead of writing it",
    )
    parser.add_argument(
        "--sample",
        nargs="?",
        type=int,
        const=10,
        metavar="RUNS",
        help="print ESTIMATES measured from the latest RUNS (default 10) runs of each scheduled "
        "workflow in %s, using the GitHub CLI" % SAMPLE_REPO,
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="path of the SVG image")
    args = parser.parse_args()
    if args.sample is not None:
        try:
            sample_estimates(args.sample)
        except ScheduleError as e:
            print("ERROR: %s" % e, file=sys.stderr)
            return 1
        return 0
    try:
        image = generate()
    except ScheduleError as e:
        print("ERROR: %s" % e, file=sys.stderr)
        return 1
    output = os.path.relpath(args.output)
    if args.check:
        try:
            with open(args.output, encoding="utf-8", newline="") as f:
                current = f.read()
        except FileNotFoundError:
            current = None
        if current is None:
            print(
                "ERROR: %s does not exist. Run %s to generate it." % (output, SCRIPT_NAME),
                file=sys.stderr,
            )
            return 1
        if current != image:
            print(
                "ERROR: %s is out of date with the schedules in .github/workflows or the "
                "estimates in %s. Run %s to regenerate it." % (output, SCRIPT_NAME, SCRIPT_NAME),
                file=sys.stderr,
            )
            return 1
        print("%s is up to date." % output)
        return 0
    with open(args.output, "w", encoding="utf-8", newline="\n") as f:
        f.write(image)
    print("Wrote %s." % output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
