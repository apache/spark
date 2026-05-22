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
"""Flask app for the local Spark Connect client UI.

Importing this module requires Flask to be installed. The package's
``start_in_background`` helper handles the missing-Flask case gracefully;
callers that want to construct the app directly should expect ImportError
here when Flask is absent.
"""

from __future__ import annotations

import datetime as _dt
import os
from typing import List, Optional, TYPE_CHECKING

from flask import Flask, render_template_string

from pyspark.sql.connect.ui import SqlExecutionSummary, list_sql_executions

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession


_PAGE = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="{{ refresh_seconds }}">
  <title>Spark Connect UI - SQL</title>
  <style>
    body { font-family: -apple-system, system-ui, sans-serif; margin: 1.5em; }
    h1 { margin-bottom: 0.25em; }
    .meta { color: #666; font-size: 0.9em; margin-bottom: 1em; }
    table { border-collapse: collapse; width: 100%; }
    th, td { padding: 0.4em 0.6em; border-bottom: 1px solid #eee;
             text-align: left; vertical-align: top; font-size: 0.9em; }
    th { background: #fafafa; }
    .status-RUNNING   { color: #1565c0; font-weight: 600; }
    .status-COMPLETED { color: #2e7d32; }
    .status-FAILED    { color: #c62828; font-weight: 600; }
    .desc { max-width: 50em; }
    code { background: #f4f4f4; padding: 0.05em 0.25em; border-radius: 3px; }
  </style>
</head>
<body>
  <h1>SQL Executions</h1>
  <div class="meta">
    Connected to <code>{{ remote }}</code>.
    Fetched at {{ fetched_at }}. Auto-refresh every {{ refresh_seconds }}s.
    Showing {{ executions|length }} executions.
  </div>
  {% if executions %}
  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th class="desc">Description</th>
        <th>Status</th>
        <th>Submitted</th>
        <th>Duration</th>
        <th>Jobs</th>
      </tr>
    </thead>
    <tbody>
      {% for e in executions %}
      <tr>
        <td>{{ e.execution_id }}</td>
        <td class="desc">{{ e.description }}</td>
        <td class="status-{{ e.status }}">{{ e.status }}</td>
        <td>{{ fmt_time(e.submission_time_ms) }}</td>
        <td>{{ fmt_duration(e.submission_time_ms, e.completion_time_ms) }}</td>
        <td>{{ e.job_ids|join(", ") }}</td>
      </tr>
      {% if e.error_message %}
      <tr><td></td><td colspan="5"><pre>{{ e.error_message }}</pre></td></tr>
      {% endif %}
      {% endfor %}
    </tbody>
  </table>
  {% else %}
  <p>No SQL executions yet.</p>
  {% endif %}
</body>
</html>
"""


def _fmt_time(ms: int) -> str:
    if not ms:
        return ""
    return _dt.datetime.fromtimestamp(ms / 1000.0).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_duration(start_ms: int, end_ms: Optional[int]) -> str:
    if not start_ms:
        return ""
    end = end_ms if end_ms else int(_dt.datetime.now().timestamp() * 1000)
    secs = max(0, (end - start_ms) / 1000.0)
    if secs < 60:
        return f"{secs:.1f}s"
    mins, secs = divmod(secs, 60)
    return f"{int(mins)}m {secs:.0f}s"


def make_app(
    spark: "SparkSession",
    refresh_seconds: int = 5,
    remote: Optional[str] = None,
) -> Flask:
    # Best-effort label for the page header. Avoid round-tripping to the server
    # just to render a string, since auto-start runs during session init.
    display_remote = remote or os.environ.get("SPARK_REMOTE") or "<connect server>"
    app = Flask(__name__)

    @app.route("/")
    def index() -> str:
        return _render(spark, display_remote, refresh_seconds)

    @app.route("/sql")
    def sql() -> str:
        return _render(spark, display_remote, refresh_seconds)

    return app


def _render(spark: "SparkSession", remote: str, refresh_seconds: int) -> str:
    executions: List[SqlExecutionSummary] = list_sql_executions(
        spark, offset=0, length=200
    )
    # Newest first.
    executions.sort(key=lambda e: e.execution_id, reverse=True)
    return render_template_string(
        _PAGE,
        executions=executions,
        remote=remote,
        refresh_seconds=refresh_seconds,
        fetched_at=_dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        fmt_time=_fmt_time,
        fmt_duration=_fmt_duration,
    )
