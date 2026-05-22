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
"""Pure-Python client-side UI helpers for Spark Connect.

Wraps server-side UI RPCs (currently ``ListSqlExecutions``) and exposes them
as plain Python dataclasses, so a Connect client can render its own UI
without reaching the server's HTTP UI port.
"""

import logging
import os
import re
import sys
import threading
import time
import warnings
from dataclasses import dataclass, field
from typing import Callable, Iterable, List, Optional, TYPE_CHECKING

import pyspark.sql.connect.proto as pb2
from pyspark.errors import PySparkAssertionError
from pyspark.errors.exceptions.connect import SparkConnectException
from pyspark.sql.connect.shell.progress import (
    ProgressHandler as _ProgressHandlerBase,
    StageInfo,
)

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.metrics import ExecutionInfo

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StageProgress:
    stage_id: int
    num_tasks: int
    num_completed_tasks: int
    num_bytes_read: int
    done: bool


@dataclass(frozen=True)
class ProgressSnapshot:
    """Immutable snapshot of one Connect operation's progress for the UI."""

    operation_id: Optional[str]
    total_tasks: int
    completed_tasks: int
    inflight_tasks: int
    bytes_read: int
    elapsed_seconds: float
    done: bool
    started_wall_ms: int = 0
    stages: List[StageProgress] = field(default_factory=list)
    execution_info: Optional[dict] = None


# How long after an operation reports ``done=True`` we keep its snapshot
# around. The UI renders done snapshots dimly for a brief moment so the user
# sees a query completed rather than the bar simply vanishing.
_DONE_RETENTION_SECONDS = 3.0


class _OpProgress:
    """Mutable per-operation progress state, guarded by the handler's lock."""

    __slots__ = ("operation_id", "started_at", "started_wall_ms", "last_seen_at", "stages",
                 "inflight_tasks", "done", "done_at")

    def __init__(self, operation_id: Optional[str], started_at: float) -> None:
        self.operation_id = operation_id
        self.started_at = started_at
        self.started_wall_ms: int = int(time.time() * 1000)
        self.last_seen_at = started_at
        self.stages: List[StageInfo] = []
        self.inflight_tasks: int = 0
        self.done: bool = False
        self.done_at: Optional[float] = None


class _UIProgressHandler(_ProgressHandlerBase):
    """``ProgressHandler`` that retains progress events from concurrent operations.

    A single SparkSession can have multiple in-flight queries (e.g. when several
    Python threads each call ``collect`` on different DataFrames). Each operation
    fires events with its own ``operation_id``; we keep one ``_OpProgress`` per id
    so all of them can be rendered in parallel.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ops: "dict[str, _OpProgress]" = {}
        self._exec_info: "dict[str, dict]" = {}  # key -> serialized ExecutionInfo

    def __call__(
        self,
        stages: Optional[Iterable[StageInfo]],
        inflight_tasks: int,
        operation_id: Optional[str],
        done: bool,
    ) -> None:
        now = time.monotonic()
        # Treat missing operation_id as a single anonymous slot (older code paths
        # that don't propagate the id still get tracked).
        key = operation_id or ""
        with self._lock:
            op = self._ops.get(key)
            if op is None:
                op = _OpProgress(operation_id=operation_id, started_at=now)
                self._ops[key] = op
            op.stages = list(stages or [])
            op.inflight_tasks = inflight_tasks
            op.last_seen_at = now
            if done and not op.done:
                op.done_at = now
            op.done = done
            self._gc_locked(now)

    def _gc_locked(self, now: float) -> None:
        # Drop entries for operations that have been done for a while -- otherwise
        # the dict grows unboundedly across a long-lived session.
        stale = [
            k for k, op in self._ops.items()
            if op.done and op.done_at is not None
            and now - op.done_at > _DONE_RETENTION_SECONDS
        ]
        for k in stale:
            del self._ops[k]
            self._exec_info.pop(k, None)

    def _to_snapshot(self, op: _OpProgress, now: float) -> ProgressSnapshot:
        stages = [
            StageProgress(
                stage_id=s.stage_id,
                num_tasks=s.num_tasks,
                num_completed_tasks=s.num_completed_tasks,
                num_bytes_read=s.num_bytes_read,
                done=s.done,
            )
            for s in op.stages
        ]
        if op.done:
            elapsed = op.last_seen_at - op.started_at
        else:
            elapsed = now - op.started_at
        key = op.operation_id or ""
        return ProgressSnapshot(
            operation_id=op.operation_id,
            started_wall_ms=op.started_wall_ms,
            total_tasks=sum(s.num_tasks for s in stages),
            completed_tasks=sum(s.num_completed_tasks for s in stages),
            inflight_tasks=op.inflight_tasks,
            bytes_read=sum(s.num_bytes_read for s in stages),
            elapsed_seconds=elapsed,
            done=op.done,
            stages=stages,
            execution_info=self._exec_info.get(key),
        )

    def on_execution_info(self, operation_id: str, ei: "ExecutionInfo") -> None:
        """Callback registered on the client to capture per-node metrics after execution."""
        try:
            plan_nodes = (
                [m.to_dict() for m in ei.metrics._metrics]
                if ei.metrics is not None else []
            )
            flows = [{"name": name, "pairs": pairs} for name, pairs in ei.flows]
        except Exception:
            return
        if not plan_nodes and not flows:
            return
        key = operation_id or ""
        with self._lock:
            self._exec_info[key] = {"plan_nodes": plan_nodes, "flows": flows}

    def snapshots(self) -> List[ProgressSnapshot]:
        """Return one snapshot per recently-observed operation, oldest first."""
        with self._lock:
            now = time.monotonic()
            self._gc_locked(now)
            ops = sorted(self._ops.values(), key=lambda o: o.started_at)
            return [self._to_snapshot(op, now) for op in ops]

    # Legacy single-snapshot accessor (returns the most recent op, or an idle
    # placeholder). Kept for backwards compatibility with callers outside the UI.
    def snapshot(self) -> ProgressSnapshot:
        snaps = self.snapshots()
        if not snaps:
            return ProgressSnapshot(
                operation_id=None,
                total_tasks=0,
                completed_tasks=0,
                inflight_tasks=0,
                bytes_read=0,
                elapsed_seconds=0.0,
                done=True,
                stages=[],
            )
        return snaps[-1]


@dataclass(frozen=True)
class SqlExecutionSummary:
    execution_id: int
    root_execution_id: int
    description: str
    status: str
    submission_time_ms: int
    completion_time_ms: Optional[int]
    error_message: Optional[str]
    job_ids: List[int]
    query_id: Optional[str] = None
    details: Optional[str] = None
    stage_count: int = 0
    # Best-effort parse of ``description`` -- Connect sets the SQL job description to
    # ``"Spark Connect - <protobuf text dump of ExecutePlanRequest>"`` truncated to ~128 chars,
    # which surfaces user_id, the request session_id, and the leading plan operation.
    user_id: Optional[str] = None
    request_session_id: Optional[str] = None
    operation: Optional[str] = None
    # Tags set on the Connect operation via ``spark.addTag()``.
    tags: List[str] = field(default_factory=list)


_PARSE_USER_ID = re.compile(r'user_id:\s*"([^"]*)"')
_PARSE_SESSION_ID = re.compile(r'session_id:\s*"([^"]*)"')
_PARSE_TAGS = re.compile(r'\btags:\s*"([^"]*)"')
_PARSE_PLAN_OPEN = re.compile(r"plan\s*\{\s*(?:root|command)\s*\{")
_PARSE_FIELD_OPEN = re.compile(r"\s*([a-z_][a-z0-9_]*)\s*\{")


def _skip_balanced_block(text: str, start: int) -> int:
    """Return the index just past the ``}`` that closes a ``{`` already opened before ``start``.

    Returns ``-1`` if the block is unterminated (e.g. truncated mid-stream).
    """
    depth = 1
    i = start
    while i < len(text) and depth > 0:
        c = text[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
        i += 1
    return i if depth == 0 else -1


def _parse_connect_proto_text(
    text: str,
) -> "tuple[Optional[str], Optional[str], Optional[str], List[str]]":
    """Extract ``(user_id, session_id, operation, tags)`` from a Connect proto-text dump.

    Handles both the short SQL job description (~128 chars, prefixed with ``"Spark Connect - "``)
    and the longer ``callSite.long`` dump (~2048 chars, the bare ``ExecutePlanRequest`` text).
    Both share the same structure -- only the truncation depth differs.
    """
    if not text or "user_context" not in text or "plan {" not in text:
        return (None, None, None, [])
    user = _PARSE_USER_ID.search(text)
    sess = _PARSE_SESSION_ID.search(text)
    tags = _PARSE_TAGS.findall(text)

    operation: Optional[str] = None
    plan_open = _PARSE_PLAN_OPEN.search(text)
    if plan_open is not None:
        pos = plan_open.end()
        # Inside `root { ... }` (or `command { ... }`) the proto text always begins with a
        # `common { ... }` metadata block. Skip past it by counting braces -- the block has
        # nested children so a simple regex won't suffice.
        common_open = re.match(r"\s*common\s*\{", text[pos:])
        if common_open is not None:
            past_common = _skip_balanced_block(text, pos + common_open.end())
            if past_common == -1:
                pos = -1  # truncated mid-common, can't find the real op
            else:
                pos = past_common
        if pos != -1:
            op_match = _PARSE_FIELD_OPEN.match(text, pos)
            if op_match is not None:
                operation = op_match.group(1)

    return (
        user.group(1) if user else None,
        sess.group(1) if sess else None,
        operation,
        tags,
    )


def _parse_description(
    description: str, details: Optional[str]
) -> "tuple[Optional[str], Optional[str], Optional[str], List[str]]":
    """Return ``(user_id, session_id, operation, tags)`` for a Connect SQL execution.

    Prefers the long ``details`` (the ``callSite.long`` of the Connect job, ~2048 chars) for
    ``operation`` because the short ``description`` is truncated to ~128 chars and almost
    always cuts off mid-``common`` block. Falls back to ``description`` if ``details`` is
    missing or non-Connect.
    """
    user, sess, op, tags = _parse_connect_proto_text(details or "")
    if user is None and sess is None and op is None:
        user, sess, op, tags = _parse_connect_proto_text(description)
    elif op is None:
        # Details parsed but operation wasn't extractable; try description as a last resort.
        _, _, fallback_op, extra_tags = _parse_connect_proto_text(description)
        op = fallback_op
        if extra_tags and not tags:
            tags = extra_tags
    return (user, sess, op, tags)


def _verify_ui_response(client: "object", resp: "pb2.ListSqlExecutionsResponse") -> None:
    # Mirrors SparkConnectClient._verify_response_integrity but tolerates an empty
    # server_side_session_id, which the server returns when no session exists yet (the UI
    # polls before any other RPC has caused one to be created). Storing that empty string
    # into the client would later mismatch real RPC responses.
    if client._session_id != resp.session_id:  # type: ignore[attr-defined]
        raise PySparkAssertionError(
            "Received incorrect session identifier for request: "
            f"{resp.session_id} != {client._session_id}"  # type: ignore[attr-defined]
        )
    if not resp.server_side_session_id:
        return
    if client._server_session_id is None:  # type: ignore[attr-defined]
        client._server_session_id = resp.server_side_session_id  # type: ignore[attr-defined]
    elif resp.server_side_session_id != client._server_session_id:  # type: ignore[attr-defined]
        client._closed = True  # type: ignore[attr-defined]
        raise PySparkAssertionError(
            "Received incorrect server side session identifier for request. "
            "Please create a new Spark Session to reconnect. "
            f"({resp.server_side_session_id} != "
            f"{client._server_session_id})"  # type: ignore[attr-defined]
        )


def _status_name(value: int) -> str:
    enum_cls = pb2.ListSqlExecutionsResponse.SqlExecutionStatus
    # Resolve the int back to its proto name, then strip the SQL_EXECUTION_STATUS_
    # prefix for ergonomic display ("RUNNING" instead of "SQL_EXECUTION_STATUS_RUNNING").
    name = enum_cls.Name(value) if value in enum_cls.values() else "UNSPECIFIED"
    return name.removeprefix("SQL_EXECUTION_STATUS_")


def list_sql_executions(
    spark: "SparkSession",
    offset: int = 0,
    length: int = 100,
) -> List[SqlExecutionSummary]:
    """Fetch the SQL executions visible to this Connect session.

    Calls the ``ListSqlExecutions`` RPC. Plan strings and per-node metrics are
    intentionally omitted -- use a follow-up ``GetSqlExecution`` (not yet
    implemented) for the heavy fields of a single execution.
    """
    client = spark.client

    req = pb2.ListSqlExecutionsRequest()
    req.session_id = client._session_id
    req.client_type = client._builder.userAgent
    if client._user_id:
        req.user_context.user_id = client._user_id
    if client._server_session_id:
        req.client_observed_server_side_session_id = client._server_session_id
    req.offset = offset
    req.length = length

    resp = None
    try:
        for attempt in client._retrying():
            with attempt:
                resp = client._stub.ListSqlExecutions(
                    req, metadata=client._builder.metadata()
                )
                _verify_ui_response(client, resp)
                break
    except Exception as error:
        client._handle_error(error)

    if resp is None:
        raise SparkConnectException("Invalid state during retry exception handling.")

    summaries = []
    for e in resp.executions:
        details = e.details if e.HasField("details") else None
        user_id, request_session_id, operation, tags = _parse_description(e.description, details)
        summaries.append(
            SqlExecutionSummary(
                execution_id=e.execution_id,
                root_execution_id=e.root_execution_id,
                description=e.description,
                status=_status_name(e.status),
                submission_time_ms=e.submission_time_ms,
                completion_time_ms=(
                    e.completion_time_ms if e.HasField("completion_time_ms") else None
                ),
                error_message=e.error_message if e.HasField("error_message") else None,
                job_ids=list(e.job_ids),
                query_id=e.query_id if e.HasField("query_id") else None,
                details=e.details if e.HasField("details") else None,
                stage_count=e.stage_count,
                user_id=user_id,
                request_session_id=request_session_id,
                operation=operation,
                tags=tags,
            )
        )
    return summaries


def start_in_background(
    spark: "SparkSession",
    host: str = "127.0.0.1",
    port: Optional[int] = None,
    refresh_seconds: int = 5,
) -> Optional[str]:
    """Start the Connect client UI on a background daemon thread.

    Returns the URL on success, or ``None`` if startup was skipped (e.g. Flask
    not installed, or the chosen port was unavailable). Errors after startup
    are logged and do not propagate, since this is a best-effort convenience.

    The thread is a daemon so it does not block interpreter shutdown.
    """
    try:
        from werkzeug.serving import make_server

        from pyspark.sql.connect.ui.web import make_app
    except ImportError:
        warnings.warn(
            "Spark Connect client UI is disabled because Flask is not installed. "
            "Install it with 'pip install flask' to enable.",
            RuntimeWarning,
            stacklevel=2,
        )
        return None

    progress_handler = _UIProgressHandler()
    try:
        spark.registerProgressHandler(progress_handler)
        spark.client.register_execution_info_callback(progress_handler.on_execution_info)
    except Exception as e:  # noqa: BLE001 - non-fatal: UI still works without progress
        _logger.warning("Failed to register Connect UI progress handler: %s", e)
        progress_handler = None  # type: ignore[assignment]

    # 0 = ephemeral. Each SparkSession gets its own UI, so collisions on a fixed
    # port would otherwise force the second session onto a different one anyway.
    bind_port = port if port is not None else 0
    try:
        app = make_app(
            spark, refresh_seconds=refresh_seconds, progress_handler=progress_handler
        )
        server = make_server(host, bind_port, app, threaded=True)
    except OSError as e:
        warnings.warn(
            f"Spark Connect client UI failed to bind {host}:{bind_port}: {e}. "
            "Set a different port via start_in_background(port=...) to retry.",
            RuntimeWarning,
            stacklevel=2,
        )
        return None

    bound_host, bound_port = server.server_address[:2]
    url = f"http://{bound_host}:{bound_port}"

    thread = threading.Thread(
        target=server.serve_forever,
        name="spark-connect-client-ui",
        daemon=True,
    )
    thread.start()

    _logger.info("Spark Connect client UI started at %s", url)
    return url


def _maybe_autostart_ui(spark: "SparkSession") -> None:
    """Best-effort auto-start of a client-side UI for this SparkSession.

    Called from ``SparkSession.__init__`` so it fires in any Python interpreter
    that creates a Connect session (shell, scripts, notebooks). Each session
    gets its own Flask server on an ephemeral port; the URL is stashed on the
    session as ``spark._client_ui_url``. Opt out with ``PYSPARK_CONNECT_UI=0``.

    Failures (missing Flask, port in use, bind errors) are logged or warned and
    swallowed -- session creation must never fail because the UI couldn't start.
    """
    if os.environ.get("PYSPARK_CONNECT_UI", "1") == "0":
        return
    try:
        url = start_in_background(spark)
    except Exception as e:  # noqa: BLE001 - never break session init
        _logger.warning("Spark Connect client UI failed to start: %s", e)
        return
    if url is not None:
        spark._client_ui_url = url  # type: ignore[attr-defined]
        print(
            "Spark Connect client UI available at %s" % url,
            file=sys.stderr,
            flush=True,
        )
        _logger.info("Spark Connect client UI available at %s", url)


__all__ = [
    "SqlExecutionSummary",
    "list_sql_executions",
    "start_in_background",
]
