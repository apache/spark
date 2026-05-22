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
import sys
import threading
import warnings
from dataclasses import dataclass
from typing import List, Optional, TYPE_CHECKING

import pyspark.sql.connect.proto as pb2
from pyspark.errors import PySparkAssertionError
from pyspark.errors.exceptions.connect import SparkConnectException

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession

_logger = logging.getLogger(__name__)


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

    return [
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
        )
        for e in resp.executions
    ]


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

    # 0 = ephemeral. Each SparkSession gets its own UI, so collisions on a fixed
    # port would otherwise force the second session onto a different one anyway.
    bind_port = port if port is not None else 0
    try:
        app = make_app(spark, refresh_seconds=refresh_seconds)
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
