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
Long-lived local Spark Connect server for the opt-in reuse path.

This module is launched as a *detached* child process by
``SparkSession._reuse_or_start_local_connect_server`` (in ``pyspark.sql.connect.session``) when
``SPARK_LOCAL_CONNECT_REUSE`` / ``spark.local.connect.reuse`` is enabled. It starts a regular
(classic) local Spark session with the Spark Connect plugin -- the same mechanism the in-process
``SparkSession._start_connect_server`` uses -- and then blocks, so one warm JVM and Spark Connect
server keeps serving many short-lived client processes. Each client connection gets its own
isolated server-side session, so session-local state (temp views, runtime SQL confs, isolated
artifacts) does not leak between runs.

Once the server is accepting connections it writes a discovery file (host, the actually bound port,
the auth token, its pid and the Spark version) that later client processes read to reconnect.

It is launched by file path (not ``python -m pyspark.sql.connect.local_server``) on purpose: it only
needs a classic PySpark install plus the Spark Connect server jar on the classpath -- exactly like
``sbin/start-connect-server.sh`` -- and must not require the Spark Connect *client* dependencies
(grpc, etc.). Importing the ``pyspark.sql.connect`` package would trigger that client-dependency
check, so this file imports only the classic ``pyspark.sql`` API.
"""
import sys

# This module is launched by file path (see SparkSession._start_persistent_local_connect_server),
# which puts its own directory -- pyspark/sql/connect -- at the front of sys.path. That directory
# holds modules whose names collide with the standard library (e.g. `types`, `logging`), so leaving
# it there shadows the stdlib and breaks ordinary imports. Drop it before importing anything else;
# pyspark itself stays importable via the remaining sys.path entries (PYTHONPATH / installation).
# `import sys` is a built-in and cannot be shadowed, so it is safe to run first.
if sys.path:
    del sys.path[0]

import argparse  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import signal  # noqa: E402
import time  # noqa: E402
from typing import Any  # noqa: E402


def _write_discovery(path: str, host: str, port: int, token: str, version: str) -> None:
    """Atomically write the discovery file with ``0600`` perms (it holds the auth token)."""
    parent = os.path.dirname(path)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    payload = {
        "host": host,
        "port": port,
        "token": token,
        "pid": os.getpid(),
        "spark_version": version,
    }
    tmp = "{}.{}.tmp".format(path, os.getpid())
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(json.dumps(payload))
    os.replace(tmp, path)


def _remove_discovery_if_ours(path: str) -> None:
    """Remove the discovery file, but only if it still points at this process."""
    try:
        with open(path, "r") as f:
            disc = json.load(f)
    except (OSError, ValueError):
        return
    if disc.get("pid") == os.getpid():
        try:
            os.remove(path)
        except OSError:
            pass


def _has_active_sessions(spark: Any) -> bool:
    """Best-effort check for whether any Spark Connect session is currently registered.

    Used only by the idle-shutdown reaper. Any failure (server not started yet, API drift, py4j
    error) returns ``True`` so the reaper never terminates a server it cannot inspect.
    """
    jvm = spark.sparkContext._jvm
    service = getattr(
        getattr(jvm, "org.apache.spark.sql.connect.service.SparkConnectService$"),
        "MODULE$",
    )
    if not service.started():
        return True
    return not service.sessionManager().listActiveSessions().isEmpty()


def _bound_port(spark: Any, requested_port: int) -> int:
    """Return the port the Connect server actually bound (``requested_port`` may have been 0)."""
    jvm = spark.sparkContext._jvm
    service = getattr(
        getattr(jvm, "org.apache.spark.sql.connect.service.SparkConnectService$"),
        "MODULE$",
    )
    try:
        return int(service.localPort())
    except Exception:
        return requested_port


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--port", type=int, default=15002)
    parser.add_argument("--token", default=None)
    parser.add_argument("--discovery", required=True)
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=3600.0,
        help="seconds with no active session after which the server self-terminates; <=0 disables",
    )
    parser.add_argument("--poll-interval", type=float, default=60.0)
    args = parser.parse_args()

    # Build a CLASSIC session: the connect-mode env vars would otherwise divert us into a client.
    for var in ("SPARK_REMOTE", "SPARK_LOCAL_REMOTE", "SPARK_CONNECT_MODE_ENABLED"):
        os.environ.pop(var, None)
    if args.token:
        os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = args.token

    from pyspark.sql import SparkSession
    from pyspark.version import __version__

    builder = (
        SparkSession.builder.master(args.master)
        .config("spark.plugins", "org.apache.spark.sql.connect.SparkConnectPlugin")
        .config("spark.connect.grpc.binding.port", str(args.port))
        # Match the isolation the in-process `.remote("local[*]")` path sets so per-session
        # artifacts (added jars/files/classes) do not leak across the sessions this server hosts.
        .config("spark.sql.artifact.isolation.enabled", "true")
        .config("spark.sql.artifact.isolation.alwaysApplyClassloader", "true")
    )
    if args.token:
        builder = builder.config("spark.connect.authenticate.token", args.token)
    spark = builder.getOrCreate()

    bound_port = _bound_port(spark, args.port)
    _write_discovery(args.discovery, "localhost", bound_port, args.token, __version__)
    # Printed to the (normally discarded) child stdout; useful when launched with stdout attached.
    print(
        "SPARK-CONNECT-LOCAL-SERVER READY port={} pid={}".format(bound_port, os.getpid()),
        flush=True,
    )

    stop = {"flag": False}

    def _handle(_signum: int, _frame: Any) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _handle)
    try:
        signal.signal(signal.SIGINT, _handle)
    except ValueError:
        # SIGINT may not be settable when not on the main thread on some platforms.
        pass

    idle_timeout = args.idle_timeout
    poll_interval = max(1.0, args.poll_interval)
    last_active = time.monotonic()
    last_poll = 0.0
    try:
        # Sleep in short slices so a SIGTERM is observed promptly regardless of the poll interval.
        while not stop["flag"]:
            time.sleep(1.0)
            if idle_timeout <= 0:
                continue
            now = time.monotonic()
            if now - last_poll < poll_interval:
                continue
            last_poll = now
            try:
                active = _has_active_sessions(spark)
            except Exception:
                active = True  # fail open: never reap a server we cannot inspect
            if active:
                last_active = now
            elif now - last_active > idle_timeout:
                break
    finally:
        _remove_discovery_if_ours(args.discovery)
        spark.stop()


if __name__ == "__main__":
    main()
