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

It is launched by file path rather than ``python -m`` so it does not require the Spark Connect
*client* dependencies (grpc, etc.): a server only needs a classic PySpark install plus the Connect
server jar, like ``sbin/start-connect-server.sh``. It imports only the classic ``pyspark.sql`` API.
"""

import sys

# Launching by file path puts this file's directory -- pyspark/sql/connect -- at the front of
# sys.path, where modules such as `types` and `logging` shadow the standard library and break
# ordinary imports. Drop it before importing anything else (`import sys` cannot be shadowed);
# pyspark stays importable via the remaining sys.path entries.
if sys.path:
    del sys.path[0]

import argparse
import json
import os
import signal
import time
from typing import Any


def _write_discovery(
    path: str, host: str, port: int, token: str, version: str, fingerprint: str = None
) -> None:
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
    if fingerprint is not None:
        payload["conf_fingerprint"] = fingerprint
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
        "--conf-file",
        default=None,
        help="path to a JSON file of extra SparkConf entries to seed the server with",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=3600.0,
        help="seconds with no active session after which the server self-terminates; <=0 disables",
    )
    parser.add_argument("--poll-interval", type=float, default=60.0)
    parser.add_argument(
        "--fingerprint",
        default=None,
        help="opaque identity of the start-up confs this server was seeded with; echoed into the"
        " discovery file so pool clients only claim servers matching their own confs",
    )
    parser.add_argument(
        "--exit-after-use",
        action="store_true",
        help="single-use (pool) mode: self-terminate once at least one client session has been"
        " observed and all sessions are gone again; the server never serves a second run",
    )
    args = parser.parse_args()

    # Build a CLASSIC session: the connect-mode env vars would otherwise divert us into a client.
    for var in ("SPARK_REMOTE", "SPARK_LOCAL_REMOTE", "SPARK_CONNECT_MODE_ENABLED"):
        os.environ.pop(var, None)
    if args.token:
        os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = args.token

    from pyspark.sql import SparkSession
    from pyspark.version import __version__

    builder = SparkSession.builder.master(args.master)
    # Seed the caller's start-up confs first so first-run behavior matches the in-process path, then
    # apply the settings the server itself controls so they always win.
    if args.conf_file:
        with open(args.conf_file, "r") as f:
            for key, value in json.load(f).items():
                builder = builder.config(key, str(value))
    builder = (
        builder.config("spark.plugins", "org.apache.spark.sql.connect.SparkConnectPlugin")
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
    _write_discovery(
        args.discovery, "localhost", bound_port, args.token, __version__, args.fingerprint
    )
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
    seen_active = False
    try:
        # Sleep in short slices so a SIGTERM is observed promptly regardless of the poll interval.
        while not stop["flag"]:
            time.sleep(1.0)
            if idle_timeout <= 0 and not args.exit_after_use:
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
                seen_active = True
                last_active = now
            elif args.exit_after_use and seen_active:
                # Single-use mode: our one client has come and gone. This is only a backstop --
                # the client normally SIGTERMs us on session.stop()/interpreter exit.
                break
            elif idle_timeout > 0 and now - last_active > idle_timeout:
                break
    finally:
        _remove_discovery_if_ours(args.discovery)
        spark.stop()


if __name__ == "__main__":
    main()
