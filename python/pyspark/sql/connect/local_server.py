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


def _debug(message: str) -> None:
    """Append a timestamped line to ``SPARK_LOCAL_CONNECT_SERVER_DEBUG_LOG``, when set.

    The daemon runs detached with its stdio discarded, so this is the supported way to see
    what phase it is in (or stuck in) when diagnosing a server that never became ready or
    never shut down.
    """
    path = os.environ.get("SPARK_LOCAL_CONNECT_SERVER_DEBUG_LOG")
    if not path:
        return
    try:
        with open(path, "a") as f:
            f.write("{:.3f} pid={} {}\n".format(time.time(), os.getpid(), message))
    except OSError:
        pass


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


def _run_warmup(spark: Any, port: int, token: str, should_stop: Any) -> None:
    """JIT-warm the query paths with fixed synthetic queries before the first real run.

    JIT and codegen caches are JVM-global, so warming them here removes most of the
    first-query latency from the one user run this (pool-mode) server will serve -- without
    any user code or state ever touching the JVM. The classic pass warms Catalyst and the
    execution engine; the self-connect pass (best effort: it needs the Connect client deps)
    warms the Connect planner, Arrow serialization and the gRPC path end to end. Warmup state
    is session-local and released; the discovery file is published before warmup starts, so a
    client in a hurry can still claim and use the server mid-warmup.

    Shutdown safety: ``should_stop`` is consulted between steps, and the self-connect pass
    runs in a bounded daemon thread. A single-use server can be claimed, used, and released
    -- SIGTERMing this whole process group, JVM included -- while warmup is still running; a
    Connect handshake against the dying JVM then blocks with no deadline, and on the main
    thread it would wedge the daemon past its own stop flag forever (observed in testing as
    an unkillable daemon holding a zombie JVM).
    """
    classic_queries = (
        "SELECT 1",
        "SELECT sum(id) AS s, count(1) AS c FROM range(100000)",
        "SELECT x, count(*) AS c FROM (SELECT id % 7 AS x FROM range(100000)) GROUP BY x",
    )
    _debug("warmup: classic phase")
    for query in classic_queries:
        if should_stop():
            return
        try:
            spark.sql(query).collect()
        except Exception:
            return
    if should_stop():
        return

    def _connect_warmup() -> None:
        try:
            if token:
                os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = token
            from pyspark.sql.connect.session import SparkSession as ConnectSession

            client = ConnectSession(connection="sc://localhost:{}".format(port))
            try:
                client.sql("SELECT 1").collect()
                client.range(100000).selectExpr("sum(id)").collect()
            finally:
                client.stop()
        except Exception:
            pass

    import threading

    _debug("warmup: connect phase")
    thread = threading.Thread(target=_connect_warmup, name="connect-warmup", daemon=True)
    thread.start()
    deadline = time.monotonic() + 60
    while thread.is_alive() and time.monotonic() < deadline and not should_stop():
        thread.join(timeout=0.5)


def _stop_and_reap_jvm(spark: Any) -> None:
    """Stop the Spark session and reap the child JVM before this daemon exits.

    This daemon must outlive and reap its child JVM: if the daemon exited first, the JVM
    would be reparented to init, and where init does not reap orphans (e.g. a container
    whose PID 1 is a non-reaping placeholder, as in CI job containers) its eventual zombie
    would keep this process group non-empty forever -- so the client's killpg-based
    group-liveness checks in ``_terminate_local_connect_server`` would see a server that
    never terminates even though every process in it is dead.

    ``spark.stop()`` is bounded by a daemon thread because it speaks py4j to a JVM that, on
    the group-SIGTERM shutdown path, is already dying and may never answer (cf. the warmup
    shutdown note in ``_run_warmup``). Closing the JVM's stdin pipe then asks a still-running
    JVM to exit cleanly -- the gateway exits on stdin EOF, running Spark's shutdown hooks --
    and SIGKILL is the backstop. The waits are sized to finish inside the ~10s the client
    gives the daemon to exit after SIGTERM before it escalates to killing the whole group.
    """
    import threading

    jvm_proc = None
    try:
        jvm_proc = spark.sparkContext._gateway.proc
    except Exception:
        pass

    def _stop() -> None:
        try:
            spark.stop()
        except Exception:
            pass  # the JVM may already be gone; exiting is what matters

    stopper = threading.Thread(target=_stop, name="spark-stop", daemon=True)
    stopper.start()
    stopper.join(timeout=3.0)

    if jvm_proc is None:
        return
    try:
        if jvm_proc.stdin is not None:
            jvm_proc.stdin.close()
    except Exception:
        pass
    try:
        jvm_proc.wait(timeout=5.0)
        return
    except Exception:
        pass
    _debug("JVM did not exit in time; killing it")
    try:
        jvm_proc.kill()
    except Exception:
        pass
    try:
        jvm_proc.wait(timeout=5.0)
    except Exception:
        pass


def _jvm_process_dead(spark: Any) -> bool:
    """Whether the child JVM process has already exited (its py4j Popen has a return code).

    When the JVM is gone the active-session probe can only fail, and failing open would keep
    this daemon alive forever; a dead JVM always means shut down.
    """
    try:
        proc = spark.sparkContext._gateway.proc
        return proc is not None and proc.poll() is not None
    except Exception:
        return False


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
    parser.add_argument(
        "--warmup",
        action="store_true",
        help="run fixed synthetic queries after start-up to JIT-warm the query paths, so the"
        " first real query on this server is fast; no user code or state is involved",
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
    _debug("starting SparkContext")
    spark = builder.getOrCreate()
    _debug("SparkContext up")

    bound_port = _bound_port(spark, args.port)

    # Install the shutdown handlers before the server becomes discoverable (and before any
    # warmup), so a client that claims and releases us immediately can already stop us cleanly.
    stop = {"flag": False}

    def _handle(_signum: int, _frame: Any) -> None:
        stop["flag"] = True
        _debug("signal {} received".format(_signum))

    signal.signal(signal.SIGTERM, _handle)
    try:
        signal.signal(signal.SIGINT, _handle)
    except ValueError:
        # SIGINT may not be settable when not on the main thread on some platforms.
        pass

    _write_discovery(
        args.discovery, "localhost", bound_port, args.token, __version__, args.fingerprint
    )
    print(
        "SPARK-CONNECT-LOCAL-SERVER READY port={} pid={}".format(bound_port, os.getpid()),
        flush=True,
    )

    if args.warmup and not stop["flag"]:
        _debug("warmup starting")
        _run_warmup(spark, bound_port, args.token, lambda: stop["flag"])
        _debug(
            "warmup done; sigterm handler intact: {}".format(
                signal.getsignal(signal.SIGTERM) is _handle
            )
        )

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
                # Fail open only while the JVM is alive: an uninspectable but running server
                # must never be reaped. A dead JVM can only ever fail inspection, so failing
                # open would keep this daemon (and its zombie child) around forever.
                if _jvm_process_dead(spark):
                    _debug("JVM process is gone; exiting")
                    break
                active = True
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
        _debug("shutting down (stop flag={})".format(stop["flag"]))
        _remove_discovery_if_ours(args.discovery)
        _stop_and_reap_jvm(spark)
        _debug("shutdown complete")


if __name__ == "__main__":
    main()
