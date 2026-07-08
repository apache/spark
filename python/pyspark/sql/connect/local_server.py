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
Opt-in reuse of a persistent local Spark Connect server (``spark.local.connect.reuse`` /
``SPARK_LOCAL_CONNECT_REUSE``).

By default ``SparkSession.builder.remote("local[*]").getOrCreate()`` boots a fresh in-process
Connect server in every Python process (see ``SparkSession._start_connect_server`` in
``pyspark.sql.connect.session``). When reuse is enabled, ``reuse_or_start_local_connect_server``
launches this module once as a detached daemon (``python -m pyspark.sql.connect.local_server``).
The daemon starts a regular (classic) local Spark session with the Spark Connect plugin -- the
same mechanism the in-process path uses -- and then blocks, so one warm JVM and Connect server
keep serving many short-lived client processes. Each client connection gets its own isolated
server-side session, so session-local state (temp views, runtime SQL confs, isolated artifacts)
does not leak between runs.

Once the server accepts connections, the daemon writes a *discovery file* recording the host, the
actually bound port, the auth token, its pid and the Spark version. Later client processes read
it and -- after validating that the version matches, the pid is alive and the port accepts
connections -- reconnect instead of starting another server.

To stop a running server::

    python -m pyspark.sql.connect.local_server --stop
"""

import argparse
import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from typing import Any, Dict, Iterator, Optional

from pyspark.errors import PySparkRuntimeError

# -- the discovery file, shared between the client-side helpers and the daemon --------------------


def _discovery_path() -> str:
    """Location of the discovery file describing the running persistent local server."""
    return os.environ.get(
        "SPARK_LOCAL_CONNECT_DISCOVERY",
        os.path.join(os.path.expanduser("~"), ".spark", "connect-local.json"),
    )


def _read_discovery() -> Optional[Dict[str, Any]]:
    """Read and validate the discovery file, returning ``None`` if it is absent or malformed.

    A returned dict always has string ``host``, ``token`` and ``spark_version`` values and int
    ``port`` and ``pid`` values, so callers can index it without re-validating.
    """
    try:
        with open(_discovery_path(), "r") as f:
            disc = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(disc, dict):
        return None
    try:
        disc["port"] = int(disc["port"])
        disc["pid"] = int(disc["pid"])
    except (KeyError, TypeError, ValueError):
        return None
    if not all(isinstance(disc.get(k), str) for k in ("host", "token", "spark_version")):
        return None
    return disc


def _write_discovery(path: str, host: str, port: int, token: str, version: str) -> None:
    """Atomically write the discovery file with ``0600`` perms (it holds the auth token)."""
    parent = os.path.dirname(path)
    if parent:
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
    """Remove the discovery file at daemon shutdown, but only if it still points at this process.

    The read-check-remove sequence could race with a new server publishing itself and delete the
    newcomer's file. Every discovery write happens while a launching client holds the start-up
    lock (the daemon writes the file during ``_launch_server``'s lock-held wait), so claiming that
    lock non-blockingly closes the race: if it is busy, a new server is starting and will
    overwrite the file anyway, and skipping the removal keeps its entry intact. Where ``fcntl``
    is unavailable the removal stays best-effort and unguarded.
    """
    lock_fd = None
    try:
        import fcntl

        lock_fd = os.open(path + ".lock", os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except ImportError:
        pass  # no fcntl (e.g. Windows): fall through to the unguarded removal
    except OSError:
        if lock_fd is not None:
            # The lock is held: a launcher is publishing a new server right now, and the file
            # (or what it is about to become) is no longer ours to remove.
            os.close(lock_fd)
            return
        # The lock file itself could not be created: fall through to the unguarded removal.
    try:
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
    finally:
        if lock_fd is not None:
            os.close(lock_fd)  # closing the fd releases the flock


# -- client side: decide between reusing the recorded server and starting a fresh one -------------


def _server_is_reusable(disc: Dict[str, Any]) -> bool:
    """Decide whether the server described by ``disc`` can be reused by this process.

    Reuse requires that the recorded Spark version matches this client's, that the recorded
    process is still alive, and that it is accepting connections on the recorded port. A version
    mismatch, dead pid, or closed port means the caller must start its own server instead. The
    pid probe runs only on POSIX: on Windows ``os.kill(pid, 0)`` *terminates* the target process
    rather than testing it, so there the port probe is the only liveness signal.
    """
    from pyspark.version import __version__

    if disc["spark_version"] != __version__:
        return False
    if os.name == "posix":
        try:
            os.kill(disc["pid"], 0)
        except ProcessLookupError:
            return False
        except OSError:
            # The process exists but is not ours to signal (e.g. PermissionError): treat as alive.
            pass
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        if sock.connect_ex((disc["host"], disc["port"])) != 0:
            return False
    return True


def _reuse_from_discovery() -> Optional[str]:
    """Return an endpoint for the recorded server if it is reusable, else ``None``.

    On success it also sets ``SPARK_CONNECT_AUTHENTICATE_TOKEN`` so the client authenticates
    against that server.
    """
    disc = _read_discovery()
    if disc is not None and _server_is_reusable(disc):
        os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = disc["token"]
        return "sc://{}:{}".format(disc["host"], disc["port"])
    return None


@contextlib.contextmanager
def _start_lock() -> Iterator[None]:
    """Exclusive cross-process file lock serializing persistent-server start-up.

    On platforms without ``fcntl`` this is a no-op: racing callers may each start a daemon, and
    the losers reconnect to the server that wins the discovery-file update.
    """
    try:
        import fcntl
    except ImportError:
        yield
        return
    path = _discovery_path() + ".lock"
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)  # closing the fd releases the flock


def _port_available(port: int) -> bool:
    """Whether ``port`` can currently be bound on localhost (best effort, subject to races)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("localhost", port))
            return True
        except OSError:
            return False


def _seed_conf(opts: Dict[str, Any]) -> Dict[str, Any]:
    """Start-up configs to seed a freshly started persistent server.

    Mirrors the merge that the in-process ``SparkSession._start_connect_server`` applies to its
    ``SparkConf`` so that first-run behavior matches (warehouse dir, app name, jars/packages,
    catalog confs, etc.). Keys the daemon controls itself (master, port, token, plugins) and the
    reuse opt-in keys are excluded. This only seeds the run that *starts* the server; a later run
    reconnecting to an already-warm JVM cannot change its static configs.
    """
    conf: Dict[str, Any] = {}
    for i in range(int(os.environ.get("PYSPARK_REMOTE_INIT_CONF_LEN", "0"))):
        conf = json.loads(os.environ["PYSPARK_REMOTE_INIT_CONF_{}".format(i)])
    conf.update(opts)
    for k in (
        "spark.remote",
        "spark.api.mode",
        "spark.master",
        "spark.connect.authenticate.token",
        "spark.connect.grpc.binding.port",
        "spark.local.connect.reuse",
        "spark.local.connect.server.port",
        "spark.local.connect.server.idleTimeout",
    ):
        conf.pop(k, None)
    return conf


def _signal_server(pid: int, sig: int) -> bool:
    """Signal a detached local Connect daemon, including its JVM on POSIX.

    The daemon is launched as a session leader, so its process group id equals its pid and
    signalling the group reaps its child JVM too. If the group id differs, ``pid`` belongs to an
    unrelated process (e.g. recycled after a stale discovery file), so only the pid itself is
    signalled -- never a group this code did not create.
    """
    try:
        if os.name == "posix" and os.getpgid(pid) == pid:
            os.killpg(pid, sig)
        else:
            os.kill(pid, sig)
        return True
    except OSError:
        return False


def _terminate_server(proc: Any) -> None:
    """Terminate a daemon started by ``_launch_server`` and its JVM.

    The group SIGTERM asks the daemon and its JVM to shut down gracefully, but the daemon exiting
    does not mean the JVM is gone: its graceful shutdown can outlive the daemon. On POSIX this
    therefore waits for the whole process group to disappear and escalates to a group SIGKILL if
    any member outlives the grace period.
    """
    if not _signal_server(proc.pid, signal.SIGTERM):
        return
    try:
        proc.wait(timeout=10)
    except Exception:
        pass
    if os.name != "posix":
        if proc.poll() is None:
            proc.kill()
        return
    # The group id stays valid while any member (i.e. the JVM) is alive, even after the daemon
    # leader has been reaped, so poll and signal the group id directly rather than via
    # _signal_server (whose getpgid lookup needs a live leader). Signalling this group is safe:
    # it was created by this launch, not read from disk.
    deadline = time.time() + 10
    while time.time() < deadline:
        proc.poll()  # reap the daemon if it exited, so only live members keep the group
        try:
            os.killpg(proc.pid, 0)
        except OSError:
            return
        time.sleep(0.2)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except OSError:
        pass
    try:
        # Reap the SIGKILLed daemon so it is not left behind as a zombie.
        proc.wait(timeout=5)
    except Exception:
        pass


def _launch_server(master: str, opts: Dict[str, Any]) -> str:
    """Launch a detached persistent local Connect server and wait until it is reachable.

    Callers must hold ``_start_lock`` (see ``reuse_or_start_local_connect_server``). If the
    server cannot be started but another process has meanwhile published a reusable one, this
    reconnects to it rather than failing.
    """
    from pyspark.sql.connect.client import DefaultChannelBuilder

    discovery_path = _discovery_path()
    # Same token precedence as the in-process ``_start_connect_server``: an explicit env token,
    # then one passed as a conf, then a fresh one.
    token = (
        os.environ.get("SPARK_CONNECT_AUTHENTICATE_TOKEN")
        or opts.get("spark.connect.authenticate.token")
        or str(uuid.uuid4())
    )

    # Choose the port. Tests use an ephemeral port (0) so they can run in parallel. Otherwise we
    # honor the configured/default port, but fall back to an ephemeral one if it is already
    # taken -- e.g. by a stale server we just rejected on version mismatch -- so a fresh server
    # can still start instead of failing to bind.
    if "SPARK_TESTING" in os.environ:
        port = 0
    else:
        port = int(
            opts.get("spark.local.connect.server.port", DefaultChannelBuilder.default_port())
        )
        if port != 0 and not _port_available(port):
            port = 0
    idle_timeout = opts.get("spark.local.connect.server.idleTimeout", "1800")

    # Seed the server with the caller's start-up confs (warehouse dir, jars, catalog, etc.) so
    # first-run behavior matches the in-process path. Passed as a JSON file since confs are
    # arbitrary key/values. Written under the discovery dir with 0600 perms as it may hold
    # sensitive values, and removed once the daemon has started.
    conf_file = None
    seed_conf = _seed_conf(opts)
    if seed_conf:
        parent = os.path.dirname(discovery_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        fd, conf_file = tempfile.mkstemp(prefix="connect-local-conf-", dir=parent or None)
        with os.fdopen(fd, "w") as f:
            json.dump(seed_conf, f)
        os.chmod(conf_file, 0o600)

    cmd = [
        sys.executable,
        "-m",
        "pyspark.sql.connect.local_server",
        "--master",
        master,
        "--port",
        str(port),
        "--discovery",
        discovery_path,
        "--idle-timeout",
        str(idle_timeout),
    ]
    if conf_file is not None:
        cmd += ["--conf-file", conf_file]

    # Launch detached so the server outlives this client process. The token travels through the
    # environment, never argv, where it would be visible in `ps` output.
    env = dict(os.environ)
    env["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = token
    for var in ("SPARK_REMOTE", "SPARK_LOCAL_REMOTE", "SPARK_CONNECT_MODE_ENABLED"):
        env.pop(var, None)
    popen_kwargs: Dict[str, Any] = dict(
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if os.name == "posix":
        popen_kwargs["start_new_session"] = True
    else:
        detached = getattr(subprocess, "DETACHED_PROCESS", 0)
        new_group = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        popen_kwargs["creationflags"] = detached | new_group
    proc = subprocess.Popen(cmd, **popen_kwargs)

    try:
        # Wait for the server to write the discovery file (which records the actual bound port)
        # and start accepting connections.
        deadline = time.time() + 120
        while time.time() < deadline:
            exit_code = proc.poll()
            if exit_code is not None:
                # Our daemon died. Another process may have published a usable server in the
                # meantime (e.g. a port race), so prefer reconnecting to it over failing.
                endpoint = _reuse_from_discovery()
                if endpoint is not None:
                    return endpoint
                raise PySparkRuntimeError(
                    errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
                    messageParameters={
                        "reason": "the server process exited with code {}".format(exit_code)
                    },
                )
            disc = _read_discovery()
            if disc is not None and disc["pid"] == proc.pid and disc["token"] == token:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(0.5)
                    if sock.connect_ex((disc["host"], disc["port"])) == 0:
                        os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = token
                        return "sc://{}:{}".format(disc["host"], disc["port"])
            time.sleep(0.25)

        # Timed out. The daemon may still be inside getOrCreate() -- i.e. before it has wired up
        # its own SIGTERM handler -- and it has already spawned a child JVM. Signal the whole
        # process group (the daemon is a session leader via start_new_session) and escalate to
        # SIGKILL, so the JVM is reaped rather than orphaned.
        _terminate_server(proc)
        raise PySparkRuntimeError(
            errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
            messageParameters={"reason": "the server did not become ready within 120 seconds"},
        )
    finally:
        if conf_file is not None:
            try:
                os.remove(conf_file)
            except OSError:
                pass


def reuse_or_start_local_connect_server(master: str, opts: Dict[str, Any]) -> str:
    """Reuse a running persistent local Connect server, or start one if none is reusable.

    Returns the ``sc://host:port`` endpoint to connect to. This is the opt-in counterpart of
    ``SparkSession._start_connect_server`` and is only reached for a ``local`` master when
    ``spark.local.connect.reuse`` / ``SPARK_LOCAL_CONNECT_REUSE`` is set.
    """
    # Fast path: reuse an already-running server without taking the cross-process lock.
    endpoint = _reuse_from_discovery()
    if endpoint is not None:
        return endpoint
    # No reusable server yet. Serialize start-up across processes when file locking is available.
    # Without it, racing callers may each start a daemon; the winner writes the discovery file
    # and the others reconnect to it.
    with _start_lock():
        endpoint = _reuse_from_discovery()
        if endpoint is not None:
            return endpoint
        return _launch_server(master, opts)


def stop_local_connect_server() -> bool:
    """Stop the persistent local Spark Connect server started by the reuse path, if any.

    Returns ``True`` if a running server was signalled to stop. Safe to call when none is
    running. Also exposed on the command line for the dev loop::

        python -m pyspark.sql.connect.local_server --stop
    """
    disc = _read_discovery()
    stopped = False
    if disc is not None and disc["pid"] != os.getpid():
        stopped = _signal_server(disc["pid"], signal.SIGTERM)
    try:
        os.remove(_discovery_path())
    except OSError:
        pass
    return stopped


# -- the daemon itself, run as `python -m pyspark.sql.connect.local_server` -----------------------


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
    parser = argparse.ArgumentParser(description="Persistent local Spark Connect server.")
    parser.add_argument(
        "--stop", action="store_true", help="stop the recorded running server, if any, and exit"
    )
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--port", type=int, default=15002)
    parser.add_argument(
        "--discovery",
        default=None,
        help="path of the discovery file; defaults to $SPARK_LOCAL_CONNECT_DISCOVERY "
        "or ~/.spark/connect-local.json",
    )
    parser.add_argument(
        "--conf-file",
        default=None,
        help="path to a JSON file of extra SparkConf entries to seed the server with",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=1800.0,
        help="seconds with no active session after which the server self-terminates; <=0 disables",
    )
    parser.add_argument("--poll-interval", type=float, default=60.0)
    args = parser.parse_args()

    if args.discovery:
        # Everything below resolves the path via _discovery_path (as do the client-side helpers,
        # e.g. under --stop), so map an explicit --discovery onto the override it honors.
        os.environ["SPARK_LOCAL_CONNECT_DISCOVERY"] = args.discovery
    discovery = _discovery_path()

    if args.stop:
        if stop_local_connect_server():
            print("Stopped the persistent local Spark Connect server.")
        else:
            print("No running persistent local Spark Connect server found.")
        return

    # Build a CLASSIC session: the connect-mode env vars would otherwise divert us into a client.
    for var in ("SPARK_REMOTE", "SPARK_LOCAL_REMOTE", "SPARK_CONNECT_MODE_ENABLED"):
        os.environ.pop(var, None)

    # The launcher passes the auth token through the environment (argv would leak it to `ps`);
    # generate one for manual runs. The child JVM inherits the env var, and the Connect server
    # falls back to it when the `spark.connect.authenticate.token` conf is not set.
    token = os.environ.get("SPARK_CONNECT_AUTHENTICATE_TOKEN")
    if not token:
        token = str(uuid.uuid4())
        os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = token

    from pyspark.sql import SparkSession
    from pyspark.version import __version__

    builder = SparkSession.builder.master(args.master)
    # Seed the caller's start-up confs first so first-run behavior matches the in-process path,
    # then apply the settings the server itself controls so they always win.
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
    spark = builder.getOrCreate()

    bound_port = _bound_port(spark, args.port)
    _write_discovery(discovery, "localhost", bound_port, token, __version__)
    print(
        "SPARK-CONNECT-LOCAL-SERVER READY port={} pid={}".format(bound_port, os.getpid()),
        flush=True,
    )

    stop = False

    def _handle(_signum: int, _frame: Any) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    idle_timeout = args.idle_timeout
    poll_interval = max(1.0, args.poll_interval)
    last_active = time.monotonic()
    last_poll = 0.0
    try:
        # Sleep in short slices so a SIGTERM is observed promptly regardless of the poll interval.
        while not stop:
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
        _remove_discovery_if_ours(discovery)
        spark.stop()


if __name__ == "__main__":
    main()
