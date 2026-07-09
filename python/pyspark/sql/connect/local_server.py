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
``pyspark.sql.connect.session``). When reuse is enabled, the first run instead starts one
long-lived server through the standard ``sbin/start-connect-server.sh`` script -- the same
daemon a user would start by hand -- and records how to reach it in a *discovery file* (host,
port, auth token, pid and Spark version). Later runs validate that record (matching Spark
version, live pid, port accepting connections) and reconnect instead of starting another
server. Each client connection gets its own isolated server-side session, so session-local
state (temp views, runtime SQL confs, session artifacts) does not leak between runs.

``sbin/spark-daemon.sh`` owns the server process: its pid file and logs are kept next to the
discovery file (``~/.spark`` by default; override with ``SPARK_LOCAL_CONNECT_DISCOVERY``). The
server runs until stopped with::

    python -m pyspark.sql.connect.local_server --stop

or ``sbin/stop-connect-server.sh``. This relies on the POSIX shell scripts under ``sbin/``, so
the reuse path is not supported on Windows.
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

_SERVER_CLASS = "org.apache.spark.sql.connect.service.SparkConnectServer"
# A fixed SPARK_IDENT_STRING keeps the spark-daemon.sh pid and log file names stable
# regardless of $USER.
_SPARK_IDENT = "local-connect"


# -- the discovery file --------------------------------------------------------------------------


def _discovery_path() -> str:
    """Location of the discovery file describing the running persistent local server."""
    return os.environ.get(
        "SPARK_LOCAL_CONNECT_DISCOVERY",
        os.path.join(os.path.expanduser("~"), ".spark", "connect-local.json"),
    )


def _runtime_dir() -> str:
    """Directory holding the discovery file, and with it the server's pid file and logs."""
    return os.path.dirname(_discovery_path()) or os.getcwd()


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


def _write_discovery(path: str, host: str, port: int, token: str, pid: int, version: str) -> None:
    """Atomically write the discovery file with ``0600`` perms (it holds the auth token)."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    payload = {
        "host": host,
        "port": port,
        "token": token,
        "pid": pid,
        "spark_version": version,
    }
    tmp = "{}.{}.tmp".format(path, os.getpid())
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(json.dumps(payload))
    os.replace(tmp, path)


# -- deciding between reusing the recorded server and starting a fresh one ------------------------


def _pid_alive(pid: int) -> bool:
    """Whether ``pid`` exists (POSIX only). A process we cannot signal counts as alive."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        pass
    return True


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
    if os.name == "posix" and not _pid_alive(disc["pid"]):
        return False
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

    On platforms without ``fcntl`` this is a no-op: racing callers may each start a server, and
    the losers reconnect to the one that wins the discovery-file update.
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


# -- starting and stopping the server through sbin/start-connect-server.sh ------------------------


def _pick_port(opts: Dict[str, Any]) -> int:
    """Choose the port for a fresh server.

    Tests always use an OS-assigned free port so suites can run in parallel. Otherwise the
    configured/default port is honored unless it is already taken -- e.g. by a stale server just
    rejected on version mismatch -- in which case an OS-assigned port is used instead. Unlike the
    in-process path, the standalone script cannot report an ephemeral port back to us, so the
    free port is picked (and released) here, subject to a small race until the server binds it.
    """

    def free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("localhost", 0))
            return sock.getsockname()[1]

    if "SPARK_TESTING" in os.environ:
        return free_port()
    from pyspark.sql.connect.client import DefaultChannelBuilder

    port = int(opts.get("spark.local.connect.server.port", DefaultChannelBuilder.default_port()))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("localhost", port))
            return port
        except OSError:
            return free_port()


def _seed_conf(opts: Dict[str, Any]) -> Dict[str, Any]:
    """Start-up configs to seed a freshly started persistent server.

    Mirrors the merge that the in-process ``SparkSession._start_connect_server`` applies to its
    ``SparkConf`` so that first-run behavior matches (warehouse dir, app name, jars/packages,
    catalog confs, etc.). Keys the launcher controls itself (master, port, token) and the
    ``spark.local.connect.*`` opt-in keys are excluded. This only seeds the run that *starts*
    the server; a later run reconnecting to an already-warm JVM cannot change its static
    configs.
    """
    conf: Dict[str, Any] = {}
    for i in range(int(os.environ.get("PYSPARK_REMOTE_INIT_CONF_LEN", "0"))):
        conf = json.loads(os.environ["PYSPARK_REMOTE_INIT_CONF_{}".format(i)])
    conf.update(opts)
    for k in list(conf):
        if k in (
            "spark.remote",
            "spark.api.mode",
            "spark.master",
            "spark.connect.authenticate.token",
            "spark.connect.grpc.binding.port",
        ) or k.startswith("spark.local.connect."):
            conf.pop(k)
    return conf


def _write_seed_properties(opts: Dict[str, Any], runtime_dir: str) -> Optional[str]:
    """Write the seed configs as a ``--properties-file`` for spark-submit, or return ``None``.

    Written with ``0600`` perms since configs may hold sensitive values; passing a file keeps
    them off the server's argv, where they would be visible in ``ps`` output.
    """
    seed = _seed_conf(opts)
    if not seed:
        return None
    fd, path = tempfile.mkstemp(prefix="connect-local-conf-", suffix=".properties", dir=runtime_dir)
    with os.fdopen(fd, "w") as f:
        for key, value in seed.items():
            escaped = str(value).replace("\\", "\\\\").replace("\n", "\\n")
            f.write("{}={}\n".format(key, escaped))
    os.chmod(path, 0o600)
    return path


def _pid_file(runtime_dir: str) -> str:
    """The pid file spark-daemon.sh maintains for the server started by this module."""
    return os.path.join(runtime_dir, "spark-{}-{}-1.pid".format(_SPARK_IDENT, _SERVER_CLASS))


def _read_pid(path: str) -> Optional[int]:
    try:
        with open(path, "r") as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def _signal_server(pid: int, sig: int) -> bool:
    """Best-effort signal to the recorded server pid (the JVM started by spark-daemon.sh)."""
    try:
        os.kill(pid, sig)
        return True
    except OSError:
        return False


def _launch_server(master: str, opts: Dict[str, Any]) -> str:
    """Start a persistent local Connect server and wait until it is reachable.

    Runs the standard ``sbin/start-connect-server.sh``, which daemonizes the server JVM through
    ``sbin/spark-daemon.sh`` (pid file, logs). Once the server accepts connections, the
    discovery file is written and the ``sc://host:port`` endpoint returned. Callers must hold
    ``_start_lock`` (see ``reuse_or_start_local_connect_server``).
    """
    from pyspark.find_spark_home import _find_spark_home
    from pyspark.version import __version__

    spark_home = os.environ.get("SPARK_HOME") or _find_spark_home()
    script = os.path.join(spark_home, "sbin", "start-connect-server.sh")
    if not os.path.isfile(script):
        raise PySparkRuntimeError(
            errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
            messageParameters={"reason": "cannot find {}".format(script)},
        )

    discovery_path = _discovery_path()
    runtime_dir = _runtime_dir()
    os.makedirs(runtime_dir, exist_ok=True)
    log_dir = os.path.join(runtime_dir, "logs")
    pid_file = _pid_file(runtime_dir)

    # Same token precedence as the in-process ``_start_connect_server``: an explicit env token,
    # then one passed as a conf, then a fresh one. The token travels through the environment,
    # never argv, where it would be visible in `ps` output.
    token = (
        os.environ.get("SPARK_CONNECT_AUTHENTICATE_TOKEN")
        or opts.get("spark.connect.authenticate.token")
        or str(uuid.uuid4())
    )
    port = _pick_port(opts)
    conf_file = _write_seed_properties(opts, runtime_dir)

    env = dict(os.environ)
    for var in ("SPARK_REMOTE", "SPARK_LOCAL_REMOTE", "SPARK_CONNECT_MODE_ENABLED"):
        env.pop(var, None)
    env["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = token
    env["SPARK_PID_DIR"] = runtime_dir
    env["SPARK_LOG_DIR"] = log_dir
    env["SPARK_IDENT_STRING"] = _SPARK_IDENT

    cmd = [
        script,
        "--master",
        master,
        "--conf",
        "spark.connect.grpc.binding.port={}".format(port),
    ]
    if conf_file is not None:
        cmd += ["--properties-file", conf_file]

    try:
        result = subprocess.run(
            cmd,
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            stale_pid = _read_pid(pid_file)
            if stale_pid is not None and _pid_alive(stale_pid):
                # spark-daemon.sh refuses to start while its pid file points at a live
                # process -- here a server this client just rejected as not reusable
                # (e.g. after a Spark upgrade).
                reason = (
                    "a local Connect server that is not reusable by this client is already "
                    "running (pid {}); stop it with "
                    "`python -m pyspark.sql.connect.local_server --stop`".format(stale_pid)
                )
            else:
                output = (result.stderr or "") + (result.stdout or "")
                last_line = output.strip().splitlines()[-1] if output.strip() else ""
                reason = "start-connect-server.sh exited with code {}: {}".format(
                    result.returncode, last_line
                )
            raise PySparkRuntimeError(
                errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
                messageParameters={"reason": reason},
            )

        # The script has daemonized the server; wait for it to accept connections.
        deadline = time.time() + 120
        while time.time() < deadline:
            pid = _read_pid(pid_file)
            if pid is not None:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(0.5)
                    if sock.connect_ex(("localhost", port)) == 0:
                        _write_discovery(discovery_path, "localhost", port, token, pid, __version__)
                        os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = token
                        return "sc://localhost:{}".format(port)
                if not _pid_alive(pid):
                    raise PySparkRuntimeError(
                        errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
                        messageParameters={
                            "reason": "the server exited during start-up; see logs under {}".format(
                                log_dir
                            )
                        },
                    )
            time.sleep(0.25)

        # Timed out: best-effort stop of the server we just started, then fail.
        pid = _read_pid(pid_file)
        if pid is not None:
            _signal_server(pid, signal.SIGTERM)
        raise PySparkRuntimeError(
            errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
            messageParameters={
                "reason": "the server did not become ready within 120 seconds; see logs "
                "under {}".format(log_dir)
            },
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
    if os.name != "posix":
        raise PySparkRuntimeError(
            errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
            messageParameters={
                "reason": "spark.local.connect.reuse relies on the POSIX scripts under sbin/; "
                "on this platform start a server manually (sbin/start-connect-server.sh) and "
                'connect with .remote("sc://...")'
            },
        )
    # Fast path: reuse an already-running server without taking the cross-process lock.
    endpoint = _reuse_from_discovery()
    if endpoint is not None:
        return endpoint
    # No reusable server yet. Serialize start-up across processes when file locking is available.
    # Without it, racing callers may each start a server; the winner writes the discovery file
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
    # Also drop the spark-daemon.sh pid file so a fresh start is never refused on its account.
    for path in (_discovery_path(), _pid_file(_runtime_dir())):
        try:
            os.remove(path)
        except OSError:
            pass
    return stopped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manage the persistent local Spark Connect server used by the opt-in "
        "spark.local.connect.reuse path. The server itself is started on demand through "
        "sbin/start-connect-server.sh."
    )
    parser.add_argument(
        "--stop", action="store_true", help="stop the recorded running server, if any"
    )
    args = parser.parse_args()

    if not args.stop:
        parser.print_help(sys.stderr)
        sys.exit(2)
    if stop_local_connect_server():
        print("Stopped the persistent local Spark Connect server.")
    else:
        print("No running persistent local Spark Connect server found.")


if __name__ == "__main__":
    main()
