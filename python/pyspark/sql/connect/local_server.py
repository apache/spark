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

The module is built from three components:

- :class:`LocalConnectServer` -- one persistent server: its ``url``, whether this client can
  reuse it (:meth:`LocalConnectServer.is_reusable`) and stopping it
  (:meth:`LocalConnectServer.stop`).
- :class:`Discovery` -- the discovery file: where it lives, loading it back as a
  ``LocalConnectServer``, atomically saving one, and the cross-process lock serializing
  server start-up.
- :class:`ServerLauncher` -- starting a fresh server through ``sbin/start-connect-server.sh``
  (which daemonizes it via ``sbin/spark-daemon.sh``) and waiting until it accepts connections.

The discovery file -- and with it the daemon's pid file and logs -- lives in a per-user
directory under the system temp dir (``<tempdir>/spark-connect-<uid>``, mode ``0700``), so
nothing accumulates in permanent directories and stale records do not survive a reboot. Set
``SPARK_LOCAL_CONNECT_DISCOVERY`` to relocate the discovery file (and the rest with it).

Threat model: this is a local development convenience, and the boundary drawn is *other
users on the same machine*. The server binds ``localhost`` and requires the auth token; the
token lives in the ``0600`` discovery file inside the ``0700`` per-user directory, whose
ownership is verified before trusting anything read from it (the temp dir itself is
world-writable). Processes of the same user are trusted -- they may reuse, reconfigure, or
stop the shared server, which is the point of the feature.

The server runs until stopped with::

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


def _start_failed(reason: str) -> PySparkRuntimeError:
    return PySparkRuntimeError(
        errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
        messageParameters={"reason": reason},
    )


def _pid_alive(pid: int) -> bool:
    """Whether ``pid`` exists (POSIX only). A process we cannot signal counts as alive."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        pass
    return True


def _per_user_runtime_dir() -> str:
    """Create (if needed) and return the per-user runtime directory under the temp dir."""
    try:
        owner = str(os.getuid())
    except AttributeError:  # Windows; the reuse path is rejected there anyway
        import getpass

        owner = getpass.getuser()
    path = os.path.join(tempfile.gettempdir(), "spark-connect-{}".format(owner))
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        # The temp dir is world-writable; never trust a directory someone else created.
        if hasattr(os, "getuid") and os.stat(path).st_uid != os.getuid():
            raise _start_failed("{} exists but is not owned by the current user".format(path))
        os.chmod(path, 0o700)
    return path


class LocalConnectServer:
    """One persistent local Spark Connect server, as recorded in the discovery file.

    A plain record of how to reach the server (host, port, auth token) and identify it (pid,
    Spark version), plus the operations scoped to it: the client ``url``, probing whether this
    client can reuse it, and stopping it.
    """

    def __init__(self, host: str, port: int, token: str, pid: int, spark_version: str):
        self.host = host
        self.port = port
        self.token = token
        self.pid = pid
        self.spark_version = spark_version

    @property
    def url(self) -> str:
        """The ``sc://host:port`` endpoint clients connect to."""
        return "sc://{}:{}".format(self.host, self.port)

    def is_listening(self) -> bool:
        """Whether the server currently accepts TCP connections on its recorded port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            return sock.connect_ex((self.host, self.port)) == 0

    def is_reusable(self) -> bool:
        """Decide whether this client process can reuse the server.

        Reuse requires that the recorded Spark version matches this client's, that the
        recorded process is still alive, and that it is accepting connections on the recorded
        port. A version mismatch, dead pid, or closed port means the caller must start its own
        server instead. The pid probe runs only on POSIX: on Windows ``os.kill(pid, 0)``
        *terminates* the target process rather than testing it, so there the port probe is the
        only liveness signal.
        """
        from pyspark.version import __version__

        if self.spark_version != __version__:
            return False
        if os.name == "posix" and not _pid_alive(self.pid):
            return False
        return self.is_listening()

    def stop(self) -> bool:
        """Best-effort SIGTERM to the server JVM; ``False`` if it could not be signalled."""
        try:
            os.kill(self.pid, signal.SIGTERM)
            return True
        except OSError:
            return False


class Discovery:
    """The discovery file through which runs find the persistent local server.

    Owns everything scoped to that file: its location, parsing it back into a
    :class:`LocalConnectServer` (:meth:`load`), atomically writing one with ``0600`` perms
    (:meth:`save`, the file holds the auth token), and the cross-process file lock that
    serializes server start-up (:meth:`lock`). Its :attr:`directory` also hosts the daemon's
    pid file and logs.

    The default location is ``spark-connect-<uid>/connect-local.json`` under the system temp
    dir -- per-user, mode ``0700``, and gone after a reboot -- and can be overridden with the
    ``SPARK_LOCAL_CONNECT_DISCOVERY`` environment variable.
    """

    def __init__(self, path: Optional[str] = None):
        self.path = (
            path
            or os.environ.get("SPARK_LOCAL_CONNECT_DISCOVERY")
            or os.path.join(_per_user_runtime_dir(), "connect-local.json")
        )

    @property
    def directory(self) -> str:
        """Directory holding the discovery file, the daemon's pid file, and its logs."""
        return os.path.dirname(self.path) or os.getcwd()

    def load(self) -> Optional[LocalConnectServer]:
        """Read the discovery file, returning ``None`` if it is absent or malformed."""
        try:
            with open(self.path, "r") as f:
                data = json.load(f)
        except (OSError, ValueError):
            return None
        if not isinstance(data, dict):
            return None
        try:
            server = LocalConnectServer(
                host=data["host"],
                port=int(data["port"]),
                token=data["token"],
                pid=int(data["pid"]),
                spark_version=data["spark_version"],
            )
        except (KeyError, TypeError, ValueError):
            return None
        if not all(isinstance(v, str) for v in (server.host, server.token, server.spark_version)):
            return None
        return server

    def save(self, server: LocalConnectServer) -> None:
        """Atomically write the discovery file with ``0600`` perms (it holds the auth token)."""
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        payload = {
            "host": server.host,
            "port": server.port,
            "token": server.token,
            "pid": server.pid,
            "spark_version": server.spark_version,
        }
        tmp = "{}.{}.tmp".format(self.path, os.getpid())
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(json.dumps(payload))
        os.replace(tmp, self.path)

    def clear(self) -> None:
        """Remove the discovery file, if present."""
        with contextlib.suppress(OSError):
            os.remove(self.path)

    @contextlib.contextmanager
    def lock(self) -> Iterator[None]:
        """Exclusive cross-process file lock serializing persistent-server start-up.

        On platforms without ``fcntl`` this is a no-op: racing callers may each start a
        server, and the losers reconnect to the one that wins the discovery-file update.
        """
        try:
            import fcntl
        except ImportError:
            yield
            return
        path = self.path + ".lock"
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)  # closing the fd releases the flock


def _daemon_pid_file(directory: str) -> str:
    """The pid file spark-daemon.sh maintains for the server started by this module.

    Shared knowledge between :class:`ServerLauncher` (reads it while waiting for start-up)
    and :func:`stop_local_connect_server` (removes it so a fresh start is never refused).
    """
    return os.path.join(directory, "spark-{}-{}-1.pid".format(_SPARK_IDENT, _SERVER_CLASS))


class ServerLauncher:
    """Starts a persistent local Connect server through ``sbin/start-connect-server.sh``.

    Owns the launch-time machinery: choosing the port, seeding start-up configs through a
    ``--properties-file``, invoking the sbin script (which daemonizes the server JVM via
    ``sbin/spark-daemon.sh``), and waiting until the server accepts connections -- at which
    point the discovery file is written and the running :class:`LocalConnectServer` returned.
    Callers must hold ``Discovery.lock`` (see :func:`reuse_or_start_local_connect_server`).
    """

    _READY_TIMEOUT = 120

    def __init__(self, master: str, opts: Dict[str, Any], discovery: Discovery):
        self._master = master
        self._opts = opts
        self._discovery = discovery
        self._directory = discovery.directory
        self._log_dir = os.path.join(self._directory, "logs")
        self._pid_file = _daemon_pid_file(self._directory)

    def launch(self) -> LocalConnectServer:
        """Start the server, wait until it is reachable, and record it for later runs."""
        os.makedirs(self._directory, exist_ok=True)
        token = self._token()
        port = self._pick_port()
        conf_file = self._write_seed_properties()
        try:
            self._run_script(port, token, conf_file)
            return self._await_ready(port, token)
        finally:
            if conf_file is not None:
                with contextlib.suppress(OSError):
                    os.remove(conf_file)

    def _token(self) -> str:
        """The auth token for the new server.

        Same precedence as the in-process ``_start_connect_server``: an explicit env token,
        then one passed as a conf, then a fresh one. The token travels through the
        environment, never argv, where it would be visible in ``ps`` output.
        """
        return (
            os.environ.get("SPARK_CONNECT_AUTHENTICATE_TOKEN")
            or self._opts.get("spark.connect.authenticate.token")
            or str(uuid.uuid4())
        )

    def _pick_port(self) -> int:
        """Choose the port for the fresh server.

        Tests always use an OS-assigned free port so suites can run in parallel. Otherwise
        the configured/default port is honored unless it is already taken -- e.g. by a stale
        server just rejected on version mismatch -- in which case an OS-assigned port is used
        instead. Unlike the in-process path, the standalone script cannot report an ephemeral
        port back to us, so the free port is picked (and released) here, subject to a small
        race until the server binds it.
        """

        def free_port() -> int:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.bind(("localhost", 0))
                return sock.getsockname()[1]

        if "SPARK_TESTING" in os.environ:
            return free_port()
        from pyspark.sql.connect.client import DefaultChannelBuilder

        port = int(
            self._opts.get("spark.local.connect.server.port", DefaultChannelBuilder.default_port())
        )
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("localhost", port))
                return port
            except OSError:
                return free_port()

    def _seed_conf(self) -> Dict[str, Any]:
        """Start-up configs to seed the freshly started server with.

        Mirrors the merge that the in-process ``SparkSession._start_connect_server`` applies
        to its ``SparkConf`` so that first-run behavior matches (warehouse dir, app name,
        jars/packages, catalog confs, etc.). Keys the launcher controls itself (master, port,
        token) and the ``spark.local.connect.*`` opt-in keys are excluded. This only seeds
        the run that *starts* the server; a later run reconnecting to an already-warm JVM
        cannot change its static configs.
        """
        conf: Dict[str, Any] = {}
        for i in range(int(os.environ.get("PYSPARK_REMOTE_INIT_CONF_LEN", "0"))):
            conf = json.loads(os.environ["PYSPARK_REMOTE_INIT_CONF_{}".format(i)])
        conf.update(self._opts)
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

    def _write_seed_properties(self) -> Optional[str]:
        """Write the seed configs as a ``--properties-file`` for spark-submit, or ``None``.

        Written with ``0600`` perms since configs may hold sensitive values; passing a file
        keeps them off the server's argv, where they would be visible in ``ps`` output.
        """
        seed = self._seed_conf()
        if not seed:
            return None
        fd, path = tempfile.mkstemp(
            prefix="connect-local-conf-", suffix=".properties", dir=self._directory
        )
        with os.fdopen(fd, "w") as f:
            for key, value in seed.items():
                escaped = str(value).replace("\\", "\\\\").replace("\n", "\\n")
                f.write("{}={}\n".format(key, escaped))
        os.chmod(path, 0o600)
        return path

    def _daemon_pid(self) -> Optional[int]:
        """The pid spark-daemon.sh recorded for the server it started, if any."""
        try:
            with open(self._pid_file, "r") as f:
                return int(f.read().strip())
        except (OSError, ValueError):
            return None

    def _run_script(self, port: int, token: str, conf_file: Optional[str]) -> None:
        """Run ``sbin/start-connect-server.sh``, which daemonizes the server JVM."""
        from pyspark.find_spark_home import _find_spark_home

        spark_home = os.environ.get("SPARK_HOME") or _find_spark_home()
        script = os.path.join(spark_home, "sbin", "start-connect-server.sh")
        if not os.path.isfile(script):
            raise _start_failed("cannot find {}".format(script))

        env = dict(os.environ)
        for var in ("SPARK_REMOTE", "SPARK_LOCAL_REMOTE", "SPARK_CONNECT_MODE_ENABLED"):
            env.pop(var, None)
        env["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = token
        env["SPARK_PID_DIR"] = self._directory
        env["SPARK_LOG_DIR"] = self._log_dir
        env["SPARK_IDENT_STRING"] = _SPARK_IDENT

        cmd = [
            script,
            "--master",
            self._master,
            "--conf",
            "spark.connect.grpc.binding.port={}".format(port),
        ]
        if conf_file is not None:
            cmd += ["--properties-file", conf_file]

        result = subprocess.run(
            cmd,
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            stale_pid = self._daemon_pid()
            if stale_pid is not None and _pid_alive(stale_pid):
                # spark-daemon.sh refuses to start while its pid file points at a live
                # process -- here a server this client just rejected as not reusable
                # (e.g. after a Spark upgrade).
                raise _start_failed(
                    "a local Connect server that is not reusable by this client is already "
                    "running (pid {}); stop it with "
                    "`python -m pyspark.sql.connect.local_server --stop`".format(stale_pid)
                )
            output = (result.stderr or "") + (result.stdout or "")
            last_line = output.strip().splitlines()[-1] if output.strip() else ""
            raise _start_failed(
                "start-connect-server.sh exited with code {}: {}".format(
                    result.returncode, last_line
                )
            )

    def _await_ready(self, port: int, token: str) -> LocalConnectServer:
        """Wait for the daemonized server to accept connections, then record it."""
        from pyspark.version import __version__

        deadline = time.time() + self._READY_TIMEOUT
        while time.time() < deadline:
            pid = self._daemon_pid()
            if pid is not None:
                server = LocalConnectServer("localhost", port, token, pid, __version__)
                if server.is_listening():
                    self._discovery.save(server)
                    return server
                if not _pid_alive(pid):
                    raise _start_failed(
                        "the server exited during start-up; see logs under {}".format(self._log_dir)
                    )
            time.sleep(0.25)

        # Timed out: best-effort stop of the server we just started, then fail.
        pid = self._daemon_pid()
        if pid is not None:
            with contextlib.suppress(OSError):
                os.kill(pid, signal.SIGTERM)
        raise _start_failed(
            "the server did not become ready within {} seconds; see logs under {}".format(
                self._READY_TIMEOUT, self._log_dir
            )
        )


def reuse_or_start_local_connect_server(master: str, opts: Dict[str, Any]) -> str:
    """Reuse a running persistent local Connect server, or start one if none is reusable.

    Returns the ``sc://host:port`` endpoint to connect to, and sets
    ``SPARK_CONNECT_AUTHENTICATE_TOKEN`` so the client authenticates against that server.
    This is the opt-in counterpart of ``SparkSession._start_connect_server`` and is only
    reached for a ``local`` master when ``spark.local.connect.reuse`` /
    ``SPARK_LOCAL_CONNECT_REUSE`` is set.
    """
    if os.name != "posix":
        raise _start_failed(
            "spark.local.connect.reuse relies on the POSIX scripts under sbin/; "
            "on this platform start a server manually (sbin/start-connect-server.sh) and "
            'connect with .remote("sc://...")'
        )
    discovery = Discovery()
    # Fast path: reuse an already-running server without taking the cross-process lock.
    server = discovery.load()
    if server is None or not server.is_reusable():
        # No reusable server yet. Serialize start-up across processes when file locking is
        # available. Without it, racing callers may each start a server; the winner writes
        # the discovery file and the others reconnect to it.
        with discovery.lock():
            server = discovery.load()
            if server is None or not server.is_reusable():
                server = ServerLauncher(master, opts, discovery).launch()
    os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = server.token
    return server.url


def stop_local_connect_server() -> bool:
    """Stop the persistent local Spark Connect server started by the reuse path, if any.

    Returns ``True`` if a running server was signalled to stop. Safe to call when none is
    running. Also exposed on the command line for the dev loop::

        python -m pyspark.sql.connect.local_server --stop
    """
    discovery = Discovery()
    # Hold the start-up lock so a stop racing a concurrent launch cannot signal or unlink the
    # newcomer's record mid-start; the stop simply waits for the launch to finish first.
    with discovery.lock():
        server = discovery.load()
        stopped = False
        if server is not None and server.pid != os.getpid():
            stopped = server.stop()
        discovery.clear()
        # Also drop the spark-daemon.sh pid file so a fresh start is never refused on its
        # account.
        with contextlib.suppress(OSError):
            os.remove(_daemon_pid_file(discovery.directory))
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
