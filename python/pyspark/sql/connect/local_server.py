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
Connect server in every Python process. With reuse enabled, the first run starts one
long-lived server through ``sbin/start-connect-server.sh`` and records how to reach it (host,
port, auth token, pid, Spark version) in a discovery file; later runs reconnect to it if the
version matches, the pid is alive, and the port accepts connections. Each run still gets its
own server-side session, so session-local state does not leak between runs.

The discovery file, the daemon's pid file, and the logs live in a per-user ``0700`` directory
under the system temp dir; ``SPARK_LOCAL_CONNECT_DISCOVERY`` overrides the discovery file
location. The auth token is stored with ``0600`` and the server binds localhost, so other
users on the machine can neither read the token nor reach the server. Processes of the same
user share the server by design.

The server runs until stopped with ``python -m pyspark.sql.connect.local_server --stop`` or
``sbin/stop-connect-server.sh``. Windows is not supported, as this relies on the POSIX
scripts under ``sbin/``.
"""

import argparse
import contextlib
import getpass
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from typing import Any, Dict, Optional

from pyspark.errors import PySparkRuntimeError

_SERVER_CLASS = "org.apache.spark.sql.connect.service.SparkConnectServer"
# A fixed SPARK_IDENT_STRING keeps the spark-daemon.sh pid and log file names stable
# regardless of $USER.
_SPARK_IDENT = "local-connect"


def _pid_alive(pid: int) -> bool:
    """Whether ``pid`` exists (POSIX only). A process we cannot signal counts as alive."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        pass
    return True


class Discovery:
    """Reads and writes the discovery file recording the persistent local server.

    The file lives in a per-user directory under the system temp dir, or wherever
    ``SPARK_LOCAL_CONNECT_DISCOVERY`` points; the daemon's pid file and logs sit next to it.
    """

    @staticmethod
    def _runtime_dir() -> str:
        path = os.path.join(tempfile.gettempdir(), "spark-connect-{}".format(getpass.getuser()))
        if os.path.exists(path):
            os.chmod(path, 0o700)
        else:
            os.mkdir(path, 0o700)
        return path

    def __init__(self, path: Optional[str] = None):
        self.path = (
            path
            or os.environ.get("SPARK_LOCAL_CONNECT_DISCOVERY")
            or os.path.join(self._runtime_dir(), "connect-local.json")
        )
        self._lock_file = None

    @property
    def directory(self) -> str:
        return os.path.dirname(self.path) or os.getcwd()

    @property
    def daemon_pid_path(self) -> str:
        return os.path.join(self.directory, "spark-{}-{}-1.pid".format(_SPARK_IDENT, _SERVER_CLASS))

    def __enter__(self) -> "Discovery":
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._lock_file = open(self.path + ".lock", "a+")
        import fcntl

        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        assert self._lock_file is not None
        self._lock_file.close()
        self._lock_file = None

    def _assert_locked(self) -> None:
        assert self._lock_file is not None, "Discovery must be used as a context manager"

    def load(self) -> Optional[Dict[str, Any]]:
        """Read the discovery file, returning ``None`` if it is absent or malformed."""
        self._assert_locked()
        try:
            with open(self.path, "r") as f:
                data = json.load(f)
        except (OSError, ValueError):
            return None
        if not isinstance(data, dict):
            return None
        try:
            data["port"] = int(data["port"])
            data["pid"] = int(data["pid"])
        except (KeyError, TypeError, ValueError):
            return None
        if not all(isinstance(data[k], str) for k in ("host", "token", "spark_version")):
            return None
        return data

    def save(self, data: Dict[str, Any]) -> None:
        """Atomically write the discovery file with ``0600`` perms; it holds the auth token."""
        self._assert_locked()
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        tmp = "{}.{}.tmp".format(self.path, os.getpid())
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(json.dumps(data))
        os.replace(tmp, self.path)

    def clear(self) -> None:
        self._assert_locked()
        with contextlib.suppress(OSError):
            os.remove(self.path)
        with contextlib.suppress(OSError):
            os.remove(self.daemon_pid_path)


class LocalConnectServer:
    """The persistent server described by a locked ``Discovery``."""

    def __init__(self, discovery: Discovery):
        self._discovery = discovery
        self._reload()

    def _reload(self) -> None:
        data = self._discovery.load()
        self.host = data["host"] if data else None
        self.port = data["port"] if data else None
        self.token = data["token"] if data else None
        self.pid = data["pid"] if data else None
        self.spark_version = data["spark_version"] if data else None

    @property
    def url(self) -> str:
        assert self.host is not None and self.port is not None
        return "sc://{}:{}".format(self.host, self.port)

    def is_listening(self) -> bool:
        if self.host is None or self.port is None:
            return False
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            return sock.connect_ex((self.host, self.port)) == 0

    def is_reusable(self) -> bool:
        from pyspark.version import __version__

        if self.spark_version != __version__ or self.pid is None:
            return False
        if os.name == "posix" and not _pid_alive(self.pid):
            return False
        return self.is_listening()

    def reuse_or_start(self, master: str, opts: Dict[str, Any]) -> str:
        if not self.is_reusable():
            ServerLauncher(master, opts, self._discovery).launch()
            self._reload()
        assert self.token is not None
        os.environ["SPARK_CONNECT_AUTHENTICATE_TOKEN"] = self.token
        return self.url

    def stop(self) -> bool:
        stopped = False
        if self.pid is not None:
            try:
                os.kill(self.pid, signal.SIGTERM)
                stopped = True
            except OSError:
                pass
        self._discovery.clear()
        return stopped


class ServerLauncher:
    """Starts a persistent local server via ``sbin/start-connect-server.sh`` and waits until
    it accepts connections. Callers must hold a ``Discovery`` context.
    """

    _READY_TIMEOUT = 120

    def __init__(self, master: str, opts: Dict[str, Any], discovery: Discovery):
        self._master = master
        self._opts = opts
        self._discovery = discovery
        self._directory = discovery.directory
        self._log_dir = os.path.join(self._directory, "logs")
        self._pid_file = discovery.daemon_pid_path

    def launch(self) -> None:
        os.makedirs(self._directory, exist_ok=True)
        token = self._token()
        port = self._pick_port()
        conf_file = self._write_seed_properties()
        try:
            self._run_script(port, token, conf_file)
            self._await_ready(port, token)
        finally:
            if conf_file is not None:
                with contextlib.suppress(OSError):
                    os.remove(conf_file)

    def _token(self) -> str:
        # Same precedence as the in-process _start_connect_server: explicit env token, then
        # conf, then a fresh one. Passed via the environment so it never shows up in `ps`.
        return (
            os.environ.get("SPARK_CONNECT_AUTHENTICATE_TOKEN")
            or self._opts.get("spark.connect.authenticate.token")
            or str(uuid.uuid4())
        )

    def _pick_port(self) -> int:
        """Under SPARK_TESTING always use an OS-assigned free port so suites can run in
        parallel; otherwise honor the configured/default port, falling back to a free one if
        it is taken (e.g. by a stale server just rejected on version mismatch). The sbin
        script cannot report an ephemeral port back, so the free port is picked and released
        here, with a small race until the server binds it.
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
        """Startup confs for the new server, merged like the in-process
        ``_start_connect_server`` merges ``PYSPARK_REMOTE_INIT_CONF_*`` with the builder
        opts, minus the keys the launcher sets itself and the ``spark.local.connect.*``
        opt-in keys. Only the run that starts the server can seed static confs; later runs
        find the JVM already warm.
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
        # Written with 0600 perms since confs may hold sensitive values; a --properties-file
        # keeps them off the server's argv where they would show up in `ps`.
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
        try:
            with open(self._pid_file, "r") as f:
                return int(f.read().strip())
        except (OSError, ValueError):
            return None

    def _run_script(self, port: int, token: str, conf_file: Optional[str]) -> None:
        from pyspark.find_spark_home import _find_spark_home

        spark_home = os.environ.get("SPARK_HOME") or _find_spark_home()
        script = os.path.join(spark_home, "sbin", "start-connect-server.sh")
        if not os.path.isfile(script):
            raise PySparkRuntimeError(
                errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
                messageParameters={"reason": "cannot find {}".format(script)},
            )

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
                raise PySparkRuntimeError(
                    errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
                    messageParameters={
                        "reason": "a local Connect server that is not reusable by this client is "
                        "already running (pid {}); stop it with "
                        "`python -m pyspark.sql.connect.local_server --stop`".format(stale_pid)
                    },
                )
            output = (result.stderr or "") + (result.stdout or "")
            last_line = output.strip().splitlines()[-1] if output.strip() else ""
            raise PySparkRuntimeError(
                errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
                messageParameters={
                    "reason": "start-connect-server.sh exited with code {}: {}".format(
                        result.returncode, last_line
                    )
                },
            )

    def _await_ready(self, port: int, token: str) -> None:
        from pyspark.version import __version__

        deadline = time.time() + self._READY_TIMEOUT
        while time.time() < deadline:
            pid = self._daemon_pid()
            if pid is not None:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(0.5)
                    listening = sock.connect_ex(("localhost", port)) == 0
                if listening:
                    self._discovery.save(
                        {
                            "host": "localhost",
                            "port": port,
                            "token": token,
                            "pid": pid,
                            "spark_version": __version__,
                        }
                    )
                    return
                if not _pid_alive(pid):
                    raise PySparkRuntimeError(
                        errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
                        messageParameters={
                            "reason": "the server exited during start-up; see logs under {}".format(
                                self._log_dir
                            )
                        },
                    )
            time.sleep(0.25)

        pid = self._daemon_pid()
        if pid is not None:
            with contextlib.suppress(OSError):
                os.kill(pid, signal.SIGTERM)
        raise PySparkRuntimeError(
            errorClass="LOCAL_CONNECT_SERVER_START_FAILED",
            messageParameters={
                "reason": "the server did not become ready within {} seconds; "
                "see logs under {}".format(self._READY_TIMEOUT, self._log_dir)
            },
        )


def reuse_or_start_local_connect_server(master: str, opts: Dict[str, Any]) -> str:
    """Reuse a running persistent local Connect server, or start one if none is reusable.

    Returns the ``sc://host:port`` endpoint and sets ``SPARK_CONNECT_AUTHENTICATE_TOKEN`` so
    the client authenticates against that server. Only reached for a ``local`` master when
    the reuse opt-in is set; see ``SparkSession.getOrCreate`` in ``pyspark.sql.session``.
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
    with Discovery() as discovery:
        return LocalConnectServer(discovery).reuse_or_start(master, opts)


def stop_local_connect_server() -> bool:
    """Stop the recorded persistent local Connect server, if any; safe to call when none is
    running. Also available as ``python -m pyspark.sql.connect.local_server --stop``.
    """
    with Discovery() as discovery:
        return LocalConnectServer(discovery).stop()


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
