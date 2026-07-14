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

import contextlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

from pyspark.util import is_remote_only
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

if should_test_connect:
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.sql.connect import local_server
    from pyspark.sql.connect.local_server import Discovery, LocalConnectServer
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
    from pyspark.version import __version__


@contextlib.contextmanager
def _listening_socket():
    """A live localhost listener; yields its port."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        listener.bind(("localhost", 0))
        listener.listen(1)
        yield listener.getsockname()[1]
    finally:
        listener.close()


@unittest.skipIf(
    not should_test_connect or is_remote_only(),
    connect_requirement_message or "Requires JVM access to start a local Connect server",
)
class LocalConnectServerReuseTests(unittest.TestCase):
    """Tests for the opt-in persistent local Spark Connect server (SPARK_LOCAL_CONNECT_REUSE)."""

    def setUp(self) -> None:
        # Point discovery at a throwaway path and remember the env we override, so each test starts
        # from a clean slate and the real per-user discovery file is never touched.
        self._tmpdir = tempfile.mkdtemp()
        self._discovery_path = os.path.join(self._tmpdir, "connect-local.json")
        self._saved_env = {
            k: os.environ.get(k)
            for k in ("SPARK_LOCAL_CONNECT_DISCOVERY", "SPARK_CONNECT_AUTHENTICATE_TOKEN")
        }
        os.environ["SPARK_LOCAL_CONNECT_DISCOVERY"] = self._discovery_path

    def tearDown(self) -> None:
        try:
            # Only stop a real, separately-spawned server. The discovery-logic unit tests fabricate
            # discovery files that point at this very process, which must never be signalled.
            server = Discovery().load()
            if server is not None and server.pid != os.getpid():
                local_server.stop_local_connect_server()
                # stop_local_connect_server only signals the server and returns; wait for the JVM
                # to actually release the port so the next test starts from a clean slate.
                self._wait_port_closed(server.host, server.port)
        finally:
            for k, v in self._saved_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            # Remove the whole scratch dir: besides the discovery file it may hold a .lock file,
            # seed-conf temp files, and a seeded warehouse directory.
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _server(self, **overrides) -> "LocalConnectServer":
        fields = {
            "host": "localhost",
            "port": 0,
            "token": "t",
            "pid": os.getpid(),
            "spark_version": __version__,
        }
        fields.update(overrides)
        return LocalConnectServer(**fields)

    # -- Discovery: location, save/load roundtrip, malformed input --------------------------------

    def test_discovery_location(self) -> None:
        # The env override wins; without it the file lives in a per-user 0700 directory under
        # the system temp dir (under run-tests TMPDIR itself is a per-run scratch dir), never
        # in a permanent location like the home directory.
        self.assertEqual(Discovery().path, self._discovery_path)
        os.environ.pop("SPARK_LOCAL_CONNECT_DISCOVERY")
        default = Discovery()
        self.assertTrue(default.directory.startswith(tempfile.gettempdir()))
        if os.name == "posix":
            self.assertIn("spark-connect-{}".format(os.getuid()), default.directory)
            self.assertEqual(os.stat(default.directory).st_mode & 0o777, 0o700)

    def test_discovery_roundtrip(self) -> None:
        discovery = Discovery()
        saved = self._server(port=15002)
        discovery.save(saved)
        # The file holds the auth token, so it must not be readable by other users.
        self.assertEqual(os.stat(discovery.path).st_mode & 0o777, 0o600)
        loaded = discovery.load()
        for attr in ("host", "port", "token", "pid", "spark_version", "url"):
            self.assertEqual(getattr(loaded, attr), getattr(saved, attr), attr)
        discovery.clear()
        self.assertIsNone(discovery.load())
        discovery.clear()  # clearing again is a no-op

    def test_discovery_load_rejects_malformed_files(self) -> None:
        discovery = Discovery()
        malformed = [
            "not json",
            json.dumps(["a", "list"]),
            json.dumps({"host": "localhost"}),  # missing required keys
            json.dumps(
                {
                    "host": "localhost",
                    "port": 1,
                    "token": "t",
                    "pid": "not-a-pid",
                    "spark_version": __version__,
                }
            ),
            json.dumps(
                {"host": None, "port": 1, "token": "t", "pid": 1, "spark_version": __version__}
            ),
        ]
        for content in malformed:
            with self.subTest(content=content):
                with open(discovery.path, "w") as f:
                    f.write(content)
                self.assertIsNone(discovery.load())

    # -- LocalConnectServer: reusability and stop --------------------------------------------------

    def test_server_is_reusable(self) -> None:
        with _listening_socket() as port:
            with self.subTest("alive process listening on the port with a matching version"):
                self.assertTrue(self._server(port=port).is_reusable())
            with self.subTest("version mismatch"):
                self.assertFalse(
                    self._server(port=port, spark_version="0.0.0-not-this-build").is_reusable()
                )
            if os.name == "posix":  # the pid probe only runs on POSIX (see the test below)
                with self.subTest("dead pid"):
                    # PID 2**31 - 1 is effectively guaranteed not to exist.
                    self.assertFalse(self._server(port=port, pid=2**31 - 1).is_reusable())
            server = self._server(port=port)
        with self.subTest("port no longer listening"):
            self.assertFalse(server.is_reusable())

    def test_pid_probe_is_skipped_on_windows(self) -> None:
        """The pid liveness probe must never run on Windows.

        There ``os.kill(pid, 0)`` does not probe the process -- it unconditionally *terminates*
        it via TerminateProcess -- so the reuse check would kill the very server it is examining.
        """
        from unittest import mock

        with _listening_socket() as port:
            server = self._server(port=port)
            with mock.patch.object(os, "name", "nt"), mock.patch.object(os, "kill") as kill:
                self.assertTrue(server.is_reusable())
                kill.assert_not_called()

    # -- stopping: safe no-op, signals the recorded server, CLI ------------------------------------

    def test_stop_when_no_server_is_safe(self) -> None:
        self.assertFalse(local_server.stop_local_connect_server())

    def test_stop_signals_recorded_server_and_clears_discovery(self) -> None:
        from unittest import mock

        Discovery().save(self._server(pid=12345))
        with mock.patch.object(os, "kill") as kill:
            self.assertTrue(local_server.stop_local_connect_server())
        kill.assert_called_once_with(12345, signal.SIGTERM)
        self.assertIsNone(Discovery().load())

    def test_stop_cli_reports_when_no_server(self) -> None:
        """`python -m pyspark.sql.connect.local_server --stop` is safe with nothing running."""
        result = subprocess.run(
            [sys.executable, "-m", "pyspark.sql.connect.local_server", "--stop"],
            env=dict(os.environ),
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("No running persistent local Spark Connect server", result.stdout)

    def test_reuse_or_start_requires_posix(self) -> None:
        """The reuse path depends on the sbin shell scripts and must refuse to run elsewhere."""
        from unittest import mock

        from pyspark.errors import PySparkRuntimeError

        with mock.patch.object(os, "name", "nt"):
            with self.assertRaises(PySparkRuntimeError) as ctx:
                local_server.reuse_or_start_local_connect_server("local[2]", {})
        self.assertIn("POSIX", str(ctx.exception))

    # -- end-to-end: start a real server via sbin scripts, reconnect, verify isolation ------------

    def _release(self, session) -> None:
        """Close one client session without stopping the shared server."""
        try:
            session.client.release_session()
        except Exception:
            pass
        try:
            session.client.close()
        except Exception:
            pass

    def _wait_port_closed(self, host, port, timeout=30) -> bool:
        """Wait for ``host:port`` to stop accepting connections; return True if it closed."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.5)
                if sock.connect_ex((host, int(port))) != 0:
                    return True
            time.sleep(0.5)
        return False

    @unittest.skipUnless(os.name == "posix", "the reuse path relies on the POSIX sbin scripts")
    def test_builder_remote_local_uses_reuse_flag(self) -> None:
        spark = None
        try:
            spark = (
                PySparkSession.builder.remote("local[2]")
                .config("spark.local.connect.reuse", "true")
                .getOrCreate()
            )
            self.assertEqual(spark.range(2).count(), 2)

            server = Discovery().load()
            self.assertIsNotNone(server)
            self.assertEqual(server.spark_version, __version__)
            self.assertNotEqual(server.pid, os.getpid())
        finally:
            if spark is not None:
                spark.stop()

    @unittest.skipUnless(os.name == "posix", "the reuse path relies on the POSIX sbin scripts")
    def test_concurrent_startup_reuses_one_server(self) -> None:
        script = textwrap.dedent("""
            import json
            import os

            from pyspark.sql import SparkSession

            spark = (
                SparkSession.builder.remote("local[2]")
                .config("spark.local.connect.reuse", "true")
                .getOrCreate()
            )
            try:
                count = spark.range(1).count()
                with open(os.environ["SPARK_LOCAL_CONNECT_DISCOVERY"], "r") as f:
                    disc = json.load(f)
                print(json.dumps({"count": count, "pid": disc["pid"], "port": disc["port"]}))
            finally:
                spark.stop()
            """)
        env = dict(os.environ)
        env["SPARK_LOCAL_CONNECT_DISCOVERY"] = self._discovery_path
        env["SPARK_LOCAL_CONNECT_REUSE"] = "1"

        procs = [
            subprocess.Popen(
                [sys.executable, "-c", script],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for _ in range(3)
        ]
        outputs = []
        try:
            for proc in procs:
                stdout, stderr = proc.communicate(timeout=180)
                self.assertEqual(proc.returncode, 0, stderr)
                lines = stdout.strip().splitlines()
                self.assertTrue(lines, stderr)
                outputs.append(json.loads(lines[-1]))
        finally:
            for proc in procs:
                if proc.poll() is None:
                    proc.kill()
                    proc.communicate()

        self.assertEqual({o["count"] for o in outputs}, {1})
        self.assertEqual(len({o["pid"] for o in outputs}), 1)
        self.assertEqual(len({o["port"] for o in outputs}), 1)

    @unittest.skipUnless(os.name == "posix", "the reuse path relies on the POSIX sbin scripts")
    def test_start_reuse_and_session_isolation(self) -> None:
        # First call starts a persistent server via sbin scripts and records it in the discovery
        # file.
        endpoint = local_server.reuse_or_start_local_connect_server("local[2]", {})
        self.assertTrue(endpoint.startswith("sc://localhost:"))

        server = Discovery().load()
        self.assertIsNotNone(server)
        self.assertEqual(server.url, endpoint)
        self.assertEqual(server.spark_version, __version__)
        self.assertEqual(os.environ.get("SPARK_CONNECT_AUTHENTICATE_TOKEN"), server.token)
        first_pid = server.pid

        s1 = s2 = None
        try:
            # A second call reuses the running server: same endpoint, no new process spawned.
            endpoint2 = local_server.reuse_or_start_local_connect_server("local[2]", {})
            self.assertEqual(endpoint2, endpoint)
            self.assertEqual(Discovery().load().pid, first_pid)

            # Two independent client connections to the same server run real queries...
            s1 = RemoteSparkSession.builder.remote(endpoint).create()
            s2 = RemoteSparkSession.builder.remote(endpoint).create()
            self.assertEqual(s1.range(5).count(), 5)
            self.assertEqual(s2.range(3).count(), 3)

            # ...and session-local state (a temp view) does not leak across connections.
            s1.range(1).createOrReplaceTempView("only_in_s1")
            self.assertIn("only_in_s1", [t.name for t in s1.catalog.listTables()])
            self.assertNotIn("only_in_s1", [t.name for t in s2.catalog.listTables()])
        finally:
            if s1 is not None:
                self._release(s1)
            if s2 is not None:
                self._release(s2)

        # Stopping signals the server and removes the discovery file.
        self.assertTrue(local_server.stop_local_connect_server())
        self.assertIsNone(Discovery().load())
        # The server should stop accepting connections shortly afterwards. (We check the port
        # rather than the pid, which can linger briefly while the JVM shuts down.)
        self.assertTrue(
            self._wait_port_closed(server.host, server.port),
            "server port {} still open after stop".format(server.port),
        )

    @unittest.skipUnless(os.name == "posix", "the reuse path relies on the POSIX sbin scripts")
    def test_start_seeds_static_conf_on_the_server(self) -> None:
        """A start-up conf passed by the first caller reaches the server's SparkConf.

        The opts also carry keys the launcher must strip rather than forward (the reuse opt-in
        itself and the master); startup succeeding with them present covers that filtering.
        """
        warehouse = os.path.join(self._tmpdir, "seeded-wh")
        opts = {
            "spark.sql.warehouse.dir": warehouse,
            "spark.local.connect.reuse": "true",
            "spark.master": "local[2]",
        }
        endpoint = local_server.reuse_or_start_local_connect_server("local[2]", opts)
        spark = None
        try:
            spark = RemoteSparkSession.builder.remote(endpoint).create()
            # The per-session apply path cannot set this static conf after the JVM is running.
            self.assertTrue(spark.conf.get("spark.sql.warehouse.dir").endswith(warehouse))
        finally:
            if spark is not None:
                self._release(spark)
            # tearDown stops the server and waits for the port to close.


if __name__ == "__main__":
    from pyspark.testing import main

    main()
