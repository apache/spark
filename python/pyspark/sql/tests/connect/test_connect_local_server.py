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
import getpass
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

from pyspark.testing.connectutils import connect_requirement_message, should_test_connect
from pyspark.util import is_remote_only

if should_test_connect:
    from pyspark.sql import SparkSession as PySparkSession
    from pyspark.sql.connect import local_server
    from pyspark.sql.connect.local_server import Discovery, LocalConnectServer
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
    from pyspark.version import __version__


@contextlib.contextmanager
def _listening_socket():
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
        # Point discovery at a throwaway path so the real per-user file is never touched.
        self._tmpdir = tempfile.mkdtemp()
        self._discovery_path = os.path.join(self._tmpdir, "connect-local.json")
        self._saved_env = {
            k: os.environ.get(k)
            for k in ("SPARK_LOCAL_CONNECT_DISCOVERY", "SPARK_CONNECT_AUTHENTICATE_TOKEN")
        }
        os.environ["SPARK_LOCAL_CONNECT_DISCOVERY"] = self._discovery_path

    def tearDown(self) -> None:
        try:
            # Only stop a real, separately-spawned server. Several tests fabricate discovery
            # files pointing at this very process, which must never be signalled.
            server = self._discovered_server()
            if server.pid is not None and server.pid != os.getpid():
                local_server.stop_local_connect_server()
                # Wait for the JVM to release the port so the next test starts clean.
                self._wait_port_closed(server.host, server.port)
        finally:
            for k, v in self._saved_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _server(self, **overrides) -> "LocalConnectServer":
        from unittest import mock

        fields = {
            "host": "localhost",
            "port": 0,
            "token": "t",
            "pid": os.getpid(),
            "spark_version": __version__,
        }
        fields.update(overrides)
        discovery = mock.Mock()
        discovery.load.return_value = fields
        return LocalConnectServer(discovery)

    def _discovered_server(self) -> "LocalConnectServer":
        with Discovery() as discovery:
            return LocalConnectServer(discovery)

    def _launcher_discovery(self):
        # A stand-in Discovery for ServerLauncher unit tests: only its directory is read
        # (for the log dir and the seed properties file), so point it at the temp dir.
        from unittest import mock

        discovery = mock.Mock()
        discovery.directory = self._tmpdir
        return discovery

    @contextlib.contextmanager
    def _without_spark_testing(self):
        # _pick_port's ephemeral branch is a no-op when SPARK_TESTING is set (as it is under
        # the test runner), so drop it to exercise the production behavior.
        saved = os.environ.pop("SPARK_TESTING", None)
        try:
            yield
        finally:
            if saved is not None:
                os.environ["SPARK_TESTING"] = saved

    def test_discovery_location(self) -> None:
        self.assertEqual(Discovery().path, self._discovery_path)
        # Without the override the file lives in a per-user 0700 dir under the temp dir.
        os.environ.pop("SPARK_LOCAL_CONNECT_DISCOVERY")
        default = Discovery()
        self.assertTrue(default.directory.startswith(tempfile.gettempdir()))
        if os.name == "posix":
            self.assertIn("spark-connect-{}".format(getpass.getuser()), default.directory)
            self.assertEqual(os.stat(default.directory).st_mode & 0o777, 0o700)

    def test_startup_seed_conf(self) -> None:
        from unittest import mock

        initial = {
            "spark.sql.shuffle.partitions": "8",
            "spark.master": "local[1]",
        }
        opts = {
            "spark.sql.warehouse.dir": os.path.join(self._tmpdir, "warehouse"),
            "spark.local.connect.reuse": "true",
            "spark.connect.grpc.binding.port": "0",
        }
        env = {
            "PYSPARK_REMOTE_INIT_CONF_LEN": "1",
            "PYSPARK_REMOTE_INIT_CONF_0": json.dumps(initial),
        }
        with mock.patch.dict(os.environ, env):
            self.assertEqual(
                local_server.startup_seed_conf(opts),
                {
                    "spark.sql.shuffle.partitions": "8",
                    "spark.sql.warehouse.dir": opts["spark.sql.warehouse.dir"],
                },
            )

    def test_start_delegates_launch_options(self) -> None:
        from unittest import mock

        discovery = mock.Mock()
        discovery.load.side_effect = [
            None,
            {
                "host": "localhost",
                "port": 15002,
                "token": "t",
                "pid": os.getpid(),
                "spark_version": __version__,
            },
        ]
        server = LocalConnectServer(discovery)
        seed_conf = {"spark.sql.shuffle.partitions": "4"}
        with mock.patch.object(local_server, "ServerLauncher") as launcher:
            server.start(
                "local[2]",
                {"spark.local.connect.reuse": "true"},
                use_ephemeral_port=True,
                seed_conf=seed_conf,
            )

        launcher.assert_called_once_with(
            "local[2]",
            {"spark.local.connect.reuse": "true"},
            discovery,
            use_ephemeral_port=True,
            seed_conf=seed_conf,
        )
        launcher.return_value.launch.assert_called_once_with()
        self.assertEqual(server.port, 15002)

    def test_pick_port_uses_ephemeral_port_when_requested(self) -> None:
        # This is the production path for pool attendants, which run without SPARK_TESTING.
        # A non-integer configured port would raise int() in the configured/default branch;
        # the ephemeral branch never reads it, so returning a clean OS-assigned port proves
        # the free-port path was taken even with SPARK_TESTING unset.
        launcher = local_server.ServerLauncher(
            "local[2]",
            {"spark.local.connect.server.port": "not-a-port"},
            self._launcher_discovery(),
            use_ephemeral_port=True,
        )
        with self._without_spark_testing():
            port = launcher._pick_port()
        self.assertGreater(port, 0)

    def test_pick_port_honors_configured_port_without_testing(self) -> None:
        # With neither the ephemeral flag nor SPARK_TESTING, a free configured port is used
        # as-is rather than replaced by an OS-assigned one.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("localhost", 0))
            free = sock.getsockname()[1]
        launcher = local_server.ServerLauncher(
            "local[2]",
            {"spark.local.connect.server.port": str(free)},
            self._launcher_discovery(),
            use_ephemeral_port=False,
        )
        with self._without_spark_testing():
            self.assertEqual(launcher._pick_port(), free)

    def test_seed_conf_override_is_used_and_sanitized(self) -> None:
        # The override is taken verbatim except for launcher-managed keys, which are stripped
        # so raw builder opts passed as a seed cannot land in --properties-file.
        launcher = local_server.ServerLauncher(
            "local[2]",
            {},
            self._launcher_discovery(),
            seed_conf={
                "spark.sql.shuffle.partitions": "4",
                "spark.master": "local[9]",
                "spark.local.connect.reuse": "true",
            },
        )
        self.assertEqual(launcher._seed_conf(), {"spark.sql.shuffle.partitions": "4"})

    def test_seed_conf_empty_override_does_not_fall_through_to_env(self) -> None:
        from unittest import mock

        # Load-bearing for the pool attendant: an empty override means "seed nothing", and
        # must not silently pick up PYSPARK_REMOTE_INIT_CONF_* the way opts=None would.
        env = {
            "PYSPARK_REMOTE_INIT_CONF_LEN": "1",
            "PYSPARK_REMOTE_INIT_CONF_0": json.dumps({"spark.sql.shuffle.partitions": "8"}),
        }
        launcher = local_server.ServerLauncher(
            "local[2]", {}, self._launcher_discovery(), seed_conf={}
        )
        with mock.patch.dict(os.environ, env):
            self.assertEqual(launcher._seed_conf(), {})

    def test_seed_conf_none_override_merges_env_and_opts(self) -> None:
        from unittest import mock

        # No override: the env-plus-opts merge (minus launcher-managed keys) is used.
        env = {
            "PYSPARK_REMOTE_INIT_CONF_LEN": "1",
            "PYSPARK_REMOTE_INIT_CONF_0": json.dumps({"spark.sql.shuffle.partitions": "8"}),
        }
        launcher = local_server.ServerLauncher(
            "local[2]",
            {"spark.sql.warehouse.dir": os.path.join(self._tmpdir, "wh")},
            self._launcher_discovery(),
            seed_conf=None,
        )
        with mock.patch.dict(os.environ, env):
            self.assertEqual(
                launcher._seed_conf(),
                {
                    "spark.sql.shuffle.partitions": "8",
                    "spark.sql.warehouse.dir": os.path.join(self._tmpdir, "wh"),
                },
            )

    def test_seed_properties_file_reflects_seed_conf(self) -> None:
        # An empty seed yields no properties file, so start-connect-server.sh gets no
        # --properties-file; a non-empty seed writes a 0600 file with the seeded confs.
        launcher = local_server.ServerLauncher(
            "local[2]", {}, self._launcher_discovery(), seed_conf={}
        )
        with launcher._seed_properties_file() as path:
            self.assertIsNone(path)

        launcher = local_server.ServerLauncher(
            "local[2]",
            {},
            self._launcher_discovery(),
            seed_conf={"spark.sql.shuffle.partitions": "4"},
        )
        with launcher._seed_properties_file() as path:
            self.assertIsNotNone(path)
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
            with open(path) as f:
                contents = f.read()
        self.assertIn("spark.sql.shuffle.partitions=4", contents)

    def test_discovery_roundtrip(self) -> None:
        with Discovery() as discovery:
            saved = self._server(port=15002)
            discovery.save(
                {k: getattr(saved, k) for k in ("host", "port", "token", "pid", "spark_version")}
            )
            # The file holds the auth token and must not be readable by other users.
            self.assertEqual(os.stat(discovery.path).st_mode & 0o777, 0o600)
            loaded = LocalConnectServer(discovery)
            for attr in ("host", "port", "token", "pid", "spark_version", "url"):
                self.assertEqual(getattr(loaded, attr), getattr(saved, attr), attr)
            discovery.clear()
            self.assertIsNone(discovery.load())
            discovery.clear()  # clearing again is a no-op

    def test_discovery_load_rejects_malformed_files(self) -> None:
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
        with Discovery() as discovery:
            for content in malformed:
                with self.subTest(content=content):
                    with open(discovery.path, "w") as f:
                        f.write(content)
                    self.assertIsNone(discovery.load())

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
        # On Windows os.kill(pid, 0) terminates the target instead of probing it, so the
        # reuse check would kill the very server it is examining.
        from unittest import mock

        with _listening_socket() as port:
            server = self._server(port=port)
            with mock.patch.object(os, "name", "nt"), mock.patch.object(os, "kill") as kill:
                self.assertTrue(server.is_reusable())
                kill.assert_not_called()

    def test_stop_when_no_server_is_safe(self) -> None:
        self.assertFalse(local_server.stop_local_connect_server())

    def test_stop_signals_recorded_server_and_clears_discovery(self) -> None:
        from unittest import mock

        with Discovery() as discovery:
            server = self._server(pid=12345)
            discovery.save(
                {k: getattr(server, k) for k in ("host", "port", "token", "pid", "spark_version")}
            )
        # Avoid inspecting or signaling a real process while exercising the stop path.
        ps_result = subprocess.CompletedProcess([], 0, stdout=local_server._SERVER_CLASS)
        with (
            mock.patch.object(subprocess, "run", return_value=ps_result) as run,
            mock.patch.object(os, "kill") as kill,
        ):
            self.assertTrue(local_server.stop_local_connect_server())
        run.assert_called_once_with(
            ["ps", "-ww", "-p", "12345", "-o", "command="],
            capture_output=True,
            text=True,
            timeout=5,
        )
        kill.assert_called_once_with(12345, signal.SIGTERM)
        self.assertIsNone(self._discovered_server().pid)

    def test_stop_does_not_signal_reused_pid(self) -> None:
        from unittest import mock

        with Discovery() as discovery:
            server = self._server(pid=12345)
            discovery.save(
                {k: getattr(server, k) for k in ("host", "port", "token", "pid", "spark_version")}
            )
        # Model a recycled pid without depending on host process state.
        ps_result = subprocess.CompletedProcess([], 0, stdout="unrelated process")
        with (
            mock.patch.object(subprocess, "run", return_value=ps_result),
            mock.patch.object(os, "kill") as kill,
        ):
            self.assertFalse(local_server.stop_local_connect_server())
        kill.assert_not_called()
        self.assertIsNone(self._discovered_server().pid)

    def test_stop_preserves_discovery_when_process_cannot_be_inspected(self) -> None:
        from unittest import mock

        with Discovery() as discovery:
            server = self._server(pid=12345)
            discovery.save(
                {k: getattr(server, k) for k in ("host", "port", "token", "pid", "spark_version")}
            )
        with (
            mock.patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired("ps", 5)),
            mock.patch.object(os, "kill") as kill,
        ):
            self.assertIsNone(local_server.stop_local_connect_server())
        kill.assert_not_called()
        self.assertEqual(self._discovered_server().pid, 12345)
        with Discovery() as discovery:
            discovery.clear()

    def test_server_launcher_binds_to_loopback(self) -> None:
        from unittest import mock

        with Discovery() as discovery:
            launcher = local_server.ServerLauncher("local[2]", {}, discovery)
            # Capture the launcher argv without starting an external daemon.
            with (
                mock.patch.dict(os.environ, {"SPARK_HOME": self._tmpdir}),
                mock.patch.object(os.path, "isfile", return_value=True) as isfile,
                mock.patch.object(
                    subprocess, "run", return_value=subprocess.CompletedProcess([], 0)
                ) as run,
            ):
                launcher._run_script(15002, "token", None)
        isfile.assert_called_once_with(
            os.path.join(self._tmpdir, "sbin", "start-connect-server.sh")
        )
        self.assertIn("spark.connect.grpc.binding.address=127.0.0.1", run.call_args.args[0])

    def test_stop_cli_reports_when_no_server(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "pyspark.sql.connect.local_server", "--stop"],
            env=dict(os.environ),
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("No running persistent local Spark Connect server", result.stdout)

    def test_stop_cli_fails_when_process_cannot_be_inspected(self) -> None:
        from unittest import mock

        with (
            mock.patch.object(sys, "argv", ["local_server", "--stop"]),
            mock.patch.object(local_server, "stop_local_connect_server", return_value=None),
            self.assertRaises(SystemExit) as raised,
        ):
            local_server.main()
        self.assertEqual(raised.exception.code, 1)

    def test_reuse_or_start_requires_posix(self) -> None:
        from unittest import mock

        from pyspark.errors import PySparkRuntimeError

        with mock.patch.object(os, "name", "nt"):
            with self.assertRaises(PySparkRuntimeError) as ctx:
                local_server.reuse_or_start_local_connect_server("local[2]", {})
        self.assertIn("POSIX", str(ctx.exception))

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

            server = self._discovered_server()
            self.assertIsNotNone(server.pid)
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
        endpoint = local_server.reuse_or_start_local_connect_server("local[2]", {})
        self.assertTrue(endpoint.startswith("sc://localhost:"))

        server = self._discovered_server()
        self.assertIsNotNone(server.pid)
        self.assertEqual(server.url, endpoint)
        self.assertEqual(server.spark_version, __version__)
        self.assertEqual(os.environ.get("SPARK_CONNECT_AUTHENTICATE_TOKEN"), server.token)
        first_pid = server.pid

        s1 = s2 = None
        try:
            # A second call reuses the running server instead of spawning a new one.
            endpoint2 = local_server.reuse_or_start_local_connect_server("local[2]", {})
            self.assertEqual(endpoint2, endpoint)
            self.assertEqual(self._discovered_server().pid, first_pid)

            s1 = RemoteSparkSession.builder.remote(endpoint).create()
            s2 = RemoteSparkSession.builder.remote(endpoint).create()
            self.assertEqual(s1.range(5).count(), 5)
            self.assertEqual(s2.range(3).count(), 3)

            # Session-local state must not leak across connections.
            s1.range(1).createOrReplaceTempView("only_in_s1")
            self.assertIn("only_in_s1", [t.name for t in s1.catalog.listTables()])
            self.assertNotIn("only_in_s1", [t.name for t in s2.catalog.listTables()])
        finally:
            if s1 is not None:
                self._release(s1)
            if s2 is not None:
                self._release(s2)

        self.assertTrue(local_server.stop_local_connect_server())
        self.assertIsNone(self._discovered_server().pid)
        # Check the port rather than the pid, which can linger while the JVM shuts down.
        self.assertTrue(
            self._wait_port_closed(server.host, server.port),
            "server port {} still open after stop".format(server.port),
        )

    @unittest.skipUnless(os.name == "posix", "the reuse path relies on the POSIX sbin scripts")
    def test_start_seeds_static_conf_on_the_server(self) -> None:
        # spark.local.connect.* and spark.master must be stripped from the seed, not
        # forwarded; startup succeeding with them present covers that.
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
            # A static conf cannot be set per-session after the JVM is up, so seeing it here
            # proves the seed reached the server's SparkConf.
            self.assertTrue(spark.conf.get("spark.sql.warehouse.dir").endswith(warehouse))
        finally:
            if spark is not None:
                self._release(spark)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
