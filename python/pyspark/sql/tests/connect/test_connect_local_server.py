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
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
    from pyspark.version import __version__


@unittest.skipIf(
    not should_test_connect or is_remote_only(),
    connect_requirement_message or "Requires JVM access to start a local Connect server",
)
class LocalConnectServerReuseTests(unittest.TestCase):
    """Tests for the opt-in persistent local Spark Connect server (SPARK_LOCAL_CONNECT_REUSE)."""

    def setUp(self) -> None:
        # Point discovery at a throwaway path and remember the env we override, so each test starts
        # from a clean slate and the real ~/.spark/connect-local.json is never touched.
        self._tmpdir = tempfile.mkdtemp()
        self._discovery = os.path.join(self._tmpdir, "connect-local.json")
        self._saved_env = {
            k: os.environ.get(k)
            for k in ("SPARK_LOCAL_CONNECT_DISCOVERY", "SPARK_CONNECT_AUTHENTICATE_TOKEN")
        }
        os.environ["SPARK_LOCAL_CONNECT_DISCOVERY"] = self._discovery

    def tearDown(self) -> None:
        try:
            # Only stop a real, separately-spawned server. The discovery-logic unit tests fabricate
            # discovery files that point at this very process, which must never be signalled.
            disc = RemoteSparkSession._read_local_connect_discovery()
            if disc is not None and disc.get("pid") != os.getpid():
                port = int(disc["port"])
                RemoteSparkSession._stop_local_connect_server()
                # _stop_local_connect_server only signals the daemon and returns; wait for the JVM
                # to actually release the port so the next test starts from a clean slate.
                self._wait_port_closed(disc["host"], port)
        finally:
            for k, v in self._saved_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            # Remove the whole scratch dir: besides the discovery file it may hold a .lock file,
            # seed-conf temp files, and a seeded warehouse directory.
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    # -- discovery / reuse-decision logic (no real server) ----------------------------------------

    def test_discovery_path_honors_override(self) -> None:
        self.assertEqual(RemoteSparkSession._local_connect_discovery_path(), self._discovery)
        os.environ.pop("SPARK_LOCAL_CONNECT_DISCOVERY")
        self.assertTrue(
            RemoteSparkSession._local_connect_discovery_path().endswith(
                os.path.join(".spark", "connect-local.json")
            )
        )

    def test_read_discovery_missing_or_malformed(self) -> None:
        self.assertIsNone(RemoteSparkSession._read_local_connect_discovery())
        with open(self._discovery, "w") as f:
            f.write("not json")
        self.assertIsNone(RemoteSparkSession._read_local_connect_discovery())
        with open(self._discovery, "w") as f:
            json.dump({"host": "localhost"}, f)  # missing required keys
        self.assertIsNone(RemoteSparkSession._read_local_connect_discovery())

    def _write_discovery(self, **overrides) -> dict:
        disc = {
            "host": "localhost",
            "port": 0,
            "token": "t",
            "pid": os.getpid(),
            "spark_version": __version__,
        }
        disc.update(overrides)
        with open(self._discovery, "w") as f:
            json.dump(disc, f)
        return disc

    def test_not_reusable_on_version_mismatch(self) -> None:
        disc = self._write_discovery(spark_version="0.0.0-not-this-build")
        self.assertFalse(RemoteSparkSession._local_connect_server_is_reusable(disc))

    def test_not_reusable_on_dead_pid(self) -> None:
        # PID 2**31 - 1 is effectively guaranteed not to exist.
        disc = self._write_discovery(pid=2**31 - 1, port=1)
        self.assertFalse(RemoteSparkSession._local_connect_server_is_reusable(disc))

    def test_reusable_when_alive_and_listening(self) -> None:
        # A live listening socket owned by this (alive) process with a matching version is reusable.
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind(("localhost", 0))
            listener.listen(1)
            port = listener.getsockname()[1]
            disc = self._write_discovery(port=port)
            self.assertTrue(RemoteSparkSession._local_connect_server_is_reusable(disc))
        finally:
            listener.close()
        # Once the socket is closed the port is no longer reachable, so it is not reusable.
        self.assertFalse(RemoteSparkSession._local_connect_server_is_reusable(disc))

    def test_stop_when_no_server_is_safe(self) -> None:
        self.assertFalse(RemoteSparkSession._stop_local_connect_server())

    def test_stop_signals_recorded_server_group(self) -> None:
        self._write_discovery(pid=12345)
        calls = []
        old_signal = RemoteSparkSession._signal_local_connect_server

        def fake_signal(pid, sig):
            calls.append((pid, sig))
            return True

        try:
            RemoteSparkSession._signal_local_connect_server = staticmethod(fake_signal)
            self.assertTrue(RemoteSparkSession._stop_local_connect_server())
            self.assertEqual(calls, [(12345, signal.SIGTERM)])
            self.assertFalse(os.path.exists(self._discovery))
        finally:
            RemoteSparkSession._signal_local_connect_server = old_signal

    @unittest.skipUnless(os.name == "posix", "process groups are POSIX-only")
    def test_signal_group_kills_only_session_leaders(self) -> None:
        """A pid that is not a session leader is signalled alone, never via its group.

        The daemon is always launched as a session leader (its group id equals its pid). A
        recorded pid whose group id differs was recycled by an unrelated process -- for example
        via a stale discovery file -- and group-killing it could take down the caller's own
        process group.
        """
        from unittest import mock

        sleeper = [sys.executable, "-c", "import time; time.sleep(60)"]
        # Same process group as this test (not a leader): must fall back to a plain kill.
        child = subprocess.Popen(sleeper)
        try:
            self.assertNotEqual(os.getpgid(child.pid), child.pid)
            with mock.patch("os.killpg") as killpg, mock.patch("os.kill") as kill:
                self.assertTrue(
                    RemoteSparkSession._signal_local_connect_server(child.pid, signal.SIGTERM)
                )
                killpg.assert_not_called()
                kill.assert_called_once_with(child.pid, signal.SIGTERM)
        finally:
            child.kill()
            child.communicate()
        # A session leader, like the real daemon: the whole group is signalled.
        leader = subprocess.Popen(sleeper, start_new_session=True)
        try:
            self.assertEqual(os.getpgid(leader.pid), leader.pid)
            with mock.patch("os.killpg") as killpg, mock.patch("os.kill") as kill:
                self.assertTrue(
                    RemoteSparkSession._signal_local_connect_server(leader.pid, signal.SIGTERM)
                )
                killpg.assert_called_once_with(leader.pid, signal.SIGTERM)
                kill.assert_not_called()
        finally:
            leader.kill()
            leader.communicate()

    def test_reuse_from_discovery_none_when_absent(self) -> None:
        self.assertIsNone(RemoteSparkSession._reuse_from_discovery())

    def test_local_port_available(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind(("localhost", 0))
            listener.listen(1)
            taken = listener.getsockname()[1]
            self.assertFalse(RemoteSparkSession._local_port_available(taken))
        finally:
            listener.close()
        # The port is free again once the listener is closed.
        self.assertTrue(RemoteSparkSession._local_port_available(taken))

    def test_server_conf_seeds_user_confs_and_drops_control_keys(self) -> None:
        """_local_connect_server_conf keeps user startup confs but not keys the daemon controls."""
        opts = {
            "spark.sql.warehouse.dir": "/tmp/wh",
            "spark.jars.packages": "org.example:lib:1.0",
            "spark.remote": "local[*]",
            "spark.master": "local[*]",
            "spark.connect.authenticate.token": "secret",
            "spark.connect.grpc.binding.port": "15002",
            "spark.local.connect.reuse": "true",
            "spark.local.connect.server.port": "15002",
            "spark.local.connect.server.idleTimeout": "60",
        }
        conf = RemoteSparkSession._local_connect_server_conf(opts)
        self.assertEqual(conf.get("spark.sql.warehouse.dir"), "/tmp/wh")
        self.assertEqual(conf.get("spark.jars.packages"), "org.example:lib:1.0")
        for dropped in (
            "spark.remote",
            "spark.master",
            "spark.connect.authenticate.token",
            "spark.connect.grpc.binding.port",
            "spark.local.connect.reuse",
            "spark.local.connect.server.port",
            "spark.local.connect.server.idleTimeout",
        ):
            self.assertNotIn(dropped, conf)

    def test_start_lock_roundtrip(self) -> None:
        """Acquiring and releasing the start-up lock creates the lock file and does not error."""
        fd = RemoteSparkSession._acquire_local_connect_start_lock()
        try:
            if fd is not None:  # None only where fcntl is unavailable (e.g. Windows)
                self.assertTrue(os.path.exists(self._discovery + ".lock"))
        finally:
            RemoteSparkSession._release_local_connect_start_lock(fd)

    # -- end-to-end: start a real detached server, reconnect to it, verify isolation --------------

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

    def test_builder_remote_local_uses_reuse_flag(self) -> None:
        spark = None
        try:
            spark = (
                PySparkSession.builder.remote("local[2]")
                .config("spark.local.connect.reuse", "true")
                .getOrCreate()
            )
            self.assertEqual(spark.range(2).count(), 2)

            disc = RemoteSparkSession._read_local_connect_discovery()
            self.assertIsNotNone(disc)
            self.assertEqual(disc["spark_version"], __version__)
            self.assertNotEqual(disc["pid"], os.getpid())
        finally:
            if spark is not None:
                spark.stop()

    def test_concurrent_startup_reuses_one_server(self) -> None:
        script = textwrap.dedent(
            """
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
            """
        )
        env = dict(os.environ)
        env["SPARK_LOCAL_CONNECT_DISCOVERY"] = self._discovery
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

    def test_start_reuse_and_session_isolation(self) -> None:
        # First call starts a detached persistent server and records it in the discovery file.
        endpoint = RemoteSparkSession._reuse_or_start_local_connect_server("local[2]", {})
        self.assertTrue(endpoint.startswith("sc://localhost:"))

        disc = RemoteSparkSession._read_local_connect_discovery()
        self.assertIsNotNone(disc)
        self.assertEqual(disc["spark_version"], __version__)
        self.assertEqual(os.environ.get("SPARK_CONNECT_AUTHENTICATE_TOKEN"), disc["token"])
        first_pid = disc["pid"]

        s1 = s2 = None
        try:
            # A second call reuses the running server: same endpoint, no new process spawned.
            endpoint2 = RemoteSparkSession._reuse_or_start_local_connect_server("local[2]", {})
            self.assertEqual(endpoint2, endpoint)
            self.assertEqual(RemoteSparkSession._read_local_connect_discovery()["pid"], first_pid)

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
        self.assertTrue(RemoteSparkSession._stop_local_connect_server())
        self.assertIsNone(RemoteSparkSession._read_local_connect_discovery())
        # The server should stop accepting connections shortly afterwards. (We check the port rather
        # than the pid: the detached server is a child of this long-lived test process, so once it
        # exits it lingers as an unreaped zombie for which os.kill(pid, 0) still succeeds.)
        _, _, hostport = endpoint.partition("sc://")
        host, _, port = hostport.partition(":")
        self.assertTrue(
            self._wait_port_closed(host, port), "server port {} still open after stop".format(port)
        )

    def test_start_seeds_static_conf_on_the_server(self) -> None:
        """A start-up conf passed by the first caller reaches the daemon's SparkConf."""
        warehouse = os.path.join(self._tmpdir, "seeded-wh")
        opts = {"spark.sql.warehouse.dir": warehouse}
        endpoint = RemoteSparkSession._reuse_or_start_local_connect_server("local[2]", opts)
        spark = None
        try:
            spark = RemoteSparkSession.builder.remote(endpoint).create()
            # The per-session apply path cannot set this static conf after the JVM is running.
            self.assertTrue(spark.conf.get("spark.sql.warehouse.dir").endswith(warehouse))
        finally:
            if spark is not None:
                self._release(spark)
            # tearDown stops the server and waits for the port to close.

    @staticmethod
    def _wait_process_group_gone(pgid: int, timeout: float) -> bool:
        """Wait until no process in ``pgid`` can be signalled; True if that happened in time.

        ProcessLookupError means the group is empty. PermissionError also counts as gone: on
        macOS, a dead group member lingering as a zombie (reparented to launchd) yields EPERM
        even though nothing in the group is running.
        """
        deadline = time.time() + timeout
        while True:
            try:
                os.killpg(pgid, 0)
            except (ProcessLookupError, PermissionError):
                return True
            if time.time() >= deadline:
                return False
            time.sleep(0.2)

    @unittest.skipUnless(os.name == "posix", "process-group reaping is POSIX-only")
    def test_terminate_escalates_when_group_outlives_daemon(self) -> None:
        """SIGKILL escalation covers group members that survive the daemon.

        Reproduces the CI failure mode of test_terminate_reaps_daemon_and_jvm: the daemon exits
        on SIGTERM promptly while its JVM is still shutting down (or wedged) past the grace
        period. _terminate_local_connect_server must not return while the group has live members.
        """
        # A session leader that exits on SIGTERM after spawning a SIGTERM-immune child into its
        # process group -- the child stands in for a JVM that outlives the daemon.
        leader_prog = textwrap.dedent(
            """
            import signal
            import subprocess
            import sys
            import time

            child = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    "import signal, time; "
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                    "print('ready', flush=True); "
                    "time.sleep(600)",
                ]
            )
            print("pid:%d" % child.pid, flush=True)
            signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
            time.sleep(600)
            """
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", leader_prog],
            stdout=subprocess.PIPE,
            start_new_session=True,
        )
        pgid = os.getpgid(proc.pid)
        try:
            # One line comes from the leader (the child's pid) and one from the child itself,
            # printed only after it has ignored SIGTERM -- reading both closes the start-up race.
            lines = {proc.stdout.readline().strip() for _ in range(2)}
            self.assertIn(b"ready", lines)
            child_pid = int(next(ln for ln in lines if ln.startswith(b"pid:"))[4:])

            RemoteSparkSession._terminate_local_connect_server(proc)

            self.assertTrue(
                self._wait_process_group_gone(pgid, timeout=10),
                "process group survived _terminate_local_connect_server",
            )
            # The SIGTERM-immune child must be dead, proving the SIGKILL escalation fired.
            # os.kill(pid, 0) still succeeds for a zombie until init/launchd reaps it, so poll
            # briefly rather than asserting on the first probe.
            deadline = time.time() + 10
            child_gone = False
            while time.time() < deadline and not child_gone:
                try:
                    os.kill(child_pid, 0)
                    time.sleep(0.2)
                except OSError:
                    child_gone = True
            self.assertTrue(child_gone, "SIGTERM-immune child survived the SIGKILL escalation")
        finally:
            proc.stdout.close()
            try:
                os.killpg(pgid, signal.SIGKILL)
            except OSError:
                pass

    @unittest.skipUnless(os.name == "posix", "process-group reaping is POSIX-only")
    def test_terminate_reaps_daemon_and_jvm(self) -> None:
        """_terminate_local_connect_server kills the daemon *and* its child JVM.

        The test waits until the daemon has launched the JVM, then checks that the whole process
        group exits.
        """
        if shutil.which("pgrep") is None:
            self.skipTest("pgrep is needed to detect the child JVM")

        import pyspark.sql.connect.session as session_mod

        daemon = os.path.join(os.path.dirname(session_mod.__file__), "local_server.py")
        cmd = [
            sys.executable,
            daemon,
            "--master",
            "local[2]",
            "--port",
            "0",  # ephemeral, so a stray server elsewhere cannot interfere
            "--token",
            "reap-test",
            "--discovery",
            self._discovery,
            "--idle-timeout",
            "3600",
        ]
        env = dict(os.environ)
        for var in ("SPARK_REMOTE", "SPARK_LOCAL_REMOTE", "SPARK_CONNECT_MODE_ENABLED"):
            env.pop(var, None)
        proc = subprocess.Popen(
            cmd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,  # session/group leader, matching the production launch
        )

        def child_pids():
            try:
                out = subprocess.check_output(["pgrep", "-P", str(proc.pid)])
            except subprocess.CalledProcessError:
                return []
            return [int(p) for p in out.split()]

        pgid = os.getpgid(proc.pid)
        try:
            # Wait until the daemon has spawned its child JVM before terminating the group.
            deadline = time.time() + 90
            kids = []
            while time.time() < deadline:
                kids = child_pids()
                if kids:
                    break
                if proc.poll() is not None:
                    self.fail("daemon exited (code {}) before launching a JVM".format(proc.poll()))
                time.sleep(0.5)
            self.assertTrue(kids, "daemon never launched a JVM; cannot exercise the orphan case")

            RemoteSparkSession._terminate_local_connect_server(proc)

            self.assertTrue(
                self._wait_process_group_gone(pgid, timeout=30),
                "process group survived _terminate_local_connect_server",
            )
        finally:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except OSError:
                pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
