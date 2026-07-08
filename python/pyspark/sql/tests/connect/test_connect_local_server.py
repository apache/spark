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
    from pyspark.sql.connect import local_server
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
            disc = local_server._read_discovery()
            if disc is not None and disc["pid"] != os.getpid():
                port = disc["port"]
                local_server.stop_local_connect_server()
                # stop_local_connect_server only signals the daemon and returns; wait for the JVM
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
        self.assertEqual(local_server._discovery_path(), self._discovery)
        os.environ.pop("SPARK_LOCAL_CONNECT_DISCOVERY")
        self.assertTrue(
            local_server._discovery_path().endswith(os.path.join(".spark", "connect-local.json"))
        )

    def test_read_discovery_missing_or_malformed(self) -> None:
        self.assertIsNone(local_server._read_discovery())
        with open(self._discovery, "w") as f:
            f.write("not json")
        self.assertIsNone(local_server._read_discovery())
        with open(self._discovery, "w") as f:
            json.dump({"host": "localhost"}, f)  # missing required keys
        self.assertIsNone(local_server._read_discovery())
        self._write_discovery(pid="not-a-pid")  # all keys present, but pid is not an int
        self.assertIsNone(local_server._read_discovery())

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
        self.assertFalse(local_server._server_is_reusable(disc))

    def test_not_reusable_on_dead_pid(self) -> None:
        # PID 2**31 - 1 is effectively guaranteed not to exist.
        disc = self._write_discovery(pid=2**31 - 1, port=1)
        self.assertFalse(local_server._server_is_reusable(disc))

    def test_reusable_when_alive_and_listening(self) -> None:
        # A live listening socket owned by this (alive) process with a matching version is reusable.
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind(("localhost", 0))
            listener.listen(1)
            port = listener.getsockname()[1]
            disc = self._write_discovery(port=port)
            self.assertTrue(local_server._server_is_reusable(disc))
        finally:
            listener.close()
        # Once the socket is closed the port is no longer reachable, so it is not reusable.
        self.assertFalse(local_server._server_is_reusable(disc))

    def test_pid_probe_is_skipped_on_windows(self) -> None:
        """The pid liveness probe must never run on Windows.

        There ``os.kill(pid, 0)`` does not probe the process -- it unconditionally *terminates*
        it via TerminateProcess -- so the reuse check would kill the very server it is examining.
        """
        from unittest import mock

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind(("localhost", 0))
            listener.listen(1)
            disc = self._write_discovery(port=listener.getsockname()[1])
            with mock.patch.object(os, "name", "nt"), mock.patch.object(os, "kill") as kill:
                self.assertTrue(local_server._server_is_reusable(disc))
                kill.assert_not_called()
        finally:
            listener.close()

    def test_stop_when_no_server_is_safe(self) -> None:
        self.assertFalse(local_server.stop_local_connect_server())

    def test_stop_signals_recorded_server_group(self) -> None:
        from unittest import mock

        self._write_discovery(pid=12345)
        calls = []

        def fake_signal(pid, sig):
            calls.append((pid, sig))
            return True

        with mock.patch.object(local_server, "_signal_server", fake_signal):
            self.assertTrue(local_server.stop_local_connect_server())
        self.assertEqual(calls, [(12345, signal.SIGTERM)])
        self.assertFalse(os.path.exists(self._discovery))

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
                self.assertTrue(local_server._signal_server(child.pid, signal.SIGTERM))
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
                self.assertTrue(local_server._signal_server(leader.pid, signal.SIGTERM))
                killpg.assert_called_once_with(leader.pid, signal.SIGTERM)
                kill.assert_not_called()
        finally:
            leader.kill()
            leader.communicate()

    def test_reuse_from_discovery_none_when_absent(self) -> None:
        self.assertIsNone(local_server._reuse_from_discovery())

    def test_local_port_available(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind(("localhost", 0))
            listener.listen(1)
            taken = listener.getsockname()[1]
            self.assertFalse(local_server._port_available(taken))
        finally:
            listener.close()
        # The port is free again once the listener is closed.
        self.assertTrue(local_server._port_available(taken))

    def test_server_conf_seeds_user_confs_and_drops_control_keys(self) -> None:
        """_seed_conf keeps user startup confs but not keys the daemon controls."""
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
        conf = local_server._seed_conf(opts)
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
        """Entering and exiting the start-up lock creates the lock file and does not error."""
        with local_server._start_lock():
            try:
                import fcntl  # noqa: F401
            except ImportError:
                # Locking is a no-op without fcntl; entering the context is all we can assert.
                return
            self.assertTrue(os.path.exists(self._discovery + ".lock"))

    @unittest.skipUnless(os.name == "posix", "the removal guard needs fcntl")
    def test_remove_discovery_skipped_while_start_lock_held(self) -> None:
        """Daemon shutdown must not delete the discovery file while a launcher holds the lock.

        A held start-up lock means another process is publishing a new server; removing the file
        in that window could delete the newcomer's entry (see _remove_discovery_if_ours).
        """
        self._write_discovery(pid=os.getpid())
        with local_server._start_lock():
            local_server._remove_discovery_if_ours(self._discovery)
            self.assertTrue(os.path.exists(self._discovery))
        local_server._remove_discovery_if_ours(self._discovery)
        self.assertFalse(os.path.exists(self._discovery))

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

            disc = local_server._read_discovery()
            self.assertIsNotNone(disc)
            self.assertEqual(disc["spark_version"], __version__)
            self.assertNotEqual(disc["pid"], os.getpid())
        finally:
            if spark is not None:
                spark.stop()

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
        endpoint = local_server.reuse_or_start_local_connect_server("local[2]", {})
        self.assertTrue(endpoint.startswith("sc://localhost:"))

        disc = local_server._read_discovery()
        self.assertIsNotNone(disc)
        self.assertEqual(disc["spark_version"], __version__)
        self.assertEqual(os.environ.get("SPARK_CONNECT_AUTHENTICATE_TOKEN"), disc["token"])
        first_pid = disc["pid"]

        s1 = s2 = None
        try:
            # A second call reuses the running server: same endpoint, no new process spawned.
            endpoint2 = local_server.reuse_or_start_local_connect_server("local[2]", {})
            self.assertEqual(endpoint2, endpoint)
            self.assertEqual(local_server._read_discovery()["pid"], first_pid)

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
        self.assertIsNone(local_server._read_discovery())
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

    @staticmethod
    def _running_group_members(pgid: int) -> list:
        """Pids in process group ``pgid`` that are still running, excluding zombies.

        Zombies count as gone: they are already dead, so nothing can leak. Signal-based checks
        like ``killpg(pgid, 0)`` cannot be used instead, because a zombie keeps its group
        signalable, and orphans reparented to a pid 1 that never reaps (e.g. minimal CI
        containers) stay zombies indefinitely.
        """
        out = subprocess.run(
            ["ps", "-A", "-o", "pid=,pgid=,stat="], capture_output=True, text=True
        ).stdout
        members = []
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 3 and parts[1] == str(pgid) and not parts[2].startswith("Z"):
                members.append(int(parts[0]))
        return members

    def _wait_process_group_gone(self, pgid: int, timeout: float) -> bool:
        """Wait until nothing in ``pgid`` is still running; True if that happened in time."""
        deadline = time.time() + timeout
        while True:
            if not self._running_group_members(pgid):
                return True
            if time.time() >= deadline:
                return False
            time.sleep(0.2)

    @unittest.skipUnless(os.name == "posix", "process-group reaping is POSIX-only")
    def test_terminate_escalates_when_group_outlives_daemon(self) -> None:
        """SIGKILL escalation covers group members that survive the daemon.

        Reproduces the CI failure mode of test_terminate_reaps_daemon_and_jvm: the daemon exits
        on SIGTERM promptly while its JVM is still shutting down (or wedged) past the grace
        period. _terminate_server must not return while the group has live members.
        """
        if shutil.which("ps") is None:
            self.skipTest("ps is needed to inspect process group members")

        # A session leader that exits on SIGTERM after spawning a SIGTERM-immune child into its
        # process group -- the child stands in for a JVM that outlives the daemon.
        leader_prog = textwrap.dedent("""
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
            """)
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

            local_server._terminate_server(proc)

            self.assertTrue(
                self._wait_process_group_gone(pgid, timeout=10),
                "process group survived _terminate_server",
            )
            # The SIGTERM-immune child no longer runs, proving the SIGKILL escalation fired.
            self.assertNotIn(
                child_pid,
                self._running_group_members(pgid),
                "SIGTERM-immune child survived the SIGKILL escalation",
            )
        finally:
            proc.stdout.close()
            try:
                os.killpg(pgid, signal.SIGKILL)
            except OSError:
                pass

    @unittest.skipUnless(os.name == "posix", "process-group reaping is POSIX-only")
    def test_terminate_reaps_daemon_and_jvm(self) -> None:
        """_terminate_server kills the daemon *and* its child JVM.

        The test waits until the daemon has launched the JVM, then checks that the whole process
        group exits.
        """
        if shutil.which("pgrep") is None:
            self.skipTest("pgrep is needed to detect the child JVM")

        cmd = [
            sys.executable,
            "-m",
            "pyspark.sql.connect.local_server",
            "--master",
            "local[2]",
            "--port",
            "0",  # ephemeral, so a stray server elsewhere cannot interfere
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

            local_server._terminate_server(proc)

            self.assertTrue(
                self._wait_process_group_gone(pgid, timeout=30),
                "process group survived _terminate_server",
            )
        finally:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except OSError:
                pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
