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
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
    from pyspark.version import __version__


def _spawn_sleeper() -> "subprocess.Popen":
    """A detached long sleeper standing in for a pool server's process group."""
    return subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"],
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_pid_gone(pid: int, timeout: float = 30.0) -> bool:
    """Wait for a non-child pid to disappear (children stay signalable as zombies; use
    ``_wait_proc_dead`` for those)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except OSError:
            return True
        time.sleep(0.2)
    return False


def _wait_proc_dead(proc: "subprocess.Popen", timeout: float = 30.0) -> bool:
    try:
        proc.wait(timeout=timeout)
        return True
    except subprocess.TimeoutExpired:
        return False


@unittest.skipIf(
    not should_test_connect or is_remote_only(),
    connect_requirement_message or "Requires JVM access to start local Connect servers",
)
class LocalConnectServerPoolUnitTests(unittest.TestCase):
    """Unit tests for the pool bookkeeping (no real servers are started)."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self._pool_dir = os.path.join(self._tmpdir, "pool")
        self._saved_env = {
            k: os.environ.get(k)
            for k in (
                "SPARK_LOCAL_CONNECT_POOL_DIR",
                "SPARK_LOCAL_CONNECT_POOL",
                "SPARK_LOCAL_CONNECT_POOL_SIZE",
                "SPARK_CONNECT_AUTHENTICATE_TOKEN",
            )
        }
        os.environ["SPARK_LOCAL_CONNECT_POOL_DIR"] = self._pool_dir
        os.makedirs(self._pool_dir, exist_ok=True)
        self._sleepers = []
        RemoteSparkSession._pooled_server = None

    def tearDown(self) -> None:
        RemoteSparkSession._pooled_server = None
        for proc in self._sleepers:
            try:
                proc.kill()
                proc.communicate(timeout=10)
            except Exception:
                pass
        for k, v in self._saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _sleeper(self) -> "subprocess.Popen":
        proc = _spawn_sleeper()
        self._sleepers.append(proc)
        return proc

    def _write(self, name: str, payload: dict) -> str:
        path = os.path.join(self._pool_dir, name)
        with open(path, "w") as f:
            json.dump(payload, f)
        return path

    def _server_entry(self, uid: str, fingerprint: str, port: int, pid: int = None) -> str:
        return self._write(
            "server-{}.json".format(uid),
            {
                "host": "localhost",
                "port": port,
                "token": "t-" + uid,
                "pid": os.getpid() if pid is None else pid,
                "spark_version": __version__,
                "conf_fingerprint": fingerprint,
            },
        )

    def test_pool_dir_honors_override(self) -> None:
        self.assertEqual(RemoteSparkSession._local_connect_pool_dir(), self._pool_dir)
        os.environ.pop("SPARK_LOCAL_CONNECT_POOL_DIR")
        self.assertTrue(
            RemoteSparkSession._local_connect_pool_dir().endswith(
                os.path.join(".spark", "connect-local-pool")
            )
        )

    def test_pool_target_parsing(self) -> None:
        self.assertEqual(RemoteSparkSession._local_connect_pool_target({}), 2)
        self.assertEqual(
            RemoteSparkSession._local_connect_pool_target({"spark.local.connect.pool.size": "3"}),
            3,
        )
        os.environ["SPARK_LOCAL_CONNECT_POOL_SIZE"] = "5"
        self.assertEqual(RemoteSparkSession._local_connect_pool_target({}), 5)
        # Junk falls back to the default; values below one are clamped up.
        self.assertEqual(
            RemoteSparkSession._local_connect_pool_target({"spark.local.connect.pool.size": "abc"}),
            2,
        )
        self.assertEqual(
            RemoteSparkSession._local_connect_pool_target({"spark.local.connect.pool.size": "0"}),
            1,
        )

    def test_fingerprint_sensitive_to_master_and_conf(self) -> None:
        fp = RemoteSparkSession._local_connect_pool_fingerprint
        base = fp("local[*]", {"spark.sql.shuffle.partitions": "4"})
        self.assertEqual(base, fp("local[*]", {"spark.sql.shuffle.partitions": "4"}))
        self.assertNotEqual(base, fp("local[2]", {"spark.sql.shuffle.partitions": "4"}))
        self.assertNotEqual(base, fp("local[*]", {"spark.sql.shuffle.partitions": "8"}))
        self.assertNotEqual(base, fp("local[*]", {}))

    def test_claim_matches_fingerprint_and_consumes_launch_files(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("localhost", 0))
            listener.listen(1)
            port = listener.getsockname()[1]
            self._server_entry("aaa", "other-fp", port)
            self._server_entry("bbb", "my-fp", port)
            self._write("pending-bbb.json", {"daemon_pid": os.getpid(), "created": time.time()})
            self._write("conf-bbb.json", {"spark.foo": "bar"})

            disc = RemoteSparkSession._claim_local_connect_pool_server(self._pool_dir, "my-fp")

            self.assertIsNotNone(disc)
            self.assertEqual(disc["token"], "t-bbb")
            claim_name = "claimed-{}-bbb.json".format(os.getpid())
            self.assertEqual(os.path.basename(disc["claim_path"]), claim_name)
            entries = set(os.listdir(self._pool_dir))
            # The claimed server's discovery file was renamed, and its launch bookkeeping
            # (pending marker, conf seed) was consumed with it; the mismatched entry remains.
            self.assertIn(claim_name, entries)
            self.assertIn("server-aaa.json", entries)
            self.assertNotIn("server-bbb.json", entries)
            self.assertNotIn("pending-bbb.json", entries)
            self.assertNotIn("conf-bbb.json", entries)
            # A second claim finds nothing: the only matching member was consumed.
            self.assertIsNone(
                RemoteSparkSession._claim_local_connect_pool_server(self._pool_dir, "my-fp")
            )

    def test_claim_skips_unreachable_server(self) -> None:
        # Reserve a port and close it again so nothing is listening there.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("localhost", 0))
            dead_port = sock.getsockname()[1]
        self._server_entry("ccc", "my-fp", dead_port)
        self.assertIsNone(
            RemoteSparkSession._claim_local_connect_pool_server(self._pool_dir, "my-fp")
        )

    def test_janitor_reaps_stale_entries(self) -> None:
        dead_pid = 2**31 - 1
        # A pending marker whose daemon died, and one whose server has since become ready.
        self._write("pending-dead.json", {"daemon_pid": dead_pid, "created": time.time()})
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("localhost", 0))
            listener.listen(1)
            port = listener.getsockname()[1]
            self._server_entry("ready", "fp", port)
            self._write("pending-ready.json", {"daemon_pid": os.getpid(), "created": time.time()})
            # A ready entry whose process is gone, and a claimed entry whose client is gone.
            self._server_entry("goner", "fp", port, pid=dead_pid)
            orphan = self._sleeper()
            self._write(
                "claimed-{}-orphan.json".format(dead_pid),
                {
                    "host": "localhost",
                    "port": port,
                    "token": "t",
                    "pid": orphan.pid,
                    "spark_version": __version__,
                },
            )
            # A retired entry whose process is already gone.
            self._write("retired-done.json", {"pid": dead_pid, "retired": time.time()})
            # Conf seed with no pending marker left.
            self._write("conf-stale.json", {"spark.foo": "bar"})

            RemoteSparkSession._local_connect_pool_janitor(self._pool_dir)

        entries = set(os.listdir(self._pool_dir))
        self.assertNotIn("pending-dead.json", entries)
        self.assertNotIn("pending-ready.json", entries)  # its server is ready
        self.assertIn("server-ready.json", entries)  # healthy member untouched
        self.assertNotIn("server-goner.json", entries)
        self.assertNotIn("claimed-{}-orphan.json".format(dead_pid), entries)
        self.assertNotIn("retired-done.json", entries)
        self.assertNotIn("conf-stale.json", entries)
        # The orphaned server (claimed by a dead client) was signalled to stop.
        self.assertTrue(_wait_proc_dead(orphan), "janitor did not reap the orphaned server")

    def test_janitor_keeps_live_claims_and_fresh_retirements(self) -> None:
        keeper = self._sleeper()
        claim = self._write(
            "claimed-{}-mine.json".format(os.getpid()),
            {
                "host": "localhost",
                "port": 1,
                "token": "t",
                "pid": keeper.pid,
                "spark_version": __version__,
            },
        )
        fresh = self._sleeper()
        retired = self._write("retired-fresh.json", {"pid": fresh.pid, "retired": time.time()})
        RemoteSparkSession._local_connect_pool_janitor(self._pool_dir)
        self.assertTrue(os.path.exists(claim))
        self.assertTrue(os.path.exists(retired))  # within the 30s grace window
        self.assertIsNone(keeper.poll())
        self.assertIsNone(fresh.poll())

    def test_release_signals_server_and_writes_retired(self) -> None:
        server = self._sleeper()
        claim = self._write(
            "claimed-{}-xyz.json".format(os.getpid()),
            {
                "host": "localhost",
                "port": 1,
                "token": "t",
                "pid": server.pid,
                "spark_version": __version__,
            },
        )
        RemoteSparkSession._pooled_server = {
            "pid": server.pid,
            "claim_path": claim,
            "token": "t",
            "endpoint": "sc://localhost:1",
        }
        RemoteSparkSession._release_pooled_local_connect_server()
        self.assertIsNone(RemoteSparkSession._pooled_server)
        self.assertFalse(os.path.exists(claim))
        retired = os.path.join(self._pool_dir, "retired-xyz.json")
        self.assertTrue(os.path.exists(retired))
        with open(retired, "r") as f:
            self.assertEqual(json.load(f)["pid"], server.pid)
        self.assertTrue(_wait_proc_dead(server), "release did not stop the server")
        # Releasing again is a no-op.
        RemoteSparkSession._release_pooled_local_connect_server()

    def test_purge_kills_all_members_and_empties_dir(self) -> None:
        warm = self._sleeper()
        pending = self._sleeper()
        self._server_entry("warm", "fp", 1, pid=warm.pid)
        self._write("pending-boot.json", {"daemon_pid": pending.pid, "created": time.time()})
        self._write("conf-boot.json", {"spark.foo": "bar"})
        killed = RemoteSparkSession._purge_local_connect_pool()
        self.assertGreaterEqual(killed, 2)
        self.assertEqual(os.listdir(self._pool_dir), [])
        self.assertTrue(_wait_proc_dead(warm))
        self.assertTrue(_wait_proc_dead(pending))

    def test_has_live_pending_trusts_fresh_placeholder(self) -> None:
        # A marker still carrying the -1 placeholder pid counts as live while its spawner is
        # alive and the marker is fresh: the spawner is between publishing the marker and
        # Popen returning (the race caught by the 4-parallel-clients scenario).
        self._write(
            "pending-boot.json",
            {
                "daemon_pid": -1,
                "spawner_pid": os.getpid(),
                "created": time.time(),
                "fingerprint": "fp",
            },
        )
        self.assertTrue(
            RemoteSparkSession._local_connect_pool_has_live_pending(self._pool_dir, "fp")
        )
        # A fresh placeholder from a dead spawner is not live.
        self._write(
            "pending-boot.json",
            {
                "daemon_pid": -1,
                "spawner_pid": 2**31 - 1,
                "created": time.time(),
                "fingerprint": "fp",
            },
        )
        self.assertFalse(
            RemoteSparkSession._local_connect_pool_has_live_pending(self._pool_dir, "fp")
        )
        # Nor is a stale placeholder, whoever spawned it.
        self._write(
            "pending-boot.json",
            {
                "daemon_pid": -1,
                "spawner_pid": os.getpid(),
                "created": time.time() - 60,
                "fingerprint": "fp",
            },
        )
        self.assertFalse(
            RemoteSparkSession._local_connect_pool_has_live_pending(self._pool_dir, "fp")
        )

    def test_pool_lock_roundtrip(self) -> None:
        fd = RemoteSparkSession._acquire_local_connect_pool_lock(self._pool_dir)
        try:
            self.assertTrue(os.path.exists(os.path.join(self._pool_dir, ".lock")))
        finally:
            RemoteSparkSession._release_local_connect_pool_lock(fd)
        # Reacquirable after release.
        fd = RemoteSparkSession._acquire_local_connect_pool_lock(self._pool_dir)
        RemoteSparkSession._release_local_connect_pool_lock(fd)


@unittest.skipIf(
    not should_test_connect or is_remote_only(),
    connect_requirement_message or "Requires JVM access to start local Connect servers",
)
class LocalConnectServerPoolE2ETests(unittest.TestCase):
    """End-to-end tests that start real pooled servers (slow)."""

    CLIENT = textwrap.dedent("""
        import json
        import os

        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.remote("local[2]")
            .config("spark.local.connect.pool", "true")
            .getOrCreate()
        )
        from pyspark.sql.connect.session import SparkSession as RemoteSparkSession

        try:
            count = spark.range(1).count()
            state = RemoteSparkSession._pooled_server
            print(json.dumps({"count": count, "server_pid": state["pid"]}))
        finally:
            spark.stop()
        """)

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self._pool_dir = os.path.join(self._tmpdir, "pool")
        self._saved_env = {
            k: os.environ.get(k)
            for k in (
                "SPARK_LOCAL_CONNECT_POOL_DIR",
                "SPARK_LOCAL_CONNECT_POOL",
                "SPARK_LOCAL_CONNECT_POOL_SIZE",
                "SPARK_CONNECT_AUTHENTICATE_TOKEN",
            )
        }
        os.environ["SPARK_LOCAL_CONNECT_POOL_DIR"] = self._pool_dir

    def tearDown(self) -> None:
        try:
            RemoteSparkSession._purge_local_connect_pool()
            deadline = time.time() + 60
            while time.time() < deadline and os.path.isdir(self._pool_dir):
                if not os.listdir(self._pool_dir):
                    break
                time.sleep(0.5)
        finally:
            for k, v in self._saved_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _client_env(self, pool_size: str) -> dict:
        env = dict(os.environ)
        env["SPARK_LOCAL_CONNECT_POOL_DIR"] = self._pool_dir
        env["SPARK_LOCAL_CONNECT_POOL"] = "1"
        env["SPARK_LOCAL_CONNECT_POOL_SIZE"] = pool_size
        return env

    def _run_clients(self, n: int, pool_size: str) -> list:
        procs = [
            subprocess.Popen(
                [sys.executable, "-c", self.CLIENT],
                env=self._client_env(pool_size),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for _ in range(n)
        ]
        outputs = []
        try:
            for proc in procs:
                stdout, stderr = proc.communicate(timeout=300)
                self.assertEqual(proc.returncode, 0, stderr)
                lines = stdout.strip().splitlines()
                self.assertTrue(lines, stderr)
                outputs.append(json.loads(lines[-1]))
        finally:
            for proc in procs:
                if proc.poll() is None:
                    proc.kill()
                    proc.communicate()
        return outputs

    def test_sequential_runs_use_fresh_servers_and_tear_them_down(self) -> None:
        first = self._run_clients(1, pool_size="1")[0]
        self.assertEqual(first["count"], 1)
        # The used server is torn down asynchronously after the run.
        self.assertTrue(
            _wait_pid_gone(first["server_pid"], timeout=60),
            "server was not torn down after its run ended",
        )
        second = self._run_clients(1, pool_size="1")[0]
        self.assertEqual(second["count"], 1)
        self.assertNotEqual(
            first["server_pid"], second["server_pid"], "a pooled server was reused across runs"
        )

    def test_concurrent_cold_clients_get_distinct_servers(self) -> None:
        outputs = self._run_clients(2, pool_size="2")
        self.assertEqual({o["count"] for o in outputs}, {1})
        pids = [o["server_pid"] for o in outputs]
        self.assertEqual(len(set(pids)), 2, "two concurrent runs shared a pooled server")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
