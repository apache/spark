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
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

from pyspark.util import is_remote_only
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

if should_test_connect:
    from pyspark.sql.connect import local_server_pool
    from pyspark.sql.connect.local_server_pool import (
        PoolDirectory,
        PoolMember,
        ServerPool,
        pool_fingerprint,
    )
    from pyspark.version import __version__


@contextlib.contextmanager
def _listening_socket():
    import socket

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        listener.bind(("localhost", 0))
        listener.listen(1)
        yield listener.getsockname()[1]
    finally:
        listener.close()


def _closed_port() -> int:
    """A port with nothing listening on it."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]


def _spawn_sleeper() -> "subprocess.Popen":
    """A long sleeper standing in for a pool server or attendant process."""
    return subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _spawn_stubborn_sleeper() -> "subprocess.Popen":
    """A sleeper that ignores SIGTERM, standing in for a server hanging in shutdown. It
    prints one line once its handler is installed so tests do not signal it too early."""
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import signal, time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "print('ready', flush=True)\n"
            "time.sleep(300)",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    assert proc.stdout is not None
    proc.stdout.readline()
    return proc


def _wait_proc_dead(proc: "subprocess.Popen", timeout: float = 30.0) -> bool:
    try:
        proc.wait(timeout=timeout)
        return True
    except subprocess.TimeoutExpired:
        return False


_SAVED_ENV_KEYS = (
    "SPARK_LOCAL_CONNECT_POOL_DIR",
    "SPARK_LOCAL_CONNECT_POOL_SIZE",
    "SPARK_LOCAL_CONNECT_POOL_IDLE_TIMEOUT",
    "PYSPARK_PYTHON",
)


@unittest.skipIf(
    not should_test_connect or is_remote_only(),
    connect_requirement_message or "Requires Spark Connect test dependencies",
)
class LocalConnectServerPoolUnitTests(unittest.TestCase):
    """Tests for the pool filesystem model; no real servers are started."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self._saved_env = {k: os.environ.get(k) for k in _SAVED_ENV_KEYS}
        for k in _SAVED_ENV_KEYS:
            os.environ.pop(k, None)
        os.environ["SPARK_LOCAL_CONNECT_POOL_DIR"] = os.path.join(self._tmpdir, "pool")
        self._directory = PoolDirectory()
        self._pool = ServerPool(self._directory)
        self._procs = []
        local_server_pool._claimed_member = None

    def tearDown(self) -> None:
        local_server_pool._claimed_member = None
        for proc in self._procs:
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
        self._procs.append(proc)
        return proc

    def _stubborn_sleeper(self) -> "subprocess.Popen":
        proc = _spawn_stubborn_sleeper()
        self._procs.append(proc)
        return proc

    def _server_data(self, port: int, pid: int, fingerprint: str = "fp", **overrides) -> dict:
        data = {
            "host": "localhost",
            "port": port,
            "token": "t",
            "pid": pid,
            "spark_version": __version__,
            "fingerprint": fingerprint,
            "created": time.time(),
        }
        data.update(overrides)
        return data

    def _write_state(self, path: str, data: dict) -> str:
        with self._directory as directory:
            directory.write_json(path, data)
        return path

    def _states(self, uid: str) -> dict:
        with self._directory as directory:
            return directory.states(uid)

    def test_pool_directory_location(self) -> None:
        self.assertEqual(self._directory.path, os.path.join(self._tmpdir, "pool"))
        os.environ.pop("SPARK_LOCAL_CONNECT_POOL_DIR")
        default = PoolDirectory()
        self.assertEqual(os.path.basename(default.path), "pool")
        self.assertTrue(default.path.startswith(tempfile.gettempdir()))

    def test_pool_size_parsing(self) -> None:
        from pyspark.sql.connect.local_server_pool import _pool_size

        self.assertEqual(_pool_size({}), 2)
        self.assertEqual(_pool_size({"spark.local.connect.pool.size": "3"}), 3)
        os.environ["SPARK_LOCAL_CONNECT_POOL_SIZE"] = "5"
        self.assertEqual(_pool_size({}), 5)
        # Junk falls back to the default; values below one are clamped up.
        self.assertEqual(_pool_size({"spark.local.connect.pool.size": "abc"}), 2)
        self.assertEqual(_pool_size({"spark.local.connect.pool.size": "0"}), 1)

    @unittest.skipUnless(
        sys.platform.startswith("linux") and os.path.isdir("/proc"),
        "requires Linux process state",
    )
    def test_pid_alive_treats_zombie_as_dead(self) -> None:
        proc = subprocess.Popen([sys.executable, "-c", "pass"])
        try:
            state = ""
            deadline = time.time() + 5
            while time.time() < deadline:
                with open(f"/proc/{proc.pid}/stat", encoding="utf-8") as stat_file:
                    fields = stat_file.read().rpartition(")")[2].split()
                state = fields[0] if fields else ""
                if state == "Z":
                    break
                time.sleep(0.01)
            self.assertEqual(state, "Z", "the child did not enter zombie state")
            self.assertFalse(local_server_pool._pid_alive(proc.pid))
        finally:
            proc.wait(timeout=10)

    def test_fingerprint_identity(self) -> None:
        base = pool_fingerprint("local[*]", {"spark.sql.shuffle.partitions": "4"})
        self.assertEqual(base, pool_fingerprint("local[*]", {"spark.sql.shuffle.partitions": "4"}))
        self.assertNotEqual(
            base, pool_fingerprint("local[2]", {"spark.sql.shuffle.partitions": "4"})
        )
        self.assertNotEqual(
            base, pool_fingerprint("local[*]", {"spark.sql.shuffle.partitions": "8"})
        )
        self.assertNotEqual(base, pool_fingerprint("local[*]", {}))
        # The working directory shapes the server (relative warehouse and metastore paths),
        # so members are never shared across directories.
        cwd = os.getcwd()
        try:
            os.chdir(self._tmpdir)
            self.assertNotEqual(base, pool_fingerprint("local[*]", {"x": "4"}))
            in_tmpdir = pool_fingerprint("local[*]", {"spark.sql.shuffle.partitions": "4"})
        finally:
            os.chdir(cwd)
        self.assertNotEqual(base, in_tmpdir)
        # So does the Python environment the server would run UDFs with.
        os.environ["PYSPARK_PYTHON"] = "/some/other/python"
        self.assertNotEqual(
            base, pool_fingerprint("local[*]", {"spark.sql.shuffle.partitions": "4"})
        )

    def test_claim_matches_fingerprint_and_renames(self) -> None:
        with _listening_socket() as port:
            sleeper = self._sleeper()
            self._write_state(
                self._directory.server_path("aaa"),
                self._server_data(port, sleeper.pid, fingerprint="other-fp"),
            )
            self._write_state(
                self._directory.server_path("bbb"),
                self._server_data(port, sleeper.pid, fingerprint="my-fp", token="t-bbb"),
            )
            with self._directory:
                member = self._pool.claim("my-fp")
        self.assertIsNotNone(member)
        self.assertEqual(member.token, "t-bbb")
        claim_name = f"claimed-{os.getpid()}-bbb.json"
        self.assertEqual(os.path.basename(member.claim_path), claim_name)
        states = self._states("bbb")
        self.assertEqual(set(states), {"claimed"})
        # The mismatched member is untouched, and a second claim finds nothing.
        self.assertEqual(set(self._states("aaa")), {"server"})
        with self._directory:
            self.assertIsNone(self._pool.claim("my-fp"))

    def test_claim_prefers_the_oldest_member(self) -> None:
        with _listening_socket() as port:
            sleeper = self._sleeper()
            for uid, created in (("young", time.time()), ("old", time.time() - 100)):
                self._write_state(
                    self._directory.server_path(uid),
                    self._server_data(port, sleeper.pid, token="t-" + uid, created=created),
                )
            with self._directory:
                member = self._pool.claim("fp")
        # Prefer the oldest ready member for deterministic FIFO claiming.
        self.assertEqual(member.token, "t-old")

    def test_claim_skips_unreachable_member(self) -> None:
        self._write_state(
            self._directory.server_path("ccc"), self._server_data(_closed_port(), os.getpid())
        )
        with self._directory:
            self.assertIsNone(self._pool.claim("fp"))

    def test_reap_pending_of_dead_attendant(self) -> None:
        # The attendant died mid-boot: its pending marker and conf seed are withdrawn, and
        # the half-started server whose pid spark-daemon.sh recorded is killed.
        from pyspark.sql.connect.local_server import Discovery

        half_started = self._sleeper()
        member_dir = self._directory.member_dir("boot")
        os.makedirs(member_dir)
        discovery = Discovery(os.path.join(member_dir, "connect-local.json"))
        with open(discovery.daemon_pid_path, "w") as f:
            f.write(str(half_started.pid))
        self._write_state(
            self._directory.pending_path("boot"),
            {"attendant_pid": 2**31 - 1, "created": time.time(), "fingerprint": "fp"},
        )
        self._write_state(self._directory.conf_path("boot"), {"spark.foo": "bar"})

        with self._directory:
            self._pool.reap("boot")

        states = self._states("boot")
        self.assertNotIn("pending", states)
        self.assertNotIn("conf", states)
        self.assertTrue(_wait_proc_dead(half_started), "the half-started server was not reaped")

    def test_reap_keeps_live_pending(self) -> None:
        attendant = self._sleeper()
        self._write_state(
            self._directory.pending_path("live"),
            {"attendant_pid": attendant.pid, "created": time.time(), "fingerprint": "fp"},
        )
        with self._directory:
            self._pool.reap("live")
        self.assertIn("pending", self._states("live"))
        self.assertIsNone(attendant.poll())

    def test_reap_server_unreachable_and_idle(self) -> None:
        with self.subTest("unreachable member is retired"):
            gone = self._sleeper()
            self._write_state(
                self._directory.server_path("dead"), self._server_data(_closed_port(), gone.pid)
            )
            with self._directory:
                self._pool.reap("dead")
            self.assertEqual(set(self._states("dead")), {"retired"})
            self.assertTrue(_wait_proc_dead(gone))
        with self.subTest("member idle past the timeout is retired"):
            os.environ["SPARK_LOCAL_CONNECT_POOL_IDLE_TIMEOUT"] = "10"
            with _listening_socket() as port:
                idle = self._sleeper()
                self._write_state(
                    self._directory.server_path("idle"),
                    self._server_data(port, idle.pid, created=time.time() - 60),
                )
                fresh = self._sleeper()
                self._write_state(
                    self._directory.server_path("fresh"), self._server_data(port, fresh.pid)
                )
                with self._directory:
                    self._pool.janitor()
                self.assertEqual(set(self._states("idle")), {"retired"})
                self.assertEqual(set(self._states("fresh")), {"server"})
                self.assertTrue(_wait_proc_dead(idle))
                self.assertIsNone(fresh.poll())

    def test_reap_claimed_of_dead_client(self) -> None:
        orphan = self._sleeper()
        dead_client = 2**31 - 1
        self._write_state(
            self._directory.claimed_path(dead_client, "orphan"),
            self._server_data(_closed_port(), orphan.pid),
        )
        ours = self._sleeper()
        self._write_state(
            self._directory.claimed_path(os.getpid(), "mine"),
            self._server_data(_closed_port(), ours.pid),
        )
        with self._directory:
            self._pool.janitor()
        # The orphaned claim is retired and its server stopped; our own claim is untouched.
        self.assertEqual(set(self._states("orphan")), {"retired"})
        self.assertTrue(_wait_proc_dead(orphan), "the orphaned server was not stopped")
        self.assertEqual(set(self._states("mine")), {"claimed"})
        self.assertIsNone(ours.poll())

    def test_reap_retired_escalates_to_sigkill(self) -> None:
        with self.subTest("a fresh retirement is left to shut down gracefully"):
            fresh = self._sleeper()
            self._write_state(
                self._directory.retired_path("fresh"),
                {"pid": fresh.pid, "retired": time.time()},
            )
            with self._directory:
                self._pool.reap("fresh")
            self.assertEqual(set(self._states("fresh")), {"retired"})
            self.assertIsNone(fresh.poll())
        with self.subTest("a hung shutdown is hard-killed"):
            stubborn = self._stubborn_sleeper()
            self._write_state(
                self._directory.retired_path("hung"),
                {"pid": stubborn.pid, "retired": time.time() - 31},
            )
            with self._directory:
                self._pool.reap("hung")
            self.assertTrue(_wait_proc_dead(stubborn), "SIGKILL escalation did not happen")
            with self._directory:
                self.assertTrue(self._pool.reap("hung"))

    def test_release_retires_the_claimed_member(self) -> None:
        server = self._sleeper()
        claim_path = self._write_state(
            self._directory.claimed_path(os.getpid(), "xyz"),
            self._server_data(_closed_port(), server.pid),
        )
        member = PoolMember(self._server_data(_closed_port(), server.pid))
        member.claim_path = claim_path
        local_server_pool._claimed_member = member

        local_server_pool.release_pooled_local_connect_server()

        self.assertIsNone(local_server_pool._claimed_member)
        states = self._states("xyz")
        self.assertEqual(set(states), {"retired"})
        with self._directory as directory:
            self.assertEqual(directory.read_json(states["retired"])["pid"], server.pid)
        self.assertTrue(_wait_proc_dead(server), "release did not stop the server")
        # Releasing again is a no-op.
        local_server_pool.release_pooled_local_connect_server()

    def test_purge_kills_everything_and_empties_the_directory(self) -> None:
        warm = self._sleeper()
        attendant = self._sleeper()
        self._write_state(
            self._directory.server_path("warm"), self._server_data(_closed_port(), warm.pid)
        )
        self._write_state(
            self._directory.pending_path("boot"),
            {"attendant_pid": attendant.pid, "created": time.time(), "fingerprint": "fp"},
        )
        self._write_state(self._directory.conf_path("boot"), {"spark.foo": "bar"})
        os.makedirs(self._directory.member_dir("warm"))

        signalled = local_server_pool.purge_local_connect_pool()

        self.assertGreaterEqual(signalled, 2)
        self.assertEqual(os.listdir(self._directory.path), [".lock"])
        self.assertTrue(_wait_proc_dead(warm))
        self.assertTrue(_wait_proc_dead(attendant))


if __name__ == "__main__":
    from pyspark.testing import main

    main()
