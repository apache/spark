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
from unittest import mock

from pyspark.testing.connectutils import connect_requirement_message, should_test_connect

if should_test_connect:
    from pyspark.sql.connect import local_server_pool
    from pyspark.sql.connect.local_server import _SERVER_CLASS, _pid_alive
    from pyspark.sql.connect.local_server_pool import (
        _JVM_ENV_VARS,
        PoolDirectory,
        PoolMember,
        RetiredState,
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


@contextlib.contextmanager
def _non_listening_socket():
    """Reserve a port without listening on it, so connection attempts are rejected."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("localhost", 0))
        yield sock.getsockname()[1]


def _spawn_live_process() -> "subprocess.Popen":
    """A child blocked on its parent pipe, standing in for a live pool server."""
    return subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdin.buffer.read()", _SERVER_CLASS],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _spawn_stubborn_process() -> "subprocess.Popen":
    """A pipe-blocked child that ignores SIGTERM, standing in for a hung server. It reports
    when its handler is installed so tests do not signal it too early."""
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import signal, sys\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "print('ready', flush=True)\n"
            "sys.stdin.buffer.read()",
            _SERVER_CLASS,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    assert proc.stdout is not None
    assert proc.stdout.readline() == "ready\n"
    return proc


def _wait_proc_dead(proc: "subprocess.Popen", timeout: float = 30.0) -> bool:
    try:
        proc.wait(timeout=timeout)
        return True
    except subprocess.TimeoutExpired:
        return False


_SAVED_ENV_KEYS = (
    "SPARK_LOCAL_CONNECT_POOL_DIR",
    "SPARK_LOCAL_CONNECT_POOL_IDLE_TIMEOUT",
    "PYSPARK_DRIVER_PYTHON",
    "PYSPARK_PYTHON",
)


# These tests start no server and exercise only stdlib filesystem code, so they do not need JVM
# access (no is_remote_only gate). should_test_connect is still required because importing
# PoolDirectory pulls in the pyspark.sql.connect package, which checks Connect dependencies.
@unittest.skipIf(
    not should_test_connect,
    connect_requirement_message or "Requires Spark Connect dependencies to import the pool module",
)
@unittest.skipUnless(os.name == "posix", "the pool relies on POSIX file locks")
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

    def _live_process(self) -> "subprocess.Popen":
        proc = _spawn_live_process()
        self._procs.append(proc)
        return proc

    def _stubborn_process(self) -> "subprocess.Popen":
        proc = _spawn_stubborn_process()
        self._procs.append(proc)
        return proc

    def _server_data(self, port: int, pid: int, fingerprint: str = "fp", **overrides) -> dict:
        process_start_id = None
        if isinstance(pid, int) and not isinstance(pid, bool):
            process_start_id = ServerPool._process_start_id(pid)
        data = {
            "host": "localhost",
            "port": port,
            "token": "t",
            "pid": pid,
            "spark_version": __version__,
            "fingerprint": fingerprint,
            "process_start_id": process_start_id or f"unobserved:{pid}",
            "created": time.time(),
        }
        data.update(overrides)
        return data

    def _retired_data(self, pid: int, retired: object) -> dict:
        process_start_id = ServerPool._process_start_id(pid)
        assert process_start_id is not None
        return {
            "pid": pid,
            "process_start_id": process_start_id,
            "retired": retired,
        }

    def _write_state(self, path: str, data: dict) -> str:
        with self._directory as directory:
            directory.write_json(path, data)
        return path

    def _states(self, uid: str) -> dict:
        with self._directory as directory:
            return directory.states(uid)

    def _write_daemon_pid(self, uid: str, pid: int) -> None:
        from pyspark.sql.connect.local_server import Discovery

        member_dir = self._directory.member_dir(uid)
        os.makedirs(member_dir, exist_ok=True)
        discovery = Discovery(os.path.join(member_dir, "connect-local.json"))
        with open(discovery.daemon_pid_path, "w") as pid_file:
            pid_file.write(str(pid))

    def test_pool_directory_location(self) -> None:
        self.assertEqual(self._directory.path, os.path.join(self._tmpdir, "pool"))
        os.environ.pop("SPARK_LOCAL_CONNECT_POOL_DIR")
        default = PoolDirectory()
        self.assertEqual(os.path.basename(default.path), "pool")
        self.assertTrue(default.path.startswith(tempfile.gettempdir()))

    def test_pool_directory_lock_and_state_file_permissions(self) -> None:
        os.makedirs(self._directory.path, mode=0o755)
        os.chmod(self._directory.path, 0o755)
        state_path = self._directory.server_path("abcdef")
        with self._directory as directory:
            self.assertEqual(os.stat(directory.path).st_mode & 0o777, 0o700)
            lock_path = os.path.join(directory.path, ".lock")
            self.assertEqual(os.stat(lock_path).st_mode & 0o777, 0o600)
            directory.write_json(state_path, {"token": "secret"})
            self.assertEqual(os.stat(state_path).st_mode & 0o777, 0o600)
            self.assertEqual(directory.read_json(state_path), {"token": "secret"})

            # Replacing an existing file with wider permissions must restore the private mode.
            os.chmod(state_path, 0o644)
            directory.write_json(state_path, {"token": "new-secret"})
            self.assertEqual(os.stat(state_path).st_mode & 0o777, 0o600)

        with open(state_path, "w") as state_file:
            state_file.write("not json")
        with self._directory as directory:
            self.assertIsNone(directory.read_json(state_path))

            directory.write_json(state_path, {"token": "old-secret"})
            with mock.patch("builtins.open", side_effect=OSError("temporary read failure")):
                with self.assertRaisesRegex(OSError, "temporary read failure"):
                    directory.read_json(state_path)
            with mock.patch.object(
                local_server_pool.os, "replace", side_effect=OSError("interrupted replace")
            ):
                with self.assertRaisesRegex(OSError, "interrupted replace"):
                    directory.write_json(state_path, {"token": "new-secret"})
            self.assertEqual(directory.read_json(state_path), {"token": "old-secret"})
            self.assertFalse(
                [
                    name
                    for name in os.listdir(directory.path)
                    if name.startswith(directory._STATE_TEMP_PREFIX)
                ]
            )

        stale_temp = os.path.join(self._directory.path, ".pool-state-orphan")
        with open(stale_temp, "w") as temp_file:
            temp_file.write("partial")
        with self._directory:
            self.assertFalse(os.path.exists(stale_temp))

    def test_failed_state_write_does_not_close_a_reused_descriptor(self) -> None:
        real_fdopen = os.fdopen
        victim_fd = None
        test_case = self

        class FailingStateFile:
            def __init__(self, fd: int):
                self.fd = fd
                self.file = real_fdopen(fd, "w")

            def __enter__(self):
                return self

            def write(self, data: str) -> None:
                raise OSError("interrupted write")

            def __exit__(self, exc_type, exc_value, traceback) -> None:
                nonlocal victim_fd
                self.file.close()
                # Reuse the just-released descriptor before write_json handles the failure. Once
                # fdopen succeeds, it owns the original descriptor, so cleanup must not close the
                # new file that happens to receive the same number.
                victim_fd = os.open(os.devnull, os.O_RDONLY)
                test_case.assertEqual(victim_fd, self.fd)

        try:
            with self._directory as directory:
                with mock.patch.object(
                    local_server_pool.os,
                    "fdopen",
                    side_effect=lambda fd, mode: FailingStateFile(fd),
                ):
                    with self.assertRaisesRegex(OSError, "interrupted write"):
                        directory.write_json(directory.server_path("f00d"), {"a": 1})
            assert victim_fd is not None
            os.fstat(victim_fd)
        finally:
            if victim_fd is not None:
                with contextlib.suppress(OSError):
                    os.close(victim_fd)

    def test_pool_directory_does_not_hide_listing_failures(self) -> None:
        state_path = self._write_state(
            self._directory.retired_path("abc123"),
            {"pid": os.getpid(), "process_start_id": "unused", "retired": time.time()},
        )
        with self._directory:
            with mock.patch.object(
                local_server_pool.os, "listdir", side_effect=OSError("temporary listing failure")
            ):
                with self.assertRaisesRegex(OSError, "temporary listing failure"):
                    self._pool.reap("abc123")

        self.assertTrue(os.path.exists(state_path))
        with mock.patch.object(
            local_server_pool.os, "listdir", side_effect=OSError("failed during enter")
        ):
            with self.assertRaisesRegex(OSError, "failed during enter"):
                with self._directory:
                    pass
        self.assertIsNone(self._directory._lock_fd)
        with self._directory:
            pass

    def test_pool_directory_lock_blocks_another_process(self) -> None:
        child = (
            "import errno\n"
            "import fcntl\n"
            "import os\n"
            "import sys\n"
            "fd = os.open(sys.argv[1], os.O_RDWR)\n"
            "try:\n"
            "    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
            "except OSError as error:\n"
            "    if error.errno not in (errno.EACCES, errno.EAGAIN):\n"
            "        raise\n"
            "else:\n"
            "    raise RuntimeError('acquired a held lock')\n"
            "finally:\n"
            "    os.close(fd)\n"
        )
        with self._directory:
            result = subprocess.run(
                [sys.executable, "-c", child, os.path.join(self._directory.path, ".lock")],
                capture_output=True,
                text=True,
                timeout=10,
            )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_parse_entry_grammar(self) -> None:
        # uids match the acquisition layer's uuid4().hex[:12]: nonempty lowercase hex.
        uid = "0123456789ab"
        cases = [
            # Well-formed state entries of every kind.
            (f"pending-{uid}.json", ("pending", uid)),
            (f"conf-{uid}.json", ("conf", uid)),
            (f"server-{uid}.json", ("server", uid)),
            (f"retired-{uid}.json", ("retired", uid)),
            (f"claimed-4321-{uid}.json", ("claimed", uid)),
            (f"member-{uid}", ("member", uid)),
            # Short but valid hex uids.
            ("member-abc", ("member", "abc")),
            ("server-abc.json", ("server", "abc")),
            # The lock file and unrelated entries.
            (".lock", (None, None)),
            ("random.txt", (None, None)),
            # Editor droppings: the finding-2 cases that used to slip through as phantom uids.
            (f"member-{uid}.json.swp", (None, None)),
            (f"server-{uid}.json.swp", (None, None)),
            # Empty uids are rejected for every kind.
            ("server-.json", (None, None)),
            ("member-", (None, None)),
            ("claimed-1234-.json", (None, None)),
            # Non-hex uids (uppercase, out-of-range letters) are rejected.
            ("server-ABCDEF.json", (None, None)),
            ("server-ghij.json", (None, None)),
            # Malformed claimed stems (non-numeric or missing pid, or a malformed uid).
            (f"claimed-notapid-{uid}.json", (None, None)),
            (f"claimed-{uid}.json", (None, None)),
            ("claimed-4321-ABCDEF.json", (None, None)),
            # A pid that is str.isdigit() but not int()-parsable (superscript two, U+00B2) must
            # classify as "not claimed", never raise, since parse_entry runs over every entry.
            (f"claimed-{chr(0xB2)}-{uid}.json", (None, None)),
        ]
        for name, expected in cases:
            with self.subTest(name=name):
                self.assertEqual(PoolDirectory.parse_entry(name), expected)

    def test_claiming_pid(self) -> None:
        uid = "0123456789ab"
        path = self._directory.claimed_path(4321, uid)
        self.assertEqual(PoolDirectory.claiming_pid(path), 4321)
        # Parses from the basename alone, independent of the directory prefix.
        self.assertEqual(PoolDirectory.claiming_pid(f"claimed-7-{uid}.json"), 7)

    def test_locked_accessors_enumerate_state(self) -> None:
        uid_a, uid_b = "aaaaaaaaaaaa", "bbbbbbbbbbbb"
        with self._directory as directory:
            directory.write_json(directory.server_path(uid_a), {"a": 1})
            directory.write_json(directory.pending_path(uid_a), {"a": 2})
            directory.write_json(directory.server_path(uid_b), {"b": 1})
            os.makedirs(directory.member_dir(uid_a), mode=0o700)

            self.assertEqual(sorted(directory.uids()), [uid_a, uid_b])

            states_a = directory.states(uid_a)
            self.assertEqual(set(states_a), {"server", "pending", "member"})
            self.assertEqual(states_a["server"], directory.server_path(uid_a))
            self.assertEqual(states_a["member"], directory.member_dir(uid_a))

            servers = dict(directory.paths_of_kind("server"))
            self.assertEqual(set(servers), {uid_a, uid_b})
            self.assertEqual(servers[uid_a], directory.server_path(uid_a))
            self.assertEqual(directory.paths_of_kind("retired"), [])

    def test_rename_remove_and_member_dir(self) -> None:
        uid = "cccccccccccc"
        with self._directory as directory:
            src = directory.pending_path(uid)
            dst = directory.server_path(uid)
            directory.write_json(src, {"x": 1})
            directory.rename(src, dst)
            self.assertFalse(os.path.exists(src))
            self.assertEqual(directory.read_json(dst), {"x": 1})

            directory.remove(dst)
            self.assertFalse(os.path.exists(dst))
            # Removing a missing path is a no-op.
            directory.remove(dst)

            member = directory.member_dir(uid)
            os.makedirs(member, mode=0o700)
            with open(os.path.join(member, "inner"), "w") as f:
                f.write("data")
            directory.remove_member_dir(uid)
            self.assertFalse(os.path.exists(member))
            # Removing a missing member directory is a no-op.
            directory.remove_member_dir(uid)

    def test_states_rejects_duplicate_claimed(self) -> None:
        uid = "dddddddddddd"
        with self._directory as directory:
            directory.write_json(directory.claimed_path(111, uid), {})
            directory.write_json(directory.claimed_path(222, uid), {})
            with self.assertRaisesRegex(AssertionError, "duplicate claimed"):
                directory.states(uid)

    def test_accessors_require_the_lock(self) -> None:
        # The locked accessors must refuse to run outside the context manager.
        with self.assertRaisesRegex(AssertionError, "context manager"):
            self._directory.uids()

    def test_not_reentrant(self) -> None:
        with self._directory:
            with self.assertRaisesRegex(AssertionError, "not reentrant"):
                with self._directory:
                    pass

    @unittest.skipUnless(
        sys.platform.startswith("linux") and os.path.isdir("/proc") and hasattr(os, "waitid"),
        "requires Linux process state and waitid",
    )
    def test_pid_alive_treats_zombie_as_dead(self) -> None:
        proc = subprocess.Popen([sys.executable, "-c", "pass"])
        try:
            # Wait for the child to exit but leave it waitable, which keeps it as a zombie
            # until the finally block reaps it.
            os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOWAIT)
            self.assertFalse(_pid_alive(proc.pid))
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
        # Match SparkConnectPlanner.pythonExec's PYSPARK_PYTHON -> PYSPARK_DRIVER_PYTHON ->
        # python3 precedence for Connect UDFs, so clients never claim a server that resolved a
        # different fallback interpreter.
        os.environ.pop("PYSPARK_PYTHON")
        os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
        self.assertEqual(base, pool_fingerprint("local[*]", {"spark.sql.shuffle.partitions": "4"}))
        os.environ["PYSPARK_DRIVER_PYTHON"] = "/driver/python"
        self.assertNotEqual(
            base, pool_fingerprint("local[*]", {"spark.sql.shuffle.partitions": "4"})
        )
        # PYSPARK_DRIVER_PYTHON also feeds PythonUtils.defaultPythonExec (Python data sources),
        # which prefers it over PYSPARK_PYTHON. So even with PYSPARK_PYTHON fixed, changing
        # PYSPARK_DRIVER_PYTHON changes the server a run would boot and must change the identity.
        os.environ["PYSPARK_PYTHON"] = "/worker/python"
        worker_python = pool_fingerprint("local[*]", {"spark.sql.shuffle.partitions": "4"})
        os.environ["PYSPARK_DRIVER_PYTHON"] = "/other/driver/python"
        self.assertNotEqual(
            worker_python,
            pool_fingerprint("local[*]", {"spark.sql.shuffle.partitions": "4"}),
        )

    def test_fingerprint_resolves_server_python_from_path(self) -> None:
        # Relative worker commands are resolved through PATH by the server. Include that
        # resolution so equal command strings cannot identify different Python environments.
        executable_name = "pool-test-python"
        executable_dirs = [os.path.join(self._tmpdir, name) for name in ("env-a", "env-b")]
        for directory in executable_dirs:
            os.makedirs(directory)
            executable = os.path.join(directory, executable_name)
            with open(executable, "w") as executable_file:
                executable_file.write("#!/bin/sh\n")
            os.chmod(executable, 0o700)
        os.environ["PYSPARK_PYTHON"] = executable_name
        with mock.patch.dict(os.environ, {"PATH": executable_dirs[0]}):
            first_environment = pool_fingerprint("local[*]", {})
        with mock.patch.dict(os.environ, {"PATH": executable_dirs[1]}):
            self.assertNotEqual(first_environment, pool_fingerprint("local[*]", {}))

    def test_fingerprint_includes_python_and_spark_paths(self) -> None:
        with mock.patch.dict(os.environ, {"PYTHONPATH": "/python/a", "SPARK_HOME": "/spark/a"}):
            first_environment = pool_fingerprint("local[*]", {})
        with mock.patch.dict(os.environ, {"PYTHONPATH": "/python/b", "SPARK_HOME": "/spark/a"}):
            self.assertNotEqual(first_environment, pool_fingerprint("local[*]", {}))
        with mock.patch.dict(os.environ, {"PYTHONPATH": "/python/a", "SPARK_HOME": "/spark/b"}):
            self.assertNotEqual(first_environment, pool_fingerprint("local[*]", {}))

    def test_fingerprint_conf_order_independent_and_string_keyed(self) -> None:
        # sorted() over seed_conf makes the identity independent of dict insertion order, so a
        # run that builds the same confs in a different order still matches.
        self.assertEqual(
            pool_fingerprint("local[*]", {"a": "1", "b": "2"}),
            pool_fingerprint("local[*]", {"b": "2", "a": "1"}),
        )
        # Confs serialize to a properties file, so values are compared as strings: 1 and "1"
        # are the same seed and share an identity.
        self.assertEqual(
            pool_fingerprint("local[*]", {"k": 1}), pool_fingerprint("local[*]", {"k": "1"})
        )

    def test_fingerprint_includes_python_executable(self) -> None:
        # sys.executable is the client interpreter the server inherits; a packaging change that
        # moves it must not silently reuse a server booted under the old one.
        base = pool_fingerprint("local[*]", {})
        with mock.patch.object(sys, "executable", "/other/python"):
            self.assertNotEqual(base, pool_fingerprint("local[*]", {}))

    def test_fingerprint_includes_jvm_env(self) -> None:
        # Every JVM-shaping variable must change the identity. SPARK_CONF_DIR is the common CI
        # case (it selects the spark-defaults.conf / spark-env.sh the server reads); the rest
        # feed the classpath, heap, and JVM options. The expected list is spelled out here
        # independently rather than derived from _JVM_ENV_VARS: the fingerprint reads that same
        # tuple, so dropping a variable from it would silently leave the fingerprint AND a loop
        # over it in agreement. The equality check catches such drift (a removal or an unlisted
        # addition), and the loop proves each variable still changes the identity.
        expected = (
            "SPARK_CONF_DIR",
            "JAVA_HOME",
            "SPARK_DIST_CLASSPATH",
            "SPARK_DAEMON_MEMORY",
            "SPARK_DRIVER_MEMORY",
            "SPARK_SUBMIT_OPTS",
            "SPARK_DAEMON_JAVA_OPTS",
        )
        self.assertEqual(set(_JVM_ENV_VARS), set(expected))
        for var in expected:
            with self.subTest(var=var):
                with mock.patch.dict(os.environ, {var: "/value/a"}):
                    with_a = pool_fingerprint("local[*]", {})
                with mock.patch.dict(os.environ, {var: "/value/b"}):
                    self.assertNotEqual(with_a, pool_fingerprint("local[*]", {}))

    def test_pool_member_validation(self) -> None:
        valid = self._server_data(12345, 123, created=1)
        member = PoolMember.from_data(valid)
        self.assertIsNotNone(member)
        self.assertEqual(member.host, "localhost")
        self.assertEqual(member.port, 12345)
        self.assertEqual(member.pid, 123)
        self.assertEqual(member.process_start_id, valid["process_start_id"])
        self.assertEqual(member.created, 1.0)
        self.assertEqual(member.url, "sc://localhost:12345")

        invalid_records = {
            "missing fields": {"fingerprint": "fp"},
            "empty token": self._server_data(12345, 123, token=""),
            "non-string host": self._server_data(12345, 123, host=None),
            "empty process start id": self._server_data(12345, 123, process_start_id=""),
            "non-string process start id": self._server_data(12345, 123, process_start_id=None),
            "boolean port": self._server_data(True, 123),
            "string port": self._server_data("12345", 123),
            "fractional port": self._server_data(12345.5, 123),
            "zero port": self._server_data(0, 123),
            "out-of-range port": self._server_data(65536, 123),
            "boolean pid": self._server_data(12345, True),
            "string pid": self._server_data(12345, "123"),
            "fractional pid": self._server_data(12345, 123.5),
            "zero pid": self._server_data(12345, 0),
            "string created": self._server_data(12345, 123, created="1"),
            "boolean created": self._server_data(12345, 123, created=True),
            "negative created": self._server_data(12345, 123, created=-1),
            "nan created": self._server_data(12345, 123, created=float("nan")),
            "infinite created": self._server_data(12345, 123, created=float("inf")),
            # A finite float, but far past any real clock: rejected so it cannot look
            # perpetually fresh to age-based reaping.
            "far-future created": self._server_data(12345, 123, created=2**100),
            # Too large to convert to float at all -- the OverflowError guard in from_data keeps
            # a corrupt state file (this round-trips through json) from crashing the caller.
            "overflow created": self._server_data(12345, 123, created=10**400),
        }
        for name, data in invalid_records.items():
            with self.subTest(name=name):
                self.assertIsNone(PoolMember.from_data(data))

    def test_retired_state_fields_and_validation(self) -> None:
        retired = RetiredState.from_data(
            {"pid": 456, "process_start_id": "process-1", "retired": 2}
        )
        assert retired is not None
        self.assertEqual(retired.pid, 456)
        self.assertEqual(retired.process_start_id, "process-1")
        self.assertEqual(retired.retired, 2.0)
        self.assertFalse(retired.signalled)
        self.assertEqual(
            retired.as_data(),
            {
                "pid": 456,
                "process_start_id": "process-1",
                "retired": 2.0,
                "signalled": False,
            },
        )
        delivered = RetiredState.from_data(
            {
                "pid": 456,
                "process_start_id": "process-1",
                "retired": 2,
                "signalled": True,
            }
        )
        assert delivered is not None
        self.assertTrue(delivered.signalled)
        self.assertIsNone(
            RetiredState.from_data(
                {"pid": 456, "process_start_id": "process-1", "retired": "not-a-time"}
            )
        )
        self.assertIsNone(
            RetiredState.from_data(
                {
                    "pid": 456,
                    "process_start_id": "process-1",
                    "retired": 2,
                    "signalled": 1,
                }
            )
        )
        self.assertIsNone(RetiredState.from_data({"pid": 456, "retired": 2}))

    def test_claim_matches_fingerprint_and_renames(self) -> None:
        with _listening_socket() as port:
            server_process = self._live_process()
            self._write_state(
                self._directory.server_path("aaa"),
                self._server_data(port, server_process.pid, fingerprint="other-fp"),
            )
            self._write_state(
                self._directory.server_path("bbb"),
                self._server_data(port, server_process.pid, fingerprint="my-fp", token="t-bbb"),
            )
            with self._directory:
                member = self._pool.claim("my-fp")
        self.assertIsNotNone(member)
        self.assertEqual(member.token, "t-bbb")
        claim_name = f"claimed-{os.getpid()}-bbb.json"
        self.assertEqual(os.path.basename(member.claim_path), claim_name)
        states = self._states("bbb")
        self.assertEqual(set(states), {"claimed"})
        with self._directory as directory:
            claimed = directory.read_json(states["claimed"])
        assert claimed is not None
        self.assertEqual(
            claimed["client_process_start_id"], ServerPool._process_start_id(os.getpid())
        )
        # The mismatched member is untouched, and a second claim finds nothing.
        self.assertEqual(set(self._states("aaa")), {"server"})
        with self._directory:
            self.assertFalse(self._pool.reap("bbb"))
            self.assertIsNone(self._pool.claim("my-fp"))

    def test_claim_prefers_the_oldest_member(self) -> None:
        with _listening_socket() as port:
            server_process = self._live_process()
            for uid, created in (("aaaa", time.time()), ("bbbb", time.time() - 100)):
                self._write_state(
                    self._directory.server_path(uid),
                    self._server_data(port, server_process.pid, token="t-" + uid, created=created),
                )
            with self._directory:
                member = self._pool.claim("fp")
        # Prefer the oldest ready member. Ordering is by wall-clock created, so this is
        # approximate FIFO rather than a guarantee (see ServerPool.claim).
        self.assertEqual(member.token, "t-bbbb")

    def test_claim_requires_the_lock(self) -> None:
        # claim reaches the directory only through the locked accessors, so it inherits their
        # assertion; pin the caller obligation directly so a future reordering that touches the
        # directory before the first locked accessor is still caught.
        with self.assertRaisesRegex(AssertionError, "context manager"):
            self._pool.claim("fp")

    def test_claim_ignores_already_claimed_member(self) -> None:
        # The kind filter is the exclusion invariant: a member already renamed to claimed-* is
        # no longer of kind "server", so claim never hands it out a second time. A live, usable
        # record under a claimed-* name must still be invisible to a new claimer.
        with _listening_socket() as port:
            server_process = self._live_process()
            uid = "feed"
            self._write_state(
                self._directory.claimed_path(os.getpid() + 1, uid),
                self._server_data(port, server_process.pid),
            )
            with self._directory:
                self.assertIsNone(self._pool.claim("fp"))
        self.assertEqual(set(self._states(uid)), {"claimed"})

    def test_concurrent_claimers_claim_one_member_once(self) -> None:
        child = (
            "import sys\n"
            "from pyspark.sql.connect.local_server_pool import PoolDirectory, ServerPool\n"
            "directory = PoolDirectory(sys.argv[1])\n"
            "with directory:\n"
            "    member = ServerPool(directory).claim('fp')\n"
            "print(member.token if member is not None else 'NONE')\n"
        )
        claimers = []
        results = []
        with _listening_socket() as port:
            server_process = self._live_process()
            uid = "cafe"
            self._write_state(
                self._directory.server_path(uid),
                self._server_data(port, server_process.pid, token="claimed-once"),
            )
            try:
                with self._directory:
                    for _ in range(2):
                        claimers.append(
                            subprocess.Popen(
                                [sys.executable, "-c", child, self._directory.path],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True,
                            )
                        )
                for proc in claimers:
                    stdout, stderr = proc.communicate(timeout=20)
                    self.assertEqual(proc.returncode, 0, stderr)
                    results.append(stdout.strip())
            finally:
                for proc in claimers:
                    if proc.poll() is None:
                        proc.kill()
                        proc.communicate(timeout=10)

        # Pins "claimed at most once": one child claims the member and the other sees none. It
        # does not guarantee the two ever contend on the lock -- if the first child finishes
        # before the second reaches flock(), the second simply finds no "server" entry -- so
        # read this as a regression test for the exclusion invariant, not for lock contention.
        self.assertEqual(sorted(results), ["NONE", "claimed-once"])
        states = self._states(uid)
        self.assertEqual(set(states), {"claimed"})
        claiming_pid = PoolDirectory.claiming_pid(states["claimed"])
        self.assertIn(claiming_pid, [proc.pid for proc in claimers])

    def test_claim_skips_unreachable_member(self) -> None:
        with _non_listening_socket() as port:
            self._write_state(
                self._directory.server_path("ccc"), self._server_data(port, os.getpid())
            )
            with self._directory:
                self.assertIsNone(self._pool.claim("fp"))

    def test_claim_skips_directory_with_state_filename(self) -> None:
        os.makedirs(self._directory.path)
        os.makedirs(self._directory.server_path("dead"))
        with _listening_socket() as port:
            server_process = self._live_process()
            self._write_state(
                self._directory.server_path("cafe"), self._server_data(port, server_process.pid)
            )
            with self._directory:
                member = self._pool.claim("fp")

        self.assertIsNotNone(member)
        self.assertEqual(os.path.basename(member.claim_path), f"claimed-{os.getpid()}-cafe.json")

    def test_claim_skips_member_with_mismatched_process_identity(self) -> None:
        with _listening_socket() as port:
            server_process = self._live_process()
            self._write_state(
                self._directory.server_path("fade"),
                self._server_data(
                    port,
                    server_process.pid,
                    process_start_id="a-different-process-generation",
                ),
            )
            with self._directory:
                self.assertIsNone(self._pool.claim("fp"))

        self.assertEqual(set(self._states("fade")), {"server"})
        self.assertIsNone(server_process.poll())

    def test_claim_skips_malformed_and_incompatible_members(self) -> None:
        with _listening_socket() as port:
            server_process = self._live_process()
            bad_pid = self._server_data(port, server_process.pid)
            bad_pid["pid"] = "not-a-pid"
            bad_port = self._server_data(port, server_process.pid)
            bad_port["port"] = "not-a-port"
            bad_created = self._server_data(port, server_process.pid)
            bad_created["created"] = "not-a-time"
            non_finite_created = self._server_data(port, server_process.pid)
            non_finite_created["created"] = float("nan")
            out_of_range_port = self._server_data(port, server_process.pid)
            out_of_range_port["port"] = 65536
            bad_host = self._server_data(port, server_process.pid)
            bad_host["host"] = None
            records = {
                "a0": {"fingerprint": "fp"},
                "a1": bad_pid,
                "a2": bad_port,
                "a3": bad_created,
                "a4": non_finite_created,
                "a5": out_of_range_port,
                "a6": bad_host,
                "a7": self._server_data(port, server_process.pid, spark_version="not-this-version"),
                "a8": self._server_data(port, 2**31 - 1),
                "a9": self._server_data(port, 2**100),
            }
            for uid, data in records.items():
                self._write_state(self._directory.server_path(uid), data)
            self._write_state(
                self._directory.server_path("b0"),
                self._server_data(port, server_process.pid, token="valid-token"),
            )

            with self._directory:
                member = self._pool.claim("fp")

        self.assertIsInstance(member, PoolMember)
        self.assertEqual(member.token, "valid-token")
        for uid in records:
            self.assertEqual(set(self._states(uid)), {"server"})

    def test_idle_timeout_configuration(self) -> None:
        for value in ("0", "-1"):
            with self.subTest(value=value):
                os.environ["SPARK_LOCAL_CONNECT_POOL_IDLE_TIMEOUT"] = value
                self.assertEqual(self._pool._idle_timeout(), int(value))

        os.environ["SPARK_LOCAL_CONNECT_POOL_IDLE_TIMEOUT"] = "not-an-integer"
        self.assertEqual(self._pool._idle_timeout(), ServerPool._DEFAULT_IDLE_TIMEOUT_SECONDS)

    def test_reap_server_unreachable_and_idle(self) -> None:
        with self.subTest("unreachable member is retired"):
            gone = self._live_process()
            with _non_listening_socket() as port:
                self._write_state(
                    self._directory.server_path("dead"), self._server_data(port, gone.pid)
                )
                with self._directory:
                    self._pool.reap("dead")
            self.assertEqual(set(self._states("dead")), {"retired"})
            self.assertTrue(_wait_proc_dead(gone))
        with self.subTest("member idle past the timeout is retired"):
            os.environ["SPARK_LOCAL_CONNECT_POOL_IDLE_TIMEOUT"] = "10"
            with _listening_socket() as port:
                idle = self._live_process()
                self._write_state(
                    self._directory.server_path("1d1e"),
                    self._server_data(port, idle.pid, created=time.time() - 60),
                )
                with self._directory:
                    self._pool.janitor()
                self.assertEqual(set(self._states("1d1e")), {"retired"})
                self.assertTrue(_wait_proc_dead(idle))

    def test_reap_server_does_not_expire_when_idle_retirement_is_disabled(self) -> None:
        with _listening_socket() as port:
            for uid, value in (("d150", "0"), ("d151", "-1")):
                with self.subTest(value=value):
                    os.environ["SPARK_LOCAL_CONNECT_POOL_IDLE_TIMEOUT"] = value
                    server = self._live_process()
                    self._write_state(
                        self._directory.server_path(uid),
                        self._server_data(port, server.pid, created=0),
                    )

                    with self._directory:
                        self.assertFalse(self._pool.reap(uid))

                    self.assertEqual(set(self._states(uid)), {"server"})
                    self.assertIsNone(server.poll())

    def test_janitor_preserves_healthy_non_idle_server(self) -> None:
        os.environ["SPARK_LOCAL_CONNECT_POOL_IDLE_TIMEOUT"] = "10"
        server = self._live_process()
        with _listening_socket() as port:
            self._write_state(
                self._directory.server_path("f2e5"), self._server_data(port, server.pid)
            )

            with self._directory:
                self._pool.janitor()

        self.assertEqual(set(self._states("f2e5")), {"server"})
        self.assertIsNone(server.poll())

    def test_reap_does_not_signal_reused_server_pid(self) -> None:
        unrelated = subprocess.Popen(
            [sys.executable, "-c", "import sys; sys.stdin.buffer.read()"],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._procs.append(unrelated)
        with _non_listening_socket() as port:
            self._write_state(
                self._directory.server_path("bad6"), self._server_data(port, unrelated.pid)
            )
            with self._directory:
                self._pool.reap("bad6")

        self.assertEqual(set(self._states("bad6")), {"retired"})
        self.assertIsNone(unrelated.poll())

    def test_reap_malformed_member_recovers_server_pid(self) -> None:
        # An out-of-range created value makes the full member invalid, but its independently
        # valid pid must still be retired and tracked rather than leaving a live JVM orphaned.
        invalid_created = self._live_process()
        with _non_listening_socket() as port:
            self._write_state(
                self._directory.server_path("bad0"),
                self._server_data(port, invalid_created.pid, created=2**100),
            )
            with self._directory:
                self._pool.reap("bad0")
        states = self._states("bad0")
        self.assertEqual(set(states), {"retired"})
        with self._directory as directory:
            self.assertEqual(directory.read_json(states["retired"])["pid"], invalid_created.pid)
        self.assertTrue(_wait_proc_dead(invalid_created))

        # Claimed records use the same recovery when their client has disappeared.
        claimed = self._live_process()
        with _non_listening_socket() as port:
            self._write_state(
                self._directory.claimed_path(2**31 - 1, "bad2"),
                self._server_data(port, claimed.pid, created=2**100),
            )
            with self._directory:
                self._pool.reap("bad2")
        states = self._states("bad2")
        self.assertEqual(set(states), {"retired"})
        with self._directory as directory:
            self.assertEqual(directory.read_json(states["retired"])["pid"], claimed.pid)
        self.assertTrue(_wait_proc_dead(claimed))

        # If the record itself has no usable pid, fall back to spark-daemon.sh's pid file. The
        # fallback keeps the shutdown tracked but lacks the process identity needed to signal it.
        unreadable_pid = self._live_process()
        self._write_daemon_pid("bad1", unreadable_pid.pid)
        self._write_state(self._directory.server_path("bad1"), {"malformed": True})
        with self._directory:
            self._pool.reap("bad1")
        states = self._states("bad1")
        self.assertEqual(set(states), {"member", "retired"})
        with self._directory as directory:
            retired = directory.read_json(states["retired"])
            assert retired is not None
            self.assertEqual(retired["pid"], unreadable_pid.pid)
            self.assertNotIn("process_start_id", retired)
        self.assertIsNone(unreadable_pid.poll())

    def test_reap_claimed_of_dead_client(self) -> None:
        orphan = self._live_process()
        dead_client = 2**31 - 1
        ours = self._live_process()
        with _non_listening_socket() as port:
            self._write_state(
                self._directory.claimed_path(dead_client, "0a0a"),
                self._server_data(port, orphan.pid),
            )
            self._write_state(
                self._directory.claimed_path(os.getpid(), "0b0b"),
                self._server_data(port, ours.pid),
            )
            self._write_state(
                self._directory.claimed_path(os.getpid(), "0c0c"),
                self._server_data(port, 2**31 - 1),
            )
            with self._directory:
                self._pool.janitor()
        # The orphaned claim and our dead server are retired; our live claim is untouched.
        self.assertEqual(set(self._states("0a0a")), {"retired"})
        self.assertTrue(_wait_proc_dead(orphan), "the orphaned server was not stopped")
        self.assertEqual(set(self._states("0b0b")), {"claimed"})
        self.assertIsNone(ours.poll())
        self.assertEqual(set(self._states("0c0c")), {"retired"})

    def test_reap_claimed_detects_a_reused_client_pid(self) -> None:
        server = self._live_process()
        data = self._server_data(12345, server.pid)
        data["client_process_start_id"] = "a-different-process-generation"
        self._write_state(self._directory.claimed_path(os.getpid(), "bad5"), data)

        with self._directory:
            self.assertFalse(self._pool.reap("bad5"))

        self.assertEqual(set(self._states("bad5")), {"retired"})
        self.assertTrue(_wait_proc_dead(server), "the orphaned server was not stopped")

    def test_reap_claimed_detects_a_reused_server_pid(self) -> None:
        unrelated = self._stubborn_process()
        data = self._server_data(
            12345,
            unrelated.pid,
            process_start_id="a-different-process-generation",
        )
        self._write_state(self._directory.claimed_path(os.getpid(), "bad6"), data)

        with self._directory:
            self.assertFalse(self._pool.reap("bad6"))

        self.assertEqual(set(self._states("bad6")), {"retired"})
        self.assertIsNone(unrelated.poll())

    def test_reap_retired_escalates_to_sigkill(self) -> None:
        with self.subTest("a fresh retirement is left to shut down gracefully"):
            fresh = self._stubborn_process()
            self._write_state(
                self._directory.retired_path("f2e5"),
                self._retired_data(fresh.pid, time.time()),
            )
            with self._directory:
                self._pool.reap("f2e5")
            self.assertEqual(set(self._states("f2e5")), {"retired"})
            self.assertIsNone(fresh.poll())
        with self.subTest("a hung shutdown is hard-killed"):
            stubborn = self._stubborn_process()
            self._write_state(
                self._directory.retired_path("a0a0"),
                self._retired_data(
                    stubborn.pid, time.time() - ServerPool._RETIRE_KILL_AFTER_SECONDS - 1
                ),
            )
            with self._directory:
                self._pool.reap("a0a0")
            self.assertTrue(_wait_proc_dead(stubborn), "SIGKILL escalation did not happen")
            with self._directory:
                self.assertTrue(self._pool.reap("a0a0"))
        with self.subTest("a late first reaper hard-kills before giving up"):
            abandoned = self._stubborn_process()
            self._write_state(
                self._directory.retired_path("ab4d"),
                self._retired_data(
                    abandoned.pid, time.time() - ServerPool._RETIRE_GIVE_UP_AFTER_SECONDS - 1
                ),
            )
            with self._directory:
                self.assertTrue(self._pool.reap("ab4d"))
            self.assertTrue(_wait_proc_dead(abandoned), "the abandoned server was not killed")

    def test_reap_repairs_malformed_retired_state(self) -> None:
        server = self._stubborn_process()
        malformed = self._retired_data(server.pid, "not-a-time")
        malformed["signalled"] = True
        path = self._write_state(
            self._directory.retired_path("bad4"),
            malformed,
        )

        with self._directory as directory:
            self._pool.reap("bad4")
            repaired = directory.read_json(path)

        assert repaired is not None
        self.assertEqual(repaired["pid"], server.pid)
        self.assertIsInstance(repaired["retired"], float)
        self.assertTrue(repaired["signalled"])
        self.assertIsNone(server.poll())

    def test_reap_keeps_retired_state_when_process_inspection_fails(self) -> None:
        server = self._stubborn_process()
        path = self._write_state(
            self._directory.retired_path("bad9"),
            self._retired_data(
                server.pid, time.time() - ServerPool._RETIRE_GIVE_UP_AFTER_SECONDS - 1
            ),
        )

        with mock.patch.object(local_server_pool, "_is_local_connect_server", return_value=None):
            with self._directory:
                self.assertFalse(self._pool.reap("bad9"))

        self.assertEqual(set(self._states("bad9")), {"retired"})
        self.assertTrue(os.path.exists(path))
        self.assertIsNone(server.poll())

    def test_reap_keeps_retired_state_without_a_process_identity(self) -> None:
        server = self._stubborn_process()
        original = {
            "pid": server.pid,
            "retired": time.time() - ServerPool._RETIRE_GIVE_UP_AFTER_SECONDS - 1,
        }
        path = self._write_state(
            self._directory.retired_path("bad7"),
            original,
        )

        with self._directory as directory:
            self.assertFalse(self._pool.reap("bad7"))
            self.assertFalse(self._pool.reap("bad7"))
            retained = directory.read_json(path)

        self.assertEqual(retained, original)
        self.assertIsNone(server.poll())

    def test_reap_eventually_removes_state_without_a_process_handle(self) -> None:
        uid = "fade"
        self._write_state(self._directory.server_path(uid), {"malformed": True})

        with self._directory as directory:
            self.assertFalse(self._pool.reap(uid))
            states = self._directory.states(uid)
            self.assertEqual(set(states), {"retired"})
            retired = directory.read_json(states["retired"])
            assert retired is not None
            self.assertNotIn("pid", retired)
            retired["retired"] = time.time() - ServerPool._RETIRE_GIVE_UP_AFTER_SECONDS - 1
            directory.write_json(states["retired"], retired)
            self.assertTrue(self._pool.reap(uid))

        self.assertFalse(self._states(uid))

    def test_reap_keeps_a_daemon_pid_without_a_process_identity(self) -> None:
        server = self._stubborn_process()
        uid = "daed"
        original = {"retired": time.time() - ServerPool._RETIRE_GIVE_UP_AFTER_SECONDS - 1}
        path = self._write_state(
            self._directory.retired_path(uid),
            original,
        )
        self._write_daemon_pid(uid, server.pid)

        with self._directory as directory:
            self.assertFalse(self._pool.reap(uid))
            retained = directory.read_json(path)

        self.assertEqual(retained, original)
        self.assertIsNone(server.poll())

        server.kill()
        self.assertTrue(_wait_proc_dead(server))
        with self._directory:
            self.assertTrue(self._pool.reap(uid))
        self.assertFalse(os.path.exists(path))
        self.assertFalse(os.path.exists(self._directory.member_dir(uid)))

    def test_reap_prefers_the_record_pid_and_its_process_identity(self) -> None:
        recorded_server = self._stubborn_process()
        daemon_server = self._stubborn_process()
        uid = "d00d"
        path = self._write_state(
            self._directory.retired_path(uid),
            self._server_data(12345, recorded_server.pid),
        )
        self._write_daemon_pid(uid, daemon_server.pid)

        with self._directory as directory:
            self.assertFalse(self._pool.reap(uid))
            repaired = directory.read_json(path)

        assert repaired is not None
        self.assertEqual(repaired["pid"], recorded_server.pid)
        self.assertEqual(
            repaired["process_start_id"], ServerPool._process_start_id(recorded_server.pid)
        )
        self.assertIsNone(recorded_server.poll())
        self.assertIsNone(daemon_server.poll())

    def test_reap_does_not_signal_a_reused_server_pid(self) -> None:
        other_server = self._stubborn_process()
        stale = self._retired_data(
            other_server.pid, time.time() - ServerPool._RETIRE_KILL_AFTER_SECONDS - 1
        )
        stale["process_start_id"] = "a-different-process-generation"
        self._write_state(self._directory.retired_path("bad8"), stale)

        with self._directory:
            self.assertTrue(self._pool.reap("bad8"))

        self.assertIsNone(other_server.poll())

    def test_retire_survives_interrupted_state_rewrite(self) -> None:
        server = self._stubborn_process()
        with _non_listening_socket() as port:
            server_path = self._write_state(
                self._directory.server_path("c0de"), self._server_data(port, server.pid)
            )
            with self._directory:
                with mock.patch.object(
                    local_server_pool.os,
                    "replace",
                    side_effect=OSError("interrupted rewrite"),
                ):
                    with self.assertRaisesRegex(OSError, "interrupted rewrite"):
                        process_start_id = ServerPool._process_start_id(server.pid)
                        assert process_start_id is not None
                        self._pool._retire(server_path, server.pid, process_start_id)

        states = self._states("c0de")
        self.assertEqual(set(states), {"retired"})
        with self._directory as directory:
            # The atomic rename preserved the old member payload. The next reaper recovers its
            # pid, repairs the missing retirement timestamp, and keeps tracking the live JVM.
            self._pool.reap("c0de")
            repaired = directory.read_json(states["retired"])
        assert repaired is not None
        self.assertEqual(repaired["pid"], server.pid)
        self.assertIsInstance(repaired["retired"], float)
        self.assertIsNone(server.poll())

    def test_reap_retries_sigterm_only_until_delivered(self) -> None:
        server = self._stubborn_process()
        process_start_id = ServerPool._process_start_id(server.pid)
        assert process_start_id is not None
        state_path = self._write_state(
            self._directory.server_path("fade"),
            self._server_data(12345, server.pid),
        )

        with self._directory as directory:
            with mock.patch.object(
                ServerPool,
                "_signal_server",
                side_effect=[False, False, True],
            ) as signal_server:
                self._pool._retire(state_path, server.pid, process_start_id)
                retired_path = self._directory.retired_path("fade")
                initial = directory.read_json(retired_path)
                assert initial is not None
                self.assertFalse(initial["signalled"])

                self.assertFalse(self._pool.reap("fade"))
                failed_retry = directory.read_json(retired_path)
                self.assertEqual(failed_retry, initial)

                self.assertFalse(self._pool.reap("fade"))
                delivered = directory.read_json(retired_path)
                assert delivered is not None
                self.assertTrue(delivered["signalled"])
                self.assertEqual(delivered["retired"], initial["retired"])

                self.assertFalse(self._pool.reap("fade"))
                self.assertEqual(signal_server.call_count, 3)

        self.assertIsNone(server.poll())

    def test_release_retires_the_claimed_member(self) -> None:
        server = self._live_process()
        with _non_listening_socket() as port:
            server_data = self._server_data(port, server.pid)
            claim_path = self._write_state(
                self._directory.claimed_path(os.getpid(), "a1a1"), server_data
            )
            member = PoolMember(server_data)
            member.claim_path = claim_path
            local_server_pool._claimed_member = member

            # Release must use the directory that owns the claim even if the override changes
            # between acquisition and process-exit cleanup.
            os.environ["SPARK_LOCAL_CONNECT_POOL_DIR"] = os.path.join(self._tmpdir, "other-pool")
            local_server_pool.release_pooled_local_connect_server()

        self.assertIsNone(local_server_pool._claimed_member)
        states = self._states("a1a1")
        self.assertEqual(set(states), {"retired"})
        with self._directory as directory:
            retired = directory.read_json(states["retired"])
            assert retired is not None
            self.assertEqual(retired["pid"], server.pid)
            self.assertTrue(retired["signalled"])
        self.assertTrue(_wait_proc_dead(server), "release did not stop the server")
        # Releasing again is a no-op.
        local_server_pool.release_pooled_local_connect_server()

    def test_release_does_not_signal_a_mismatched_process_identity(self) -> None:
        server = self._live_process()
        server_data = self._server_data(
            12345,
            server.pid,
            process_start_id="a-different-process-generation",
        )
        claim_path = self._write_state(
            self._directory.claimed_path(os.getpid(), "a1a4"), server_data
        )
        member = PoolMember(server_data)
        member.claim_path = claim_path

        self._pool.release(member)

        self.assertEqual(set(self._states("a1a4")), {"retired"})
        self.assertIsNone(server.poll())

    def test_forked_child_does_not_release_its_parents_claim(self) -> None:
        server = self._live_process()
        with _non_listening_socket() as port:
            server_data = self._server_data(port, server.pid)
            parent_pid = os.getpid() + 1
            claim_path = self._write_state(
                self._directory.claimed_path(parent_pid, "a1a3"), server_data
            )
            member = PoolMember(server_data)
            member.claim_path = claim_path

            self._pool.release(member)

        self.assertEqual(set(self._states("a1a3")), {"claimed"})
        self.assertIsNone(server.poll())

    def test_release_retries_failures_and_tolerates_prior_retirement(self) -> None:
        server = self._stubborn_process()
        with _non_listening_socket() as port:
            server_data = self._server_data(port, server.pid)
            claim_path = self._write_state(
                self._directory.claimed_path(os.getpid(), "a1a2"), server_data
            )
            member = PoolMember(server_data)
            member.claim_path = claim_path
            local_server_pool._claimed_member = member

            with mock.patch.object(
                PoolDirectory, "write_json", side_effect=OSError("interrupted retirement")
            ):
                with self.assertRaisesRegex(OSError, "interrupted retirement"):
                    local_server_pool.release_pooled_local_connect_server()
            self.assertIs(local_server_pool._claimed_member, member)
            self.assertEqual(set(self._states("a1a2")), {"retired"})

            local_server_pool.release_pooled_local_connect_server()

        self.assertIsNone(local_server_pool._claimed_member)
        self.assertEqual(set(self._states("a1a2")), {"retired"})
        # A concurrent janitor already completed the claim -> retired transition.
        self._pool.release(member)
        self.assertEqual(set(self._states("a1a2")), {"retired"})


if __name__ == "__main__":
    from pyspark.testing import main

    main()
