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

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

if should_test_connect:
    from pyspark.sql.connect.local_server_pool import PoolDirectory


_SAVED_ENV_KEYS = ("SPARK_LOCAL_CONNECT_POOL_DIR",)


# These tests start no server and exercise only stdlib filesystem code, so they do not need JVM
# access (no is_remote_only gate). should_test_connect is still required because importing
# PoolDirectory pulls in the pyspark.sql.connect package, which checks Connect dependencies.
@unittest.skipIf(
    not should_test_connect,
    connect_requirement_message or "Requires Spark Connect dependencies to import the pool module",
)
@unittest.skipUnless(os.name == "posix", "the pool relies on POSIX file locks")
class LocalConnectServerPoolUnitTests(unittest.TestCase):
    """Tests for the pool filesystem storage; no real servers are started."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self._saved_env = {k: os.environ.get(k) for k in _SAVED_ENV_KEYS}
        for k in _SAVED_ENV_KEYS:
            os.environ.pop(k, None)
        os.environ["SPARK_LOCAL_CONNECT_POOL_DIR"] = os.path.join(self._tmpdir, "pool")
        self._directory = PoolDirectory()

    def tearDown(self) -> None:
        for k, v in self._saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_pool_directory_location(self) -> None:
        self.assertEqual(self._directory.path, os.path.join(self._tmpdir, "pool"))
        os.environ.pop("SPARK_LOCAL_CONNECT_POOL_DIR")
        default = PoolDirectory()
        self.assertEqual(os.path.basename(default.path), "pool")
        self.assertTrue(default.path.startswith(tempfile.gettempdir()))

    def test_pool_directory_lock_and_state_file_permissions(self) -> None:
        os.makedirs(self._directory.path, mode=0o755)
        os.chmod(self._directory.path, 0o755)
        state_path = self._directory.server_path("secure")
        with self._directory as directory:
            self.assertEqual(os.stat(directory.path).st_mode & 0o777, 0o700)
            lock_path = os.path.join(directory.path, ".lock")
            self.assertEqual(os.stat(lock_path).st_mode & 0o777, 0o600)
            directory.write_json(state_path, {"token": "secret"})
            self.assertEqual(os.stat(state_path).st_mode & 0o777, 0o600)
            self.assertEqual(directory.read_json(state_path), {"token": "secret"})

            # O_CREAT does not apply its mode to an existing file, so writes must re-assert it.
            os.chmod(state_path, 0o644)
            directory.write_json(state_path, {"token": "new-secret"})
            self.assertEqual(os.stat(state_path).st_mode & 0o777, 0o600)

        with open(state_path, "w") as state_file:
            state_file.write("not json")
        with self._directory as directory:
            self.assertIsNone(directory.read_json(state_path))

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


if __name__ == "__main__":
    from pyspark.testing import main

    main()
