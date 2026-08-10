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

from pyspark.util import is_remote_only
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

if should_test_connect:
    from pyspark.sql.connect.local_server_pool import PoolDirectory


_SAVED_ENV_KEYS = ("SPARK_LOCAL_CONNECT_POOL_DIR",)


@unittest.skipIf(
    not should_test_connect or is_remote_only(),
    connect_requirement_message or "Requires Spark Connect test dependencies",
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


if __name__ == "__main__":
    from pyspark.testing import main

    main()
