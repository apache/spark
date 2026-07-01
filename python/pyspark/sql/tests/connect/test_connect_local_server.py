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
import tempfile
import time
import unittest

from pyspark.util import is_remote_only
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

if should_test_connect:
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
                RemoteSparkSession._stop_local_connect_server()
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
        deadline = time.time() + 30
        closed = False
        while time.time() < deadline:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.5)
                if sock.connect_ex((host, int(port))) != 0:
                    closed = True
                    break
            time.sleep(0.5)
        self.assertTrue(closed, "server port {} still open after stop".format(port))

    def test_start_seeds_static_conf_on_the_server(self) -> None:
        """A start-up conf passed by the first caller reaches the daemon's SparkConf.

        Uses a static conf (``spark.sql.warehouse.dir``) that the per-session ``_apply_options``
        path cannot set on an already-running JVM, so observing it on the server proves the seed
        conf was forwarded rather than applied client-side afterwards.
        """
        warehouse = os.path.join(self._tmpdir, "seeded-wh")
        opts = {"spark.sql.warehouse.dir": warehouse}
        endpoint = RemoteSparkSession._reuse_or_start_local_connect_server("local[2]", opts)
        spark = None
        try:
            spark = RemoteSparkSession.builder.remote(endpoint).create()
            # Spark normalizes the warehouse dir to a file: URI; the path still identifies our dir,
            # which proves the seed conf reached the daemon (a per-session apply cannot set it).
            self.assertTrue(spark.conf.get("spark.sql.warehouse.dir").endswith(warehouse))
        finally:
            if spark is not None:
                self._release(spark)
            RemoteSparkSession._stop_local_connect_server()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
