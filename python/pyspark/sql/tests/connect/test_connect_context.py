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
# PEP 563: make annotations lazy strings so any annotation referencing a
# grpc-only connect type is never evaluated at class-definition time. The
# connect.context imports below are guarded by should_test_connect, so on a
# classic-only / no-grpc image those names are undefined; lazy annotations keep
# module collection from raising NameError there.
from __future__ import annotations

import os
import warnings
from unittest.mock import patch

from pyspark.errors import PySparkNotImplementedError
from pyspark.sql import HiveContext as ClassicHiveContext
from pyspark.sql import SQLContext as ClassicSQLContext
from pyspark.testing.connectutils import ReusedConnectTestCase, should_test_connect

if should_test_connect:
    from pyspark.sql.connect.context import HiveContext, SQLContext


class SQLContextConnectTests(ReusedConnectTestCase):
    """Connect-specific SQLContext tests not covered by the parity mixin."""

    def setUp(self) -> None:
        super().setUp()
        SQLContext._instantiatedContext = None

    def tearDown(self) -> None:
        super().tearDown()
        SQLContext._instantiatedContext = None

    def _release_session(self, session) -> None:
        """Release only the given session server-side and close its client channel.

        We must NOT call SparkSession.stop() here: under SPARK_LOCAL_REMOTE (the test
        harness) it terminates the shared local Connect server, breaking the rest of
        the suite. Leaving the client open is also wrong -- once tearDownClass stops
        the shared server, its atexit _on_exit -> _cleanup_ml_cache retries against
        the dead server and hangs until the test times out.
        """
        client = session.client
        try:
            client.release_session()
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass

    def test_init_emits_deprecation_warning(self) -> None:
        with self.assertWarns(FutureWarning):
            SQLContext(self.spark)

    def test_registerJavaFunction_raises(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            ctx = SQLContext(self.spark)
        with self.assertRaises(PySparkNotImplementedError):
            ctx.registerJavaFunction("f", "com.example.F")

    def test_hive_context_raises(self) -> None:
        with self.assertRaises(PySparkNotImplementedError):
            HiveContext(self.spark)

    def test_getOrCreate_emits_deprecation_and_returns_connect_context(self) -> None:
        """SQLContext.getOrCreate() in Connect mode returns a Connect-backed context."""
        with patch("pyspark.sql.utils.is_remote", return_value=True):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                ctx = ClassicSQLContext.getOrCreate()
        self.assertTrue(any(issubclass(w.category, FutureWarning) for w in caught))
        self.assertIsInstance(ctx, SQLContext)

    def test_newSession_returns_fresh_state(self) -> None:
        """Connect newSession() returns a fresh, independent session that does NOT inherit
        the parent's state (e.g. temp views), matching classic newSession() semantics."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            ctx = SQLContext(self.spark)
        self.spark.createDataFrame([(1,)], ["x"]).createOrReplaceTempView("ctx_fresh_view")
        ctx2 = None
        try:
            ctx2 = ctx.newSession()
            # newSession() starts with empty state, so the parent's temp view is not visible.
            self.assertNotIn("ctx_fresh_view", ctx2.tableNames())
            # The new session bypasses SparkSession.__init__, so make sure it still
            # carries the attributes that SparkSession.stop() reads.
            self.assertTrue(ctx2.sparkSession.release_session_on_close)
        finally:
            if ctx2 is not None:
                self._release_session(ctx2.sparkSession)
            self.spark.catalog.dropTempView("ctx_fresh_view")

    def test_get_or_create_from_session_caches_per_session(self) -> None:
        """_get_or_create_from_session returns the cached context for the same session,
        and replaces it with a new context when the active session changes."""
        ctx1 = SQLContext._get_or_create_from_session(self.spark)
        self.assertIs(SQLContext._get_or_create_from_session(self.spark), ctx1)
        self.assertIs(ctx1.sparkSession, self.spark)

        other = self.spark.newSession()
        try:
            ctx2 = SQLContext._get_or_create_from_session(other)
            self.assertIsNot(ctx2, ctx1)
            self.assertIs(ctx2.sparkSession, other)
            self.assertIs(SQLContext._instantiatedContext, ctx2)
        finally:
            self._release_session(other)

    def test_stop_clears_cached_context_only_for_stopped_session(self) -> None:
        """SparkSession.stop() resets the SQLContext cache only when the cache wraps
        the stopped session, so a later getOrCreate() builds a fresh context."""
        # Stopping a session other than the cached context's one leaves the cache alone.
        ctx_shared = SQLContext._get_or_create_from_session(self.spark)
        other = self.spark.newSession()
        try:
            # Mask SPARK_LOCAL_REMOTE so stop() only releases this one session instead
            # of terminating the shared local Connect server used by the whole suite.
            # patch.dict restores the original environment on exit.
            with patch.dict(os.environ):
                os.environ.pop("SPARK_LOCAL_REMOTE", None)
                other.stop()
            self.assertIs(SQLContext._instantiatedContext, ctx_shared)
        finally:
            self._release_session(other)

        # Stopping the session the cached context wraps clears the cache, and the next
        # getOrCreate builds a fresh context for the new active session.
        other2 = self.spark.newSession()
        try:
            ctx_other = SQLContext._get_or_create_from_session(other2)
            self.assertIs(SQLContext._instantiatedContext, ctx_other)
            with patch.dict(os.environ):
                os.environ.pop("SPARK_LOCAL_REMOTE", None)
                other2.stop()
            self.assertIsNone(SQLContext._instantiatedContext)

            ctx_new = SQLContext._get_or_create_from_session(self.spark)
            self.assertIsNot(ctx_new, ctx_other)
            self.assertIs(ctx_new.sparkSession, self.spark)
        finally:
            self._release_session(other2)

    def test_hive_context_getOrCreate_raises(self) -> None:
        """HiveContext.getOrCreate() in Connect mode raises PySparkNotImplementedError."""
        with patch("pyspark.sql.utils.is_remote", return_value=True):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", FutureWarning)
                with self.assertRaises(PySparkNotImplementedError):
                    ClassicHiveContext.getOrCreate()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
