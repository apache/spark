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
import unittest
import uuid

from pyspark.util import is_remote_only
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession as PySparkSession
from pyspark.testing.connectutils import (
    should_test_connect,
    ReusedConnectTestCase,
    connect_requirement_message,
)
from pyspark.testing.utils import timeout

if should_test_connect:
    import grpc
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
    from pyspark.sql.connect.client import ChannelBuilder
    from pyspark.errors.exceptions.connect import (
        AnalysisException,
        SparkConnectException,
        SparkConnectGrpcException,
        SparkUpgradeException,
    )


@unittest.skipIf(is_remote_only(), "Session creation different from local mode")
class SparkConnectSessionTests(ReusedConnectTestCase):
    def setUp(self) -> None:
        self.spark = (
            PySparkSession.builder.config(conf=self.conf())
            .appName(self.__class__.__name__)
            .remote("local[4]")
            .getOrCreate()
        )

    def tearDown(self):
        self.spark.stop()

    @timeout(10)
    def test_progress_handler(self):
        handler_called = []

        def handler(**kwargs):
            nonlocal handler_called
            handler_called.append(kwargs)

        self.spark.registerProgressHandler(handler)
        self.spark.sql("select 1").collect()
        self.assertGreaterEqual(len(handler_called), 1)

        handler_called = []
        self.spark.removeProgressHandler(handler)
        self.spark.sql("select 1").collect()
        self.assertEqual(len(handler_called), 0)

        self.spark.registerProgressHandler(handler)
        self.spark.clearProgressHandlers()
        self.spark.sql("select 1").collect()
        self.assertGreaterEqual(len(handler_called), 0)

    def _check_no_active_session_error(self, e: PySparkException):
        self.check_error(exception=e, errorClass="NO_ACTIVE_SESSION", messageParameters=dict())

    @timeout(10)
    def test_stop_session(self):
        df = self.spark.sql("select 1 as a, 2 as b")
        catalog = self.spark.catalog
        self.spark.stop()

        # _execute_and_fetch
        with self.assertRaises(SparkConnectException) as e:
            self.spark.sql("select 1")
        self._check_no_active_session_error(e.exception)

        with self.assertRaises(SparkConnectException) as e:
            catalog.tableExists("table")
        self._check_no_active_session_error(e.exception)

        # _execute
        with self.assertRaises(SparkConnectException) as e:
            self.spark.udf.register("test_func", lambda x: x + 1)
        self._check_no_active_session_error(e.exception)

        # _analyze
        with self.assertRaises(SparkConnectException) as e:
            df._explain_string(extended=True)
        self._check_no_active_session_error(e.exception)

        # Config
        with self.assertRaises(SparkConnectException) as e:
            self.spark.conf.get("some.conf")
        self._check_no_active_session_error(e.exception)

    def test_error_enrichment_message(self):
        with self.sql_conf(
            {
                "spark.sql.connect.enrichError.enabled": True,
                "spark.sql.connect.serverStacktrace.enabled": False,
                "spark.sql.pyspark.jvmStacktrace.enabled": False,
            }
        ):
            name = "test" * 10000
            with self.assertRaises(AnalysisException) as e:
                self.spark.sql("select " + name).collect()
            self.assertTrue(name in e.exception._message)
            self.assertFalse("JVM stacktrace" in e.exception._message)

    def test_error_enrichment_jvm_stacktrace(self):
        with self.sql_conf(
            {
                "spark.sql.connect.enrichError.enabled": True,
                "spark.sql.pyspark.jvmStacktrace.enabled": False,
                "spark.sql.legacy.timeParserPolicy": "EXCEPTION",
            }
        ):
            with self.sql_conf({"spark.sql.connect.serverStacktrace.enabled": False}):
                with self.assertRaises(SparkUpgradeException) as e:
                    self.spark.sql(
                        """select from_json(
                            '{"d": "02-29"}', 'd date', map('dateFormat', 'MM-dd'))"""
                    ).collect()
                self.assertFalse("JVM stacktrace" in e.exception._message)

            with self.sql_conf({"spark.sql.connect.serverStacktrace.enabled": True}):
                with self.assertRaises(SparkUpgradeException) as e:
                    self.spark.sql(
                        """select from_json(
                            '{"d": "02-29"}', 'd date', map('dateFormat', 'MM-dd'))"""
                    ).collect()
                self.assertTrue("JVM stacktrace" in str(e.exception))
                self.assertTrue("org.apache.spark.SparkUpgradeException" in str(e.exception))
                self.assertTrue(
                    "at org.apache.spark.sql.errors.ExecutionErrors"
                    ".failToParseDateTimeInNewParserError" in str(e.exception)
                )
                self.assertTrue("Caused by: java.time.DateTimeException:" in str(e.exception))

    def test_not_hitting_netty_header_limit(self):
        with self.sql_conf({"spark.sql.pyspark.jvmStacktrace.enabled": True}):
            with self.assertRaises(AnalysisException):
                self.spark.sql("select " + "test" * 1).collect()

    def test_error_stack_trace(self):
        with self.sql_conf({"spark.sql.connect.enrichError.enabled": False}):
            with self.sql_conf({"spark.sql.pyspark.jvmStacktrace.enabled": True}):
                with self.assertRaises(AnalysisException) as e:
                    self.spark.sql("select x").collect()
                self.assertTrue("JVM stacktrace" in str(e.exception))
                self.assertIsNotNone(e.exception.getStackTrace())
                self.assertTrue(
                    "at org.apache.spark.sql.catalyst.analysis.CheckAnalysis" in str(e.exception)
                )

            with self.sql_conf({"spark.sql.pyspark.jvmStacktrace.enabled": False}):
                with self.assertRaises(AnalysisException) as e:
                    self.spark.sql("select x").collect()
                self.assertFalse("JVM stacktrace" in str(e.exception))
                self.assertIsNone(e.exception.getStackTrace())
                self.assertFalse(
                    "at org.apache.spark.sql.catalyst.analysis.CheckAnalysis" in str(e.exception)
                )

        # Create a new session with a different stack trace size.
        self.spark.stop()
        spark = (
            PySparkSession.builder.config(conf=self.conf())
            .config("spark.connect.grpc.maxMetadataSize", 128)
            .remote("local[4]")
            .getOrCreate()
        )
        spark.conf.set("spark.sql.connect.enrichError.enabled", False)
        spark.conf.set("spark.sql.pyspark.jvmStacktrace.enabled", True)
        with self.assertRaises(AnalysisException) as e:
            spark.sql("select x").collect()
        self.assertTrue("JVM stacktrace" in str(e.exception))
        self.assertIsNotNone(e.exception.getStackTrace())
        self.assertFalse(
            "at org.apache.spark.sql.catalyst.analysis.CheckAnalysis" in str(e.exception)
        )
        spark.stop()

    def test_can_create_multiple_sessions_to_different_remotes(self):
        self.spark.stop()
        self.assertIsNotNone(self.spark._client)
        # Creates a new remote session.
        other = PySparkSession.builder.remote("sc://other.remote:114/").create()
        self.assertNotEqual(self.spark, other)

        # Gets currently active session.
        same = PySparkSession.builder.remote("sc://other.remote.host:114/").getOrCreate()
        self.assertEqual(other, same)
        same.release_session_on_close = False  # avoid sending release to dummy connection
        same.stop()

        # Make sure the environment is clean.
        self.spark.stop()
        with self.assertRaises(RuntimeError) as e:
            PySparkSession.builder.create()
            self.assertIn("Create a new SparkSession is only supported with SparkConnect.", str(e))

    def test_get_message_parameters_without_enriched_error(self):
        with self.sql_conf({"spark.sql.connect.enrichError.enabled": False}):
            exception = None
            try:
                self.spark.sql("""SELECT a""")
            except AnalysisException as e:
                exception = e

            self.assertIsNotNone(exception)
            self.assertEqual(exception.getMessageParameters(), {"objectName": "`a`"})

    @timeout(10)
    def test_custom_channel_builder(self):
        # Access self.spark's DefaultChannelBuilder to reuse same endpoint
        endpoint = self.spark._client._builder.endpoint

        class CustomChannelBuilder(ChannelBuilder):
            def toChannel(self):
                creds = grpc.local_channel_credentials()

                if self.token is not None:
                    creds = grpc.composite_channel_credentials(
                        creds, grpc.access_token_call_credentials(self.token)
                    )
                return self._secure_channel(endpoint, creds)

        session = RemoteSparkSession.builder.channelBuilder(CustomChannelBuilder()).create()
        session.sql("select 1 + 1")

    def test_reset_when_server_and_client_sessionids_mismatch(self):
        session = RemoteSparkSession.builder.remote("sc://localhost").getOrCreate()
        # run a simple query so the session id is synchronized.
        session.range(3).collect()

        # trigger a mismatch between client session id and server session id.
        session._client._session_id = str(uuid.uuid4())
        with self.assertRaises(SparkConnectException):
            session.range(3).collect()

        # assert that getOrCreate() generates a new session
        session = RemoteSparkSession.builder.remote("sc://localhost").getOrCreate()
        session.range(3).collect()

    def test_reset_when_server_session_id_mismatch(self):
        session = RemoteSparkSession.builder.remote("sc://localhost").getOrCreate()
        # run a simple query so the session id is synchronized.
        session.range(3).collect()

        # trigger a mismatch
        session._client._server_session_id = str(uuid.uuid4())
        with self.assertRaises(SparkConnectException):
            session.range(3).collect()

        # assert that getOrCreate() generates a new session
        session = RemoteSparkSession.builder.remote("sc://localhost").getOrCreate()
        session.range(3).collect()

    def test_stop_invalid_session(self):  # SPARK-47986
        session = RemoteSparkSession.builder.remote("sc://localhost").getOrCreate()
        # run a simple query so the session id is synchronized.
        session.range(3).collect()

        # change the server side session id to simulate that the server has terminated this session.
        session._client._server_session_id = str(uuid.uuid4())

        # Should not throw any error
        session.stop()

    def test_api_mode(self):
        session = (
            PySparkSession.builder.config("spark.api.mode", "connect")
            .master("sc://localhost")
            .getOrCreate()
        )
        self.assertEqual(session.range(1).first()[0], 0)
        self.assertIsInstance(session, RemoteSparkSession)

    def test_authentication(self):
        # All servers start with a default token of "deadbeef", so supply in invalid one
        session = RemoteSparkSession.builder.remote("sc://localhost/;token=invalid").create()

        with self.assertRaises(SparkConnectGrpcException) as e:
            session.range(3).collect()

        self.assertTrue("Invalid authentication token" in str(e.exception))


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class SparkConnectSessionWithOptionsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = (
            PySparkSession.builder.config("string", "foo")
            .config("integer", 1)
            .config("boolean", False)
            .appName(self.__class__.__name__)
            .remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]"))
            .getOrCreate()
        )

    def tearDown(self):
        self.spark.stop()

    def test_config(self):
        # Config
        self.assertEqual(self.spark.conf.get("string"), "foo")
        self.assertEqual(self.spark.conf.get("boolean"), "false")
        self.assertEqual(self.spark.conf.get("integer"), "1")


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_session import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
