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
from collections import defaultdict

from pyspark.util import is_remote_only
from pyspark.errors import (
    PySparkException,
    PySparkValueError,
    RetriesExceeded,
)
from pyspark.sql import SparkSession as PySparkSession
from pyspark.sql.connect.client.retries import RetryPolicy

from pyspark.testing.connectutils import (
    should_test_connect,
    ReusedConnectTestCase,
    connect_requirement_message,
)
from pyspark.errors.exceptions.connect import (
    AnalysisException,
    SparkConnectException,
    SparkUpgradeException,
)

if should_test_connect:
    import grpc
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
    from pyspark.sql.connect.client import DefaultChannelBuilder, ChannelBuilder
    from pyspark.sql.connect.client.core import Retrying, SparkConnectClient


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
        self.check_error(exception=e, error_class="NO_ACTIVE_SESSION", message_parameters=dict())

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

    def test_custom_channel_builder(self):
        # Access self.spark's DefaultChannelBuilder to reuse same endpoint
        endpoint = self.spark._client._builder.endpoint

        class CustomChannelBuilder(ChannelBuilder):
            def toChannel(self):
                return self._insecure_channel(endpoint)

        session = RemoteSparkSession.builder.channelBuilder(CustomChannelBuilder()).create()
        session.sql("select 1 + 1")

    def test_reset_when_server_session_changes(self):
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


class TestError(grpc.RpcError, Exception):
    def __init__(self, code: grpc.StatusCode):
        self._code = code

    def code(self):
        return self._code


class TestPolicy(RetryPolicy):
    # Put a small value for initial backoff so that tests don't spend
    # Time waiting
    def __init__(self, initial_backoff=10, **kwargs):
        super().__init__(initial_backoff=initial_backoff, **kwargs)

    def can_retry(self, exception: BaseException):
        return isinstance(exception, TestError)


class TestPolicySpecificError(TestPolicy):
    def __init__(self, specific_code: grpc.StatusCode, **kwargs):
        super().__init__(**kwargs)
        self.specific_code = specific_code

    def can_retry(self, exception: BaseException):
        return exception.code() == self.specific_code


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class RetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.call_wrap = defaultdict(int)

    def stub(self, retries, code):
        self.call_wrap["attempts"] += 1
        if self.call_wrap["attempts"] < retries:
            self.call_wrap["raised"] += 1
            raise TestError(code)

    def test_simple(self):
        # Check that max_retries 1 is only one retry so two attempts.
        for attempt in Retrying(TestPolicy(max_retries=1)):
            with attempt:
                self.stub(2, grpc.StatusCode.INTERNAL)

        self.assertEqual(2, self.call_wrap["attempts"])
        self.assertEqual(1, self.call_wrap["raised"])

    def test_below_limit(self):
        # Check that if we have less than 4 retries all is ok.
        for attempt in Retrying(TestPolicy(max_retries=4)):
            with attempt:
                self.stub(2, grpc.StatusCode.INTERNAL)

        self.assertLess(self.call_wrap["attempts"], 4)
        self.assertEqual(self.call_wrap["raised"], 1)

    def test_exceed_retries(self):
        # Exceed the retries.
        with self.assertRaises(RetriesExceeded):
            for attempt in Retrying(TestPolicy(max_retries=2)):
                with attempt:
                    self.stub(5, grpc.StatusCode.INTERNAL)

        self.assertLess(self.call_wrap["attempts"], 5)
        self.assertEqual(self.call_wrap["raised"], 3)

    def test_throw_not_retriable_error(self):
        with self.assertRaises(ValueError):
            for attempt in Retrying(TestPolicy(max_retries=2)):
                with attempt:
                    raise ValueError

    def test_specific_exception(self):
        # Check that only specific exceptions are retried.
        # Check that if we have less than 4 retries all is ok.
        policy = TestPolicySpecificError(max_retries=4, specific_code=grpc.StatusCode.UNAVAILABLE)

        for attempt in Retrying(policy):
            with attempt:
                self.stub(2, grpc.StatusCode.UNAVAILABLE)

        self.assertLess(self.call_wrap["attempts"], 4)
        self.assertEqual(self.call_wrap["raised"], 1)

    def test_specific_exception_exceed_retries(self):
        # Exceed the retries.
        policy = TestPolicySpecificError(max_retries=2, specific_code=grpc.StatusCode.UNAVAILABLE)
        with self.assertRaises(RetriesExceeded):
            for attempt in Retrying(policy):
                with attempt:
                    self.stub(5, grpc.StatusCode.UNAVAILABLE)

        self.assertLess(self.call_wrap["attempts"], 4)
        self.assertEqual(self.call_wrap["raised"], 3)

    def test_rejected_by_policy(self):
        # Test that another error is always thrown.
        policy = TestPolicySpecificError(max_retries=4, specific_code=grpc.StatusCode.UNAVAILABLE)

        with self.assertRaises(TestError):
            for attempt in Retrying(policy):
                with attempt:
                    self.stub(5, grpc.StatusCode.INTERNAL)

        self.assertEqual(self.call_wrap["attempts"], 1)
        self.assertEqual(self.call_wrap["raised"], 1)

    def test_multiple_policies(self):
        policy1 = TestPolicySpecificError(max_retries=2, specific_code=grpc.StatusCode.UNAVAILABLE)
        policy2 = TestPolicySpecificError(max_retries=4, specific_code=grpc.StatusCode.INTERNAL)

        # Tolerate 2 UNAVAILABLE errors and 4 INTERNAL errors

        error_suply = iter([grpc.StatusCode.UNAVAILABLE] * 2 + [grpc.StatusCode.INTERNAL] * 4)

        for attempt in Retrying([policy1, policy2]):
            with attempt:
                error = next(error_suply, None)
                if error:
                    raise TestError(error)

        self.assertEqual(next(error_suply, None), None)

    def test_multiple_policies_exceed(self):
        policy1 = TestPolicySpecificError(max_retries=2, specific_code=grpc.StatusCode.INTERNAL)
        policy2 = TestPolicySpecificError(max_retries=4, specific_code=grpc.StatusCode.INTERNAL)

        with self.assertRaises(RetriesExceeded):
            for attempt in Retrying([policy1, policy2]):
                with attempt:
                    self.stub(10, grpc.StatusCode.INTERNAL)

        self.assertEqual(self.call_wrap["attempts"], 7)
        self.assertEqual(self.call_wrap["raised"], 7)


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class ChannelBuilderTests(unittest.TestCase):
    def test_invalid_connection_strings(self):
        invalid = [
            "scc://host:12",
            "http://host",
            "sc:/host:1234/path",
            "sc://host/path",
            "sc://host/;parm1;param2",
        ]
        for i in invalid:
            self.assertRaises(PySparkValueError, DefaultChannelBuilder, i)

    def test_sensible_defaults(self):
        chan = DefaultChannelBuilder("sc://host")
        self.assertFalse(chan.secure, "Default URL is not secure")

        chan = DefaultChannelBuilder("sc://host/;token=abcs")
        self.assertTrue(chan.secure, "specifying a token must set the channel to secure")
        self.assertRegex(
            chan.userAgent, r"^_SPARK_CONNECT_PYTHON spark/[^ ]+ os/[^ ]+ python/[^ ]+$"
        )
        chan = DefaultChannelBuilder("sc://host/;use_ssl=abcs")
        self.assertFalse(chan.secure, "Garbage in, false out")

    def test_user_agent(self):
        chan = DefaultChannelBuilder("sc://host/;user_agent=Agent123%20%2F3.4")
        self.assertIn("Agent123 /3.4", chan.userAgent)

    def test_user_agent_len(self):
        user_agent = "x" * 2049
        chan = DefaultChannelBuilder(f"sc://host/;user_agent={user_agent}")
        with self.assertRaises(SparkConnectException) as err:
            chan.userAgent
        self.assertRegex(err.exception._message, "'user_agent' parameter should not exceed")

        user_agent = "%C3%A4" * 341  # "%C3%A4" -> "ä"; (341 * 6 = 2046) < 2048
        expected = "ä" * 341
        chan = DefaultChannelBuilder(f"sc://host/;user_agent={user_agent}")
        self.assertIn(expected, chan.userAgent)

    def test_valid_channel_creation(self):
        chan = DefaultChannelBuilder("sc://host").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

        # Sets up a channel without tokens because ssl is not used.
        chan = DefaultChannelBuilder("sc://host/;use_ssl=true;token=abc").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

        chan = DefaultChannelBuilder("sc://host/;use_ssl=true").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

    def test_channel_properties(self):
        chan = DefaultChannelBuilder(
            "sc://host/;use_ssl=true;token=abc;user_agent=foo;param1=120%2021"
        )
        self.assertEqual("host:15002", chan.endpoint)
        self.assertIn("foo", chan.userAgent.split(" "))
        self.assertEqual(True, chan.secure)
        self.assertEqual("120 21", chan.get("param1"))

    def test_metadata(self):
        chan = DefaultChannelBuilder(
            "sc://host/;use_ssl=true;token=abc;param1=120%2021;x-my-header=abcd"
        )
        md = chan.metadata()
        self.assertEqual([("param1", "120 21"), ("x-my-header", "abcd")], md)

    def test_metadata_with_session_id(self):
        id = str(uuid.uuid4())
        chan = DefaultChannelBuilder(f"sc://host/;session_id={id}")
        self.assertEqual(id, chan.session_id)

        chan = DefaultChannelBuilder(
            f"sc://host/;session_id={id};user_agent=acbd;token=abcd;use_ssl=true"
        )
        md = chan.metadata()
        for kv in md:
            self.assertNotIn(
                kv[0],
                [
                    ChannelBuilder.PARAM_SESSION_ID,
                    ChannelBuilder.PARAM_TOKEN,
                    ChannelBuilder.PARAM_USER_ID,
                    ChannelBuilder.PARAM_USER_AGENT,
                    ChannelBuilder.PARAM_USE_SSL,
                ],
                "Metadata must not contain fixed params",
            )

        with self.assertRaises(ValueError) as ve:
            chan = DefaultChannelBuilder("sc://host/;session_id=abcd")
            SparkConnectClient(chan)
        self.assertIn("Parameter value session_id must be a valid UUID format", str(ve.exception))

        chan = DefaultChannelBuilder("sc://host/")
        self.assertIsNone(chan.session_id)

    def test_channel_options(self):
        # SPARK-47694
        chan = DefaultChannelBuilder(
            "sc://host", [("grpc.max_send_message_length", 1860), ("test", "robert")]
        )
        options = chan._channel_options
        self.assertEqual(
            [k for k, _ in options].count("grpc.max_send_message_length"),
            1,
            "only one occurrence for defaults",
        )
        self.assertEqual(
            next(v for k, v in options if k == "grpc.max_send_message_length"),
            1860,
            "overwrites defaults",
        )
        self.assertEqual(
            next(v for k, v in options if k == "test"), "robert", "new values are picked up"
        )


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_session import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
