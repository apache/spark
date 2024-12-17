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
import unittest.mock
from io import StringIO

from pyspark import SparkConf, SparkContext
from pyspark.errors import PySparkRuntimeError, PySparkValueError
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import col
from pyspark.testing.connectutils import (
    should_test_connect,
    connect_requirement_message,
)
from pyspark.sql.profiler import Profile
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import PySparkTestCase, PySparkErrorTestUtils


class SparkSessionTests(ReusedSQLTestCase):
    def test_sqlcontext_reuses_sparksession(self):
        sqlContext1 = SQLContext(self.sc)
        sqlContext2 = SQLContext(self.sc)
        self.assertTrue(sqlContext1.sparkSession is sqlContext2.sparkSession)


class SparkSessionTests1(ReusedSQLTestCase):
    # We can't include this test into SQLTests because we will stop class's SparkContext and cause
    # other tests failed.
    def test_sparksession_with_stopped_sparkcontext(self):
        self.sc.stop()
        sc = SparkContext("local[4]", self.sc.appName)
        spark = SparkSession.builder.getOrCreate()
        try:
            df = spark.createDataFrame([(1, 2)], ["c", "c"])
            df.collect()
        finally:
            spark.stop()
            sc.stop()


class SparkSessionTests2(PySparkTestCase):
    # This test is separate because it's closely related with session's start and stop.
    # See SPARK-23228.
    def test_set_jvm_default_session(self):
        spark = SparkSession.builder.getOrCreate()
        try:
            self.assertTrue(spark._jvm.SparkSession.getDefaultSession().isDefined())
        finally:
            spark.stop()
            self.assertTrue(spark._jvm.SparkSession.getDefaultSession().isEmpty())

    def test_jvm_default_session_already_set(self):
        # Here, we assume there is the default session already set in JVM.
        jsession = self.sc._jvm.SparkSession(self.sc._jsc.sc())
        self.sc._jvm.SparkSession.setDefaultSession(jsession)

        spark = SparkSession.builder.getOrCreate()
        try:
            self.assertTrue(spark._jvm.SparkSession.getDefaultSession().isDefined())
            # The session should be the same with the exiting one.
            self.assertTrue(jsession.equals(spark._jvm.SparkSession.getDefaultSession().get()))
        finally:
            spark.stop()


class SparkSessionTests3(unittest.TestCase, PySparkErrorTestUtils):
    def test_active_session(self):
        with self.assertRaises(PySparkRuntimeError) as pe1:
            SparkSession.active()

        self.check_error(
            exception=pe1.exception,
            errorClass="NO_ACTIVE_OR_DEFAULT_SESSION",
            messageParameters={},
        )

        spark = SparkSession.builder.master("local").getOrCreate()
        try:
            activeSession = SparkSession.getActiveSession()
            df = activeSession.createDataFrame([(1, "Alice")], ["age", "name"])
            self.assertEqual(df.collect(), [Row(age=1, name="Alice")])
            with self.assertRaises(ValueError):
                activeSession.createDataFrame(activeSession._sc.parallelize([[], []]))
        finally:
            spark.stop()

    def test_get_active_session_when_no_active_session(self):
        active = SparkSession.getActiveSession()
        self.assertEqual(active, None)
        spark = SparkSession.builder.master("local").getOrCreate()
        active = SparkSession.getActiveSession()
        self.assertEqual(active, spark)
        spark.stop()
        active = SparkSession.getActiveSession()
        self.assertEqual(active, None)

    def test_spark_session(self):
        spark = SparkSession.builder.master("local").config("some-config", "v2").getOrCreate()
        try:
            self.assertEqual(spark.conf.get("some-config"), "v2")
            self.assertEqual(spark.sparkContext._conf.get("some-config"), "v2")
            self.assertEqual(spark.version, spark.sparkContext.version)
            spark.sql("CREATE DATABASE test_db")
            spark.catalog.setCurrentDatabase("test_db")
            self.assertEqual(spark.catalog.currentDatabase(), "test_db")
            spark.sql("CREATE TABLE table1 (name STRING, age INT) USING parquet")
            self.assertEqual(spark.table("table1").columns, ["name", "age"])
            self.assertEqual(spark.range(3).count(), 3)

            try:
                from lxml import etree

                try:
                    etree.parse(StringIO(spark._repr_html_()), etree.HTMLParser(recover=False))
                except Exception as e:
                    self.fail(f"Generated HTML from `_repr_html_` was invalid: {e}")
            except ImportError:
                pass

            # SPARK-37516: Only plain column references work as variable in SQL.
            self.assertEqual(
                spark.sql("select {c} from range(1)", c=col("id")).first(), spark.range(1).first()
            )
            with self.assertRaisesRegex(ValueError, "Column"):
                spark.sql("select {c} from range(10)", c=col("id") + 1)
        finally:
            spark.sql("DROP DATABASE test_db CASCADE")
            spark.stop()

    def test_global_default_session(self):
        spark = SparkSession.builder.master("local").getOrCreate()
        try:
            self.assertEqual(SparkSession.builder.getOrCreate(), spark)
        finally:
            spark.stop()

    def test_default_and_active_session(self):
        spark = SparkSession.builder.master("local").getOrCreate()
        activeSession = spark._jvm.SparkSession.getActiveSession()
        defaultSession = spark._jvm.SparkSession.getDefaultSession()
        try:
            self.assertEqual(activeSession, defaultSession)
        finally:
            spark.stop()

    def test_config_option_propagated_to_existing_session(self):
        session1 = SparkSession.builder.master("local").config("spark-config1", "a").getOrCreate()
        self.assertEqual(session1.conf.get("spark-config1"), "a")
        session2 = SparkSession.builder.config("spark-config1", "b").getOrCreate()
        try:
            self.assertEqual(session1, session2)
            self.assertEqual(session1.conf.get("spark-config1"), "b")
        finally:
            session1.stop()

    def test_new_session(self):
        session = SparkSession.builder.master("local").getOrCreate()
        newSession = session.newSession()
        try:
            self.assertNotEqual(session, newSession)
        finally:
            session.stop()
            newSession.stop()

    def test_create_new_session_if_old_session_stopped(self):
        session = SparkSession.builder.master("local").getOrCreate()
        session.stop()
        newSession = SparkSession.builder.master("local").getOrCreate()
        try:
            self.assertNotEqual(session, newSession)
        finally:
            newSession.stop()

    def test_create_new_session_with_statement(self):
        with SparkSession.builder.master("local").getOrCreate() as session:
            session.range(5).collect()

    def test_active_session_with_None_and_not_None_context(self):
        sc = None
        session = None
        try:
            sc = SparkContext._active_spark_context
            self.assertEqual(sc, None)
            activeSession = SparkSession.getActiveSession()
            self.assertEqual(activeSession, None)
            sparkConf = SparkConf()
            sc = SparkContext.getOrCreate(sparkConf)
            activeSession = sc._jvm.SparkSession.getActiveSession()
            self.assertFalse(activeSession.isDefined())
            session = SparkSession(sc)
            activeSession = sc._jvm.SparkSession.getActiveSession()
            self.assertTrue(activeSession.isDefined())
            activeSession2 = SparkSession.getActiveSession()
            self.assertNotEqual(activeSession2, None)
        finally:
            if session is not None:
                session.stop()
            if sc is not None:
                sc.stop()

    @unittest.skipIf(not should_test_connect, connect_requirement_message)
    def test_session_with_spark_connect_mode_enabled(self):
        with unittest.mock.patch.dict(os.environ, {"SPARK_CONNECT_MODE_ENABLED": "1"}):
            with self.assertRaisesRegex(RuntimeError, "Cannot create a Spark Connect session"):
                SparkSession.builder.appName("test").getOrCreate()

    def test_unsupported_api(self):
        with SparkSession.builder.master("local").getOrCreate() as session:
            unsupported = [
                (lambda: session.client, "client"),
                (session.addArtifacts, "addArtifact(s)"),
                (lambda: session.copyFromLocalToFs("", ""), "copyFromLocalToFs"),
                (lambda: session.interruptTag(""), "interruptTag"),
                (lambda: session.interruptOperation(""), "interruptOperation"),
            ]

            for func, name in unsupported:
                with self.assertRaises(PySparkRuntimeError) as pe1:
                    func()

                self.check_error(
                    exception=pe1.exception,
                    errorClass="ONLY_SUPPORTED_WITH_SPARK_CONNECT",
                    messageParameters={"feature": f"SparkSession.{name}"},
                )


class SparkSessionTests4(ReusedSQLTestCase):
    def test_get_active_session_after_create_dataframe(self):
        session2 = None
        try:
            activeSession1 = SparkSession.getActiveSession()
            session1 = self.spark
            self.assertEqual(session1, activeSession1)
            session2 = self.spark.newSession()
            activeSession2 = SparkSession.getActiveSession()
            self.assertEqual(session1, activeSession2)
            self.assertNotEqual(session2, activeSession2)
            session2.createDataFrame([(1, "Alice")], ["age", "name"])
            activeSession3 = SparkSession.getActiveSession()
            self.assertEqual(session2, activeSession3)
            session1.createDataFrame([(1, "Alice")], ["age", "name"])
            activeSession4 = SparkSession.getActiveSession()
            self.assertEqual(session1, activeSession4)
        finally:
            if session2 is not None:
                session2.stop()


class SparkSessionTests5(unittest.TestCase):
    def setUp(self):
        # These tests require restarting the Spark context so we set up a new one for each test
        # rather than at the class level.
        self.sc = SparkContext("local[4]", self.__class__.__name__, conf=SparkConf())
        self.spark = SparkSession(self.sc)

    def tearDown(self):
        self.sc.stop()
        self.spark.stop()

    def test_sqlcontext_with_stopped_sparksession(self):
        # SPARK-30856: test that SQLContext.getOrCreate() returns a usable instance after
        # the SparkSession is restarted.
        sql_context = SQLContext.getOrCreate(self.spark.sparkContext)
        self.spark.stop()
        spark = SparkSession.builder.master("local[4]").appName(self.sc.appName).getOrCreate()
        new_sql_context = SQLContext.getOrCreate(spark.sparkContext)

        self.assertIsNot(new_sql_context, sql_context)
        self.assertIs(SQLContext.getOrCreate(spark.sparkContext).sparkSession, spark)
        try:
            df = spark.createDataFrame([(1, 2)], ["c", "c"])
            df.collect()
        finally:
            spark.stop()
            self.assertIsNone(SQLContext._instantiatedContext)

    def test_sqlcontext_with_stopped_sparkcontext(self):
        # SPARK-30856: test initialization via SparkSession when only the SparkContext is stopped
        self.sc.stop()
        spark = SparkSession.builder.master("local[4]").appName(self.sc.appName).getOrCreate()
        self.sc = spark.sparkContext
        self.assertIs(SQLContext.getOrCreate(self.sc).sparkSession, spark)

    def test_get_sqlcontext_with_stopped_sparkcontext(self):
        # SPARK-30856: test initialization via SQLContext.getOrCreate() when only the SparkContext
        # is stopped
        self.sc.stop()
        self.sc = SparkContext("local[4]", self.sc.appName)
        self.assertIs(SQLContext.getOrCreate(self.sc)._sc, self.sc)


class SparkSessionBuilderTests(unittest.TestCase, PySparkErrorTestUtils):
    def test_create_spark_context_first_then_spark_session(self):
        sc = None
        session = None
        try:
            conf = SparkConf().set("key1", "value1")
            sc = SparkContext("local[4]", "SessionBuilderTests", conf=conf)
            session = SparkSession.builder.config("key2", "value2").getOrCreate()

            self.assertEqual(session.conf.get("key1"), "value1")
            self.assertEqual(session.conf.get("key2"), "value2")
            self.assertEqual(session.sparkContext, sc)

            self.assertFalse(sc.getConf().contains("key2"))
            self.assertEqual(sc.getConf().get("key1"), "value1")
        finally:
            if session is not None:
                session.stop()
            if sc is not None:
                sc.stop()

    def test_another_spark_session(self):
        session1 = None
        session2 = None
        try:
            session1 = SparkSession.builder.config("key1", "value1").getOrCreate()
            session2 = SparkSession.builder.config(
                "spark.sql.codegen.comments", "true"
            ).getOrCreate()

            self.assertEqual(session1.conf.get("key1"), "value1")
            self.assertEqual(session2.conf.get("key1"), "value1")
            self.assertEqual(session1.conf.get("spark.sql.codegen.comments"), "false")
            self.assertEqual(session2.conf.get("spark.sql.codegen.comments"), "false")
            self.assertEqual(session1.sparkContext, session2.sparkContext)

            self.assertEqual(session1.sparkContext.getConf().get("key1"), "value1")
            self.assertFalse(session1.sparkContext.getConf().contains("key2"))
        finally:
            if session1 is not None:
                session1.stop()
            if session2 is not None:
                session2.stop()

    def test_create_spark_context_with_initial_session_options(self):
        sc = None
        session = None
        try:
            conf = SparkConf().set("key1", "value1")
            sc = SparkContext("local[4]", "SessionBuilderTests", conf=conf)
            session = (
                SparkSession.builder.config("spark.sql.codegen.comments", "true")
                .enableHiveSupport()
                .getOrCreate()
            )

            self.assertEqual(session._jsparkSession.sharedState().conf().get("key1"), "value1")
            self.assertEqual(
                session._jsparkSession.sharedState().conf().get("spark.sql.codegen.comments"),
                "true",
            )
            self.assertEqual(
                session._jsparkSession.sharedState().conf().get("spark.sql.catalogImplementation"),
                "hive",
            )
            self.assertEqual(session.sparkContext, sc)
        finally:
            if session is not None:
                session.stop()
            if sc is not None:
                sc.stop()

    def test_create_spark_context_with_initial_session_options_bool(self):
        session = None
        # Test if `True` is set as "true".
        try:
            session = SparkSession.builder.config(
                "spark.sql.pyspark.jvmStacktrace.enabled", True
            ).getOrCreate()
            self.assertEqual(session.conf.get("spark.sql.pyspark.jvmStacktrace.enabled"), "true")
        finally:
            if session is not None:
                session.stop()
        # Test if `False` is set as "false".
        try:
            session = SparkSession.builder.config(
                "spark.sql.pyspark.jvmStacktrace.enabled", False
            ).getOrCreate()
            self.assertEqual(session.conf.get("spark.sql.pyspark.jvmStacktrace.enabled"), "false")
        finally:
            if session is not None:
                session.stop()

    def test_create_spark_context_with_invalid_configs(self):
        with self.assertRaises(PySparkRuntimeError) as pe1:
            SparkSession.builder.config(map={"spark.master": "x", "spark.remote": "y"})

        self.check_error(
            exception=pe1.exception,
            errorClass="CANNOT_CONFIGURE_SPARK_CONNECT_MASTER",
            messageParameters={"master_url": "x", "connect_url": "y"},
        )

        with unittest.mock.patch.dict(
            "os.environ", {"SPARK_REMOTE": "remote_url", "SPARK_LOCAL_REMOTE": "true"}
        ):
            with self.assertRaises(PySparkRuntimeError) as pe2:
                SparkSession.builder.config("spark.remote", "different_remote_url")

            self.check_error(
                exception=pe2.exception,
                errorClass="CANNOT_CONFIGURE_SPARK_CONNECT",
                messageParameters={
                    "existing_url": "remote_url",
                    "new_url": "different_remote_url",
                },
            )

    def test_master_remote_conflicts(self):
        with self.assertRaises(PySparkRuntimeError) as pe2:
            SparkSession.builder.config("spark.master", "1").config("spark.remote", "2")

        self.check_error(
            exception=pe2.exception,
            errorClass="CANNOT_CONFIGURE_SPARK_CONNECT_MASTER",
            messageParameters={"connect_url": "2", "master_url": "1"},
        )

        try:
            os.environ["SPARK_REMOTE"] = "2"
            os.environ["SPARK_LOCAL_REMOTE"] = "2"
            with self.assertRaises(PySparkRuntimeError) as pe2:
                SparkSession.builder.config("spark.remote", "1")

            self.check_error(
                exception=pe2.exception,
                errorClass="CANNOT_CONFIGURE_SPARK_CONNECT",
                messageParameters={
                    "new_url": "1",
                    "existing_url": "2",
                },
            )
        finally:
            del os.environ["SPARK_REMOTE"]
            del os.environ["SPARK_LOCAL_REMOTE"]

    @unittest.skipIf(not should_test_connect, connect_requirement_message)
    def test_invalid_create(self):
        with self.assertRaises(PySparkRuntimeError) as pe2:
            SparkSession.builder.config("spark.remote", "local").create()

        self.check_error(
            exception=pe2.exception,
            errorClass="UNSUPPORTED_LOCAL_CONNECTION_STRING",
            messageParameters={},
        )


class SparkSessionProfileTests(unittest.TestCase, PySparkErrorTestUtils):
    def setUp(self):
        self.profiler_collector_mock = unittest.mock.Mock()
        self.profile = Profile(self.profiler_collector_mock)

    def test_show_memory_type(self):
        self.profile.show(type="memory")
        self.profiler_collector_mock.show_memory_profiles.assert_called_with(None)
        self.profiler_collector_mock.show_perf_profiles.assert_not_called()

    def test_show_perf_type(self):
        self.profile.show(type="perf")
        self.profiler_collector_mock.show_perf_profiles.assert_called_with(None)
        self.profiler_collector_mock.show_memory_profiles.assert_not_called()

    def test_show_no_type(self):
        self.profile.show()
        self.profiler_collector_mock.show_perf_profiles.assert_called_with(None)
        self.profiler_collector_mock.show_memory_profiles.assert_called_with(None)

    def test_show_invalid_type(self):
        with self.assertRaises(PySparkValueError) as e:
            self.profile.show(type="invalid")
        self.check_error(
            exception=e.exception,
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "type",
                "allowed_values": str(["perf", "memory"]),
            },
        )

    def test_dump_memory_type(self):
        self.profile.dump("path/to/dump", type="memory")
        self.profiler_collector_mock.dump_memory_profiles.assert_called_with("path/to/dump", None)
        self.profiler_collector_mock.dump_perf_profiles.assert_not_called()

    def test_dump_perf_type(self):
        self.profile.dump("path/to/dump", type="perf")
        self.profiler_collector_mock.dump_perf_profiles.assert_called_with("path/to/dump", None)
        self.profiler_collector_mock.dump_memory_profiles.assert_not_called()

    def test_dump_no_type(self):
        self.profile.dump("path/to/dump")
        self.profiler_collector_mock.dump_perf_profiles.assert_called_with("path/to/dump", None)
        self.profiler_collector_mock.dump_memory_profiles.assert_called_with("path/to/dump", None)

    def test_dump_invalid_type(self):
        with self.assertRaises(PySparkValueError) as e:
            self.profile.dump("path/to/dump", type="invalid")
        self.check_error(
            exception=e.exception,
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "type",
                "allowed_values": str(["perf", "memory"]),
            },
        )

    def test_clear_memory_type(self):
        self.profile.clear(type="memory")
        self.profiler_collector_mock.clear_memory_profiles.assert_called_once()
        self.profiler_collector_mock.clear_perf_profiles.assert_not_called()

    def test_clear_perf_type(self):
        self.profile.clear(type="perf")
        self.profiler_collector_mock.clear_perf_profiles.assert_called_once()
        self.profiler_collector_mock.clear_memory_profiles.assert_not_called()

    def test_clear_no_type(self):
        self.profile.clear()
        self.profiler_collector_mock.clear_perf_profiles.assert_called_once()
        self.profiler_collector_mock.clear_memory_profiles.assert_called_once()

    def test_clear_invalid_type(self):
        with self.assertRaises(PySparkValueError) as e:
            self.profile.clear(type="invalid")
        self.check_error(
            exception=e.exception,
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "type",
                "allowed_values": str(["perf", "memory"]),
            },
        )


class SparkExtensionsTest(unittest.TestCase):
    # These tests are separate because it uses 'spark.sql.extensions' which is
    # static and immutable. This can't be set or unset, for example, via `spark.conf`.

    @classmethod
    def setUpClass(cls):
        import glob
        from pyspark.find_spark_home import _find_spark_home

        SPARK_HOME = _find_spark_home()
        filename_pattern = (
            "sql/core/target/scala-*/test-classes/org/apache/spark/sql/"
            "SparkSessionExtensionSuite.class"
        )
        if not glob.glob(os.path.join(SPARK_HOME, filename_pattern)):
            raise unittest.SkipTest(
                "'org.apache.spark.sql.SparkSessionExtensionSuite' is not "
                "available. Will skip the related tests."
            )

        # Note that 'spark.sql.extensions' is a static immutable configuration.
        cls.spark = (
            SparkSession.builder.master("local[4]")
            .appName(cls.__name__)
            .config("spark.sql.extensions", "org.apache.spark.sql.MyExtensions")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_use_custom_class_for_extensions(self):
        self.assertTrue(
            self.spark._jsparkSession.sessionState()
            .planner()
            .strategies()
            .contains(
                self.spark._jvm.org.apache.spark.sql.MySparkStrategy(self.spark._jsparkSession)
            ),
            "MySparkStrategy not found in active planner strategies",
        )
        self.assertTrue(
            self.spark._jsparkSession.sessionState()
            .analyzer()
            .extendedResolutionRules()
            .contains(self.spark._jvm.org.apache.spark.sql.MyRule(self.spark._jsparkSession)),
            "MyRule not found in extended resolution rules",
        )


if __name__ == "__main__":
    from pyspark.sql.tests.test_session import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
