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
import shutil
import tempfile
import os
import functools
import unittest
import uuid
import contextlib
from typing import Callable, Optional

from pyspark import Row, SparkConf
from pyspark.loose_version import LooseVersion
from pyspark.util import is_remote_only
from pyspark.testing.utils import (
    have_pandas,
    pandas_requirement_message,
    pyarrow_requirement_message,
    have_graphviz,
    graphviz_requirement_message,
    grpc_requirement_message,
    have_grpc,
    grpc_status_requirement_message,
    have_grpc_status,
    googleapis_common_protos_requirement_message,
    have_googleapis_common_protos,
    connect_requirement_message,
    should_test_connect,
    PySparkErrorTestUtils,
)
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.sql.session import SparkSession as PySparkSession


if should_test_connect:
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect.plan import Read, Range, SQL, LogicalPlan
    from pyspark.sql.connect.session import SparkSession
    import pyspark.sql.connect.proto as pb2


class MockRemoteSession:
    def __init__(self):
        self.hooks = {}
        self.session_id = str(uuid.uuid4())
        self.is_mock_session = True

    def set_hook(self, name, hook):
        self.hooks[name] = hook

    def drop_hook(self, name):
        self.hooks.pop(name)

    def __getattr__(self, item):
        if item not in self.hooks:
            raise LookupError(f"{item} is not defined as a method hook in MockRemoteSession")
        return functools.partial(self.hooks[item])


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class PlanOnlyTestFixture(unittest.TestCase, PySparkErrorTestUtils):
    if should_test_connect:

        class MockDF(DataFrame):
            """Helper class that must only be used for the mock plan tests."""

            def __init__(self, plan: LogicalPlan, session: SparkSession):
                super().__init__(plan, session)

            def __getattr__(self, name):
                """All attributes are resolved to columns, because none really exist in the
                mocked DataFrame."""
                return self[name]

        @classmethod
        def _read_table(cls, table_name):
            return cls._df_mock(Read(table_name))

        @classmethod
        def _udf_mock(cls, *args, **kwargs):
            return "internal_name"

        @classmethod
        def _df_mock(cls, plan: LogicalPlan) -> MockDF:
            return PlanOnlyTestFixture.MockDF(plan, cls.connect)

        @classmethod
        def _session_range(
            cls,
            start,
            end,
            step=1,
            num_partitions=None,
        ):
            return cls._df_mock(Range(start, end, step, num_partitions))

        @classmethod
        def _session_sql(cls, query):
            return cls._df_mock(SQL(query))

        @classmethod
        def _set_relation_in_plan(self, plan: pb2.Plan, relation: pb2.Relation) -> None:
            # Skip plan compression in plan-only tests.
            plan.root.CopyFrom(relation)

        @classmethod
        def _set_command_in_plan(self, plan: pb2.Plan, command: pb2.Command) -> None:
            # Skip plan compression in plan-only tests.
            plan.command.CopyFrom(command)

        if have_pandas:

            @classmethod
            def _with_plan(cls, plan):
                return cls._df_mock(plan)

        @classmethod
        def setUpClass(cls):
            cls.connect = MockRemoteSession()
            cls.tbl_name = "test_connect_plan_only_table_1"

            cls.connect.set_hook("readTable", cls._read_table)
            cls.connect.set_hook("range", cls._session_range)
            cls.connect.set_hook("sql", cls._session_sql)
            cls.connect.set_hook("with_plan", cls._with_plan)
            cls.connect.set_hook("_set_relation_in_plan", cls._set_relation_in_plan)
            cls.connect.set_hook("_set_command_in_plan", cls._set_command_in_plan)

        @classmethod
        def tearDownClass(cls):
            cls.connect.drop_hook("readTable")
            cls.connect.drop_hook("range")
            cls.connect.drop_hook("sql")
            cls.connect.drop_hook("with_plan")


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class ReusedConnectTestCase(unittest.TestCase, SQLTestUtils, PySparkErrorTestUtils):
    """
    Spark Connect version of :class:`pyspark.testing.sqlutils.ReusedSQLTestCase`.
    """

    @classmethod
    def conf(cls):
        """
        Override this in subclasses to supply a more specific conf
        """
        conf = SparkConf(loadDefaults=False)
        # Make the server terminate reattachable streams every 1 second and 123 bytes,
        # to make the tests exercise reattach.
        if conf._jconf is not None:
            conf._jconf.remove("spark.master")
        conf.set("spark.connect.execute.reattachable.senderMaxStreamDuration", "1s")
        conf.set("spark.connect.execute.reattachable.senderMaxStreamSize", "123")
        # Set a static token for all tests so the parallelism doesn't overwrite each
        # tests' environment variables
        conf.set("spark.connect.authenticate.token", "deadbeef")
        return conf

    @classmethod
    def master(cls):
        return os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[4]")

    @classmethod
    def setUpClass(cls):
        # This environment variable is for interrupting hanging ML-handler and making the
        # tests fail fast.
        os.environ["SPARK_CONNECT_ML_HANDLER_INTERRUPTION_TIMEOUT_MINUTES"] = "5"
        cls.spark = (
            PySparkSession.builder.config(conf=cls.conf())
            .appName(cls.__name__)
            .remote(cls.master())
            .getOrCreate()
        )
        cls._client = cls.spark.client
        cls._legacy_sc = None
        if not is_remote_only():
            cls._legacy_sc = PySparkSession._instantiatedSession._sc
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.df = cls.spark.createDataFrame(cls.testData)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)
        cls.spark.stop()

    def setUp(self) -> None:
        # force to clean up the ML cache before each test
        self._client._cleanup_ml_cache()

    def tearDown(self):
        try:
            if self._legacy_sc is not None and self._client._server_session_id is not None:
                self._legacy_sc._jvm.PythonSQLUtils.cleanupPythonWorkerLogs(
                    self._client._server_session_id, self._legacy_sc._jsc.sc()
                )
        finally:
            super().tearDown()

    def test_assert_remote_mode(self):
        from pyspark.sql import is_remote

        self.assertTrue(is_remote())

    def quiet(self):
        from pyspark.testing.utils import QuietTest

        if self._legacy_sc is not None:
            return QuietTest(self._legacy_sc)
        else:
            return contextlib.nullcontext()


@unittest.skipIf(
    not should_test_connect or is_remote_only(),
    connect_requirement_message or "Requires JVM access",
)
class ReusedMixedTestCase(ReusedConnectTestCase, SQLTestUtils):
    @classmethod
    def setUpClass(cls):
        super(ReusedMixedTestCase, cls).setUpClass()
        # Disable the shared namespace so pyspark.sql.functions, etc point the regular
        # PySpark libraries.
        os.environ["PYSPARK_NO_NAMESPACE_SHARE"] = "1"

        cls.connect = cls.spark  # Switch Spark Connect session and regular PySpark session.
        cls.spark = PySparkSession._instantiatedSession
        assert cls.spark is not None

    @classmethod
    def tearDownClass(cls):
        try:
            # Stopping Spark Connect closes the session in JVM at the server.
            cls.spark = cls.connect
            del os.environ["PYSPARK_NO_NAMESPACE_SHARE"]
        finally:
            super(ReusedMixedTestCase, cls).tearDownClass()

    def compare_by_show(self, df1, df2, n: int = 20, truncate: int = 20):
        from pyspark.sql.classic.dataframe import DataFrame as SDF
        from pyspark.sql.connect.dataframe import DataFrame as CDF

        assert isinstance(df1, (SDF, CDF))
        if isinstance(df1, SDF):
            str1 = df1._jdf.showString(n, truncate, False)
        else:
            str1 = df1._show_string(n, truncate, False)

        assert isinstance(df2, (SDF, CDF))
        if isinstance(df2, SDF):
            str2 = df2._jdf.showString(n, truncate, False)
        else:
            str2 = df2._show_string(n, truncate, False)

        self.assertEqual(str1, str2)

    def test_assert_remote_mode(self):
        # no need to test this in mixed mode
        pass

    def connect_conf(self, conf_dict):
        """Context manager to set configuration on Spark Connect session"""

        @contextlib.contextmanager
        def _connect_conf():
            old_values = {}
            for key, value in conf_dict.items():
                old_values[key] = self.connect.conf.get(key, None)
                self.connect.conf.set(key, value)
            try:
                yield
            finally:
                for key, old_value in old_values.items():
                    if old_value is None:
                        self.connect.conf.unset(key)
                    else:
                        self.connect.conf.set(key, old_value)

        return _connect_conf()

    def both_conf(self, conf_dict):
        """Context manager to set configuration on both classic and Connect sessions"""

        @contextlib.contextmanager
        def _both_conf():
            with contextlib.ExitStack() as stack:
                stack.enter_context(self.sql_conf(conf_dict))
                stack.enter_context(self.connect_conf(conf_dict))
                yield

        return _both_conf()


def skip_if_server_version_is(
    cond: Callable[[LooseVersion], bool], reason: Optional[str] = None
) -> Callable:
    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            version = self.spark.version
            if cond(LooseVersion(version)):
                raise unittest.SkipTest(
                    f"Skipping test {f.__name__} because server version is {version}"
                    + (f" ({reason})" if reason else "")
                )
            return f(self, *args, **kwargs)

        return wrapper

    return decorator


def skip_if_server_version_is_greater_than_or_equal_to(
    version: str, reason: Optional[str] = None
) -> Callable:
    return skip_if_server_version_is(lambda v: v >= LooseVersion(version), reason)
