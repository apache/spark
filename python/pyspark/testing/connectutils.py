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
import types
import typing
import os
import functools
import unittest
import uuid

from pyspark import Row, SparkConf
from pyspark.testing.utils import PySparkErrorTestUtils
from pyspark.testing.sqlutils import (
    have_pandas,
    pandas_requirement_message,
    pyarrow_requirement_message,
    SQLTestUtils,
)
from pyspark.sql.session import SparkSession as PySparkSession


grpc_requirement_message = None
try:
    import grpc
except ImportError as e:
    grpc_requirement_message = str(e)
have_grpc = grpc_requirement_message is None


grpc_status_requirement_message = None
try:
    import grpc_status
except ImportError as e:
    grpc_status_requirement_message = str(e)
have_grpc_status = grpc_status_requirement_message is None

googleapis_common_protos_requirement_message = None
try:
    from google.rpc import error_details_pb2
except ImportError as e:
    googleapis_common_protos_requirement_message = str(e)
have_googleapis_common_protos = googleapis_common_protos_requirement_message is None


connect_requirement_message = (
    pandas_requirement_message
    or pyarrow_requirement_message
    or grpc_requirement_message
    or googleapis_common_protos_requirement_message
    or grpc_status_requirement_message
)
should_test_connect: str = typing.cast(str, connect_requirement_message is None)

if should_test_connect:
    from pyspark.sql.connect.dataframe import DataFrame
    from pyspark.sql.connect.plan import Read, Range, SQL, LogicalPlan
    from pyspark.sql.connect.session import SparkSession


class MockRemoteSession:
    def __init__(self):
        self.hooks = {}
        self.session_id = str(uuid.uuid4())

    def set_hook(self, name, hook):
        self.hooks[name] = hook

    def drop_hook(self, name):
        self.hooks.pop(name)

    def __getattr__(self, item):
        if item not in self.hooks:
            raise LookupError(f"{item} is not defined as a method hook in MockRemoteSession")
        return functools.partial(self.hooks[item])


class MockDF(DataFrame):
    """Helper class that must only be used for the mock plan tests."""

    def __init__(self, session: SparkSession, plan: LogicalPlan):
        super().__init__(session)
        self._plan = plan

    def __getattr__(self, name):
        """All attributes are resolved to columns, because none really exist in the
        mocked DataFrame."""
        return self[name]


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class PlanOnlyTestFixture(unittest.TestCase, PySparkErrorTestUtils):
    @classmethod
    def _read_table(cls, table_name):
        return cls._df_mock(Read(table_name))

    @classmethod
    def _udf_mock(cls, *args, **kwargs):
        return "internal_name"

    @classmethod
    def _df_mock(cls, plan: LogicalPlan) -> MockDF:
        return MockDF(cls.connect, plan)

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

    if have_pandas:

        @classmethod
        def _with_plan(cls, plan):
            return cls._df_mock(plan)

    @classmethod
    def setUpClass(cls):
        cls.connect = MockRemoteSession()
        cls.session = SparkSession.builder.remote().getOrCreate()
        cls.tbl_name = "test_connect_plan_only_table_1"

        cls.connect.set_hook("readTable", cls._read_table)
        cls.connect.set_hook("range", cls._session_range)
        cls.connect.set_hook("sql", cls._session_sql)
        cls.connect.set_hook("with_plan", cls._with_plan)

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
        # Disable JVM stack trace in Spark Connect tests to prevent the
        # HTTP header size from exceeding the maximum allowed size.
        conf.set("spark.sql.pyspark.jvmStacktrace.enabled", "false")
        # Make the server terminate reattachable streams every 1 second and 123 bytes,
        # to make the tests exercise reattach.
        conf.set("spark.connect.execute.reattachable.senderMaxStreamDuration", "1s")
        conf.set("spark.connect.execute.reattachable.senderMaxStreamSize", "123")
        return conf

    @classmethod
    def master(cls):
        return "local[4]"

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            PySparkSession.builder.config(conf=cls.conf())
            .appName(cls.__name__)
            .remote(cls.master())
            .getOrCreate()
        )
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.df = cls.spark.createDataFrame(cls.testData)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)
        cls.spark.stop()
