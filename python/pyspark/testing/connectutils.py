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
import typing
import os
import functools
import unittest

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
    from pyspark.sql.connect.plan import Read, Range, SQL
    from pyspark.sql.connect.session import SparkSession


class MockRemoteSession:
    def __init__(self):
        self.hooks = {}

    def set_hook(self, name, hook):
        self.hooks[name] = hook

    def drop_hook(self, name):
        self.hooks.pop(name)

    def __getattr__(self, item):
        if item not in self.hooks:
            raise LookupError(f"{item} is not defined as a method hook in MockRemoteSession")
        return functools.partial(self.hooks[item])


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class PlanOnlyTestFixture(unittest.TestCase):
    @classmethod
    def _read_table(cls, table_name):
        return DataFrame.withPlan(Read(table_name), cls.connect)

    @classmethod
    def _udf_mock(cls, *args, **kwargs):
        return "internal_name"

    @classmethod
    def _session_range(
        cls,
        start,
        end,
        step=1,
        num_partitions=None,
    ):
        return DataFrame.withPlan(Range(start, end, step, num_partitions), cls.connect)

    @classmethod
    def _session_sql(cls, query):
        return DataFrame.withPlan(SQL(query), cls.connect)

    if have_pandas:

        @classmethod
        def _with_plan(cls, plan):
            return DataFrame.withPlan(plan, cls.connect)

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
        return SparkConf(loadDefaults=False)

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            PySparkSession.builder.config(conf=cls.conf())
            .appName(cls.__name__)
            .remote("local[4]")
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
