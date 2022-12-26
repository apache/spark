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
import typing
import os
import functools
import unittest

from pyspark.testing.sqlutils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


grpc_requirement_message = None
try:
    import grpc
except ImportError as e:
    grpc_requirement_message = str(e)
have_grpc = grpc_requirement_message is None

connect_not_compiled_message = None
if have_pandas and have_pyarrow and have_grpc:
    from pyspark.sql.connect import DataFrame
    from pyspark.sql.connect.plan import Read, Range, SQL
    from pyspark.testing.utils import search_jar
    from pyspark.sql.connect.session import SparkSession

    connect_jar = search_jar("connector/connect/server", "spark-connect-assembly-", "spark-connect")
    existing_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
    connect_url = "--remote sc://localhost"
    jars_args = "--jars %s" % connect_jar
    plugin_args = "--conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin"
    os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join(
        [connect_url, jars_args, plugin_args, existing_args]
    )
else:
    connect_not_compiled_message = (
        "Skipping all Spark Connect Python tests as the optional Spark Connect project was "
        "not compiled into a JAR. To run these tests, you need to build Spark with "
        "'build/sbt package' or 'build/mvn package' before running this test."
    )


connect_requirement_message = (
    pandas_requirement_message
    or pyarrow_requirement_message
    or grpc_requirement_message
    or connect_not_compiled_message
)
should_test_connect: str = typing.cast(str, connect_requirement_message is None)


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

        cls.connect.set_hook("register_udf", cls._udf_mock)
        cls.connect.set_hook("readTable", cls._read_table)
        cls.connect.set_hook("range", cls._session_range)
        cls.connect.set_hook("sql", cls._session_sql)
        cls.connect.set_hook("with_plan", cls._with_plan)

    @classmethod
    def tearDownClass(cls):
        cls.connect.drop_hook("register_udf")
        cls.connect.drop_hook("readTable")
        cls.connect.drop_hook("range")
        cls.connect.drop_hook("sql")
        cls.connect.drop_hook("with_plan")
