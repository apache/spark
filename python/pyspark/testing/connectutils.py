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
from typing import Any, Dict, Optional
import functools
import unittest

from pyspark.testing.sqlutils import have_pandas

if have_pandas:
    from pyspark.sql.connect import DataFrame
    from pyspark.sql.connect.plan import Read, Range, SQL
    from pyspark.testing.utils import search_jar
    from pyspark.sql.connect.plan import LogicalPlan
    from pyspark.sql.connect.session import SparkSession

    connect_jar = search_jar("connector/connect", "spark-connect-assembly-", "spark-connect")
else:
    connect_jar = None


if connect_jar is None:
    connect_requirement_message = (
        "Skipping all Spark Connect Python tests as the optional Spark Connect project was "
        "not compiled into a JAR. To run these tests, you need to build Spark with "
        "'build/sbt package' or 'build/mvn package' before running this test."
    )
else:
    existing_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
    jars_args = "--jars %s" % connect_jar
    plugin_args = "--conf spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin"
    os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join([jars_args, plugin_args, existing_args])
    connect_requirement_message = None  # type: ignore

should_test_connect = connect_requirement_message is None and have_pandas


class MockRemoteSession:
    def __init__(self) -> None:
        self.hooks: Dict[str, Any] = {}

    def set_hook(self, name: str, hook: Any) -> None:
        self.hooks[name] = hook

    def drop_hook(self, name: str) -> None:
        self.hooks.pop(name)

    def __getattr__(self, item: str) -> Any:
        if item not in self.hooks:
            raise LookupError(f"{item} is not defined as a method hook in MockRemoteSession")
        return functools.partial(self.hooks[item])


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class PlanOnlyTestFixture(unittest.TestCase):

    connect: "MockRemoteSession"
    session: SparkSession

    @classmethod
    def _read_table(cls, table_name: str) -> "DataFrame":
        return DataFrame.withPlan(Read(table_name), cls.connect)  # type: ignore

    @classmethod
    def _udf_mock(cls, *args, **kwargs) -> str:
        return "internal_name"

    @classmethod
    def _session_range(
        cls,
        start: int,
        end: int,
        step: int = 1,
        num_partitions: Optional[int] = None,
    ) -> "DataFrame":
        return DataFrame.withPlan(
            Range(start, end, step, num_partitions), cls.connect  # type: ignore
        )

    @classmethod
    def _session_sql(cls, query: str) -> "DataFrame":
        return DataFrame.withPlan(SQL(query), cls.connect)  # type: ignore

    @classmethod
    def _with_plan(cls, plan: LogicalPlan) -> "DataFrame":
        return DataFrame.withPlan(plan, cls.connect)  # type: ignore

    @classmethod
    def setUpClass(cls: Any) -> None:
        cls.connect = MockRemoteSession()
        cls.session = SparkSession.builder.remote().getOrCreate()
        cls.tbl_name = "test_connect_plan_only_table_1"

        cls.connect.set_hook("register_udf", cls._udf_mock)
        cls.connect.set_hook("readTable", cls._read_table)
        cls.connect.set_hook("range", cls._session_range)
        cls.connect.set_hook("sql", cls._session_sql)
        cls.connect.set_hook("with_plan", cls._with_plan)

    @classmethod
    def tearDownClass(cls: Any) -> None:
        cls.connect.drop_hook("register_udf")
        cls.connect.drop_hook("readTable")
        cls.connect.drop_hook("range")
        cls.connect.drop_hook("sql")
        cls.connect.drop_hook("with_plan")
