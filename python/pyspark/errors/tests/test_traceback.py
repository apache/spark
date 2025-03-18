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
import platform
import tempfile
import unittest
from datetime import datetime
from decimal import Decimal
from typing import Callable, Iterable, List, Union

from pyspark.errors import AnalysisException, PythonException
from pyspark.sql.datasource import (
    CaseInsensitiveDict,
    DataSource,
    DataSourceArrowWriter,
    DataSourceReader,
    DataSourceWriter,
    EqualNullSafe,
    EqualTo,
    Filter,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    InputPartition,
    IsNotNull,
    IsNull,
    LessThan,
    LessThanOrEqual,
    StringContains,
    StringEndsWith,
    StringStartsWith,
    WriterCommitMessage,
)
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.functions import udf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import Row, StructType
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import (
    SPARK_HOME,
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


have_requirements = have_pandas and have_pyarrow
message = pandas_requirement_message or pyarrow_requirement_message


@unittest.skipIf(not have_requirements, message)
class BaseTracebackTestsMixin:
    spark: SparkSession

    def test_basic_data_source_class(self):
        @udf()
        def foo():
            raise ValueError("bar")

        df = self.spark.range(1).select(foo())
        df.show()


class TracebackTests(BaseTracebackTestsMixin, ReusedSQLTestCase):
    ...


if __name__ == "__main__":
    from pyspark.errors.tests.test_traceback import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
