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

import unittest

from pyspark.sql.functions import col, udf
from pyspark.sql import Row
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    assertDataFrameEqual,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,  # type: ignore[arg-type]
)
class ArrowPythonUDFCachedInputTests(ReusedSQLTestCase):
    """
    Arrow-optimized Python UDFs reading columnar input from an Arrow-cached relation.

    The Arrow-cached scan is a columnar Arrow-backed source, so it drives the columnar Arrow
    Python runner (ColumnarArrowPythonWithNamedArgumentRunner) end to end -- the path an
    Arrow-backed DSv2 source would also take. This pins the init-message protocol between that
    runner and the worker: a mismatched section there desyncs the worker's init parse and hangs
    the task (SPARK-58241).

    This suite runs in its own module because spark.sql.cache.serializer is a static conf latched
    into a JVM-wide singleton on first cache materialization.
    """

    @classmethod
    def conf(cls):
        conf = super().conf()
        # Static conf: must be set before the session is created.
        conf.set(
            "spark.sql.cache.serializer",
            "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer",
        )
        return conf

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled")
        finally:
            super().tearDownClass()

    def test_udf_on_cached_strings(self):
        df = self.spark.createDataFrame(
            [("hello",), (None,), ("world",)], schema="v string"
        ).cache()
        try:
            result = df.select(udf(lambda x: x.upper() if x is not None else None)(col("v")))
            assertDataFrameEqual(result, [Row("HELLO"), Row(None), Row("WORLD")])
        finally:
            df.unpersist()

    def test_udf_on_cached_numeric_column_alongside_unselected_string(self):
        # Only the UDF's input columns are serialized to the worker; the column that is not a
        # UDF input rides through the pass-through recombination, so select it alongside the
        # UDF result to observe that its values line up with the corresponding rows.
        df = self.spark.createDataFrame([(1, "a"), (2, "b")], schema="i long, s string").cache()
        try:
            result = df.select(col("s"), udf(lambda x: x + 1, "long")(col("i")))
            assertDataFrameEqual(result, [Row("a", 2), Row("b", 3)])
        finally:
            df.unpersist()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
