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

"""
End-to-end integration tests for in-process Python UDFs.

These tests exercise the full decorator-to-execution path:
    @inprocess_udf  →  InProcessUDFWrapper.__call__
                    →  sc._jvm.InProcessPythonUDFBuilder.build
                    →  InProcessPythonUDF (Catalyst expression)
                    →  ExtractInProcessPythonUDFs (optimizer rule)
                    →  InProcessArrowEvalExec (physical plan)
                    →  InProcessPythonRuntime.invoke (jep call)
                    →  _inprocess_invoke  →  pa.foreign_buffer  →  UDF
                    →  array_to_result  →  ArrowColumnVector

Requirements:
    - jep >= 4.2 on the JVM classpath (``spark.driver.extraClassPath``)
    - Python cloudpickle + pyarrow installed
    - Spark executor configured for single-task-per-executor:
        spark.executor.cores == spark.task.cpus  (enforced by InProcessPythonChecks)

Skip: all tests are skipped when jep is not importable or when the
      INPROCESS_TESTS environment variable is not set to "1".
"""

import contextlib
import os
import unittest

from pyspark.sql import SparkSession
from pyspark.testing.sqlutils import ReusedSQLTestCase


def _have_pyarrow() -> bool:
    try:
        import pyarrow  # noqa: F401
        return True
    except ImportError:
        return False


def _have_cloudpickle() -> bool:
    try:
        import cloudpickle  # noqa: F401
        return True
    except ImportError:
        return False


_run_inprocess = (
    os.environ.get("INPROCESS_TESTS", "0") == "1"
    and _have_pyarrow()
    and _have_cloudpickle()
)

_skip_reason = (
    "Skipping in-process UDF tests: requires INPROCESS_TESTS=1 "
    "and pyarrow + cloudpickle installed, "
    "and jep JAR on the Spark executor classpath"
)


@unittest.skipUnless(_run_inprocess, _skip_reason)
class InProcessUDFTests(ReusedSQLTestCase):
    """
    End-to-end tests for @inprocess_udf that require jep + CPython + PyArrow.

    The SparkSession is shared via ReusedSQLTestCase. InProcessPythonRuntime
    is initialized once per class via setUpClass by calling the JVM singleton
    directly -- this avoids needing spark.plugins set at SparkSession startup.

    For local-mode tests the default is executor.cores=1, task.cpus=1 which
    satisfies the InProcessPythonChecks constraint automatically.
    """

    @classmethod
    def setUpClass(cls):
        # Use local[1] so all tasks execute on a single thread, ensuring the
        # lazily-initialized SharedInterpreter is always called from the same thread.
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("InProcessUDFTests")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        # Shut down the runtime before stopping the session
        sc = cls.spark.sparkContext
        sc._jvm.org.apache.spark.sql.execution.python.InProcessPythonRuntime.shutdown()
        cls.spark.stop()

    @contextlib.contextmanager
    def _raw_sqlconf(self, pairs):
        """Set SQLConf key/value pairs directly, bypassing static-config restrictions.

        ``spark.conf.set()`` rejects static configs (e.g. spark.executor.cores,
        spark.task.cpus) with CANNOT_MODIFY_CONFIG.  Calling
        ``SQLConf.setConfString`` directly (no static check there) lets tests
        override these values and restore them afterwards.
        """
        jvm = self.spark.sparkContext._jvm
        sqlconf = jvm.org.apache.spark.sql.internal.SQLConf.get()
        saved = {}
        for k in pairs:
            try:
                saved[k] = sqlconf.getConfString(k)
            except Exception:
                saved[k] = None
        for k, v in pairs.items():
            sqlconf.setConfString(k, v)
        try:
            yield
        finally:
            for k, old_v in saved.items():
                if old_v is None:
                    sqlconf.unsetConf(k)
                else:
                    sqlconf.setConfString(k, old_v)

    # ------------------------------------------------------------------
    # Basic numeric UDFs
    # ------------------------------------------------------------------

    def test_double_long(self):
        """@inprocess_udf with LongType input/output doubles each value."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType

        @inprocess_udf(return_type=LongType())
        def double(x):
            return pc.multiply(x, 2)

        df = self.spark.range(1, 6)  # [1, 2, 3, 4, 5]
        result = df.select(double(df["id"])).collect()
        self.assertEqual([r[0] for r in result], [2, 4, 6, 8, 10])

    def test_negate_double(self):
        """@inprocess_udf with DoubleType negates each value."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import DoubleType

        @inprocess_udf(return_type=DoubleType())
        def negate(x):
            return pc.negate(x)

        data = [(1.5,), (2.5,), (3.0,)]
        df = self.spark.createDataFrame(data, ["v"])
        result = [r[0] for r in df.select(negate(df["v"])).collect()]
        self.assertAlmostEqual(result[0], -1.5)
        self.assertAlmostEqual(result[1], -2.5)
        self.assertAlmostEqual(result[2], -3.0)

    def test_identity_integer(self):
        """@inprocess_udf with IntegerType passes values through unchanged."""
        import pyarrow as pa
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import IntegerType

        @inprocess_udf(return_type=IntegerType())
        def identity(x):
            return x

        data = [(i,) for i in range(5)]
        df = self.spark.createDataFrame(data, ["v"])
        result = [r[0] for r in df.select(identity(df["v"])).collect()]
        self.assertEqual(result, list(range(5)))

    def test_boolean_not(self):
        """@inprocess_udf with BooleanType inverts each boolean."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import BooleanType

        @inprocess_udf(return_type=BooleanType())
        def invert(x):
            return pc.invert(x)

        data = [(True,), (False,), (True,)]
        df = self.spark.createDataFrame(data, ["v"])
        result = [r[0] for r in df.select(invert(df["v"])).collect()]
        self.assertEqual(result, [False, True, False])

    # ------------------------------------------------------------------
    # Null handling
    # ------------------------------------------------------------------

    def test_null_passthrough(self):
        """Null values in the input must produce null in the output."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType

        @inprocess_udf(return_type=LongType())
        def negate(x):
            return pc.negate(x)

        data = [1, None, 3]
        df = self.spark.createDataFrame([(v,) for v in data], ["v"])
        rows = df.select(negate(df["v"])).collect()

        self.assertEqual(rows[0][0], -1)
        self.assertIsNone(rows[1][0])
        self.assertEqual(rows[2][0], -3)

    def test_all_nulls(self):
        """Column of all-null values: every output row must be null."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType, StructType, StructField

        @inprocess_udf(return_type=LongType())
        def double(x):
            return pc.multiply(x, 2)

        schema = StructType([StructField("v", LongType(), nullable=True)])
        data = [(None,), (None,), (None,)]
        df = self.spark.createDataFrame(data, schema)
        rows = df.select(double(df["v"])).collect()

        for row in rows:
            self.assertIsNone(row[0])

    # ------------------------------------------------------------------
    # Multi-column UDFs
    # ------------------------------------------------------------------

    def test_two_column_add(self):
        """UDF that adds two LongType columns together."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType

        @inprocess_udf(return_type=LongType())
        def add(a, b):
            return pc.add(a, b)

        data = [(1, 10), (2, 20), (3, 30)]
        df = self.spark.createDataFrame(data, ["a", "b"])
        result = [r[0] for r in df.select(add(df["a"], df["b"])).collect()]
        self.assertEqual(result, [11, 22, 33])

    def test_two_column_multiply(self):
        """UDF that multiplies two DoubleType columns."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import DoubleType

        @inprocess_udf(return_type=DoubleType())
        def multiply(a, b):
            return pc.multiply(a, b)

        data = [(2.0, 3.0), (4.0, 5.0)]
        df = self.spark.createDataFrame(data, ["a", "b"])
        result = [r[0] for r in df.select(multiply(df["a"], df["b"])).collect()]
        self.assertAlmostEqual(result[0], 6.0)
        self.assertAlmostEqual(result[1], 20.0)

    # ------------------------------------------------------------------
    # UDF reuse and multiple UDFs on the same query
    # ------------------------------------------------------------------

    def test_two_udfs_same_select(self):
        """Two different @inprocess_udf calls in the same select are both executed."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType

        @inprocess_udf(return_type=LongType())
        def double(x):
            return pc.multiply(x, 2)

        @inprocess_udf(return_type=LongType())
        def triple(x):
            return pc.multiply(x, 3)

        df = self.spark.range(1, 4)  # [1, 2, 3]
        rows = df.select(double(df["id"]), triple(df["id"])).collect()
        self.assertEqual([r[0] for r in rows], [2, 4, 6])
        self.assertEqual([r[1] for r in rows], [3, 6, 9])

    def test_udf_reuse_across_queries(self):
        """The same InProcessUDFWrapper can be applied to different DataFrames."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType

        @inprocess_udf(return_type=LongType())
        def double(x):
            return pc.multiply(x, 2)

        df1 = self.spark.range(1, 4)
        df2 = self.spark.range(10, 13)

        result1 = [r[0] for r in df1.select(double(df1["id"])).collect()]
        result2 = [r[0] for r in df2.select(double(df2["id"])).collect()]

        self.assertEqual(result1, [2, 4, 6])
        self.assertEqual(result2, [20, 22, 24])

    # ------------------------------------------------------------------
    # Concurrency config validation
    # ------------------------------------------------------------------

    def test_config_check_rejects_multi_task_executor(self):
        """InProcessPythonChecks must raise when executor.cores > task.cpus."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType
        from pyspark.errors import IllegalArgumentException

        @inprocess_udf(return_type=LongType())
        def double(x):
            return pc.multiply(x, 2)

        df = self.spark.range(5)
        # spark.executor.cores and spark.task.cpus are static configs; use
        # _raw_sqlconf to bypass the CANNOT_MODIFY_CONFIG restriction.
        with self._raw_sqlconf({"spark.executor.cores": "4", "spark.task.cpus": "1"}):
            with self.assertRaisesRegex(IllegalArgumentException, "concurrent tasks"):
                df.select(double(df["id"])).collect()

    def test_config_check_passes_when_single_task(self):
        """InProcessPythonChecks must not raise when executor.cores == task.cpus."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType

        @inprocess_udf(return_type=LongType())
        def double(x):
            return pc.multiply(x, 2)

        df = self.spark.range(1, 4)
        with self._raw_sqlconf({"spark.executor.cores": "2", "spark.task.cpus": "2"}):
            result = [r[0] for r in df.select(double(df["id"])).collect()]
        self.assertEqual(result, [2, 4, 6])

    # ------------------------------------------------------------------
    # Closure capture
    # ------------------------------------------------------------------

    def test_udf_captures_closure(self):
        """UDF closure values defined in outer scope are serialized correctly."""
        import pyarrow.compute as pc
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType

        factor = 7  # captured in closure

        @inprocess_udf(return_type=LongType())
        def scale(x):
            return pc.multiply(x, factor)

        df = self.spark.range(1, 4)
        result = [r[0] for r in df.select(scale(df["id"])).collect()]
        self.assertEqual(result, [7, 14, 21])


if __name__ == "__main__":
    from pyspark.testing.utils import PySparkTestCase

    unittest.main()
