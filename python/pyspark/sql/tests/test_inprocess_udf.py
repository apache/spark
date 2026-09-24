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

"""End-to-end tests for in-process Python UDFs.

Run with python/run-tests like other SQL tests. JEP paths are discovered from the
selected Python environment before the Spark JVM starts. ARROW_C_DATA_JAR must
point to the provided Arrow CDI JAR. Set INPROCESS_TESTS=1
to require the suite (missing dependencies then fail), or 0 to disable it.
Otherwise, the suite runs when JEP, PyArrow and the CDI JAR are available.
"""

import os
import shutil
import tempfile
import unittest
from importlib.util import find_spec
from pathlib import Path
from unittest.mock import patch

from pyspark.testing.sqlutils import ReusedSQLTestCase

_jep_spec = find_spec("jep")
_cdi_jar = os.environ.get("ARROW_C_DATA_JAR")
_test_mode = os.environ.get("INPROCESS_TESTS")
_run_inprocess = _test_mode == "1" or (
    _test_mode != "0"
    and _jep_spec is not None
    and find_spec("pyarrow") is not None
    and _cdi_jar is not None
    and Path(_cdi_jar).is_file()
)


@unittest.skipUnless(_run_inprocess, "Requires JEP, PyArrow and ARROW_C_DATA_JAR")
class InProcessUDFTests(ReusedSQLTestCase):
    """
    End-to-end tests for @inprocess_udf that require jep + CPython + PyArrow.

    The plugin initializes JEP before any task starts. Calls from task threads and
    shutdown must use the same dedicated interpreter thread.
    """

    @classmethod
    def master(cls):
        return "local[2]"

    @classmethod
    def conf(cls):
        return (
            super()
            .conf()
            .set("spark.task.cpus", "0.5")
            .set("spark.driver.extraClassPath", os.pathsep.join([str(cls.jep_jar), cls.cdi_jar]))
            .set("spark.driver.extraLibraryPath", str(cls.jep_dir))
            .set("spark.inprocess.python.sitePackages", cls.site_packages)
            .set("spark.plugins", "org.apache.spark.sql.execution.python.InProcessPythonPlugin")
        )

    @classmethod
    def setUpClass(cls):
        if _jep_spec is None:
            raise RuntimeError("INPROCESS_TESTS=1 requires JEP in the selected Python environment")
        # Do not import jep: it can only be imported by an embedded interpreter.
        cls.jep_dir = Path(_jep_spec.origin).parent
        jars = list(cls.jep_dir.glob("jep-*.jar"))
        if len(jars) != 1:
            raise RuntimeError(f"Expected one JEP JAR in {cls.jep_dir}, found {len(jars)}")
        cls.jep_jar = jars[0]
        if not _cdi_jar or not Path(_cdi_jar).is_file():
            raise RuntimeError("Set ARROW_C_DATA_JAR to the provided Arrow CDI JAR")
        cls.cdi_jar = str(Path(_cdi_jar).resolve())
        cls.site_packages = tempfile.mkdtemp()
        helper_dir = os.path.join(cls.site_packages, "extra")
        system_dir = os.path.join(cls.site_packages, "system")
        os.mkdir(helper_dir)
        os.mkdir(system_dir)
        with open(os.path.join(system_dir, "_inprocess_test_helper.py"), "w") as f:
            f.write("MAGIC = -1\n")
        with open(os.path.join(helper_dir, "_inprocess_test_helper.py"), "w") as f:
            f.write("MAGIC = 99\n")
        with open(os.path.join(cls.site_packages, "helper.pth"), "w") as f:
            f.write("extra\n")
        try:
            # Embedded CPython needs the selected environment before JEP initializes.
            python_path = os.pathsep.join(
                [system_dir, str(cls.jep_dir.parent), os.environ.get("PYTHONPATH", "")]
            )
            with patch.dict(os.environ, {"PYTHONPATH": python_path}):
                super().setUpClass()
        except Exception:
            shutil.rmtree(cls.site_packages)
            raise

    @classmethod
    def tearDownClass(cls):
        try:
            super().tearDownClass()
        finally:
            shutil.rmtree(cls.site_packages)

    def test_expression_arguments_and_multiple_batches(self):
        import pyarrow.compute as pc

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.functions import lit
        from pyspark.sql.types import LongType

        add = inprocess_udf(LongType())(lambda x, y: pc.add(x, y))
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": "2"}):
            df = self.spark.range(11, numPartitions=3)
            values = df.select(add(df.id + 1, lit(2).cast("long"))).collect()
            self.assertEqual([r[0] for r in values], list(range(3, 14)))

    def test_preserved_child_columns_produce_collectable_rows(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        identity = inprocess_udf(LongType())(lambda x: x)
        df = self.spark.range(3)
        rows = df.select(df.id, identity(df.id)).collect()
        self.assertEqual([tuple(r) for r in rows], [(0, 0), (1, 1), (2, 2)])

    def test_unlimited_batch_size(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        identity = inprocess_udf(LongType())(lambda x: x)
        for batch_size in (0, -1):
            with (
                self.subTest(batch_size=batch_size),
                self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": str(batch_size)}),
            ):
                df = self.spark.range(5)
                values = [r[0] for r in df.select(identity(df.id)).collect()]
                self.assertEqual(values, list(range(5)))

    def test_byte_limit_applies_without_a_row_limit(self):
        import pyarrow as pa

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        lengths = inprocess_udf(LongType())(lambda x: pa.array([len(x)] * len(x), type=pa.int64()))
        with self.sql_conf(
            {
                "spark.sql.execution.arrow.maxRecordsPerBatch": "0",
                "spark.sql.execution.arrow.maxBytesPerBatch": "1",
            }
        ):
            df = self.spark.range(4)
            self.assertEqual([r[0] for r in df.select(lengths(df.id)).collect()], [1] * 4)

    def test_wrong_result_length_fails_before_rows_are_read(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        short = inprocess_udf(LongType())(lambda x: x.slice(0, len(x) - 1))
        df = self.spark.range(3, numPartitions=1)
        with self.assertRaisesRegex(Exception, "returned 2 rows; expected 3"):
            df.select(short(df.id)).collect()

    def test_arrow_memory_is_released_on_success_limit_and_failure(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        identity = inprocess_udf(LongType())(lambda x: x)

        @inprocess_udf(LongType())
        def fail(x):
            raise ValueError("second UDF failed")

        arrow_utils = self.spark.sparkContext._jvm.org.apache.spark.sql.util.ArrowUtils
        allocator = arrow_utils.rootAllocator()
        before = allocator.getAllocatedMemory()
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": "2"}):
            df = self.spark.range(9, numPartitions=3)
            for _ in range(3):
                self.assertEqual(df.select(identity(df.id)).collect()[0][0], 0)
                self.assertEqual(allocator.getAllocatedMemory(), before)
            df.select(identity(df.id)).limit(1).collect()
            self.assertEqual(allocator.getAllocatedMemory(), before)
            with self.assertRaisesRegex(Exception, "second UDF failed"):
                df.select(identity(df.id), fail(df.id)).collect()
            self.assertEqual(allocator.getAllocatedMemory(), before)
            # Deserialization fails before Python imports any input CDI structures.
            identity._serialized = b"invalid pickle"
            with self.assertRaisesRegex(Exception, "UnpicklingError"):
                df.select(identity(df.id)).collect()
            self.assertEqual(allocator.getAllocatedMemory(), before)

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
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import IntegerType

        @inprocess_udf(return_type=IntegerType())
        def identity(x):
            return x

        data = [(i,) for i in range(5)]
        df = self.spark.createDataFrame(data, "v int")
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
        from pyspark.sql.types import LongType, StructField, StructType

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
    # Concurrency
    # ------------------------------------------------------------------

    def test_concurrent_tasks_have_separate_function_state(self):
        import pyarrow as pa

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        state = []

        @inprocess_udf(LongType(), deterministic=False)
        def counter(x):
            state.append(1)
            return pa.array([len(state)] * len(x), type=pa.int64())

        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": "2"}):
            df = self.spark.range(8, numPartitions=2)
            for _ in range(2):
                self.assertEqual(
                    [r[0] for r in df.select(counter(df.id)).collect()],
                    [1, 1, 2, 2, 1, 1, 2, 2],
                )

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

    # ------------------------------------------------------------------
    # Non-deterministic flag
    # ------------------------------------------------------------------

    def test_nondeterministic_udf_executes(self):
        """A UDF declared deterministic=False executes and returns correct values."""
        import pyarrow.compute as pc

        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType

        @inprocess_udf(return_type=LongType(), deterministic=False)
        def double(x):
            return pc.multiply(x, 2)

        df = self.spark.range(1, 4)
        result = [r[0] for r in df.select(double(df["id"])).collect()]
        self.assertEqual(result, [2, 4, 6])

    def test_nondeterministic_flag_propagates_to_expression(self):
        """deterministic=False must be reflected in the PythonUDF expression."""
        import pyarrow.compute as pc

        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType

        @inprocess_udf(return_type=LongType(), deterministic=False)
        def double(x):
            return pc.multiply(x, 2)

        df = self.spark.range(3)
        jdf = df.select(double(df["id"]))._jdf
        # Python UDF extraction happens during optimization.
        optimized = jdf.queryExecution().optimizedPlan()

        # Walk the logical plan via children() (no PartialFunction needed)
        # to locate the ArrowEvalPython node inserted during optimization.
        def find_node(plan):
            if plan.getClass().getSimpleName() == "ArrowEvalPython":
                return plan
            children = plan.children().toList()
            for i in range(children.length()):
                found = find_node(children.apply(i))
                if found is not None:
                    return found
            return None

        inprocess_node = find_node(optimized)
        self.assertIsNotNone(inprocess_node, "ArrowEvalPython not found in analyzed plan")
        udfs = inprocess_node.udfs().toList()
        self.assertGreater(udfs.length(), 0)
        self.assertFalse(
            udfs.apply(0).deterministic(),
            "PythonUDF with deterministic=False must have deterministic()==False",
        )

    # ------------------------------------------------------------------
    # String type
    # ------------------------------------------------------------------

    def test_string_identity(self):
        """@inprocess_udf with StringType passes strings through unchanged."""
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import StringType

        @inprocess_udf(return_type=StringType())
        def identity(s):
            return s

        data = [("hello",), ("world",), (None,)]
        df = self.spark.createDataFrame(data, ["v"])
        result = [r[0] for r in df.select(identity(df["v"])).collect()]
        self.assertEqual(result[0], "hello")
        self.assertEqual(result[1], "world")
        self.assertIsNone(result[2])

    def test_string_upper(self):
        """@inprocess_udf with StringType applies utf8_upper transformation."""
        import pyarrow.compute as pc

        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import StringType

        @inprocess_udf(return_type=StringType())
        def upper(s):
            return pc.utf8_upper(s)

        data = [("hello",), ("world",)]
        df = self.spark.createDataFrame(data, ["v"])
        result = [r[0] for r in df.select(upper(df["v"])).collect()]
        self.assertEqual(result, ["HELLO", "WORLD"])

    # ------------------------------------------------------------------
    # Binary type
    # ------------------------------------------------------------------

    def test_binary_identity(self):
        """@inprocess_udf with BinaryType passes bytes through unchanged."""
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import BinaryType

        @inprocess_udf(return_type=BinaryType())
        def identity(b):
            return b

        data = [(b"hello",), (b"world",), (None,)]
        df = self.spark.createDataFrame(data, ["v"])
        result = [r[0] for r in df.select(identity(df["v"])).collect()]
        self.assertEqual(bytes(result[0]), b"hello")
        self.assertEqual(bytes(result[1]), b"world")
        self.assertIsNone(result[2])

    # ------------------------------------------------------------------
    # Array type
    # ------------------------------------------------------------------

    def test_array_identity(self):
        """@inprocess_udf with ArrayType(LongType()) passes arrays through unchanged."""
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import ArrayType, LongType

        @inprocess_udf(return_type=ArrayType(LongType()))
        def identity(arr):
            return arr

        data = [([1, 2, 3],), ([4, 5],), (None,)]
        df = self.spark.createDataFrame(data, ["v"])
        result = [r[0] for r in df.select(identity(df["v"])).collect()]
        self.assertEqual(list(result[0]), [1, 2, 3])
        self.assertEqual(list(result[1]), [4, 5])
        self.assertIsNone(result[2])

    # ------------------------------------------------------------------
    # Struct type
    # ------------------------------------------------------------------

    def test_struct_identity(self):
        """@inprocess_udf with StructType passes structs through unchanged."""
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import DoubleType, LongType, StructField, StructType

        inner = StructType([StructField("a", LongType()), StructField("b", DoubleType())])
        outer = StructType([StructField("v", inner)])

        @inprocess_udf(return_type=inner)
        def identity(s):
            return s

        data = [((1, 2.0),), ((3, 4.0),)]
        df = self.spark.createDataFrame(data, outer)
        result = [r[0] for r in df.select(identity(df["v"])).collect()]
        self.assertEqual(result[0]["a"], 1)
        self.assertAlmostEqual(result[0]["b"], 2.0)
        self.assertEqual(result[1]["a"], 3)
        self.assertAlmostEqual(result[1]["b"], 4.0)

    # ------------------------------------------------------------------
    # Date type
    # ------------------------------------------------------------------

    def test_date_identity(self):
        """@inprocess_udf with DateType passes dates through unchanged."""
        import datetime

        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import DateType

        @inprocess_udf(return_type=DateType())
        def identity(d):
            return d

        dates = [datetime.date(2024, 1, 1), datetime.date(2024, 6, 15), None]
        data = [(d,) for d in dates]
        df = self.spark.createDataFrame(data, ["v"])
        result = [r[0] for r in df.select(identity(df["v"])).collect()]
        self.assertEqual(result[0], datetime.date(2024, 1, 1))
        self.assertEqual(result[1], datetime.date(2024, 6, 15))
        self.assertIsNone(result[2])

    # ------------------------------------------------------------------
    # Timestamp type
    # ------------------------------------------------------------------

    def test_timestamp_identity(self):
        """@inprocess_udf with TimestampType passes timestamps through unchanged."""
        import datetime

        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import TimestampType

        @inprocess_udf(return_type=TimestampType())
        def identity(ts):
            return ts

        data = [(datetime.datetime(2024, 3, 15, 10, 30, 0),), (None,)]
        df = self.spark.createDataFrame(data, ["v"])
        result = [r[0] for r in df.select(identity(df["v"])).collect()]
        self.assertIsNone(result[1])
        # Check date components are preserved (timezone handling may shift hours)
        self.assertEqual(result[0].year, 2024)
        self.assertEqual(result[0].month, 3)
        self.assertEqual(result[0].day, 15)

    # ------------------------------------------------------------------
    # sitePackages / sys.path extension
    # ------------------------------------------------------------------

    def test_site_packages_path_extension_works_in_interpreter(self):
        """The plugin's actual sitePackages config exposes a module to the interpreter."""
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        @inprocess_udf(LongType())
        def read_magic(x):
            import _inprocess_test_helper
            import pyarrow as pa

            return pa.array([_inprocess_test_helper.MAGIC] * len(x), type=pa.int64())

        df = self.spark.range(1)
        self.assertEqual(df.select(read_magic(df.id)).first()[0], 99)

    def test_runtime_restart_from_driver_thread(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        identity = inprocess_udf(LongType())(lambda x: x)
        df = self.spark.range(3)
        expected = df.select(identity(df.id)).collect()
        runtime = self.spark.sparkContext._jvm.org.apache.spark.sql.execution.python
        runtime.InProcessPythonRuntime.shutdown()
        try:
            with self.assertRaisesRegex(Exception, "not running"):
                df.select(identity(df.id)).collect()
        finally:
            paths = self.spark.sparkContext._jvm.java.util.ArrayList()
            paths.add(self.site_packages)
            runtime.InProcessPythonRuntime.initialize(
                self.spark.sparkContext._jvm.PythonUtils.toSeq(paths)
            )
        self.assertEqual(df.select(identity(df.id)).collect(), expected)

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    def test_buggy_udf_exposes_python_traceback(self):
        """A UDF that raises an exception must include the Python traceback in the error.

        The traceback must name the exception type, the error message, and the
        file/line where the exception was raised, matching what you would see in a
        standard Python traceback.
        """
        from pyspark.inprocess.udf import inprocess_udf
        from pyspark.sql.types import LongType

        @inprocess_udf(return_type=LongType())
        def always_fails(x):
            raise ValueError("intentional test error from always_fails")

        df = self.spark.range(1)
        try:
            df.select(always_fails(df["id"])).collect()
            self.fail("Expected exception was not raised")
        except Exception as e:
            error_msg = str(e)
            self.assertIn(
                "ValueError", error_msg, "Exception type must appear in the error message"
            )
            self.assertIn(
                "intentional test error from always_fails",
                error_msg,
                "Exception message must appear in the error",
            )
            self.assertIn(
                "always_fails", error_msg, "UDF function name must appear in the traceback"
            )

    def test_sliced_results_are_read_correctly_by_arrow_java(self):
        import pyarrow as pa

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        cases = [
            (pa.array([9, 1, None, 3]).slice(1), LongType()),
            (pa.array([False, True, None, False]).slice(1), BooleanType()),
            (pa.array(["discard", "one", None, "three"]).slice(1), StringType()),
            (pa.array([[9], [1], None, [3]]).slice(1), ArrayType(LongType())),
            (
                pa.StructArray.from_arrays([pa.array([9, 1, None, 3]).slice(1)], names=["x"]),
                StructType([StructField("x", LongType())]),
            ),
        ]
        df = self.spark.range(3, numPartitions=1)
        for value, return_type in cases:
            with self.subTest(return_type=return_type):
                identity = inprocess_udf(return_type)(lambda x: value)
                actual = [r[0] for r in df.select(identity(df.id)).collect()]
                if isinstance(return_type, StructType):
                    actual = [r.asDict() for r in actual]
                self.assertEqual(actual, value.to_pylist())

    def test_retained_inputs_are_not_overwritten_by_later_batches(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        retained = []

        @inprocess_udf(LongType(), deterministic=False)
        def remember(x):
            for previous, snapshot in retained:
                if previous.to_pylist() != snapshot:
                    raise ValueError("retained input changed")
            retained.append((x, x.to_pylist()))
            return x

        df = self.spark.createDataFrame([(1,), (None,), (3,), (4,), (None,), (6,)], "x long")
        df = df.coalesce(1)
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": "2"}):
            self.assertEqual(
                [r[0] for r in df.select(remember("x")).collect()], [1, None, 3, 4, None, 6]
            )

    def test_pass_through_struct_with_duplicate_names(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql import functions as F
        from pyspark.sql.types import LongType

        identity = inprocess_udf(LongType())(lambda x: x)
        df = self.spark.range(3).select(
            "id", F.struct(F.col("id"), F.col("id").cast("string").alias("id")).alias("s")
        )
        # Materialize the struct so CollapseProject cannot move it above the UDF.
        df.cache()
        try:
            rows = df.select("s", identity("id")).collect()
            self.assertEqual(
                [(tuple(r[0]), r[1]) for r in rows], [((i, str(i)), i) for i in range(3)]
            )
        finally:
            df.unpersist()

    def test_pass_through_interval_does_not_require_arrow_conversion(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        identity = inprocess_udf(LongType())(lambda x: x)
        df = self.spark.range(3).selectExpr(
            "id", "make_interval(0, 0, 0, 0, 0, 0, 10000000000 + id) AS c"
        )
        df.cache()
        try:
            # CalendarInterval cannot be converted to a Python Row. Compare JVM Row values.
            expected = df.select("c", "id")._jdf.collect()
            actual = df.select("c", identity("id"))._jdf.collect()
            self.assertEqual(
                [(r.get(0).toString(), r.getLong(1)) for r in actual],
                [(r.get(0).toString(), r.getLong(1)) for r in expected],
            )
        finally:
            df.unpersist()

    def test_mixed_worker_and_inprocess_udfs(self):
        import pyarrow.compute as pc

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.functions import udf
        from pyspark.sql.types import LongType

        double = inprocess_udf(LongType())(lambda x: pc.multiply(x, 2))
        plus_one = udf(lambda x: x + 1, LongType(), useArrow=False)
        df = self.spark.range(4)
        rows = df.select(double(plus_one("id")), plus_one(double("id"))).collect()
        self.assertEqual([tuple(r) for r in rows], [(2 * (i + 1), 2 * i + 1) for i in range(4)])

    def test_duplicate_names_in_udf_arguments_are_rejected(self):
        import pyarrow as pa

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql import functions as F
        from pyspark.sql.types import LongType

        size = inprocess_udf(LongType())(lambda x: pa.array([len(x)] * len(x), type=pa.int64()))
        df = self.spark.range(3)
        duplicate = F.struct(F.col("id"), F.col("id").cast("string").alias("id"))
        with self.assertRaisesRegex(Exception, "DUPLICATED_FIELD_NAME"):
            df.select(size(duplicate)).collect()

    def test_grouping_aggregate_results_and_nested_calls(self):
        import pyarrow.compute as pc

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql import functions as F
        from pyspark.sql.types import LongType

        double = inprocess_udf(LongType())(lambda x: pc.multiply(x, 2))
        df = self.spark.range(4).selectExpr("id % 2 AS k", "id AS v")
        grouped = df.groupBy("k").agg(double("k").alias("r"))
        self.assertEqual(sorted(r.r for r in grouped.collect()), [0, 2])
        summed = df.groupBy("k").agg(F.sum("v").alias("s")).select(double("s"))
        self.assertEqual(sorted(r[0] for r in summed.collect()), [4, 8])
        constants = df.agg(F.count("*"), double(F.lit(1)))
        self.assertEqual(tuple(constants.first()), (4, 2))
        repeated = df.groupBy(double("k")).agg(double("k"))
        self.assertEqual(sorted(tuple(r) for r in repeated.collect()), [(0, 0), (2, 2)])
        nested = df.select(double("v").alias("x")).select(double("x"))
        self.assertEqual(sorted(r[0] for r in nested.collect()), [0, 4, 8, 12])

    def test_nondeterministic_grouping_and_ordering(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        identity = inprocess_udf(LongType(), deterministic=False)(lambda x: x)
        df = self.spark.range(4)
        self.assertEqual(len(df.groupBy(identity("id")).count().collect()), 4)
        self.assertEqual([r.id for r in df.orderBy(identity("id")).collect()], list(range(4)))

    def test_non_udf_predicates_run_before_udf(self):
        import pyarrow.compute as pc

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql import functions as F
        from pyspark.sql.types import LongType

        divide = inprocess_udf(LongType())(lambda n, d: pc.divide(n, d))
        df = self.spark.createDataFrame([(4, 0), (4, 2)], "n long, d long")
        result = df.filter(F.col("d") != 0).filter(divide("n", "d") > 1).collect()
        self.assertEqual([tuple(r) for r in result], [(4, 2)])

    def test_partition_filters_do_not_evaluate_udfs_on_driver(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql import functions as F
        from pyspark.sql.types import LongType

        identity = inprocess_udf(LongType())(lambda x: x)
        with tempfile.TemporaryDirectory() as path:
            self.spark.range(4).selectExpr("id", "id % 2 AS k").write.partitionBy("k").parquet(
                path, mode="overwrite"
            )
            df = self.spark.read.parquet(path)
            partition_identity = inprocess_udf(df.schema["k"].dataType)(lambda x: x)
            self.assertEqual(
                sorted(r.id for r in df.filter(partition_identity("k") == 1).collect()), [1, 3]
            )
            filtered = df.filter((F.col("k") == 1) & (identity("id") > 1))
            self.assertEqual([r.id for r in filtered.collect()], [3])
            plan = filtered._jdf.queryExecution().executedPlan().toString()
            self.assertRegex(plan, r"PartitionFilters: \[[^\]]*k#")

    def test_nested_nullability_is_compatible(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType, StringType

        identity = inprocess_udf(ArrayType(StringType()))(lambda x: x)
        df = self.spark.createDataFrame([("a,b",), (None,)], "s string")
        self.assertEqual(
            [r[0] for r in df.select(identity(F.split("s", ","))).collect()], [["a", "b"], None]
        )

    def test_captured_spark_broadcasts_and_accumulators_are_rejected(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        broadcast = self.spark.sparkContext.broadcast(1)
        accumulator = self.spark.sparkContext.accumulator(0)
        try:
            for value in (broadcast, accumulator):
                with self.subTest(value=type(value).__name__):
                    with self.assertRaisesRegex(TypeError, "broadcasts or accumulators"):
                        inprocess_udf(LongType())(lambda x: value.value)("id")
        finally:
            broadcast.destroy()

    def test_sql_registration_fails_early(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        identity = inprocess_udf(LongType())(lambda x: x)
        with self.assertRaisesRegex(TypeError, "INVALID_UDF_EVAL_TYPE"):
            self.spark.udf.register("inprocess_identity", identity)

    def test_named_arguments_partition_evaluator_and_columnar_child(self):
        import pyarrow.compute as pc

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        subtract = inprocess_udf(LongType())(lambda x, y: pc.subtract(x, y))
        df = self.spark.range(8).selectExpr("id", "id + 1 as other").cache()
        try:
            df.count()
            for evaluator in ("true", "false"):
                with self.sql_conf(
                    {
                        "spark.sql.execution.usePartitionEvaluator": evaluator,
                        "spark.sql.execution.arrow.pythonUDF.columnarInput.enabled": "true",
                        "spark.sql.adaptive.enabled": "false",
                    }
                ):
                    query = df.select(subtract(y="id", x="other"))
                    self.assertEqual([r[0] for r in query.collect()], [1] * 8)
                    plan = query._jdf.queryExecution().executedPlan().toString()
                    self.assertIn("ArrowEvalPython", plan)
                    nodes = [query._jdf.queryExecution().executedPlan()]
                    metrics = None
                    while nodes:
                        node = nodes.pop()
                        if node.nodeName() == "ArrowEvalPython":
                            # A dual-mode cache scan can execute rows directly without a transition.
                            self.assertTrue(node.child().supportsColumnar())
                            self.assertFalse(node.supportsColumnar())
                            metrics = node.metrics()
                        children = node.children().iterator()
                        while children.hasNext():
                            nodes.append(children.next())
                    self.assertIsNotNone(metrics)
                    self.assertEqual(metrics.apply("pythonNumRowsReceived").value(), 8)
                    self.assertGreater(metrics.apply("pythonDataSent").value(), 0)
                    self.assertGreater(metrics.apply("pythonDataReceived").value(), 0)
        finally:
            df.unpersist()

    def test_nonroot_limit_and_offset_preserve_order(self):
        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        identity = inprocess_udf(LongType())(lambda x: x)
        with self.sql_conf(
            {
                "spark.sql.execution.topKSortFallbackThreshold": "1",
                "spark.sql.shuffle.partitions": "4",
                "spark.sql.adaptive.enabled": "false",
            }
        ):
            df = self.spark.range(100, numPartitions=4).orderBy("id")
            mapped = df.select(identity("id").alias("v"))
            for offset in (0, 7):
                query = mapped.offset(offset).limit(10).distinct()
                self.assertEqual(
                    sorted(r.v for r in query.collect()), list(range(offset, offset + 10))
                )
                self.assertIn("Sort [id#", query._jdf.queryExecution().executedPlan().toString())

    def test_map_entries_offset_and_custom_names(self):
        import pyarrow as pa

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType, MapType, StringType

        def make_map(x):
            map_type = pa.map_(pa.field("k", pa.string(), False), pa.field("v", pa.int64()))
            entries = pa.StructArray.from_arrays(
                [pa.array(["hidden", "a", "b", "c"]), pa.array([None, 1, 2, 3])],
                fields=[map_type.key_field, map_type.item_field],
            )
            offsets = pa.array([0, 1, 3], type=pa.int32())
            return pa.Array.from_buffers(
                map_type, 2, [None, offsets.buffers()[1]], children=[entries.slice(1)]
            )

        udf = inprocess_udf(MapType(StringType(), LongType()))(make_map)
        rows = self.spark.range(2, numPartitions=1).select(udf("id")).collect()
        self.assertEqual([r[0] for r in rows], [{"a": 1}, {"b": 2, "c": 3}])

    def test_hidden_struct_nulls(self):
        import pyarrow as pa
        import pyarrow.compute as pc

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import IntegerType, StructField, StructType

        @inprocess_udf(StructType([StructField("len", IntegerType(), False)]))
        def lengths(s):
            return pa.StructArray.from_arrays(
                [pc.utf8_length(s)], names=["len"], mask=pc.is_null(s)
            )

        df = self.spark.createDataFrame([(None,), ("a",)], "s string")
        self.assertEqual([r[0] for r in df.select(lengths("s")).collect()], [None, (1,)])

    def test_large_closure_uses_bulk_transfer(self):
        import pyarrow as pa

        from pyspark.inprocess import inprocess_udf
        from pyspark.sql.types import LongType

        model = bytes(range(256)) * 16384

        @inprocess_udf(LongType())
        def lookup(x):
            return pa.array([model[i % len(model)] for i in x.to_pylist()], type=pa.int64())

        df = self.spark.range(256, numPartitions=4)
        self.assertEqual([r[0] for r in df.select(lookup("id")).collect()], list(range(256)))


if __name__ == "__main__":
    from pyspark.testing import main

    main()
