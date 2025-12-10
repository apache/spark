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

from decimal import Decimal
import unittest

from pyspark.errors import AnalysisException, PythonException, PySparkNotImplementedError
from pyspark.sql import Row
from pyspark.sql.functions import udf, col
from pyspark.sql.tests.test_udf import BaseUDFTestsMixin
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DayTimeIntervalType,
    DecimalType,
    MapType,
    StringType,
    StructField,
    StructType,
    VarcharType,
)
from pyspark.testing.sqlutils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
    ReusedSQLTestCase,
)
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.util import PythonEvalType


@unittest.skipIf(
    not have_pandas or not have_pyarrow, pandas_requirement_message or pyarrow_requirement_message
)
class ArrowPythonUDFTestsMixin(BaseUDFTestsMixin):
    @unittest.skip("Unrelated test, and it fails when it runs duplicatedly.")
    def test_broadcast_in_udf(self):
        super().test_broadcast_in_udf()

    @unittest.skip("Unrelated test, and it fails when it runs duplicatedly.")
    def test_register_java_function(self):
        super().test_register_java_function()

    @unittest.skip("Unrelated test, and it fails when it runs duplicatedly.")
    def test_register_java_udaf(self):
        super().test_register_java_udaf()

    def test_complex_input_types(self):
        row = (
            self.spark.range(1)
            .selectExpr("array(1, 2, 3) as array", "map('a', 'b') as map", "struct(1, 2) as struct")
            .select(
                udf(lambda x: str(x))("array"),
                udf(lambda x: str(x))("map"),
                udf(lambda x: str(x))("struct"),
            )
            .first()
        )

        self.assertIn(row[0], ["[1, 2, 3]", "[np.int32(1), np.int32(2), np.int32(3)]"])
        self.assertEqual(row[1], "{'a': 'b'}")
        self.assertEqual(row[2], "Row(col1=1, col2=2)")

    def test_use_arrow(self):
        # useArrow=True
        row_true = (
            self.spark.range(1)
            .selectExpr(
                "array(1, 2, 3) as array",
            )
            .select(
                udf(lambda x: str(x), useArrow=True)("array"),
            )
            .first()
        )

        # useArrow=None
        row_none = (
            self.spark.range(1)
            .selectExpr(
                "array(1, 2, 3) as array",
            )
            .select(
                udf(lambda x: str(x), useArrow=None)("array"),
            )
            .first()
        )

        self.assertEqual(row_true[0], row_none[0])  # "[1, 2, 3]"

        # useArrow=False
        row_false = (
            self.spark.range(1)
            .selectExpr(
                "array(1, 2, 3) as array",
            )
            .select(
                udf(lambda x: str(x), useArrow=False)("array"),
            )
            .first()
        )
        self.assertEqual(row_false[0], "[1, 2, 3]")

    def test_eval_type(self):
        self.assertEqual(
            udf(lambda x: str(x), useArrow=True).evalType, PythonEvalType.SQL_ARROW_BATCHED_UDF
        )
        self.assertEqual(
            udf(lambda x: str(x), useArrow=False).evalType, PythonEvalType.SQL_BATCHED_UDF
        )

    def test_register(self):
        df = self.spark.range(1).selectExpr(
            "array(1, 2, 3) as array",
        )

        with self.temp_func("str_repr"):
            str_repr_func = self.spark.udf.register(
                "str_repr", udf(lambda x: str(x), useArrow=True)
            )

            # To verify that Arrow optimization is on
            self.assertIn(
                df.selectExpr("str_repr(array) AS str_id").first()[0],
                ["[1, 2, 3]", "[np.int32(1), np.int32(2), np.int32(3)]"],
                # The input is a NumPy array when the Arrow optimization is on
            )

            # To verify that a UserDefinedFunction is returned
            self.assertListEqual(
                df.selectExpr("str_repr(array) AS str_id").collect(),
                df.select(str_repr_func("array").alias("str_id")).collect(),
            )

    def test_nested_array_input(self):
        df = self.spark.range(1).selectExpr("array(array(1, 2), array(3, 4)) as nested_array")
        self.assertIn(
            df.select(
                udf(lambda x: str(x), returnType="string", useArrow=True)("nested_array")
            ).first()[0],
            [
                "[[1, 2], [3, 4]]",
                "[[np.int32(1), np.int32(2)], [np.int32(3), np.int32(4)]]",
            ],
        )

    def test_type_coercion_string_to_numeric(self):
        df_int_value = self.spark.createDataFrame(["1", "2"], schema="string")
        df_floating_value = self.spark.createDataFrame(["1.1", "2.2"], schema="string")

        int_ddl_types = ["tinyint", "smallint", "int", "bigint"]
        floating_ddl_types = ["double", "float"]

        for ddl_type in int_ddl_types:
            # df_int_value
            res = df_int_value.select(udf(lambda x: x, ddl_type)("value").alias("res"))
            self.assertEqual(res.collect(), [Row(res=1), Row(res=2)])
            self.assertEqual(res.dtypes[0][1], ddl_type)

        floating_results = [
            [Row(res=1.1), Row(res=2.2)],
            [Row(res=1.100000023841858), Row(res=2.200000047683716)],
        ]
        for ddl_type, floating_res in zip(floating_ddl_types, floating_results):
            # df_int_value
            res = df_int_value.select(udf(lambda x: x, ddl_type)("value").alias("res"))
            self.assertEqual(res.collect(), [Row(res=1.0), Row(res=2.0)])
            self.assertEqual(res.dtypes[0][1], ddl_type)
            # df_floating_value
            res = df_floating_value.select(udf(lambda x: x, ddl_type)("value").alias("res"))
            self.assertEqual(res.collect(), floating_res)
            self.assertEqual(res.dtypes[0][1], ddl_type)

        # invalid
        with self.assertRaises(PythonException):
            df_floating_value.select(udf(lambda x: x, "int")("value").alias("res")).collect()

        with self.assertRaises(PythonException):
            df_int_value.select(udf(lambda x: x, "decimal")("value").alias("res")).collect()

        with self.assertRaises(PythonException):
            df_floating_value.select(udf(lambda x: x, "decimal")("value").alias("res")).collect()

    def test_arrow_udf_int_to_decimal_coercion(self):
        with self.sql_conf(
            {"spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": False}
        ):
            df = self.spark.range(0, 3)

            @udf(returnType="decimal(10,2)", useArrow=True)
            def int_to_decimal_udf(val):
                values = [123, 456, 789]
                return values[int(val) % len(values)]

            # Test with coercion enabled
            with self.sql_conf(
                {"spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled": True}
            ):
                result = df.select(int_to_decimal_udf("id").alias("decimal_val")).collect()
                self.assertEqual(result[0]["decimal_val"], Decimal("123.00"))
                self.assertEqual(result[1]["decimal_val"], Decimal("456.00"))
                self.assertEqual(result[2]["decimal_val"], Decimal("789.00"))

            # Test with coercion disabled (should fail)
            with self.sql_conf(
                {"spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled": False}
            ):
                with self.assertRaisesRegex(
                    PythonException, "An exception was thrown from the Python worker"
                ):
                    df.select(int_to_decimal_udf("id").alias("decimal_val")).collect()

            @udf(returnType="decimal(25,1)", useArrow=True)
            def high_precision_udf(val):
                values = [1, 2, 3]
                return values[int(val) % len(values)]

            # Test high precision decimal with coercion enabled
            with self.sql_conf(
                {"spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled": True}
            ):
                result = df.select(high_precision_udf("id").alias("decimal_val")).collect()
                self.assertEqual(len(result), 3)
                self.assertEqual(result[0]["decimal_val"], Decimal("1.0"))
                self.assertEqual(result[1]["decimal_val"], Decimal("2.0"))
                self.assertEqual(result[2]["decimal_val"], Decimal("3.0"))

            # Test high precision decimal with coercion disabled (should fail)
            with self.sql_conf(
                {"spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled": False}
            ):
                with self.assertRaisesRegex(
                    PythonException, "An exception was thrown from the Python worker"
                ):
                    df.select(high_precision_udf("id").alias("decimal_val")).collect()

    def test_decimal_round(self):
        with self.sql_conf(
            {"spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": False}
        ):
            df = self.spark.sql("SELECT DOUBLE(1.234) AS v")

            @udf(returnType=DecimalType(38, 18))
            def f(v: float):
                return Decimal(v)

            rounded = df.select(f("v").alias("d")).first().d
            self.assertEqual(rounded, Decimal("1.233999999999999986"))

    def test_err_return_type(self):
        with self.assertRaises(PySparkNotImplementedError) as pe:
            udf(lambda x: x, VarcharType(10), useArrow=True)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_IMPLEMENTED",
            messageParameters={
                "feature": "Invalid return type with Arrow-optimized Python UDF: VarcharType(10)"
            },
        )

    def test_named_arguments_negative(self):
        @udf("int")
        def test_udf(a, b):
            return a + b

        with self.temp_func("test_udf"):
            self.spark.udf.register("test_udf", test_udf)

            with self.assertRaisesRegex(
                AnalysisException,
                "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
            ):
                self.spark.sql("SELECT test_udf(a => id, a => id * 10) FROM range(2)").show()

            with self.assertRaisesRegex(AnalysisException, "UNEXPECTED_POSITIONAL_ARGUMENT"):
                self.spark.sql("SELECT test_udf(a => id, id * 10) FROM range(2)").show()

            with self.assertRaises(PythonException):
                self.spark.sql("SELECT test_udf(c => 'x') FROM range(2)").show()

            with self.assertRaises(PythonException):
                self.spark.sql("SELECT test_udf(id, a => id * 10) FROM range(2)").show()

    def test_udf_with_udt(self):
        for fallback in [False, True]:
            with self.subTest(fallback=fallback), self.sql_conf(
                {"spark.sql.execution.pythonUDF.arrow.legacy.fallbackOnUDT": fallback}
            ):
                super().test_udf_with_udt()

    def test_udf_use_arrow_and_session_conf(self):
        with self.sql_conf({"spark.sql.execution.pythonUDF.arrow.enabled": "true"}):
            self.assertEqual(
                udf(lambda x: str(x), useArrow=None).evalType, PythonEvalType.SQL_ARROW_BATCHED_UDF
            )
            self.assertEqual(
                udf(lambda x: str(x), useArrow=True).evalType, PythonEvalType.SQL_ARROW_BATCHED_UDF
            )
            self.assertEqual(
                udf(lambda x: str(x), useArrow=False).evalType, PythonEvalType.SQL_BATCHED_UDF
            )
        with self.sql_conf({"spark.sql.execution.pythonUDF.arrow.enabled": "false"}):
            self.assertEqual(
                udf(lambda x: str(x), useArrow=None).evalType, PythonEvalType.SQL_BATCHED_UDF
            )
            self.assertEqual(
                udf(lambda x: str(x), useArrow=True).evalType, PythonEvalType.SQL_ARROW_BATCHED_UDF
            )
            self.assertEqual(
                udf(lambda x: str(x), useArrow=False).evalType, PythonEvalType.SQL_BATCHED_UDF
            )

    def test_day_time_interval_type_casting(self):
        """Test that DayTimeIntervalType UDFs work with Arrow and preserve field specifications."""

        # HOUR TO SECOND
        @udf(useArrow=True, returnType=DayTimeIntervalType(1, 3))
        def return_interval(x):
            return x

        # UDF input: HOUR TO SECOND, UDF output: HOUR TO SECOND
        df = self.spark.sql("SELECT INTERVAL '200:13:50.3' HOUR TO SECOND as value").select(
            return_interval("value").alias("result")
        )
        self.assertEqual(df.schema.fields[0].dataType, DayTimeIntervalType(1, 3))
        self.assertIsNotNone(df.collect()[0]["result"])

        # UDF input: DAY TO SECOND, UDF output: HOUR TO SECOND
        df2 = self.spark.sql("SELECT INTERVAL '1 10:30:45.123' DAY TO SECOND as value").select(
            return_interval("value").alias("result")
        )
        self.assertEqual(df.schema.fields[0].dataType, DayTimeIntervalType(1, 3))
        self.assertIsNotNone(df2.collect()[0]["result"])

    def test_day_time_interval_in_struct(self):
        """Test that DayTimeIntervalType works within StructType with Arrow UDFs."""

        struct_type = StructType(
            [
                StructField("interval_field", DayTimeIntervalType(1, 3)),
                StructField("name", StringType()),
            ]
        )

        @udf(useArrow=True, returnType=struct_type)
        def create_struct_with_interval(interval_val, name_val):
            return Row(interval_field=interval_val, name=name_val)

        df = self.spark.sql(
            """
            SELECT INTERVAL '15:30:45.678' HOUR TO SECOND as interval_val,
                   'test_name' as name_val
        """
        ).select(create_struct_with_interval("interval_val", "name_val").alias("result"))

        self.assertEqual(df.schema.fields[0].dataType, struct_type)
        self.assertEqual(df.schema.fields[0].dataType.fields[0].dataType, DayTimeIntervalType(1, 3))
        result = df.collect()[0]["result"]
        self.assertIsNotNone(result["interval_field"])
        self.assertEqual(result["name"], "test_name")


@unittest.skipIf(
    not have_pandas or not have_pyarrow, pandas_requirement_message or pyarrow_requirement_message
)
class ArrowPythonUDFLegacyTestsMixin(ArrowPythonUDFTestsMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled")
        finally:
            super().tearDownClass()

    def test_udf_binary_type(self):
        def get_binary_type(x):
            return type(x).__name__

        binary_udf = udf(get_binary_type, returnType="string", useArrow=True)

        df = self.spark.createDataFrame(
            [Row(b=b"hello"), Row(b=b"world")], schema=StructType([StructField("b", BinaryType())])
        )
        expected = self.spark.createDataFrame([Row(type_name="bytes"), Row(type_name="bytes")])
        # For Arrow Python UDF with legacy conversion BinaryType is always mapped to bytes
        for conf_val in ["true", "false"]:
            with self.sql_conf({"spark.sql.execution.pyspark.binaryAsBytes": conf_val}):
                result = df.select(binary_udf(col("b")).alias("type_name"))
                assertDataFrameEqual(result, expected)

    def test_udf_binary_type_in_nested_structures(self):
        # For Arrow Python UDF with legacy conversion BinaryType is always mapped to bytes
        # Test binary in array
        def check_array_binary_type(arr):
            return type(arr[0]).__name__

        array_udf = udf(check_array_binary_type, returnType="string")
        df_array = self.spark.createDataFrame(
            [Row(arr=[b"hello", b"world"])],
            schema=StructType([StructField("arr", ArrayType(BinaryType()))]),
        )
        expected = self.spark.createDataFrame([Row(type_name="bytes")])
        for conf_val in ["true", "false"]:
            with self.sql_conf({"spark.sql.execution.pyspark.binaryAsBytes": conf_val}):
                result = df_array.select(array_udf(col("arr")).alias("type_name"))
                assertDataFrameEqual(result, expected)

        # Test binary in map value
        def check_map_binary_type(m):
            return type(list(m.values())[0]).__name__

        map_udf = udf(check_map_binary_type, returnType="string")
        df_map = self.spark.createDataFrame(
            [Row(m={"key": b"value"})],
            schema=StructType([StructField("m", MapType(StringType(), BinaryType()))]),
        )
        for conf_val in ["true", "false"]:
            with self.sql_conf({"spark.sql.execution.pyspark.binaryAsBytes": conf_val}):
                result = df_map.select(map_udf(col("m")).alias("type_name"))
                assertDataFrameEqual(result, expected)

        # Test binary in struct
        def check_struct_binary_type(s):
            return type(s.binary_field).__name__

        struct_udf = udf(check_struct_binary_type, returnType="string")
        df_struct = self.spark.createDataFrame(
            [Row(s=Row(binary_field=b"test", other_field="value"))],
            schema=StructType(
                [
                    StructField(
                        "s",
                        StructType(
                            [
                                StructField("binary_field", BinaryType()),
                                StructField("other_field", StringType()),
                            ]
                        ),
                    )
                ]
            ),
        )
        for conf_val in ["true", "false"]:
            with self.sql_conf({"spark.sql.execution.pyspark.binaryAsBytes": conf_val}):
                result = df_struct.select(struct_udf(col("s")).alias("type_name"))
                assertDataFrameEqual(result, expected)


class ArrowPythonUDFNonLegacyTestsMixin(ArrowPythonUDFTestsMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set(
            "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled", "false"
        )

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled")
        finally:
            super().tearDownClass()


class ArrowPythonUDFTests(ArrowPythonUDFTestsMixin, ReusedSQLTestCase):
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

    @unittest.skip("Duplicate test; it is tested separately in legacy and non-legacy tests")
    def test_udf_binary_type(self):
        super().test_udf_binary_type()

    @unittest.skip("Duplicate test; it is tested separately in legacy and non-legacy tests")
    def test_udf_binary_type_in_nested_structures(self):
        super().test_udf_binary_type_in_nested_structures()


class ArrowPythonUDFLegacyTests(ArrowPythonUDFLegacyTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.concurrency.level", "4")
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.execution.pythonUDF.arrow.concurrency.level")
            cls.spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled")
        finally:
            super().tearDownClass()


class ArrowPythonUDFNonLegacyTests(ArrowPythonUDFNonLegacyTestsMixin, ReusedSQLTestCase):
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


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_python_udf import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
