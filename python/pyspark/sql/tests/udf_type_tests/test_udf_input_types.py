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
import unittest
import pandas as pd

from pyspark.sql import Row
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.loose_version import LooseVersion
from pyspark.testing.utils import (
    have_pyarrow,
    have_pandas,
    have_numpy,
    pyarrow_requirement_message,
    pandas_requirement_message,
    numpy_requirement_message,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase
from .type_table_utils import generate_table_diff, format_type_table

if have_numpy:
    import numpy as np


@unittest.skipIf(
    not have_pandas
    or not have_pyarrow
    or not have_numpy
    or LooseVersion(np.__version__) < LooseVersion("2.0.0")
    or platform.system() == "Darwin",
    pandas_requirement_message
    or pyarrow_requirement_message
    or numpy_requirement_message
    or "float128 not supported on macos",
)
class UDFInputTypeTests(ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        super().setUp()

    def test_udf_input_types_arrow_disabled(self):
        golden_file = os.path.join(
            os.path.dirname(__file__), "golden_udf_input_types_arrow_disabled.txt"
        )
        self._run_udf_input_type_coercion_test(
            config={},
            use_arrow=False,
            golden_file=golden_file,
            test_name="UDF input types - Arrow disabled",
        )

    def test_udf_input_types_arrow_legacy_pandas(self):
        golden_file = os.path.join(
            os.path.dirname(__file__), "golden_udf_input_types_arrow_legacy_pandas.txt"
        )
        self._run_udf_input_type_coercion_test(
            config={"spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": "true"},
            use_arrow=True,
            golden_file=golden_file,
            test_name="UDF input types - Arrow with legacy pandas",
        )

    def test_udf_input_types_arrow_enabled(self):
        golden_file = os.path.join(
            os.path.dirname(__file__), "golden_udf_input_types_arrow_enabled.txt"
        )
        self._run_udf_input_type_coercion_test(
            config={"spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": "false"},
            use_arrow=True,
            golden_file=golden_file,
            test_name="UDF input types - Arrow enabled",
        )

    def _run_udf_input_type_coercion_test(self, config, use_arrow, golden_file, test_name):
        with self.sql_conf(config):
            results = self._generate_udf_input_type_coercion_results(use_arrow)
            actual_output = format_type_table(
                results,
                ["Test Case", "Spark Type", "Spark Value", "Python Type", "Python Value"],
                column_width=85,
            )
            self._compare_or_create_golden_file(actual_output, golden_file, test_name)

    def _generate_udf_input_type_coercion_results(self, use_arrow):
        results = []
        test_cases = self._get_input_type_test_cases()

        for test_name, spark_type, data_func in test_cases:
            input_df = data_func(spark_type).repartition(1)
            input_data = [row["value"] for row in input_df.collect()]
            result_row = [test_name, spark_type.simpleString(), str(input_data)]

            try:

                def type_udf(x):
                    if x is None:
                        return "NoneType"
                    else:
                        return type(x).__name__

                def value_udf(x):
                    return x

                def value_str(x):
                    return str(x)

                type_test_udf = udf(type_udf, returnType=StringType(), useArrow=use_arrow)
                value_test_udf = udf(value_udf, returnType=spark_type, useArrow=use_arrow)
                value_str_udf = udf(value_str, returnType=StringType(), useArrow=use_arrow)

                result_df = input_df.select(
                    value_test_udf("value").alias("python_value"),
                    type_test_udf("value").alias("python_type"),
                    value_str_udf("value").alias("python_value_str"),
                )
                results_data = result_df.collect()
                values = [row["python_value"] for row in results_data]
                types = [row["python_type"] for row in results_data]
                values_str = [row["python_value_str"] for row in results_data]

                # Assert that the UDF output values match the input values
                assert values == input_data, f"Input {values} != output {input_data}"

                result_row.append(str(types))
                result_row.append(str(values_str).replace("\n", " "))

            except Exception as e:
                print("error_msg", e)
                # Clean up exception message to remove newlines and extra whitespace
                error_msg = str(e).replace("\n", " ").replace("\r", " ")
                result_row.append(f"✗ {error_msg}")

            results.append(result_row)

        return results

    def test_pandas_udf_input(self):
        golden_file = os.path.join(os.path.dirname(__file__), "golden_pandas_udf_input_types.txt")
        results = self._generate_pandas_udf_input_type_coercion_results()
        actual_output = format_type_table(
            results,
            ["Test Case", "Spark Type", "Spark Value", "Python Type", "Python Value"],
            column_width=85,
        )
        self._compare_or_create_golden_file(actual_output, golden_file, "Pandas UDF input types")

    def _generate_pandas_udf_input_type_coercion_results(self):
        results = []
        test_cases = self._get_input_type_test_cases()

        for test_name, spark_type, data_func in test_cases:
            input_df = data_func(spark_type).repartition(1)
            input_data = [row["value"] for row in input_df.collect()]
            result_row = [test_name, spark_type.simpleString(), str(input_data)]

            try:

                def type_pandas_udf(data):
                    if hasattr(data, "dtype"):
                        # Series case
                        return pd.Series([str(data.dtype)] * len(data))
                    else:
                        # DataFrame case (for struct types)
                        return pd.Series([str(type(data).__name__)] * len(data))

                def value_pandas_udf(series):
                    return series

                type_test_pandas_udf = pandas_udf(type_pandas_udf, returnType=StringType())
                value_test_pandas_udf = pandas_udf(value_pandas_udf, returnType=spark_type)

                result_df = input_df.select(
                    value_test_pandas_udf("value").alias("python_value"),
                    type_test_pandas_udf("value").alias("python_type"),
                )
                results_data = result_df.collect()
                values = [row["python_value"] for row in results_data]
                types = [row["python_type"] for row in results_data]

                result_row.append(str(types))
                result_row.append(str(values).replace("\n", " "))

            except Exception as e:
                print("error_msg", e)
                error_msg = str(e).replace("\n", " ").replace("\r", " ")
                result_row.append(f"✗ {error_msg}")

            results.append(result_row)

        return results

    def _compare_or_create_golden_file(self, actual_output, golden_file, test_name):
        """Compare actual output with golden file or create golden file if it doesn't exist.

        Args:
            actual_output: The actual output to compare
            golden_file: Path to the golden file
            test_name: Name of the test for error messages
        """
        if os.path.exists(golden_file):
            with open(golden_file, "r") as f:
                expected_output = f.read()

            if actual_output != expected_output:
                diff_output = generate_table_diff(actual_output, expected_output, cell_width=85)
                self.fail(
                    f"""
                    Results don't match golden file for :{test_name}.\n
                    Diff:\n{diff_output}
                    """
                )
        else:
            with open(golden_file, "w") as f:
                f.write(actual_output)
            self.fail(f"Golden file created for {test_name}. Please review and re-run the test.")

    def _create_value_schema(self, data_type):
        """Helper to create a StructType schema with a single 'value' column of the given type."""
        return StructType([StructField("value", data_type, True)])

    def _get_input_type_test_cases(self):
        from pyspark.sql.types import StructType, StructField
        import datetime
        from decimal import Decimal

        def df(args):
            def create_df(data_type):
                # For StructType where the data contains Row objects (not wrapped in tuples)
                if (
                    isinstance(data_type, StructType)
                    and len(args) > 0
                    and args[0][0] is not None
                    and hasattr(args[0][0], "_fields")
                ):
                    schema = data_type
                else:
                    # For all other types, wrap in a "value" column
                    schema = StructType([StructField("value", data_type, True)])
                return self.spark.createDataFrame(args, schema)

            return create_df

        return [
            ("byte_values", ByteType(), df([(-128,), (127,), (0,)])),
            ("byte_null", ByteType(), df([(None,), (42,)])),
            ("short_values", ShortType(), df([(-32768,), (32767,), (0,)])),
            ("short_null", ShortType(), df([(None,), (123,)])),
            ("int_values", IntegerType(), df([(-2147483648,), (2147483647,), (0,)])),
            ("int_null", IntegerType(), df([(None,), (456,)])),
            (
                "long_values",
                LongType(),
                df([(-9223372036854775808,), (9223372036854775807,), (0,)]),
            ),
            ("long_null", LongType(), df([(None,), (789,)])),
            ("float_values", FloatType(), df([(0.0,), (1.0,), (3.14,)])),
            ("float_null", FloatType(), df([(None,), (3.14,)])),
            ("double_values", DoubleType(), df([(0.0,), (1.0,), (1.0 / 3,)])),
            ("double_null", DoubleType(), df([(None,), (2.71,)])),
            ("decimal_values", DecimalType(3, 2), df([(Decimal("5.35"),), (Decimal("1.23"),)])),
            ("decimal_null", DecimalType(3, 2), df([(None,), (Decimal("9.99"),)])),
            ("string_values", StringType(), df([("abc",), ("",), ("hello",)])),
            ("string_null", StringType(), df([(None,), ("test",)])),
            ("binary_values", BinaryType(), df([(b"abc",), (b"",), (bytearray([65, 66, 67]),)])),
            ("binary_null", BinaryType(), df([(None,), (b"test",)])),
            ("boolean_values", BooleanType(), df([(True,), (False,)])),
            ("boolean_null", BooleanType(), df([(None,), (True,)])),
            (
                "date_values",
                DateType(),
                df([(datetime.date(2020, 2, 2),), (datetime.date(1970, 1, 1),)]),
            ),
            ("date_null", DateType(), df([(None,), (datetime.date(2023, 1, 1),)])),
            (
                "timestamp_values",
                TimestampType(),
                df([(datetime.datetime(2020, 2, 2, 12, 15, 16, 123000),)]),
            ),
            (
                "timestamp_null",
                TimestampType(),
                df([(None,), (datetime.datetime(2023, 1, 1, 12, 0, 0),)]),
            ),
            (
                "array_int_values",
                ArrayType(IntegerType()),
                df([([1, 2, 3],), ([],), ([1, None, 3],)]),
            ),
            ("array_int_null", ArrayType(IntegerType()), df([(None,), ([4, 5, 6],)])),
            (
                "map_str_int_values",
                MapType(StringType(), IntegerType()),
                df([({"hello": 1, "world": 2},), ({},)]),
            ),
            (
                "map_str_int_null",
                MapType(StringType(), IntegerType()),
                df([(None,), ({"test": 123},)]),
            ),
            (
                "struct_int_str_values",
                StructType([StructField("a1", IntegerType()), StructField("a2", StringType())]),
                df([(Row(a1=1, a2="hello"),), (Row(a1=2, a2="world"),)]),
            ),
            (
                "struct_int_str_null",
                StructType([StructField("a1", IntegerType()), StructField("a2", StringType())]),
                df([(None,), (Row(a1=99, a2="test"),)]),
            ),
            (
                "array_array_int",
                ArrayType(ArrayType(IntegerType())),
                df([([[1, 2, 3]],), ([[1], [2, 3]],)]),
            ),
            (
                "array_map_str_int",
                ArrayType(MapType(StringType(), IntegerType())),
                df([([{"hello": 1, "world": 2}],), ([{"a": 1}, {"b": 2}],)]),
            ),
            (
                "array_struct_int_str",
                ArrayType(
                    StructType([StructField("a1", IntegerType()), StructField("a2", StringType())])
                ),
                df([([Row(a1=1, a2="hello")],), ([Row(a1=1, a2="hello"), Row(a1=2, a2="world")],)]),
            ),
            (
                "map_int_array_int",
                MapType(IntegerType(), ArrayType(IntegerType())),
                df([({1: [1, 2, 3]},), ({1: [1], 2: [2, 3]},)]),
            ),
            (
                "map_int_map_str_int",
                MapType(IntegerType(), MapType(StringType(), IntegerType())),
                df([({1: {"hello": 1, "world": 2}},)]),
            ),
            (
                "map_int_struct_int_str",
                MapType(
                    IntegerType(),
                    StructType([StructField("a1", IntegerType()), StructField("a2", StringType())]),
                ),
                df([({1: Row(a1=1, a2="hello")},)]),
            ),
            (
                "struct_int_array_int",
                StructType(
                    [StructField("a", IntegerType()), StructField("b", ArrayType(IntegerType()))]
                ),
                df([(Row(a=1, b=[1, 2, 3]),)]),
            ),
            (
                "struct_int_map_str_int",
                StructType(
                    [
                        StructField("a", IntegerType()),
                        StructField("b", MapType(StringType(), IntegerType())),
                    ]
                ),
                df([(Row(a=1, b={"hello": 1, "world": 2}),)]),
            ),
            (
                "struct_int_struct_int_str",
                StructType(
                    [
                        StructField("a", IntegerType()),
                        StructField(
                            "b",
                            StructType(
                                [StructField("a1", IntegerType()), StructField("a2", StringType())]
                            ),
                        ),
                    ]
                ),
                df([(Row(a=1, b=Row(a1=1, a2="hello")),)]),
            ),
        ]


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
