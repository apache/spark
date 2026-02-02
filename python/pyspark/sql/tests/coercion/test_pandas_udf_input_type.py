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
import datetime
import unittest

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    Row,
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
from pyspark.testing.goldenutils import GoldenFileTestMixin

if have_numpy:
    import numpy as np
if have_pandas:
    import pandas as pd

# If you need to re-generate the golden files, you need to set the
# SPARK_GENERATE_GOLDEN_FILES=1 environment variable before running this test,
# e.g.:
# SPARK_GENERATE_GOLDEN_FILES=1 python/run-tests -k
# --testnames 'pyspark.sql.tests.coercion.test_pandas_udf_input_type'
# If package tabulate https://pypi.org/project/tabulate/ is installed,
# it will also re-generate the Markdown files.


@unittest.skipIf(
    not have_pandas
    or not have_pyarrow
    or not have_numpy
    or LooseVersion(np.__version__) < LooseVersion("2.0.0"),
    pandas_requirement_message or pyarrow_requirement_message or numpy_requirement_message,
)
class PandasUDFInputTypeTests(ReusedSQLTestCase, GoldenFileTestMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.setup_timezone()

    @classmethod
    def tearDownClass(cls):
        cls.teardown_timezone()
        super().tearDownClass()

    @property
    def prefix(self):
        return "golden_pandas_udf_input_type_coercion"

    @property
    def test_cases(self):
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

    @property
    def column_names(self):
        return ["Spark Type", "Spark Value", "Python Type", "Python Value"]

    def run_single_test(self, test_case):
        case_name, spark_type, data_func = test_case
        input_df = data_func(spark_type).repartition(1)
        input_data = [row["value"] for row in input_df.collect()]

        spark_type_str = self.repr_spark_type(spark_type)
        spark_value_str = self.clean_result(str(input_data))

        def type_pandas_udf(data):
            if hasattr(data, "dtype"):
                return pd.Series([str(data.dtype)] * len(data))
            else:
                return pd.Series([str(type(data).__name__)] * len(data))

        type_udf = pandas_udf(type_pandas_udf, returnType=StringType())
        value_udf = pandas_udf(lambda s: s, returnType=spark_type)

        try:
            result_df = input_df.select(
                value_udf("value").alias("python_value"),
                type_udf("value").alias("python_type"),
            )
            results_data = result_df.collect()
            values = [row["python_value"] for row in results_data]
            types = [row["python_type"] for row in results_data]

            python_type_str = self._repr_container(types, container="Series")
            python_value_str = self.clean_result(str(values))
        except Exception as e:
            print("Exception", e)
            python_type_str = "X"
            python_value_str = "X"

        return (
            case_name,
            [
                ("Spark Type", spark_type_str),
                ("Spark Value", spark_value_str),
                ("Python Type", python_type_str),
                ("Python Value", python_value_str),
            ],
        )

    def test_pandas_input_type_coercion_vanilla(self):
        self.run_tests("base")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
