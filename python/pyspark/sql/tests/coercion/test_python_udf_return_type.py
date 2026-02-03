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

import array
import datetime
from decimal import Decimal
import unittest

from pyspark.sql import Row
from pyspark.sql.functions import udf
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
from pyspark.testing.goldenutils import GoldenFileTestMixin

if have_numpy:
    import numpy as np

# If you need to re-generate the golden files, you need to set the
# SPARK_GENERATE_GOLDEN_FILES=1 environment variable before running this test,
# e.g.:
# SPARK_GENERATE_GOLDEN_FILES=1 python/run-tests -k
# --testnames 'pyspark.sql.tests.coercion.test_python_udf_return_type'
# If package tabulate https://pypi.org/project/tabulate/ is installed,
# it will also re-generate the Markdown files.


@unittest.skipIf(
    not have_pandas
    or not have_pyarrow
    or not have_numpy
    or LooseVersion(np.__version__) < LooseVersion("2.0.0"),
    pandas_requirement_message or pyarrow_requirement_message or numpy_requirement_message,
)
class UDFReturnTypeTests(GoldenFileTestMixin, ReusedSQLTestCase):
    @property
    def prefix(self):
        return "golden_python_udf_return_type_coercion"

    @property
    def test_data(self):
        return [
            None,
            True,
            1,
            "a",
            datetime.date(1970, 1, 1),
            datetime.datetime(1970, 1, 1, 0, 0),
            1.0,
            array.array("i", [1]),
            [1],
            (1,),
            bytearray([65, 66, 67]),
            Decimal(1),
            {"a": 1},
            Row(kwargs=1),
            Row("namedtuple")(1),
        ]

    @property
    def test_types(self):
        return [
            BooleanType(),
            ByteType(),
            ShortType(),
            IntegerType(),
            LongType(),
            StringType(),
            DateType(),
            TimestampType(),
            FloatType(),
            DoubleType(),
            ArrayType(IntegerType()),
            BinaryType(),
            DecimalType(10, 0),
            MapType(StringType(), IntegerType()),
            StructType([StructField("_1", IntegerType())]),
        ]

    @property
    def test_cases(self):
        return self.test_data

    @property
    def column_names(self):
        return [self.repr_spark_type(t) for t in self.test_types]

    def run_single_test(self, value):
        source_value = self.repr_value(value)
        results = []

        for spark_type in self.test_types:
            target_type = self.repr_spark_type(spark_type)
            try:
                test_udf = udf(lambda _: value, spark_type)
                row = self.spark.range(1).select(test_udf("id")).first()
                return_value = self.repr_value(row[0])
            except Exception:
                return_value = "X"
            results.append((target_type, return_value))

        return (source_value, results)

    def test_str_repr(self):
        self.assertEqual(
            len(self.test_types),
            len(set(self.repr_spark_type(t) for t in self.test_types)),
            "String representations of types should be different!",
        )
        self.assertEqual(
            len(self.test_data),
            len(set(self.repr_value(d) for d in self.test_data)),
            "String representations of values should be different!",
        )

    def test_python_return_type_coercion_vanilla(self):
        with self.sql_conf({"spark.sql.execution.pythonUDF.arrow.enabled": False}):
            self.run_tests("vanilla")

    def test_python_return_type_coercion_with_arrow(self):
        with self.sql_conf({"spark.sql.execution.pythonUDF.arrow.enabled": True}):
            self.run_tests("with_arrow")

    def test_python_return_type_coercion_with_arrow_and_pandas(self):
        with self.sql_conf(
            {
                "spark.sql.execution.pythonUDF.arrow.enabled": True,
                "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": True,
            }
        ):
            self.run_tests("with_arrow_and_pandas")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
