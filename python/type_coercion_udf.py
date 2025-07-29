#!/usr/bin/env python3

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
PySpark UDF dev util for @udf, to test the type coercion difference for different
Spark configs (e.g. arrow enabled, legacy pandas conversion enabled).

Usage (see optional configs below):
# prereq: build Spark locally
python ./type_coercion_udf.py
"""

import array
import datetime
from decimal import Decimal
import argparse
from typing import List, Any

from pyspark.sql import Row, SparkSession
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

TEST_DATA = [
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

TEST_TYPES = [
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


def create_spark_session(use_arrow: bool, legacy_pandas: bool) -> SparkSession:
    """Create Spark session with Arrow and legacy pandas configs"""
    return (
        SparkSession.builder.appName("TypeCoercionUDF")
        .master("local[*]")
        .config("spark.sql.execution.pythonUDF.arrow.enabled", str(use_arrow).lower())
        .config(
            "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled",
            str(legacy_pandas).lower(),
        )
        .getOrCreate()
    )


def run_type_coercion_tests(
    spark: SparkSession,
    test_data: List[Any],
    test_types: List[Any],
    use_arrow: bool,
    legacy_pandas: bool,
) -> str:
    """Type coercion behavior for test data and types."""
    results = []
    count = 0
    total = len(test_types) * len(test_data)

    print("\nTesting configs:")
    print(f"  Arrow enabled: {use_arrow}")
    print(f"  Legacy pandas: {legacy_pandas}")
    print()

    for spark_type in test_types:
        result = []
        for value in test_data:
            try:
                test_udf = udf(lambda _: value, spark_type, useArrow=use_arrow)
                row = spark.range(1).select(test_udf("id")).first()
                result_value = repr(row[0])
            except Exception:
                result_value = "X"

            result.append(result_value)

            count += 1
            print(f"Test {count}/{total}:")
            print(f"  Spark Type: {spark_type.simpleString()}")
            print(f"  Python Value: {value} (type: {type(value).__name__})")
            print(f"  Result: {result_value}\n")

        results.append([spark_type.simpleString()] + list(map(str, result)))

    schema = ["SQL Type \\ Python Value(Type)"] + [
        f"{str(v)}({type(v).__name__})" for v in test_data
    ]

    return spark.createDataFrame(results, schema=schema)._show_string(truncate=False)


def parse_args():
    """Parse command line args for Arrow and legacy pandas settings"""
    parser = argparse.ArgumentParser(
        description="Test PySpark UDF type coercion behavior with/without arrow and legacy pandas",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument("--arrow", action="store_true", help="Enable Arrow-based UDF execution")

    parser.add_argument(
        "--legacy-pandas", action="store_true", help="Enable legacy pandas UDF conversion"
    )

    return parser.parse_args()


def main():
    """Example usage:
    # Test with Arrow enabled, legacy pandas enabled:
    python type_coercion_udf.py --arrow --legacy-pandas

    # Test with Arrow enabled, legacy pandas disabled:
    python type_coercion_udf.py --arrow

    # Test with Arrow disabled (legacy pandas setting ignored):
    python type_coercion_udf.py
    """
    args = parse_args()
    spark = create_spark_session(args.arrow, args.legacy_pandas)

    try:
        results = run_type_coercion_tests(
            spark, TEST_DATA, TEST_TYPES, args.arrow, args.legacy_pandas
        )
        print("\nResults:")
        print(results)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
