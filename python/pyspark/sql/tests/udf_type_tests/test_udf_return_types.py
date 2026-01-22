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

import concurrent.futures
import os
import platform
import unittest
from decimal import Decimal
import pandas as pd

from pyspark.sql.functions import pandas_udf
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
class UDFReturnTypeTests(ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        super().setUp()

        self.test_types = [
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

        self.pandas_test_data = [
            [None, None],
            [True, False],
            np.arange(1, 3).astype("int8"),
            np.arange(1, 3).astype("int16"),
            np.arange(1, 3).astype("int32"),
            np.arange(1, 3).astype("int64"),
            np.arange(1, 3).astype("uint8"),
            np.arange(1, 3).astype("uint16"),
            np.arange(1, 3).astype("uint32"),
            np.arange(1, 3).astype("uint64"),
            np.arange(1, 3).astype("float16"),
            np.arange(1, 3).astype("float32"),
            np.arange(1, 3).astype("float64"),
            np.arange(1, 3).astype("float128"),
            np.arange(1, 3).astype("complex64"),
            np.arange(1, 3).astype("complex128"),
            list("ab"),
            ["12", "34"],
            [np.array([1, 2, 3], dtype=np.int32), np.array([1, 2, 3], dtype=np.int32)],
            [Decimal("1"), Decimal("2")],
            pd.date_range("19700101", periods=2).values,
            pd.date_range("19700101", periods=2, tz="US/Eastern").values,
            [pd.Timedelta("1 day"), pd.Timedelta("2 days")],
            pd.Categorical(["A", "B"]),
            pd.DataFrame({"_1": [1, 2]}),
            [{"a": 1}, {"b": 2}],
        ]

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
                diff_output = generate_table_diff(actual_output, expected_output)
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

    def test_pandas_udf_return_type_coercion(self):
        golden_file = os.path.join(
            os.path.dirname(__file__), "golden_pandas_udf_return_type_coercion.txt"
        )

        test_name = "Pandas UDF type coercion"

        results = self._generate_pandas_udf_type_coercion_results()
        header = ["SQL Type \\ Pandas Value(Type)"] + [
            f"{str(v).replace(chr(10), ' ')}({type(v).__name__})" for v in self.pandas_test_data
        ]
        actual_output = format_type_table(results, header)
        self._compare_or_create_golden_file(actual_output, golden_file, test_name)

    def _generate_pandas_udf_type_coercion_results(self):
        def work(spark_type):
            result = [spark_type.simpleString()]
            for value in self.pandas_test_data:
                try:

                    @pandas_udf(returnType=spark_type)
                    def pandas_udf_func(series: pd.Series) -> pd.Series:
                        assert len(series) == 2
                        if isinstance(value, pd.DataFrame):
                            return value
                        else:
                            return pd.Series(value)

                    rows = (
                        self.spark.range(0, 2, 1, 1)
                        .select(pandas_udf_func("id").alias("result"))
                        .collect()
                    )
                    ret_str = repr([row[0] for row in rows])
                except Exception:
                    ret_str = "X"
                result.append(ret_str)
            return result

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            return list(executor.map(work, self.test_types))


if __name__ == "__main__":
    from pyspark.testing import main

    main()
