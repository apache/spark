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
import os
import platform
import unittest
from decimal import Decimal
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
class UDFReturnTypeTests(ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.test_data = [
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

    def test_udf_return_type_coercion_arrow_disabled(self):
        golden_file = os.path.join(
            os.path.dirname(__file__), "golden_udf_return_type_coercion_arrow_disabled.txt"
        )
        self._run_udf_return_type_coercion_test(
            use_arrow=False,
            legacy_pandas=False,
            golden_file=golden_file,
            test_name="Arrow disabled",
        )

    def test_udf_return_type_coercion_arrow_legacy_pandas(self):
        golden_file = os.path.join(
            os.path.dirname(__file__), "golden_udf_return_type_coercion_arrow_legacy_pandas.txt"
        )
        self._run_udf_return_type_coercion_test(
            use_arrow=True,
            legacy_pandas=True,
            golden_file=golden_file,
            test_name="Arrow enabled, legacy pandas enabled",
        )

    def test_udf_return_type_coercion_arrow_enabled(self):
        golden_file = os.path.join(
            os.path.dirname(__file__), "golden_udf_return_type_coercion_arrow_enabled.txt"
        )
        self._run_udf_return_type_coercion_test(
            use_arrow=True,
            legacy_pandas=False,
            golden_file=golden_file,
            test_name="Arrow enabled, legacy pandas disabled",
        )

    def _run_udf_return_type_coercion_test(self, use_arrow, legacy_pandas, golden_file, test_name):
        with self.sql_conf(
            {
                "spark.sql.execution.pythonUDF.arrow.enabled": str(use_arrow).lower(),
                "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": str(
                    legacy_pandas
                ).lower(),
            }
        ):
            results = self._generate_udf_return_type_coercion_results(use_arrow)
            header = ["SQL Type \\ Python Value(Type)"] + [
                f"{str(v)}({type(v).__name__})" for v in self.test_data
            ]
            actual_output = format_type_table(results, header)
            self._compare_or_create_golden_file(actual_output, golden_file, test_name)

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

    def _generate_udf_return_type_coercion_results(self, use_arrow):
        results = []

        for spark_type in self.test_types:
            result = [spark_type.simpleString()]
            for value in self.test_data:
                try:
                    test_udf = udf(lambda _: value, spark_type, useArrow=use_arrow)
                    row = self.spark.range(1).select(test_udf("id")).first()
                    result_value = repr(row[0])
                    # Normalize Java object hash codes to make tests deterministic
                    import re

                    result_value = re.sub(r"@[a-fA-F0-9]+", "@<hash>", result_value)
                except Exception:
                    result_value = "X"
                result.append(result_value)
            results.append(result)

        return results

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
        results = []

        for spark_type in self.test_types:
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
            results.append(result)

        return results


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
