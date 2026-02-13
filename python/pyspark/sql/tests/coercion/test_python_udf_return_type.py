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
import concurrent.futures
import datetime
from decimal import Decimal
import itertools
import os
import re
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
if have_pandas:
    import pandas as pd

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

    def test_str_repr(self):
        self.assertEqual(
            len(self.test_types),
            len(set(self.repr_type(t) for t in self.test_types)),
            "String representations of types should be different!",
        )
        self.assertEqual(
            len(self.test_data),
            len(set(self.repr_value(d) for d in self.test_data)),
            "String representations of values should be different!",
        )

    def test_python_return_type_coercion_vanilla(self):
        self._run_udf_return_type_coercion(
            use_arrow=False,
            legacy_pandas=False,
            golden_file=f"{self.prefix}_vanilla",
            test_name="Vanilla Python UDF",
        )

    def test_python_return_type_coercion_with_arrow(self):
        self._run_udf_return_type_coercion(
            use_arrow=True,
            legacy_pandas=False,
            golden_file=f"{self.prefix}_with_arrow",
            test_name="Arrow Optimized Python UDF",
        )

    def test_python_return_type_coercion_with_arrow_and_pandas(self):
        self._run_udf_return_type_coercion(
            use_arrow=True,
            legacy_pandas=True,
            golden_file=f"{self.prefix}_with_arrow_and_pandas",
            test_name="Arrow Optimized Python UDF with Legacy Pandas Conversion",
        )

    def _run_udf_return_type_coercion(self, use_arrow, legacy_pandas, golden_file, test_name):
        with self.sql_conf(
            {
                "spark.sql.execution.pythonUDF.arrow.enabled": use_arrow,
                "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": legacy_pandas,
            }
        ):
            self._compare_or_generate_golden(golden_file, test_name)

    def _compare_or_generate_golden(self, golden_file, test_name):
        generating = self.is_generating_golden()

        golden_csv = os.path.join(os.path.dirname(__file__), f"{golden_file}.csv")
        golden_md = os.path.join(os.path.dirname(__file__), f"{golden_file}.md")

        golden = None
        if not generating:
            golden = self.load_golden_csv(golden_csv)

        def work(arg):
            spark_type, value = arg
            str_t = self.repr_type(spark_type)
            str_v = self.repr_value(value)

            try:
                test_udf = udf(lambda _: value, spark_type)
                row = self.spark.range(1).select(test_udf("id")).first()
                result = repr(row[0])
                # Normalize Java object hash codes to make tests deterministic
                result = re.sub(r"@[a-fA-F0-9]+", "@<hash>", result)
                result = result[:40]
            except Exception:
                result = "X"

            # Clean up exception message to remove newlines and extra whitespace
            result = self.clean_result(result)

            err = None
            if not generating:
                expected = golden.loc[str_t, str_v]
                if expected != result:
                    err = f"{str_v} => {spark_type} expects {expected} but got {result}"

            return (str_t, str_v, result, err)

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            results = list(
                executor.map(
                    work,
                    itertools.product(self.test_types, self.test_data),
                )
            )

        if not generating:
            errs = []
            for _, _, _, err in results:
                if err is not None:
                    errs.append(err)
            self.assertTrue(len(errs) == 0, "\n" + "\n".join(errs) + "\n")

        else:
            index = pd.Index(
                [self.repr_type(t) for t in self.test_types],
                name="SQL Type \\ Pandas Value(Type)",
            )
            new_golden = pd.DataFrame({}, index=index)
            for v in self.test_data:
                str_v = self.repr_value(v)
                new_golden[str_v] = "?"

            for str_t, str_v, res, _ in results:
                new_golden.loc[str_t, str_v] = res

            self.save_golden(new_golden, golden_csv, golden_md)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
