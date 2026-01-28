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
from decimal import Decimal
import itertools
import os
import time
import unittest

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

if have_numpy:
    import numpy as np
if have_pandas:
    import pandas as pd

# If you need to re-generate the golden files, you need to set the
# SPARK_GENERATE_GOLDEN_FILES=1 environment variable before running this test,
# e.g.:
# SPARK_GENERATE_GOLDEN_FILES=1 python/run-tests -k
# --testnames 'pyspark.sql.tests.coercion.test_pandas_udf_return_type'
# If package tabulate https://pypi.org/project/tabulate/ is installed,
# it will also re-generate the Markdown files.


@unittest.skipIf(
    not have_pandas
    or not have_pyarrow
    or not have_numpy
    or LooseVersion(np.__version__) < LooseVersion("2.0.0"),
    pandas_requirement_message or pyarrow_requirement_message or numpy_requirement_message,
)
class PandasUDFReturnTypeTests(ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # Synchronize default timezone between Python and Java
        cls.tz_prev = os.environ.get("TZ", None)  # save current tz if set
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()

        cls.sc.environment["TZ"] = tz
        cls.spark.conf.set("spark.sql.session.timeZone", tz)

    @classmethod
    def tearDownClass(cls):
        del os.environ["TZ"]
        if cls.tz_prev is not None:
            os.environ["TZ"] = cls.tz_prev
        time.tzset()

        super().tearDownClass()

    @property
    def prefix(self):
        return "golden_pandas_udf_return_type_coercion"

    @property
    def test_data(self):
        data = [
            [None, None],
            [True, False],
            list("ab"),
            ["12", "34"],
            [Decimal("1"), Decimal("2")],
            [{"a": 1}, {"b": 2}],
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
            # float128 is not supported on macOS
            # np.arange(1, 3).astype("float128"),
            np.arange(1, 3).astype("complex64"),
            np.arange(1, 3).astype("complex128"),
            [np.array([1, 2, 3], dtype=np.int32), np.array([1, 2, 3], dtype=np.int32)],
            pd.date_range("19700101", periods=2).values,
            pd.date_range("19700101", periods=2, tz="US/Eastern").values,
            [pd.Timedelta("1 day"), pd.Timedelta("2 days")],
            pd.Categorical(["A", "B"]),
            pd.DataFrame({"_1": [1, 2]}),
        ]
        return data

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

    def repr_type(self, spark_type):
        return spark_type.simpleString()

    def repr_value(self, value):
        v_str = value.to_json() if isinstance(value, pd.DataFrame) else str(value)
        v_str = v_str.replace(chr(10), " ")
        v_str = v_str[:32]
        if isinstance(value, np.ndarray):
            return f"{v_str}@ndarray[{value.dtype.name}]"
        elif isinstance(value, pd.DataFrame):
            simple_schema = ", ".join([f"{t} {d.name}" for t, d in value.dtypes.items()])
            return f"{v_str}@Dataframe[{simple_schema}]"
        return f"{v_str}@{type(value).__name__}"

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

    def test_pandas_return_type_coercion_vanilla(self):
        self._run_pandas_udf_return_type_coercion(
            golden_file=f"{self.prefix}_base",
            test_name="Pandas UDF",
        )

    def _run_pandas_udf_return_type_coercion(self, golden_file, test_name):
        self._compare_or_generate_golden(golden_file, test_name)

    def _compare_or_generate_golden(self, golden_file, test_name):
        testing = os.environ.get("SPARK_GENERATE_GOLDEN_FILES", "?") != "1"

        golden_csv = os.path.join(os.path.dirname(__file__), f"{golden_file}.csv")
        golden_md = os.path.join(os.path.dirname(__file__), f"{golden_file}.md")

        golden = None
        if testing:
            golden = pd.read_csv(
                golden_csv,
                sep="\t",
                index_col=0,
                dtype="str",
                na_filter=False,
                engine="python",
            )

        def work(arg):
            spark_type, value = arg
            str_t = self.repr_type(spark_type)
            str_v = self.repr_value(value)

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
                result = repr([row[0] for row in rows])
                # "\t" is used as the delimiter
                result = result.replace("\t", "")
                result = result[:40]
            except Exception:
                result = "X"

            err = None
            if testing:
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

        if testing:
            errs = []
            for _, _, _, err in results:
                if err is not None:
                    errs.append(err)
            self.assertTrue(len(errs) == 0, "\n" + "\n".join(errs) + "\n")

        else:
            index = pd.Index(
                [self.repr_type(t) for t in self.test_types],
                name="SQL Type \\  Value@Type",
            )
            new_golden = pd.DataFrame({}, index=index)
            for v in self.test_data:
                str_v = self.repr_value(v)
                new_golden[str_v] = "?"

            for str_t, str_v, res, _ in results:
                new_golden.loc[str_t, str_v] = res

            # generating the CSV file as the golden file
            new_golden.to_csv(golden_csv, sep="\t", header=True, index=True)

            try:
                # generating the GitHub flavored Markdown file
                # package tabulate is required
                new_golden.to_markdown(golden_md, index=True, tablefmt="github")
            except Exception as e:
                print(
                    f"{test_name} return type coercion: "
                    f"fail to write the markdown file due to {e}!"
                )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
