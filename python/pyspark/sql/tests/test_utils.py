# -*- encoding: utf-8 -*-
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
import difflib
import unittest
from itertools import zip_longest

import pyspark.sql.functions as F
from pyspark.errors import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    PySparkAssertionError,
    PySparkTypeError,
    PySparkValueError,
    QueryContextType,
    SparkUpgradeException,
)
from pyspark.sql import Row
from pyspark.sql.functions import from_unixtime, to_date, unix_timestamp
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase, have_pandas, have_pyarrow
from pyspark.testing.utils import (
    _context_diff,
    assertColumnNonNull,
    assertColumnUnique,
    assertColumnValuesInSet,
    assertDataFrameEqual,
    assertReferentialIntegrity,
    assertSchemaEqual,
    have_numpy,
)


class UtilsTestsMixin:
    def test_assert_equal_inttype(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                ("2", 3000),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                ("2", 3000),
            ],
            schema=["id", "amount"],
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_equal_arraytype(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("john", ["Python", "Java"]),
                ("jane", ["Scala", "SQL", "Java"]),
            ],
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("languages", ArrayType(StringType()), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("john", ["Python", "Java"]),
                ("jane", ["Scala", "SQL", "Java"]),
            ],
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("languages", ArrayType(StringType()), True),
                ]
            ),
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_approx_equal_arraytype_float(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("student1", [97.01, 89.23]),
                ("student2", [91.86, 84.34]),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grades", ArrayType(FloatType()), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("student1", [97.01, 89.23]),
                ("student2", [91.86, 84.339999]),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grades", ArrayType(FloatType()), True),
                ]
            ),
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_approx_equal_arraytype_float_default_rtol_fail(self):
        # fails with default rtol, 1e-5
        df1 = self.spark.createDataFrame(
            data=[
                ("student1", [97.01, 89.23]),
                ("student2", [91.86, 84.34]),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grades", ArrayType(FloatType()), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("student1", [97.01, 89.23]),
                ("student2", [91.86, 84.341]),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grades", ArrayType(FloatType()), True),
                ]
            ),
        )

        rows_str1 = ""
        rows_str2 = ""

        # count different rows
        for r1, r2 in list(zip_longest(df1.collect(), df2.collect())):
            rows_str1 += str(r1) + "\n"
            rows_str2 += str(r2) + "\n"

        generated_diff = _context_diff(
            actual=rows_str1.splitlines(), expected=rows_str2.splitlines(), n=2
        )

        error_msg = "Results do not match: "
        percent_diff = (1 / 2) * 100
        error_msg += "( %.5f %% )" % percent_diff
        error_msg += "\n" + "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

    def test_assert_approx_equal_arraytype_float_custom_rtol_pass(self):
        # passes with custom rtol, 1e-2
        df1 = self.spark.createDataFrame(
            data=[
                ("student1", [97.01, 89.23]),
                ("student2", [91.86, 84.34]),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grades", ArrayType(FloatType()), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("student1", [97.01, 89.23]),
                ("student2", [91.86, 84.341]),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grades", ArrayType(FloatType()), True),
                ]
            ),
        )

        assertDataFrameEqual(df1, df2, rtol=1e-2)

    def test_assert_approx_equal_doubletype_custom_rtol_pass(self):
        # passes with custom rtol, 1e-2
        df1 = self.spark.createDataFrame(
            data=[
                ("student1", 97.01),
                ("student2", 84.34),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grade", DoubleType(), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("student1", 97.01),
                ("student2", 84.341),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grade", DoubleType(), True),
                ]
            ),
        )

        assertDataFrameEqual(df1, df2, rtol=1e-2)

    def test_assert_approx_equal_decimaltype_custom_rtol_pass(self):
        # passes with custom rtol, 1e-2
        df1 = self.spark.createDataFrame(
            data=[
                ("student1", 83.14),
                ("student2", 97.12),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grade", DoubleType(), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("student1", 83.14),
                ("student2", 97.111),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grade", DoubleType(), True),
                ]
            ),
        )

        # cast to DecimalType
        df1 = df1.withColumn("col_1", F.col("grade").cast("decimal(5,3)"))
        df2 = df2.withColumn("col_1", F.col("grade").cast("decimal(5,3)"))

        assertDataFrameEqual(df1, df2, rtol=1e-1)

    def test_assert_notequal_arraytype(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("Amy", ["C++", "Rust"]),
                ("John", ["Python", "Java"]),
                ("Jane", ["Scala", "SQL", "Java"]),
            ],
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("languages", ArrayType(StringType()), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("Amy", ["C++", "Rust"]),
                ("John", ["Python", "Java"]),
                ("Jane", ["Scala", "Java"]),
            ],
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("languages", ArrayType(StringType()), True),
                ]
            ),
        )

        rows_str1 = ""
        rows_str2 = ""

        sorted_list1 = sorted(df1.collect(), key=lambda x: str(x))
        sorted_list2 = sorted(df2.collect(), key=lambda x: str(x))

        # count different rows
        for r1, r2 in list(zip_longest(sorted_list1, sorted_list2)):
            rows_str1 += str(r1) + "\n"
            rows_str2 += str(r2) + "\n"

        generated_diff = _context_diff(
            actual=rows_str1.splitlines(), expected=rows_str2.splitlines(), n=3
        )

        error_msg = "Results do not match: "
        percent_diff = (1 / 3) * 100
        error_msg += "( %.5f %% )" % percent_diff
        error_msg += "\n" + "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

        rows_str1 = ""
        rows_str2 = ""

        # count different rows
        for r1, r2 in list(zip_longest(df1.collect(), df2.collect())):
            rows_str1 += str(r1) + "\n"
            rows_str2 += str(r2) + "\n"

        generated_diff = _context_diff(
            actual=rows_str1.splitlines(), expected=rows_str2.splitlines(), n=3
        )

        error_msg = "Results do not match: "
        percent_diff = (1 / 3) * 100
        error_msg += "( %.5f %% )" % percent_diff
        error_msg += "\n" + "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

    def test_assert_equal_maptype(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("student1", {"id": 222342203655477580}),
                ("student2", {"id": 422322203155477692}),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("properties", MapType(StringType(), LongType()), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("student1", {"id": 222342203655477580}),
                ("student2", {"id": 422322203155477692}),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("properties", MapType(StringType(), LongType()), True),
                ]
            ),
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_approx_equal_maptype_double(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("student1", {"math": 76.23, "english": 92.64}),
                ("student2", {"math": 87.89, "english": 84.48}),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grades", MapType(StringType(), DoubleType()), True),
                ]
            ),
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("student1", {"math": 76.23, "english": 92.63999999}),
                ("student2", {"math": 87.89, "english": 84.48}),
            ],
            schema=StructType(
                [
                    StructField("student", StringType(), True),
                    StructField("grades", MapType(StringType(), DoubleType()), True),
                ]
            ),
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_approx_equal_nested_struct_double(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("jane", (64.57, 76.63, 97.81)),
                ("john", (93.92, 91.57, 84.36)),
            ],
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField(
                        "grades",
                        StructType(
                            [
                                StructField("math", DoubleType(), True),
                                StructField("english", DoubleType(), True),
                                StructField("biology", DoubleType(), True),
                            ]
                        ),
                    ),
                ]
            ),
        )

        df2 = self.spark.createDataFrame(
            data=[
                ("jane", (64.57, 76.63, 97.81000001)),
                ("john", (93.92, 91.57, 84.36)),
            ],
            schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField(
                        "grades",
                        StructType(
                            [
                                StructField("math", DoubleType(), True),
                                StructField("english", DoubleType(), True),
                                StructField("biology", DoubleType(), True),
                            ]
                        ),
                    ),
                ]
            ),
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_equal_nested_struct_str(self):
        df1 = self.spark.createDataFrame(
            data=[
                (1, ("jane", "anne", "doe")),
                (2, ("john", "bob", "smith")),
            ],
            schema=StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField(
                        "name",
                        StructType(
                            [
                                StructField("first", StringType(), True),
                                StructField("middle", StringType(), True),
                                StructField("last", StringType(), True),
                            ]
                        ),
                    ),
                ]
            ),
        )

        df2 = self.spark.createDataFrame(
            data=[
                (1, ("jane", "anne", "doe")),
                (2, ("john", "bob", "smith")),
            ],
            schema=StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField(
                        "name",
                        StructType(
                            [
                                StructField("first", StringType(), True),
                                StructField("middle", StringType(), True),
                                StructField("last", StringType(), True),
                            ]
                        ),
                    ),
                ]
            ),
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_equal_nested_struct_str_duplicate(self):
        df1 = self.spark.createDataFrame(
            data=[
                (1, ("jane doe", "jane doe")),
                (2, ("john smith", "john smith")),
            ],
            schema=StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField(
                        "full name",
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("name", StringType(), True),
                            ]
                        ),
                    ),
                ]
            ),
        )

        df2 = self.spark.createDataFrame(
            data=[
                (1, ("jane doe", "jane doe")),
                (2, ("john smith", "john smith")),
            ],
            schema=StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField(
                        "full name",
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("name", StringType(), True),
                            ]
                        ),
                    ),
                ]
            ),
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_equal_duplicate_col(self):
        df1 = self.spark.createDataFrame(
            data=[
                (1, "Python", 1, 1),
                (2, "Scala", 2, 2),
            ],
            schema=["number", "language", "number", "number"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                (1, "Python", 1, 1),
                (2, "Scala", 2, 2),
            ],
            schema=["number", "language", "number", "number"],
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_equal_timestamp(self):
        df1 = self.spark.createDataFrame(
            data=[("1", "2023-01-01 12:01:01.000")], schema=["id", "timestamp"]
        )

        df2 = self.spark.createDataFrame(
            data=[("1", "2023-01-01 12:01:01.000")], schema=["id", "timestamp"]
        )

        df1 = df1.withColumn("timestamp", F.to_timestamp("timestamp"))
        df2 = df2.withColumn("timestamp", F.to_timestamp("timestamp"))

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_equal_nullrow(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                (None, None),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                (None, None),
            ],
            schema=["id", "amount"],
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_notequal_nullval(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                ("2", 2000),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                ("2", None),
            ],
            schema=["id", "amount"],
        )

        rows_str1 = ""
        rows_str2 = ""

        # count different rows
        for r1, r2 in list(zip_longest(df1.collect(), df2.collect())):
            rows_str1 += str(r1) + "\n"
            rows_str2 += str(r2) + "\n"

        generated_diff = _context_diff(
            actual=rows_str1.splitlines(), expected=rows_str2.splitlines(), n=2
        )

        error_msg = "Results do not match: "
        percent_diff = (1 / 2) * 100
        error_msg += "( %.5f %% )" % percent_diff
        error_msg += "\n" + "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

    def test_assert_equal_nulldf(self):
        df1 = None
        df2 = None

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_unequal_null_actual(self):
        df1 = None
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                ("2", 3000),
            ],
            schema=["id", "amount"],
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2)

        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
            messageParameters={
                "expected_type": "Union[DataFrame, ps.DataFrame, List[Row]]",
                "arg_name": "actual",
                "actual_type": None,
            },
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
            messageParameters={
                "expected_type": "Union[DataFrame, ps.DataFrame, List[Row]]",
                "arg_name": "actual",
                "actual_type": None,
            },
        )

    def test_assert_unequal_null_expected(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                ("2", 3000),
            ],
            schema=["id", "amount"],
        )
        df2 = None

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2)

        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
            messageParameters={
                "expected_type": "Union[DataFrame, ps.DataFrame, List[Row]]",
                "arg_name": "expected",
                "actual_type": None,
            },
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
            messageParameters={
                "expected_type": "Union[DataFrame, ps.DataFrame, List[Row]]",
                "arg_name": "expected",
                "actual_type": None,
            },
        )

    @unittest.skipIf(
        not have_pandas or not have_numpy or not have_pyarrow,
        "no pandas or numpy or pyarrow dependency",
    )
    def test_assert_equal_exact_pandas_df(self):
        import numpy as np
        import pandas as pd

        df1 = pd.DataFrame(
            data=np.array([(1, 2, 3), (4, 5, 6), (7, 8, 9)]), columns=["a", "b", "c"]
        )
        df2 = pd.DataFrame(
            data=np.array([(1, 2, 3), (4, 5, 6), (7, 8, 9)]), columns=["a", "b", "c"]
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    @unittest.skipIf(
        not have_pandas or not have_numpy or not have_pyarrow,
        "no pandas or numpy or pyarrow dependency",
    )
    def test_assert_approx_equal_pandas_df(self):
        import numpy as np
        import pandas as pd

        # test that asserts close enough equality for pandas df
        df1 = pd.DataFrame(
            data=np.array([(1, 2, 3), (4, 5, 6), (7, 8, 59)]), columns=["a", "b", "c"]
        )
        df2 = pd.DataFrame(
            data=np.array([(1, 2, 3), (4, 5, 6), (7, 8, 59.0001)]), columns=["a", "b", "c"]
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    @unittest.skipIf(
        not have_pandas or not have_numpy or not have_pyarrow,
        "no pandas or numpy or pyarrow dependency",
    )
    def test_assert_approx_equal_fail_exact_pandas_df(self):
        import numpy as np
        import pandas as pd

        # test that asserts close enough equality for pandas df
        df1 = pd.DataFrame(
            data=np.array([(1, 2, 3), (4, 5, 6), (7, 8, 59)]), columns=["a", "b", "c"]
        )
        df2 = pd.DataFrame(
            data=np.array([(1, 2, 3), (4, 5, 6), (7, 8, 59.0001)]), columns=["a", "b", "c"]
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=False, rtol=0, atol=0)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_PANDAS_DATAFRAME",
            messageParameters={
                "left": df1.to_string(),
                "left_dtype": str(df1.dtypes),
                "right": df2.to_string(),
                "right_dtype": str(df2.dtypes),
            },
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=True, rtol=0, atol=0)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_PANDAS_DATAFRAME",
            messageParameters={
                "left": df1.to_string(),
                "left_dtype": str(df1.dtypes),
                "right": df2.to_string(),
                "right_dtype": str(df2.dtypes),
            },
        )

    @unittest.skipIf(
        not have_pandas or not have_numpy or not have_pyarrow,
        "no pandas or numpy or pyarrow dependency",
    )
    def test_assert_unequal_pandas_df(self):
        import numpy as np
        import pandas as pd

        df1 = pd.DataFrame(
            data=np.array([(1, 2, 3), (4, 5, 6), (6, 5, 4)]), columns=["a", "b", "c"]
        )
        df2 = pd.DataFrame(
            data=np.array([(1, 2, 3), (4, 5, 6), (7, 8, 9)]), columns=["a", "b", "c"]
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=False)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_PANDAS_DATAFRAME",
            messageParameters={
                "left": df1.to_string(),
                "left_dtype": str(df1.dtypes),
                "right": df2.to_string(),
                "right_dtype": str(df2.dtypes),
            },
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_PANDAS_DATAFRAME",
            messageParameters={
                "left": df1.to_string(),
                "left_dtype": str(df1.dtypes),
                "right": df2.to_string(),
                "right_dtype": str(df2.dtypes),
            },
        )

    @unittest.skipIf(
        not have_pandas or not have_numpy or not have_pyarrow,
        "no pandas or numpy or pyarrow dependency",
    )
    def test_assert_type_error_pandas_df(self):
        import numpy as np
        import pandas as pd

        import pyspark.pandas as ps

        df1 = ps.DataFrame(data=[10, 20, 30], columns=["Numbers"])
        df2 = pd.DataFrame(
            data=np.array([(1, 2, 3), (4, 5, 6), (6, 5, 4)]), columns=["a", "b", "c"]
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=False)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_PANDAS_DATAFRAME",
            messageParameters={
                "left": df1.to_string(),
                "left_dtype": str(df1.dtypes),
                "right": df2.to_string(),
                "right_dtype": str(df2.dtypes),
            },
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_PANDAS_DATAFRAME",
            messageParameters={
                "left": df1.to_string(),
                "left_dtype": str(df1.dtypes),
                "right": df2.to_string(),
                "right_dtype": str(df2.dtypes),
            },
        )

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_equal_exact_pandas_on_spark_df(self):
        import pyspark.pandas as ps

        df1 = ps.DataFrame(data=[10, 20, 30], columns=["Numbers"])
        df2 = ps.DataFrame(data=[10, 20, 30], columns=["Numbers"])

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_equal_exact_pandas_on_spark_df(self):
        import pyspark.pandas as ps

        df1 = ps.DataFrame(data=[10, 20, 30], columns=["Numbers"])
        df2 = ps.DataFrame(data=[30, 20, 10], columns=["Numbers"])

        assertDataFrameEqual(df1, df2)

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_equal_approx_pandas_on_spark_df(self):
        import pyspark.pandas as ps

        df1 = ps.DataFrame(data=[10.0001, 20.32, 30.1], columns=["Numbers"])
        df2 = ps.DataFrame(data=[10.0, 20.32, 30.1], columns=["Numbers"])

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_error_pandas_pyspark_df(self):
        import pandas as pd
        import pyspark.pandas as ps

        df1 = ps.DataFrame(data=[10, 20, 30], columns=["Numbers"])
        df2 = self.spark.createDataFrame([(10,), (11,), (13,)], ["Numbers"])

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=False)

        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
            messageParameters={
                "expected_type": f"{ps.DataFrame.__name__}, "
                f"{pd.DataFrame.__name__}, "
                f"{ps.Series.__name__}, "
                f"{pd.Series.__name__}, "
                f"{ps.Index.__name__}"
                f"{pd.Index.__name__}, ",
                "arg_name": "expected",
                "actual_type": type(df2),
            },
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
            messageParameters={
                "expected_type": f"{ps.DataFrame.__name__}, "
                f"{pd.DataFrame.__name__}, "
                f"{ps.Series.__name__}, "
                f"{pd.Series.__name__}, "
                f"{ps.Index.__name__}"
                f"{pd.Index.__name__}, ",
                "arg_name": "expected",
                "actual_type": type(df2),
            },
        )

    def test_assert_error_non_pyspark_df(self):
        dict1 = {"a": 1, "b": 2}
        dict2 = {"a": 1, "b": 2}

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(dict1, dict2)

        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
            messageParameters={
                "expected_type": "Union[DataFrame, ps.DataFrame, List[Row]]",
                "arg_name": "actual",
                "actual_type": type(dict1),
            },
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(dict1, dict2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_TYPE_DF_EQUALITY_ARG",
            messageParameters={
                "expected_type": "Union[DataFrame, ps.DataFrame, List[Row]]",
                "arg_name": "actual",
                "actual_type": type(dict1),
            },
        )

    def test_assert_column_unique_single_column(self):
        # Test with a DataFrame that has unique values in a column
        df = self.spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])
        assertColumnUnique(df, "id")

        # Test with a DataFrame that has duplicate values in a column
        df_with_duplicates = self.spark.createDataFrame(
            [(1, "a"), (1, "b"), (3, "c")], ["id", "value"]
        )

        with self.assertRaises(AssertionError) as cm:
            assertColumnUnique(df_with_duplicates, "id")

        self.assertTrue("Column 'id' contains duplicate values" in str(cm.exception))

    def test_assert_column_unique_multiple_columns(self):
        # Test with a DataFrame that has unique combinations of values
        df = self.spark.createDataFrame([(1, "a"), (1, "b"), (2, "a")], ["id", "value"])
        assertColumnUnique(df, ["id", "value"])

        # Test with a DataFrame that has duplicate combinations of values
        df_with_duplicates = self.spark.createDataFrame(
            [(1, "a"), (1, "a"), (2, "b")], ["id", "value"]
        )

        with self.assertRaises(AssertionError) as cm:
            assertColumnUnique(df_with_duplicates, ["id", "value"])

        self.assertTrue("Columns ['id', 'value'] contains duplicate values" in str(cm.exception))

    def test_assert_column_unique_with_null_values(self):
        # Test with a DataFrame that has null values
        df = self.spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "value"])
        assertColumnUnique(df, "id")
        assertColumnUnique(df, "value")

        # Test with a DataFrame that has duplicate null values
        df_with_duplicate_nulls = self.spark.createDataFrame(
            [(1, None), (2, None), (3, "c")], ["id", "value"]
        )

        with self.assertRaises(AssertionError) as cm:
            assertColumnUnique(df_with_duplicate_nulls, "value")

        self.assertTrue("Column 'value' contains duplicate values" in str(cm.exception))

    def test_assert_column_unique_with_custom_message(self):
        # Test with a custom error message
        df_with_duplicates = self.spark.createDataFrame(
            [(1, "a"), (1, "b"), (3, "c")], ["id", "value"]
        )

        custom_message = "ID column must be unique for this operation."

        with self.assertRaises(AssertionError) as cm:
            assertColumnUnique(df_with_duplicates, "id", message=custom_message)

        self.assertTrue("Column 'id' contains duplicate values" in str(cm.exception))
        self.assertTrue(
            "ID column must be unique for this operation." in str(cm.exception)
        )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency"
    )
    def test_assert_column_unique_pandas_single_column(self):
        # Test with a pandas DataFrame that has unique values in a column
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        assertColumnUnique(df, "id")

        # Test with a pandas DataFrame that has duplicate values in a column
        df_with_duplicates = pd.DataFrame({"id": [1, 1, 3], "value": ["a", "b", "c"]})

        with self.assertRaises(AssertionError) as cm:
            assertColumnUnique(df_with_duplicates, "id")

        self.assertTrue("Column 'id' contains duplicate values" in str(cm.exception))

    @unittest.skipIf(
        not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency"
    )
    def test_assert_column_unique_pandas_multiple_columns(self):
        # Test with a pandas DataFrame that has unique combinations of values
        import pandas as pd

        df = pd.DataFrame({"id": [1, 1, 2], "value": ["a", "b", "a"]})
        assertColumnUnique(df, ["id", "value"])

        # Test with a pandas DataFrame that has duplicate combinations of values
        df_with_duplicates = pd.DataFrame({"id": [1, 1, 2], "value": ["a", "a", "b"]})

        with self.assertRaises(AssertionError) as cm:
            assertColumnUnique(df_with_duplicates, ["id", "value"])

        self.assertTrue(
            "Columns ['id', 'value'] contain duplicate values" in str(cm.exception)
        )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency"
    )
    def test_assert_column_unique_pandas_with_null_values(self):
        # Test with a pandas DataFrame that has null values
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", None, "c"]})
        assertColumnUnique(df, "id")
        assertColumnUnique(df, "value")

        # Test with a pandas DataFrame that has duplicate null values
        df_with_duplicate_nulls = pd.DataFrame(
            {"id": [1, 2, 3], "value": [None, None, "c"]}
        )

        with self.assertRaises(AssertionError) as cm:
            assertColumnUnique(df_with_duplicate_nulls, "value")

        self.assertTrue("Column 'value' contains duplicate values" in str(cm.exception))

    @unittest.skipIf(
        not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency"
    )
    def test_assert_column_unique_pandas_on_spark(self):
        # Test with a pandas-on-Spark DataFrame
        import pandas as pd

        import pyspark.pandas as ps

        # Create a pandas-on-Spark DataFrame with unique values
        pdf = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        psdf = ps.from_pandas(pdf)
        assertColumnUnique(psdf, "id")

        # Create a pandas-on-Spark DataFrame with duplicate values
        pdf_with_duplicates = pd.DataFrame({"id": [1, 1, 3], "value": ["a", "b", "c"]})
        psdf_with_duplicates = ps.from_pandas(pdf_with_duplicates)

        with self.assertRaises(AssertionError) as cm:
            assertColumnUnique(psdf_with_duplicates, "id")

        self.assertTrue("Column 'id' contains duplicate values" in str(cm.exception))

    def test_assert_column_non_null_single_column(self):
        # Test with a DataFrame that has no null values in a column
        df = self.spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])
        assertColumnNonNull(df, "id")

        # Test with a DataFrame that has null values in a column
        df_with_nulls = self.spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "value"])

        with self.assertRaises(AssertionError) as cm:
            assertColumnNonNull(df_with_nulls, "value")

        self.assertTrue("Column 'value' contains null values" in str(cm.exception))
        self.assertTrue("value: 1 null value" in str(cm.exception))

    def test_assert_column_non_null_multiple_columns(self):
        # Test with a DataFrame that has no null values in multiple columns
        df = self.spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"])
        assertColumnNonNull(df, ["id", "value"])

        # Test with a DataFrame that has null values in multiple columns
        df_with_nulls = self.spark.createDataFrame(
            [(1, "a"), (2, None), (None, "c")], ["id", "value"]
        )

        with self.assertRaises(AssertionError) as cm:
            assertColumnNonNull(df_with_nulls, ["id", "value"])

        self.assertTrue("Columns ['id', 'value'] contain null values" in str(cm.exception))
        self.assertTrue("id: 1 null value" in str(cm.exception))
        self.assertTrue("value: 1 null value" in str(cm.exception))

    def test_assert_column_non_null_with_custom_message(self):
        # Test with a custom error message
        df_with_nulls = self.spark.createDataFrame([(1, "a"), (2, None), (3, "c")], ["id", "value"])

        custom_message = "Value column must not contain nulls for this operation."

        with self.assertRaises(AssertionError) as cm:
            assertColumnNonNull(df_with_nulls, "value", message=custom_message)

        self.assertTrue("Column 'value' contains null values" in str(cm.exception))
        self.assertTrue(
            "Value column must not contain nulls for this operation." in str(cm.exception)
        )

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_column_non_null_pandas_single_column(self):
        # Test with a pandas DataFrame that has no null values in a column
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        assertColumnNonNull(df, "id")

        # Test with a pandas DataFrame that has null values in a column
        df_with_nulls = pd.DataFrame({"id": [1, 2, 3], "value": ["a", None, "c"]})

        with self.assertRaises(AssertionError) as cm:
            assertColumnNonNull(df_with_nulls, "value")

        self.assertTrue("Column 'value' contains null values" in str(cm.exception))
        self.assertTrue("value: 1 null value" in str(cm.exception))

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_column_non_null_pandas_multiple_columns(self):
        # Test with a pandas DataFrame that has no null values in multiple columns
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        assertColumnNonNull(df, ["id", "value"])

        # Test with a pandas DataFrame that has null values in multiple columns
        df_with_nulls = pd.DataFrame({"id": [1, None, 3], "value": ["a", "b", None]})

        with self.assertRaises(AssertionError) as cm:
            assertColumnNonNull(df_with_nulls, ["id", "value"])

        self.assertTrue("Columns ['id', 'value'] contain null values" in str(cm.exception))
        self.assertTrue("id: 1 null value" in str(cm.exception))
        self.assertTrue("value: 1 null value" in str(cm.exception))

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_column_non_null_pandas_with_custom_message(self):
        # Test with a custom error message
        import pandas as pd

        df_with_nulls = pd.DataFrame({"id": [1, 2, 3], "value": ["a", None, "c"]})

        custom_message = "Value column must not contain nulls for this operation."

        with self.assertRaises(AssertionError) as cm:
            assertColumnNonNull(df_with_nulls, "value", message=custom_message)

        self.assertTrue("Column 'value' contains null values" in str(cm.exception))
        self.assertTrue("Value column must not contain nulls for this operation." in str(cm.exception))

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_column_non_null_pandas_on_spark(self):
        # Test with a pandas-on-Spark DataFrame
        import pandas as pd
        import pyspark.pandas as ps

        # Create a pandas-on-Spark DataFrame with no null values
        pdf = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        psdf = ps.from_pandas(pdf)
        assertColumnNonNull(psdf, "id")

        # Create a pandas-on-Spark DataFrame with null values
        pdf_with_nulls = pd.DataFrame({"id": [1, 2, 3], "value": ["a", None, "c"]})
        psdf_with_nulls = ps.from_pandas(pdf_with_nulls)

        with self.assertRaises(AssertionError) as cm:
            assertColumnNonNull(psdf_with_nulls, "value")

        self.assertTrue("Column 'value' contains null values" in str(cm.exception))

    def test_assert_column_values_in_set_single_column(self):
        # Test with a DataFrame that has all values in the accepted set
        df = self.spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "category"])
        assertColumnValuesInSet(df, "category", {"A", "B", "C"})

        # Test with a DataFrame that has values not in the accepted set
        df_with_invalid = self.spark.createDataFrame(
            [(1, "A"), (2, "B"), (3, "X")], ["id", "category"]
        )

        with self.assertRaises(AssertionError) as cm:
            assertColumnValuesInSet(df_with_invalid, "category", {"A", "B", "C"})

        self.assertTrue(
            "Column 'category' contains values not in the accepted set" in str(cm.exception)
        )
        self.assertTrue("Invalid values found: ['X']" in str(cm.exception))

    def test_assert_column_values_in_set_multiple_columns_same_values(self):
        # Test with a DataFrame that has all values in the accepted set for multiple columns
        df = self.spark.createDataFrame([("A", "B"), ("B", "A"), ("C", "C")], ["col1", "col2"])
        assertColumnValuesInSet(df, ["col1", "col2"], {"A", "B", "C"})

        # Test with a DataFrame that has values not in the accepted set in multiple columns
        df_with_invalid = self.spark.createDataFrame(
            [("A", "B"), ("X", "A"), ("C", "Y")], ["col1", "col2"]
        )

        with self.assertRaises(AssertionError) as cm:
            assertColumnValuesInSet(df_with_invalid, ["col1", "col2"], {"A", "B", "C"})

        self.assertTrue(
            "Columns ['col1', 'col2'] contain values not in the accepted set" in str(cm.exception)
        )
        self.assertTrue("Invalid values found: ['X']" in str(cm.exception))
        self.assertTrue("Invalid values found: ['Y']" in str(cm.exception))

    def test_assert_column_values_in_set_multiple_columns_different_values(self):
        # Test with a DataFrame that has all values in different accepted sets for multiple columns
        df = self.spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "category"])
        assertColumnValuesInSet(
            df, ["id", "category"], {"id": {1, 2, 3}, "category": {"A", "B", "C"}}
        )

        # Test with a DataFrame that has values not in the accepted sets in multiple columns
        df_with_invalid = self.spark.createDataFrame(
            [(1, "A"), (4, "B"), (3, "X")], ["id", "category"]
        )

        with self.assertRaises(AssertionError) as cm:
            assertColumnValuesInSet(
                df_with_invalid, ["id", "category"], {"id": {1, 2, 3}, "category": {"A", "B", "C"}}
            )

        self.assertTrue(
            "Columns ['id', 'category'] contain values not in the accepted set" in str(cm.exception)
        )
        self.assertTrue("Invalid values found: [4]" in str(cm.exception))
        self.assertTrue("Invalid values found: ['X']" in str(cm.exception))

    def test_assert_column_values_in_set_with_null_values(self):
        # Test with a DataFrame that has null values (nulls are ignored)
        df_with_nulls = self.spark.createDataFrame(
            [(1, "A"), (2, None), (3, "C")], ["id", "category"]
        )
        assertColumnValuesInSet(df_with_nulls, "category", {"A", "B", "C"})

    def test_assert_column_values_in_set_with_custom_message(self):
        # Test with a custom error message
        df_with_invalid = self.spark.createDataFrame(
            [(1, "A"), (2, "B"), (3, "X")], ["id", "category"]
        )

        custom_message = "Category must be one of the allowed values: A, B, C."

        with self.assertRaises(AssertionError) as cm:
            assertColumnValuesInSet(
                df_with_invalid, "category", {"A", "B", "C"}, message=custom_message
            )

        self.assertTrue(
            "Column 'category' contains values not in the accepted set" in str(cm.exception)
        )
        self.assertTrue("Category must be one of the allowed values: A, B, C." in str(cm.exception))

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_column_values_in_set_pandas_single_column(self):
        # Test with a pandas DataFrame that has all values in the accepted set
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "category": ["A", "B", "C"]})
        assertColumnValuesInSet(df, "category", {"A", "B", "C"})

        # Test with a pandas DataFrame that has values not in the accepted set
        df_with_invalid = pd.DataFrame({"id": [1, 2, 3], "category": ["A", "B", "X"]})

        with self.assertRaises(AssertionError) as cm:
            assertColumnValuesInSet(df_with_invalid, "category", {"A", "B", "C"})

        self.assertTrue("Column 'category' contains values not in the accepted set" in str(cm.exception))
        self.assertTrue("Invalid values found: ['X']" in str(cm.exception))

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_column_values_in_set_pandas_multiple_columns_same_values(self):
        # Test with a pandas DataFrame that has all values in the accepted set for multiple columns
        import pandas as pd

        df = pd.DataFrame({"col1": ["A", "B", "C"], "col2": ["B", "A", "C"]})
        assertColumnValuesInSet(df, ["col1", "col2"], {"A", "B", "C"})

        # Test with a pandas DataFrame that has values not in the accepted set in multiple columns
        df_with_invalid = pd.DataFrame({"col1": ["A", "B", "X"], "col2": ["B", "Y", "C"]})

        with self.assertRaises(AssertionError) as cm:
            assertColumnValuesInSet(df_with_invalid, ["col1", "col2"], {"A", "B", "C"})

        self.assertTrue("Columns ['col1', 'col2'] contain values not in the accepted set" in str(cm.exception))
        self.assertTrue("Invalid values found: ['X']" in str(cm.exception))
        self.assertTrue("Invalid values found: ['Y']" in str(cm.exception))

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_column_values_in_set_pandas_multiple_columns_different_values(self):
        # Test with a pandas DataFrame that has all values in different accepted sets for multiple columns
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "category": ["A", "B", "C"]})
        assertColumnValuesInSet(df, ["id", "category"], {"id": {1, 2, 3}, "category": {"A", "B", "C"}})

        # Test with a pandas DataFrame that has values not in the accepted sets in multiple columns
        df_with_invalid = pd.DataFrame({"id": [1, 4, 3], "category": ["A", "B", "X"]})

        with self.assertRaises(AssertionError) as cm:
            assertColumnValuesInSet(
                df_with_invalid, ["id", "category"], {"id": {1, 2, 3}, "category": {"A", "B", "C"}}
            )

        self.assertTrue("Columns ['id', 'category'] contain values not in the accepted set" in str(cm.exception))
        self.assertTrue("Invalid values found: [4]" in str(cm.exception))
        self.assertTrue("Invalid values found: ['X']" in str(cm.exception))

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_column_values_in_set_pandas_with_null_values(self):
        # Test with a pandas DataFrame that has null values (which should be ignored)
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3], "category": ["A", None, "C"]})
        assertColumnValuesInSet(df, "category", {"A", "B", "C"})

        # Test with a pandas DataFrame that has null values and invalid values
        df_with_invalid = pd.DataFrame({"id": [1, 2, 3], "category": ["A", None, "X"]})

        with self.assertRaises(AssertionError) as cm:
            assertColumnValuesInSet(df_with_invalid, "category", {"A", "B", "C"})

        self.assertTrue("Column 'category' contains values not in the accepted set" in str(cm.exception))
        self.assertTrue("Invalid values found: ['X']" in str(cm.exception))

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_column_values_in_set_pandas_on_spark(self):
        # Test with a pandas-on-Spark DataFrame
        import pandas as pd
        import pyspark.pandas as ps

        # Create a pandas-on-Spark DataFrame with all values in the accepted set
        pdf = pd.DataFrame({"id": [1, 2, 3], "category": ["A", "B", "C"]})
        psdf = ps.from_pandas(pdf)
        assertColumnValuesInSet(psdf, "category", {"A", "B", "C"})

        # Create a pandas-on-Spark DataFrame with values not in the accepted set
        pdf_with_invalid = pd.DataFrame({"id": [1, 2, 3], "category": ["A", "B", "X"]})
        psdf_with_invalid = ps.from_pandas(pdf_with_invalid)

        with self.assertRaises(AssertionError) as cm:
            assertColumnValuesInSet(psdf_with_invalid, "category", {"A", "B", "C"})

        self.assertTrue("Column 'category' contains values not in the accepted set" in str(cm.exception))

    def test_row_order_ignored(self):
        # test that row order is ignored (not checked) by default
        df1 = self.spark.createDataFrame(
            data=[
                ("2", 3000.00),
                ("1", 1000.00),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000.00),
                ("2", 3000.00),
            ],
            schema=["id", "amount"],
        )

        assertDataFrameEqual(df1, df2)

    def test_check_row_order_error(self):
        # test checkRowOrder=True
        df1 = self.spark.createDataFrame(
            data=[
                ("2", 3000.00),
                ("1", 1000.00),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000.00),
                ("2", 3000.00),
            ],
            schema=["id", "amount"],
        )

        rows_str1 = ""
        rows_str2 = ""

        # count different rows
        for r1, r2 in list(zip_longest(df1.collect(), df2.collect())):
            rows_str1 += str(r1) + "\n"
            rows_str2 += str(r2) + "\n"

        generated_diff = _context_diff(
            actual=rows_str1.splitlines(), expected=rows_str2.splitlines(), n=2
        )

        error_msg = "Results do not match: "
        percent_diff = (2 / 2) * 100
        error_msg += "( %.5f %% )" % percent_diff
        error_msg += "\n" + "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

    def test_remove_non_word_characters_long(self):
        def remove_non_word_characters(col):
            return F.regexp_replace(col, "[^\\w\\s]+", "")

        source_data = [("jo&&se",), ("**li**",), ("#::luisa",), (None,)]
        source_df = self.spark.createDataFrame(source_data, ["name"])

        actual_df = source_df.withColumn("clean_name", remove_non_word_characters(F.col("name")))

        expected_data = [("jo&&se", "jose"), ("**li**", "li"), ("#::luisa", "luisa"), (None, None)]
        expected_df = self.spark.createDataFrame(expected_data, ["name", "clean_name"])

        assertDataFrameEqual(actual_df, expected_df)

    def test_assert_pyspark_approx_equal(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000.00),
                ("2", 3000.00),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000.0000001),
                ("2", 3000.00),
            ],
            schema=["id", "amount"],
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_assert_pyspark_approx_equal_custom_rtol(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000.00),
                ("2", 3000.00),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000.01),
                ("2", 3000.00),
            ],
            schema=["id", "amount"],
        )

        assertDataFrameEqual(df1, df2, rtol=1e-2)

    def test_assert_pyspark_df_not_equal(self):
        df1 = self.spark.createDataFrame(
            data=[
                ("1", 1000.00),
                ("2", 3000.00),
                ("3", 2000.00),
            ],
            schema=["id", "amount"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1001.00),
                ("2", 3000.00),
                ("3", 2003.00),
            ],
            schema=["id", "amount"],
        )

        rows_str1 = ""
        rows_str2 = ""

        # count different rows
        for r1, r2 in list(zip_longest(df1.collect(), df2.collect())):
            rows_str1 += str(r1) + "\n"
            rows_str2 += str(r2) + "\n"

        generated_diff = _context_diff(
            actual=rows_str1.splitlines(), expected=rows_str2.splitlines(), n=3
        )

        error_msg = "Results do not match: "
        percent_diff = (2 / 3) * 100
        error_msg += "( %.5f %% )" % percent_diff
        error_msg += "\n" + "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

    def test_assert_notequal_schema(self):
        df1 = self.spark.createDataFrame(
            data=[
                (1, 1000),
                (2, 3000),
            ],
            schema=["id", "number"],
        )
        df2 = self.spark.createDataFrame(
            data=[
                ("1", 1000),
                ("2", 5000),
            ],
            schema=["id", "amount"],
        )

        generated_diff = difflib.ndiff(str(df1.schema).splitlines(), str(df2.schema).splitlines())

        expected_error_msg = "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_SCHEMA",
            messageParameters={"error_msg": expected_error_msg},
        )

    def test_diff_schema_lens(self):
        df1 = self.spark.createDataFrame(
            data=[
                (1, 3000),
                (2, 1000),
            ],
            schema=["id", "amount"],
        )

        df2 = self.spark.createDataFrame(
            data=[
                (1, 3000, "a"),
                (2, 1000, "b"),
            ],
            schema=["id", "amount", "letter"],
        )

        generated_diff = difflib.ndiff(str(df1.schema).splitlines(), str(df2.schema).splitlines())

        expected_error_msg = "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, df2)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_SCHEMA",
            messageParameters={"error_msg": expected_error_msg},
        )

    def test_schema_ignore_nullable(self):
        s1 = StructType(
            [StructField("id", IntegerType(), True), StructField("name", StringType(), True)]
        )

        df1 = self.spark.createDataFrame([(1, "jane"), (2, "john")], s1)

        s2 = StructType(
            [StructField("id", IntegerType(), True), StructField("name", StringType(), False)]
        )

        df2 = self.spark.createDataFrame([(1, "jane"), (2, "john")], s2)

        assertDataFrameEqual(df1, df2)

        with self.assertRaises(PySparkAssertionError):
            assertDataFrameEqual(df1, df2, ignoreNullable=False)

    def test_schema_ignore_nullable_array_equal(self):
        s1 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])
        s2 = StructType([StructField("names", ArrayType(DoubleType(), False), False)])

        assertSchemaEqual(s1, s2)

    def test_schema_ignore_nullable_struct_equal(self):
        s1 = StructType(
            [StructField("names", StructType([StructField("age", IntegerType(), True)]), True)]
        )
        s2 = StructType(
            [StructField("names", StructType([StructField("age", IntegerType(), False)]), False)]
        )
        assertSchemaEqual(s1, s2)

    def test_schema_array_unequal(self):
        s1 = StructType([StructField("names", ArrayType(IntegerType(), True), True)])
        s2 = StructType([StructField("names", ArrayType(DoubleType(), False), False)])

        generated_diff = difflib.ndiff(str(s1).splitlines(), str(s2).splitlines())

        expected_error_msg = "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertSchemaEqual(s1, s2)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_SCHEMA",
            messageParameters={"error_msg": expected_error_msg},
        )

    def test_schema_struct_unequal(self):
        s1 = StructType(
            [StructField("names", StructType([StructField("age", DoubleType(), True)]), True)]
        )
        s2 = StructType(
            [StructField("names", StructType([StructField("age", IntegerType(), True)]), True)]
        )

        generated_diff = difflib.ndiff(str(s1).splitlines(), str(s2).splitlines())

        expected_error_msg = "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertSchemaEqual(s1, s2)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_SCHEMA",
            messageParameters={"error_msg": expected_error_msg},
        )

    def test_schema_more_nested_struct_unequal(self):
        s1 = StructType(
            [
                StructField(
                    "name",
                    StructType(
                        [
                            StructField("firstname", StringType(), True),
                            StructField("middlename", StringType(), True),
                            StructField("lastname", StringType(), True),
                        ]
                    ),
                ),
            ]
        )

        s2 = StructType(
            [
                StructField(
                    "name",
                    StructType(
                        [
                            StructField("firstname", StringType(), True),
                            StructField("middlename", BooleanType(), True),
                            StructField("lastname", StringType(), True),
                        ]
                    ),
                ),
            ]
        )

        generated_diff = difflib.ndiff(str(s1).splitlines(), str(s2).splitlines())

        expected_error_msg = "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertSchemaEqual(s1, s2)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_SCHEMA",
            messageParameters={"error_msg": expected_error_msg},
        )

    def test_schema_unsupported_type(self):
        s1 = "names: int"
        s2 = "names: int"

        with self.assertRaises(PySparkTypeError) as pe:
            assertSchemaEqual(s1, s2)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_STRUCT",
            messageParameters={"arg_name": "actual", "arg_type": "str"},
        )

    def test_spark_sql(self):
        assertDataFrameEqual(self.spark.sql("select 1 + 2 AS x"), self.spark.sql("select 3 AS x"))
        assertDataFrameEqual(
            self.spark.sql("select 1 + 2 AS x"),
            self.spark.sql("select 3 AS x"),
            checkRowOrder=True,
        )

    def test_spark_sql_sort_rows(self):
        df1 = self.spark.createDataFrame(
            data=[
                (1, 3000),
                (2, 1000),
            ],
            schema=["id", "amount"],
        )

        df2 = self.spark.createDataFrame(
            data=[
                (2, 1000),
                (1, 3000),
            ],
            schema=["id", "amount"],
        )

        df1.createOrReplaceTempView("df1")
        df2.createOrReplaceTempView("df2")

        assertDataFrameEqual(
            self.spark.sql("select * from df1 order by amount"), self.spark.sql("select * from df2")
        )

        assertDataFrameEqual(
            self.spark.sql("select * from df1 order by amount"),
            self.spark.sql("select * from df2"),
            checkRowOrder=True,
        )

    def test_empty_dataset(self):
        df1 = self.spark.range(0, 10).limit(0)

        df2 = self.spark.range(0, 10).limit(0)

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_no_column(self):
        df1 = self.spark.range(0, 10).drop("id")

        df2 = self.spark.range(0, 10).drop("id")

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_empty_no_column(self):
        df1 = self.spark.range(0, 10).drop("id").limit(0)

        df2 = self.spark.range(0, 10).drop("id").limit(0)

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_empty_expected_list(self):
        df1 = self.spark.range(0, 5).drop("id")

        df2 = [Row(), Row(), Row(), Row(), Row()]

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_no_column_expected_list(self):
        df1 = self.spark.range(0, 10).limit(0)

        df2 = []

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_empty_no_column_expected_list(self):
        df1 = self.spark.range(0, 10).drop("id").limit(0)

        df2 = []

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_special_vals(self):
        df1 = self.spark.createDataFrame(
            data=[
                (1, float("nan")),
                (2, float("inf")),
                (2, float("-inf")),
            ],
            schema=["id", "amount"],
        )

        df2 = self.spark.createDataFrame(
            data=[
                (1, float("nan")),
                (2, float("inf")),
                (2, float("-inf")),
            ],
            schema=["id", "amount"],
        )

        assertDataFrameEqual(df1, df2, checkRowOrder=False)
        assertDataFrameEqual(df1, df2, checkRowOrder=True)

    def test_df_list_row_equal(self):
        df1 = self.spark.createDataFrame(
            data=[
                (1, 3000),
                (2, 1000),
            ],
            schema=["id", "amount"],
        )

        list_of_rows = [Row(1, 3000), Row(2, 1000)]

        assertDataFrameEqual(df1, list_of_rows, checkRowOrder=False)
        assertDataFrameEqual(df1, list_of_rows, checkRowOrder=True)

    def test_list_rows_equal(self):
        list_of_rows1 = [Row(1, "abc", 5000), Row(2, "def", 1000)]
        list_of_rows2 = [Row(1, "abc", 5000), Row(2, "def", 1000)]

        assertDataFrameEqual(list_of_rows1, list_of_rows2, checkRowOrder=False)
        assertDataFrameEqual(list_of_rows1, list_of_rows2, checkRowOrder=True)

    def test_list_rows_unequal(self):
        list_of_rows1 = [Row(1, "abc", 5000), Row(2, "def", 1000)]
        list_of_rows2 = [Row(1, "abc", 5000), Row(2, "defg", 1000)]

        rows_str1 = ""
        rows_str2 = ""

        # count different rows
        for r1, r2 in list(zip_longest(list_of_rows1, list_of_rows2)):
            rows_str1 += str(r1) + "\n"
            rows_str2 += str(r2) + "\n"

        generated_diff = _context_diff(
            actual=rows_str1.splitlines(), expected=rows_str2.splitlines(), n=2
        )

        error_msg = "Results do not match: "
        percent_diff = (1 / 2) * 100
        error_msg += "( %.5f %% )" % percent_diff
        error_msg += "\n" + "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(list_of_rows1, list_of_rows2)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(list_of_rows1, list_of_rows2, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

    def test_list_row_unequal_schema(self):
        df1 = self.spark.createDataFrame(
            data=[
                (1, 3000),
                (2, 1000),
                (3, 10),
            ],
            schema=["id", "amount"],
        )

        list_of_rows = [Row(id=1, amount=300), Row(id=2, amount=100), Row(id=3, amount=10)]

        rows_str1 = ""
        rows_str2 = ""

        # count different rows
        for r1, r2 in list(zip_longest(df1, list_of_rows)):
            rows_str1 += str(r1) + "\n"
            rows_str2 += str(r2) + "\n"

        generated_diff = _context_diff(
            actual=rows_str1.splitlines(), expected=rows_str2.splitlines(), n=3
        )

        error_msg = "Results do not match: "
        percent_diff = (2 / 3) * 100
        error_msg += "( %.5f %% )" % percent_diff
        error_msg += "\n" + "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, list_of_rows)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, list_of_rows, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

    def test_list_row_unequal_schema(self):
        from pyspark.sql import Row

        df1 = self.spark.createDataFrame(
            data=[
                (1, 3000),
                (2, 1000),
            ],
            schema=["id", "amount"],
        )

        list_of_rows = [Row(1, "3000"), Row(2, "1000")]

        rows_str1 = ""
        rows_str2 = ""

        # count different rows
        for r1, r2 in list(zip_longest(df1.collect(), list_of_rows)):
            rows_str1 += str(r1) + "\n"
            rows_str2 += str(r2) + "\n"

        generated_diff = _context_diff(
            actual=rows_str1.splitlines(), expected=rows_str2.splitlines(), n=2
        )

        error_msg = "Results do not match: "
        percent_diff = (2 / 2) * 100
        error_msg += "( %.5f %% )" % percent_diff
        error_msg += "\n" + "\n".join(generated_diff)

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, list_of_rows)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

        with self.assertRaises(PySparkAssertionError) as pe:
            assertDataFrameEqual(df1, list_of_rows, checkRowOrder=True)

        self.check_error(
            exception=pe.exception,
            errorClass="DIFFERENT_ROWS",
            messageParameters={"error_msg": error_msg},
        )

    def test_dataframe_include_diff_rows(self):
        df1 = self.spark.createDataFrame(
            [("1", 1000.00), ("2", 3000.00), ("3", 2000.00)], ["id", "amount"]
        )
        df2 = self.spark.createDataFrame(
            [("1", 1001.00), ("2", 3000.00), ("3", 2003.00)], ["id", "amount"]
        )

        with self.assertRaises(PySparkAssertionError) as context:
            assertDataFrameEqual(df1, df2, includeDiffRows=True)

        # Extracting the differing rows data from the exception
        error_data = context.exception.data

        # Expected differences
        expected_diff = [
            (Row(id="1", amount=1000.0), Row(id="1", amount=1001.0)),
            (Row(id="3", amount=2000.0), Row(id="3", amount=2003.0)),
        ]

        self.assertEqual(error_data, expected_diff)

    def test_dataframe_ignore_column_order(self):
        df1 = self.spark.createDataFrame([Row(A=1, B=2), Row(A=3, B=4)])
        df2 = self.spark.createDataFrame([Row(B=2, A=1), Row(B=4, A=3)])

        with self.assertRaises(PySparkAssertionError):
            assertDataFrameEqual(df1, df2, ignoreColumnOrder=False)

        assertDataFrameEqual(df1, df2, ignoreColumnOrder=True)

    def test_dataframe_ignore_column_name(self):
        df1 = self.spark.createDataFrame([(1, 2), (3, 4)], ["A", "B"])
        df2 = self.spark.createDataFrame([(1, 2), (3, 4)], ["X", "Y"])

        with self.assertRaises(PySparkAssertionError):
            assertDataFrameEqual(df1, df2, ignoreColumnName=False)

        assertDataFrameEqual(df1, df2, ignoreColumnName=True)

    def test_dataframe_ignore_column_type(self):
        df1 = self.spark.createDataFrame([(1, "2"), (3, "4")], ["A", "B"])
        df2 = self.spark.createDataFrame([(1, 2), (3, 4)], ["A", "B"])

        with self.assertRaises(PySparkAssertionError):
            assertDataFrameEqual(df1, df2, ignoreColumnType=False)

        assertDataFrameEqual(df1, df2, ignoreColumnType=True)

    def test_dataframe_max_errors(self):
        df1 = self.spark.createDataFrame([(1, "a"), (2, "b"), (3, "c"), (4, "d")], ["id", "value"])
        df2 = self.spark.createDataFrame([(1, "a"), (2, "z"), (3, "x"), (4, "y")], ["id", "value"])

        # We expect differences in rows 2, 3, and 4.
        # Setting maxErrors to 2 will limit the reported errors.
        maxErrors = 2
        with self.assertRaises(PySparkAssertionError) as context:
            assertDataFrameEqual(df1, df2, maxErrors=maxErrors)

        # Check if the error message contains information about 2 mismatches only.
        error_message = str(context.exception)
        self.assertTrue("! Row" in error_message and error_message.count("! Row") == maxErrors * 2)

    def test_dataframe_show_only_diff(self):
        df1 = self.spark.createDataFrame(
            [(1, "apple", "red"), (2, "banana", "yellow"), (3, "cherry", "red")],
            ["id", "fruit", "color"],
        )
        df2 = self.spark.createDataFrame(
            [(1, "apple", "green"), (2, "banana", "yellow"), (3, "cherry", "blue")],
            ["id", "fruit", "color"],
        )

        with self.assertRaises(PySparkAssertionError) as context:
            assertDataFrameEqual(df1, df2, showOnlyDiff=False)

        error_message = str(context.exception)

        self.assertTrue("apple" in error_message and "banana" in error_message)

        with self.assertRaises(PySparkAssertionError) as context:
            assertDataFrameEqual(df1, df2, showOnlyDiff=True)

        error_message = str(context.exception)

        self.assertTrue("apple" in error_message and "banana" not in error_message)

    def test_capture_analysis_exception(self):
        self.assertRaises(AnalysisException, lambda: self.spark.sql("select abc"))
        self.assertRaises(AnalysisException, lambda: self.df.selectExpr("a + b").collect())

    def test_capture_user_friendly_exception(self):
        try:
            self.spark.sql("select `中文字段`")
        except AnalysisException as e:
            self.assertRegex(str(e), ".*UNRESOLVED_COLUMN.*`中文字段`.*")

    def test_spark_upgrade_exception(self):
        # SPARK-32161 : Test case to Handle SparkUpgradeException in pythonic way
        df = self.spark.createDataFrame([("2014-31-12",)], ["date_str"])
        df2 = df.select(
            "date_str", to_date(from_unixtime(unix_timestamp("date_str", "yyyy-dd-aa")))
        )
        self.assertRaises(SparkUpgradeException, df2.collect)

    def test_capture_parse_exception(self):
        self.assertRaises(ParseException, lambda: self.spark.sql("abc"))

    def test_capture_illegalargument_exception(self):
        self.assertRaisesRegex(
            IllegalArgumentException,
            "Setting negative mapred.reduce.tasks",
            lambda: self.spark.sql("SET mapred.reduce.tasks=-1"),
        )

    def test_capture_pyspark_value_exception(self):
        df = self.spark.createDataFrame([(1, 2)], ["a", "b"])
        self.assertRaisesRegex(
            PySparkValueError,
            "Value for `numBits` has to be amongst the following values",
            lambda: df.select(F.sha2(df.a, 1024)).collect(),
        )

    def test_get_error_class_state(self):
        # SPARK-36953: test CapturedException.getErrorClass and getSqlState (from SparkThrowable)
        exception = None
        try:
            self.spark.sql("""SELECT a""")
        except AnalysisException as e:
            exception = e

        self.assertIsNotNone(exception)
        self.assertEqual(exception.getCondition(), "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION")
        self.assertEqual(exception.getSqlState(), "42703")
        self.assertEqual(exception.getMessageParameters(), {"objectName": "`a`"})
        self.assertIn(
            (
                "[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function "
                "parameter with name `a` cannot be resolved.  SQLSTATE: 42703"
            ),
            exception.getMessage(),
        )
        self.assertEqual(len(exception.getQueryContext()), 1)
        qc = exception.getQueryContext()[0]
        self.assertEqual(qc.fragment(), "a")
        self.assertEqual(qc.stopIndex(), 7)
        self.assertEqual(qc.startIndex(), 7)
        self.assertEqual(qc.contextType(), QueryContextType.SQL)
        self.assertEqual(qc.objectName(), "")
        self.assertEqual(qc.objectType(), "")

        try:
            self.spark.sql("""SELECT assert_true(FALSE)""")
        except AnalysisException as e:
            self.assertIsNone(e.getCondition())
            self.assertIsNone(e.getSqlState())
            self.assertEqual(e.getMessageParameters(), {})
            self.assertEqual(e.getMessage(), "")

    def test_assert_data_frame_equal_not_support_streaming(self):
        df1 = self.spark.readStream.format("rate").load()
        df2 = self.spark.readStream.format("rate").load()
        exception_thrown = False
        try:
            assertDataFrameEqual(df1, df2)
        except PySparkAssertionError as e:
            self.assertEqual(e.getCondition(), "UNSUPPORTED_OPERATION")
            exception_thrown = True

        self.assertTrue(exception_thrown)

    def test_assert_schema_equal_with_decimal_types(self):
        """Test assertSchemaEqual with decimal types of different precision and scale
        (SPARK-51062)."""
        from pyspark.sql.types import DecimalType, StructField, StructType

        # Same precision and scale - should pass
        s1 = StructType(
            [
                StructField("price", DecimalType(10, 2), True),
            ]
        )

        s1_copy = StructType(
            [
                StructField("price", DecimalType(10, 2), True),
            ]
        )

        # This should pass
        assertSchemaEqual(s1, s1_copy)

        # Different precision and scale - should fail
        s2 = StructType(
            [
                StructField("price", DecimalType(12, 4), True),
            ]
        )

        # This should fail
        with self.assertRaises(PySparkAssertionError):
            assertSchemaEqual(s1, s2)

    def test_assert_referential_integrity_valid(self):
        # Create a "customers" DataFrame with customer IDs
        customers = self.spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"]
        )

        # Create an "orders" DataFrame with customer IDs as foreign keys
        orders = self.spark.createDataFrame(
            [(101, 1), (102, 2), (103, 3), (104, None)], ["order_id", "customer_id"]
        )

        # This should pass because all non-null customer_ids in orders exist in customers.id
        from pyspark.testing.utils import assertReferentialIntegrity

        assertReferentialIntegrity(orders, "customer_id", customers, "id")

    def test_assert_referential_integrity_invalid(self):
        # Create a "customers" DataFrame with customer IDs
        customers = self.spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"]
        )

        # Create an orders DataFrame with an invalid customer ID
        orders_invalid = self.spark.createDataFrame(
            [(101, 1), (102, 2), (103, 4)], ["order_id", "customer_id"]
        )

        # This should fail because customer_id 4 doesn't exist in customers.id
        from pyspark.testing.utils import assertReferentialIntegrity

        with self.assertRaises(AssertionError) as cm:
            assertReferentialIntegrity(orders_invalid, "customer_id", customers, "id")

        # Check that the error message contains information about the missing values
        error_message = str(cm.exception)
        self.assertIn("customer_id", error_message)
        self.assertIn("id", error_message)
        self.assertIn("4", error_message)
        self.assertIn("Missing values", error_message)

        # Verify that the total count of missing values is correct
        self.assertIn("Total missing values: 1", error_message)

    def test_assert_referential_integrity_null_values(self):
        # Create a "customers" DataFrame with customer IDs
        customers = self.spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"]
        )

        # Create an orders DataFrame with null values in the foreign key
        orders_with_nulls = self.spark.createDataFrame(
            [(101, 1), (102, 2), (103, None), (104, None)], ["order_id", "customer_id"]
        )

        # This should pass because null values in the foreign key are ignored
        from pyspark.testing.utils import assertReferentialIntegrity

        assertReferentialIntegrity(orders_with_nulls, "customer_id", customers, "id")

    def test_assert_referential_integrity_multiple_invalid(self):
        # Create a "customers" DataFrame with customer IDs
        customers = self.spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"]
        )

        # Create an orders DataFrame with multiple invalid customer IDs
        orders_multiple_invalid = self.spark.createDataFrame(
            [(101, 1), (102, 4), (103, 5), (104, 6)], ["order_id", "customer_id"]
        )

        # This should fail because multiple customer_ids don't exist in customers.id
        from pyspark.testing.utils import assertReferentialIntegrity

        with self.assertRaises(AssertionError):
            assertReferentialIntegrity(orders_multiple_invalid, "customer_id", customers, "id")

    def test_assert_referential_integrity_with_custom_message(self):
        # Create a "customers" DataFrame with customer IDs
        customers = self.spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"]
        )

        # Create an orders DataFrame with an invalid customer ID
        orders_invalid = self.spark.createDataFrame(
            [(101, 1), (102, 2), (103, 4)], ["order_id", "customer_id"]
        )

        custom_message = "All customer IDs must exist in the customers table."

        with self.assertRaises(AssertionError) as cm:
            assertReferentialIntegrity(
                orders_invalid, "customer_id", customers, "id", message=custom_message
            )

        self.assertTrue(
            "Column 'customer_id' contains values not found in target column 'id'" in str(cm.exception)
        )
        self.assertTrue("Missing values: [4]" in str(cm.exception))
        self.assertTrue("All customer IDs must exist in the customers table." in str(cm.exception))

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_referential_integrity_pandas_valid(self):
        # Create a "customers" DataFrame with customer IDs
        import pandas as pd

        customers = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        # Create an "orders" DataFrame with customer IDs as foreign keys
        orders = pd.DataFrame({"order_id": [101, 102, 103, 104], "customer_id": [1, 2, 3, None]})

        # This should pass because all non-null customer_ids in orders exist in customers.id
        assertReferentialIntegrity(orders, "customer_id", customers, "id")

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_referential_integrity_pandas_invalid(self):
        # Create a "customers" DataFrame with customer IDs
        import pandas as pd

        customers = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        # Create an orders DataFrame with an invalid customer ID
        orders_invalid = pd.DataFrame({"order_id": [101, 102, 103], "customer_id": [1, 2, 4]})

        # This should fail because customer_id 4 doesn't exist in customers.id
        with self.assertRaises(AssertionError) as cm:
            assertReferentialIntegrity(orders_invalid, "customer_id", customers, "id")

        self.assertTrue(
            "Column 'customer_id' contains values not found in target column 'id'" in str(cm.exception)
        )
        self.assertTrue("Missing values: [4]" in str(cm.exception))

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_referential_integrity_pandas_with_custom_message(self):
        # Create a "customers" DataFrame with customer IDs
        import pandas as pd

        customers = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        # Create an orders DataFrame with an invalid customer ID
        orders_invalid = pd.DataFrame({"order_id": [101, 102, 103], "customer_id": [1, 2, 4]})

        custom_message = "All customer IDs must exist in the customers table."

        with self.assertRaises(AssertionError) as cm:
            assertReferentialIntegrity(
                orders_invalid, "customer_id", customers, "id", message=custom_message
            )

        self.assertTrue(
            "Column 'customer_id' contains values not found in target column 'id'" in str(cm.exception)
        )
        self.assertTrue("Missing values: [4]" in str(cm.exception))
        self.assertTrue("All customer IDs must exist in the customers table." in str(cm.exception))

    @unittest.skipIf(not have_pandas or not have_pyarrow, "no pandas or pyarrow dependency")
    def test_assert_referential_integrity_pandas_on_spark(self):
        # Create a "customers" DataFrame with customer IDs
        import pandas as pd
        import pyspark.pandas as ps

        customers_pdf = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        customers_psdf = ps.from_pandas(customers_pdf)

        # Create an "orders" DataFrame with customer IDs as foreign keys
        orders_pdf = pd.DataFrame({"order_id": [101, 102, 103, 104], "customer_id": [1, 2, 3, None]})
        orders_psdf = ps.from_pandas(orders_pdf)

        # This should pass because all non-null customer_ids in orders exist in customers.id
        assertReferentialIntegrity(orders_psdf, "customer_id", customers_psdf, "id")

        # Create an orders DataFrame with an invalid customer ID
        orders_invalid_pdf = pd.DataFrame({"order_id": [101, 102, 103], "customer_id": [1, 2, 4]})
        orders_invalid_psdf = ps.from_pandas(orders_invalid_pdf)

        # This should fail because customer_id 4 doesn't exist in customers.id
        with self.assertRaises(AssertionError) as cm:
            assertReferentialIntegrity(orders_invalid_psdf, "customer_id", customers_psdf, "id")

        self.assertTrue(
            "Column 'customer_id' contains values not found in target column 'id'" in str(cm.exception)
        )

        # Check that the error message contains information about the missing values
        error_message = str(cm.exception)
        self.assertIn("customer_id", error_message)
        self.assertIn("id", error_message)
        # Check that at least one of the invalid values is mentioned
        self.assertTrue(any(str(val) in error_message for val in [4, 5, 6]))

        # Verify that the total count of missing values is correct (should be 3: values 4, 5, and 6)
        self.assertIn("Total missing values: 3", error_message)


class UtilsTests(ReusedSQLTestCase, UtilsTestsMixin):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_utils import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
