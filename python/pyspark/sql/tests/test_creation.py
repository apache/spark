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

import platform
from decimal import Decimal
import os
import time
import unittest
from typing import cast

from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType,
    TimestampType,
    TimestampNTZType,
)
from pyspark.errors import (
    PySparkTypeError,
    PySparkValueError,
)
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


class DataFrameCreationTestsMixin:
    def test_create_dataframe_from_array_of_long(self):
        import array

        data = [Row(longarray=array.array("l", [-9223372036854775808, 0, 9223372036854775807]))]
        df = self.spark.createDataFrame(data)
        self.assertEqual(df.first(), Row(longarray=[-9223372036854775808, 0, 9223372036854775807]))

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_create_dataframe_from_pandas_with_timestamp(self):
        import pandas as pd
        from datetime import datetime

        pdf = pd.DataFrame(
            {"ts": [datetime(2017, 10, 31, 1, 1, 1)], "d": [pd.Timestamp.now().date()]},
            columns=["d", "ts"],
        )
        # test types are inferred correctly without specifying schema
        df = self.spark.createDataFrame(pdf)
        self.assertIsInstance(df.schema["ts"].dataType, TimestampType)
        self.assertIsInstance(df.schema["d"].dataType, DateType)
        # test with schema will accept pdf as input
        df = self.spark.createDataFrame(pdf, schema="d date, ts timestamp")
        self.assertIsInstance(df.schema["ts"].dataType, TimestampType)
        self.assertIsInstance(df.schema["d"].dataType, DateType)
        df = self.spark.createDataFrame(pdf, schema="d date, ts timestamp_ntz")
        self.assertIsInstance(df.schema["ts"].dataType, TimestampNTZType)
        self.assertIsInstance(df.schema["d"].dataType, DateType)

    @unittest.skipIf(have_pandas, "Required Pandas was found.")
    def test_create_dataframe_required_pandas_not_found(self):
        with self.quiet():
            with self.assertRaisesRegex(
                ImportError, "(Pandas >= .* must be installed|No module named '?pandas'?)"
            ):
                import pandas as pd
                from datetime import datetime

                pdf = pd.DataFrame(
                    {"ts": [datetime(2017, 10, 31, 1, 1, 1)], "d": [pd.Timestamp.now().date()]}
                )
                self.spark.createDataFrame(pdf)

    # Regression test for SPARK-23360
    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_create_dataframe_from_pandas_with_dst(self):
        import pandas as pd
        from pandas.testing import assert_frame_equal
        from datetime import datetime

        pdf = pd.DataFrame({"time": [datetime(2015, 10, 31, 22, 30)]})

        df = self.spark.createDataFrame(pdf)
        assert_frame_equal(pdf, df.toPandas())

        orig_env_tz = os.environ.get("TZ", None)
        try:
            tz = "America/Los_Angeles"
            os.environ["TZ"] = tz
            time.tzset()
            with self.sql_conf({"spark.sql.session.timeZone": tz}):
                df = self.spark.createDataFrame(pdf)
                assert_frame_equal(pdf, df.toPandas())
        finally:
            del os.environ["TZ"]
            if orig_env_tz is not None:
                os.environ["TZ"] = orig_env_tz
            time.tzset()

    @unittest.skipIf(not have_pandas, pandas_requirement_message)  # type: ignore
    def test_create_dataframe_from_pandas_with_day_time_interval(self):
        # SPARK-37277: Test DayTimeIntervalType in createDataFrame without Arrow.
        import pandas as pd
        from datetime import timedelta

        df = self.spark.createDataFrame(pd.DataFrame({"a": [timedelta(microseconds=123)]}))
        self.assertEqual(df.toPandas().a.iloc[0], timedelta(microseconds=123))

    # test for SPARK-36337
    def test_create_nan_decimal_dataframe(self):
        self.assertEqual(
            self.spark.createDataFrame(data=[Decimal("NaN")], schema="decimal").collect(),
            [Row(value=None)],
        )

    def test_invalid_argument_create_dataframe(self):
        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.createDataFrame([(1, 2)], schema=123)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_LIST_OR_NONE_OR_STRUCT",
            messageParameters={"arg_name": "schema", "arg_type": "int"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            self.spark.createDataFrame(self.spark.range(1))

        self.check_error(
            exception=pe.exception,
            errorClass="INVALID_TYPE",
            messageParameters={"arg_name": "data", "arg_type": "DataFrame"},
        )

    def test_partial_inference_failure(self):
        with self.assertRaises(PySparkValueError) as pe:
            self.spark.createDataFrame([(None, 1)])

        self.check_error(
            exception=pe.exception,
            errorClass="CANNOT_DETERMINE_TYPE",
            messageParameters={},
        )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_schema_inference_from_pandas_with_dict(self):
        # SPARK-47543: test for verifying if inferring `dict` as `MapType` work properly.
        import pandas as pd

        pdf = pd.DataFrame({"str_col": ["second"], "dict_col": [{"first": 0.7, "second": 0.3}]})

        with self.sql_conf(
            {
                "spark.sql.execution.arrow.pyspark.enabled": True,
                "spark.sql.execution.arrow.pyspark.fallback.enabled": False,
                "spark.sql.execution.pandas.inferPandasDictAsMap": True,
            }
        ):
            sdf = self.spark.createDataFrame(pdf)
            self.assertEqual(
                sdf.withColumn("test", F.col("dict_col")[F.col("str_col")]).collect(),
                [Row(str_col="second", dict_col={"first": 0.7, "second": 0.3}, test=0.3)],
            )

            # Empty dict should fail
            pdf_empty_struct = pd.DataFrame({"str_col": ["second"], "dict_col": [{}]})

            with self.assertRaises(PySparkValueError) as pe:
                self.spark.createDataFrame(pdf_empty_struct)

            self.check_error(
                exception=pe.exception,
                errorClass="CANNOT_INFER_EMPTY_SCHEMA",
                messageParameters={},
            )

            # Dict has different types of values should fail
            pdf_different_type = pd.DataFrame(
                {"str_col": ["second"], "dict_col": [{"first": 0.7, "second": "0.3"}]}
            )
            self.assertRaises(
                PySparkValueError, lambda: self.spark.createDataFrame(pdf_different_type)
            )

        with self.sql_conf(
            {
                "spark.sql.execution.arrow.pyspark.enabled": False,
                "spark.sql.execution.pandas.inferPandasDictAsMap": True,
            }
        ):
            sdf = self.spark.createDataFrame(pdf)
            self.assertEqual(
                sdf.withColumn("test", F.col("dict_col")[F.col("str_col")]).collect(),
                [Row(str_col="second", dict_col={"first": 0.7, "second": 0.3}, test=0.3)],
            )


class DataFrameCreationTests(
    DataFrameCreationTestsMixin,
    ReusedSQLTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_creation import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
