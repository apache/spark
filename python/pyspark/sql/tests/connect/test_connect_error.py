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

import unittest

from pyspark.errors import PySparkAttributeError
from pyspark.errors.exceptions.base import SessionNotSameException
from pyspark.sql.types import Row
from pyspark.testing.connectutils import should_test_connect
from pyspark.errors import PySparkTypeError
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.sql.tests.connect.test_connect_basic import SparkConnectSQLTestCase

if should_test_connect:
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
    from pyspark.sql.connect import functions as CF
    from pyspark.sql.connect.column import Column


class SparkConnectErrorTests(SparkConnectSQLTestCase):
    def test_recursion_handling_for_plan_logging(self):
        """SPARK-45852 - Test that we can handle recursion in plan logging."""
        cdf = self.connect.range(1)
        for x in range(400):
            cdf = cdf.withColumn(f"col_{x}", CF.lit(x))

        # Calling schema will trigger logging the message that will in turn trigger the message
        # conversion into protobuf that will then trigger the recursion error.
        self.assertIsNotNone(cdf.schema)

        result = self.connect._client._proto_to_string(cdf._plan.to_proto(self.connect._client))
        self.assertIn("recursion", result)

    def test_error_handling(self):
        # SPARK-41533 Proper error handling for Spark Connect
        df = self.connect.range(10).select("id2")
        with self.assertRaises(AnalysisException):
            df.collect()

    def test_invalid_column(self):
        # SPARK-41812: fail df1.select(df2.col)
        data1 = [Row(a=1, b=2, c=3)]
        cdf1 = self.connect.createDataFrame(data1)

        data2 = [Row(a=2, b=0)]
        cdf2 = self.connect.createDataFrame(data2)

        with self.assertRaises(AnalysisException):
            cdf1.select(cdf2.a).schema

        with self.assertRaises(AnalysisException):
            cdf2.withColumn("x", cdf1.a + 1).schema

        # Can find the target plan node, but fail to resolve with it
        with self.assertRaisesRegex(
            AnalysisException,
            "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        ):
            cdf3 = cdf1.select(cdf1.a)
            cdf3.select(cdf1.b).schema

        # Can not find the target plan node by plan id
        with self.assertRaisesRegex(
            AnalysisException,
            "CANNOT_RESOLVE_DATAFRAME_COLUMN",
        ):
            cdf1.select(cdf2.a).schema

    def test_invalid_star(self):
        data1 = [Row(a=1, b=2, c=3)]
        cdf1 = self.connect.createDataFrame(data1)

        data2 = [Row(a=2, b=0)]
        cdf2 = self.connect.createDataFrame(data2)

        # Can find the target plan node, but fail to resolve with it
        with self.assertRaisesRegex(
            AnalysisException,
            "CANNOT_RESOLVE_DATAFRAME_COLUMN",
        ):
            cdf3 = cdf1.select(cdf1.a)
            cdf3.select(cdf1["*"]).schema

        # Can find the target plan node, but fail to resolve with it
        with self.assertRaisesRegex(
            AnalysisException,
            "CANNOT_RESOLVE_DATAFRAME_COLUMN",
        ):
            # column 'a has been replaced
            cdf3 = cdf1.withColumn("a", CF.lit(0))
            cdf3.select(cdf1["*"]).schema

        # Can not find the target plan node by plan id
        with self.assertRaisesRegex(
            AnalysisException,
            "CANNOT_RESOLVE_DATAFRAME_COLUMN",
        ):
            cdf1.select(cdf2["*"]).schema

        # cdf1["*"] exists on both side
        with self.assertRaisesRegex(
            AnalysisException,
            "AMBIGUOUS_COLUMN_REFERENCE",
        ):
            cdf1.join(cdf1).select(cdf1["*"]).schema

    def test_deduplicate_within_watermark_in_batch(self):
        df = self.connect.read.table(self.tbl_name)
        with self.assertRaisesRegex(
            AnalysisException,
            "dropDuplicatesWithinWatermark is not supported with batch DataFrames/DataSets",
        ):
            df.dropDuplicatesWithinWatermark().toPandas()

    def test_different_spark_session_join_or_union(self):
        df = self.connect.range(10).limit(3)

        spark2 = RemoteSparkSession(connection="sc://localhost")
        df2 = spark2.range(10).limit(3)

        with self.assertRaises(SessionNotSameException) as e1:
            df.union(df2).collect()
        self.check_error(
            exception=e1.exception,
            errorClass="SESSION_NOT_SAME",
            messageParameters={},
        )

        with self.assertRaises(SessionNotSameException) as e2:
            df.unionByName(df2).collect()
        self.check_error(
            exception=e2.exception,
            errorClass="SESSION_NOT_SAME",
            messageParameters={},
        )

        with self.assertRaises(SessionNotSameException) as e3:
            df.join(df2).collect()
        self.check_error(
            exception=e3.exception,
            errorClass="SESSION_NOT_SAME",
            messageParameters={},
        )

    def test_unsupported_functions(self):
        # SPARK-41225: Disable unsupported functions.
        df = self.connect.read.table(self.tbl_name)
        with self.assertRaises(NotImplementedError):
            df.toJSON()
        with self.assertRaises(NotImplementedError):
            df.rdd

    def test_unsupported_jvm_attribute(self):
        # Unsupported jvm attributes for Spark session.
        unsupported_attrs = ["_jsc", "_jconf", "_jvm", "_jsparkSession"]
        spark_session = self.connect
        for attr in unsupported_attrs:
            with self.assertRaises(PySparkAttributeError) as pe:
                getattr(spark_session, attr)

            self.check_error(
                exception=pe.exception,
                errorClass="JVM_ATTRIBUTE_NOT_SUPPORTED",
                messageParameters={"attr_name": attr},
            )

        # Unsupported jvm attributes for DataFrame.
        unsupported_attrs = ["_jseq", "_jdf", "_jmap", "_jcols"]
        cdf = self.connect.range(10)
        for attr in unsupported_attrs:
            with self.assertRaises(PySparkAttributeError) as pe:
                getattr(cdf, attr)

            self.check_error(
                exception=pe.exception,
                errorClass="JVM_ATTRIBUTE_NOT_SUPPORTED",
                messageParameters={"attr_name": attr},
            )

        # Unsupported jvm attributes for Column.
        with self.assertRaises(PySparkAttributeError) as pe:
            getattr(cdf.id, "_jc")

        self.check_error(
            exception=pe.exception,
            errorClass="JVM_ATTRIBUTE_NOT_SUPPORTED",
            messageParameters={"attr_name": "_jc"},
        )

        # Unsupported jvm attributes for DataFrameReader.
        with self.assertRaises(PySparkAttributeError) as pe:
            getattr(spark_session.read, "_jreader")

        self.check_error(
            exception=pe.exception,
            errorClass="JVM_ATTRIBUTE_NOT_SUPPORTED",
            messageParameters={"attr_name": "_jreader"},
        )

    def test_column_cannot_be_constructed_from_string(self):
        with self.assertRaises(TypeError):
            Column("col")

    def test_select_none(self):
        with self.assertRaises(PySparkTypeError) as e1:
            self.connect.range(1).select(None)

        self.check_error(
            exception=e1.exception,
            errorClass="NOT_LIST_OF_COLUMN_OR_STR",
            messageParameters={"arg_name": "columns"},
        )

    def test_ym_interval_in_collect(self):
        # YearMonthIntervalType is not supported in python side arrow conversion
        with self.assertRaises(PySparkTypeError):
            self.connect.sql("SELECT INTERVAL '10-8' YEAR TO MONTH AS interval").first()


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_error import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
