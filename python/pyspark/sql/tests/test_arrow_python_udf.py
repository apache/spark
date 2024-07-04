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

from pyspark.errors import AnalysisException, PythonException, PySparkNotImplementedError
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.tests.test_udf import BaseUDFTestsMixin
from pyspark.sql.types import VarcharType
from pyspark.testing.sqlutils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
    ReusedSQLTestCase,
)
from pyspark.util import PythonEvalType


@unittest.skipIf(
    not have_pandas or not have_pyarrow, pandas_requirement_message or pyarrow_requirement_message
)
class PythonUDFArrowTestsMixin(BaseUDFTestsMixin):
    @unittest.skip("Unrelated test, and it fails when it runs duplicatedly.")
    def test_broadcast_in_udf(self):
        super(PythonUDFArrowTests, self).test_broadcast_in_udf()

    @unittest.skip("Unrelated test, and it fails when it runs duplicatedly.")
    def test_register_java_function(self):
        super(PythonUDFArrowTests, self).test_register_java_function()

    @unittest.skip("Unrelated test, and it fails when it runs duplicatedly.")
    def test_register_java_udaf(self):
        super(PythonUDFArrowTests, self).test_register_java_udaf()

    def test_complex_input_types(self):
        row = (
            self.spark.range(1)
            .selectExpr("array(1, 2, 3) as array", "map('a', 'b') as map", "struct(1, 2) as struct")
            .select(
                udf(lambda x: str(x))("array"),
                udf(lambda x: str(x))("map"),
                udf(lambda x: str(x))("struct"),
            )
            .first()
        )

        self.assertIn(row[0], ["[1, 2, 3]", "[np.int32(1), np.int32(2), np.int32(3)]"])
        self.assertEqual(row[1], "{'a': 'b'}")
        self.assertEqual(row[2], "Row(col1=1, col2=2)")

    def test_use_arrow(self):
        # useArrow=True
        row_true = (
            self.spark.range(1)
            .selectExpr(
                "array(1, 2, 3) as array",
            )
            .select(
                udf(lambda x: str(x), useArrow=True)("array"),
            )
            .first()
        )

        # useArrow=None
        row_none = (
            self.spark.range(1)
            .selectExpr(
                "array(1, 2, 3) as array",
            )
            .select(
                udf(lambda x: str(x), useArrow=None)("array"),
            )
            .first()
        )

        self.assertEqual(row_true[0], row_none[0])  # "[1, 2, 3]"

        # useArrow=False
        row_false = (
            self.spark.range(1)
            .selectExpr(
                "array(1, 2, 3) as array",
            )
            .select(
                udf(lambda x: str(x), useArrow=False)("array"),
            )
            .first()
        )
        self.assertEqual(row_false[0], "[1, 2, 3]")

    def test_eval_type(self):
        self.assertEqual(
            udf(lambda x: str(x), useArrow=True).evalType, PythonEvalType.SQL_ARROW_BATCHED_UDF
        )
        self.assertEqual(
            udf(lambda x: str(x), useArrow=False).evalType, PythonEvalType.SQL_BATCHED_UDF
        )

    def test_register(self):
        df = self.spark.range(1).selectExpr(
            "array(1, 2, 3) as array",
        )
        str_repr_func = self.spark.udf.register("str_repr", udf(lambda x: str(x), useArrow=True))

        # To verify that Arrow optimization is on
        self.assertIn(
            df.selectExpr("str_repr(array) AS str_id").first()[0],
            ["[1, 2, 3]", "[np.int32(1), np.int32(2), np.int32(3)]"],
            # The input is a NumPy array when the Arrow optimization is on
        )

        # To verify that a UserDefinedFunction is returned
        self.assertListEqual(
            df.selectExpr("str_repr(array) AS str_id").collect(),
            df.select(str_repr_func("array").alias("str_id")).collect(),
        )

    def test_nested_array_input(self):
        df = self.spark.range(1).selectExpr("array(array(1, 2), array(3, 4)) as nested_array")
        self.assertIn(
            df.select(
                udf(lambda x: str(x), returnType="string", useArrow=True)("nested_array")
            ).first()[0],
            [
                "[[1, 2], [3, 4]]",
                "[[np.int32(1), np.int32(2)], [np.int32(3), np.int32(4)]]",
            ],
        )

    def test_type_coercion_string_to_numeric(self):
        df_int_value = self.spark.createDataFrame(["1", "2"], schema="string")
        df_floating_value = self.spark.createDataFrame(["1.1", "2.2"], schema="string")

        int_ddl_types = ["tinyint", "smallint", "int", "bigint"]
        floating_ddl_types = ["double", "float"]

        for ddl_type in int_ddl_types:
            # df_int_value
            res = df_int_value.select(udf(lambda x: x, ddl_type)("value").alias("res"))
            self.assertEqual(res.collect(), [Row(res=1), Row(res=2)])
            self.assertEqual(res.dtypes[0][1], ddl_type)

        floating_results = [
            [Row(res=1.1), Row(res=2.2)],
            [Row(res=1.100000023841858), Row(res=2.200000047683716)],
        ]
        for ddl_type, floating_res in zip(floating_ddl_types, floating_results):
            # df_int_value
            res = df_int_value.select(udf(lambda x: x, ddl_type)("value").alias("res"))
            self.assertEqual(res.collect(), [Row(res=1.0), Row(res=2.0)])
            self.assertEqual(res.dtypes[0][1], ddl_type)
            # df_floating_value
            res = df_floating_value.select(udf(lambda x: x, ddl_type)("value").alias("res"))
            self.assertEqual(res.collect(), floating_res)
            self.assertEqual(res.dtypes[0][1], ddl_type)

        # invalid
        with self.assertRaises(PythonException):
            df_floating_value.select(udf(lambda x: x, "int")("value").alias("res")).collect()

        with self.assertRaises(PythonException):
            df_int_value.select(udf(lambda x: x, "decimal")("value").alias("res")).collect()

        with self.assertRaises(PythonException):
            df_floating_value.select(udf(lambda x: x, "decimal")("value").alias("res")).collect()

    def test_err_return_type(self):
        with self.assertRaises(PySparkNotImplementedError) as pe:
            udf(lambda x: x, VarcharType(10), useArrow=True)

        self.check_error(
            exception=pe.exception,
            error_class="NOT_IMPLEMENTED",
            message_parameters={
                "feature": "Invalid return type with Arrow-optimized Python UDF: VarcharType(10)"
            },
        )

    def test_warn_no_args(self):
        with self.assertWarns(UserWarning) as w:
            udf(lambda: print("do"), useArrow=True)
        self.assertEqual(
            str(w.warning),
            "Arrow optimization for Python UDFs cannot be enabled for functions"
            " without arguments.",
        )

    def test_named_arguments_negative(self):
        @udf("int")
        def test_udf(a, b):
            return a + b

        self.spark.udf.register("test_udf", test_udf)

        with self.assertRaisesRegex(
            AnalysisException,
            "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE",
        ):
            self.spark.sql("SELECT test_udf(a => id, a => id * 10) FROM range(2)").show()

        with self.assertRaisesRegex(AnalysisException, "UNEXPECTED_POSITIONAL_ARGUMENT"):
            self.spark.sql("SELECT test_udf(a => id, id * 10) FROM range(2)").show()

        with self.assertRaises(PythonException):
            self.spark.sql("SELECT test_udf(c => 'x') FROM range(2)").show()

        with self.assertRaises(PythonException):
            self.spark.sql("SELECT test_udf(id, a => id * 10) FROM range(2)").show()


class PythonUDFArrowTests(PythonUDFArrowTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super(PythonUDFArrowTests, cls).setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled")
        finally:
            super(PythonUDFArrowTests, cls).tearDownClass()


if __name__ == "__main__":
    from pyspark.sql.tests.test_arrow_python_udf import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
