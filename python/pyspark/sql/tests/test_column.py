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

from pyspark.sql import Column, Row
from pyspark.sql.types import StructType, StructField, LongType
from pyspark.sql.utils import AnalysisException
from pyspark.testing.sqlutils import ReusedSQLTestCase


class ColumnTests(ReusedSQLTestCase):

    def test_column_name_encoding(self):
        """Ensure that created columns has `str` type consistently."""
        columns = self.spark.createDataFrame([('Alice', 1)], ['name', u'age']).columns
        self.assertEqual(columns, ['name', 'age'])
        self.assertTrue(isinstance(columns[0], str))
        self.assertTrue(isinstance(columns[1], str))

    def test_and_in_expression(self):
        self.assertEqual(4, self.df.filter((self.df.key <= 10) & (self.df.value <= "2")).count())
        self.assertRaises(ValueError, lambda: (self.df.key <= 10) and (self.df.value <= "2"))
        self.assertEqual(14, self.df.filter((self.df.key <= 3) | (self.df.value < "2")).count())
        self.assertRaises(ValueError, lambda: self.df.key <= 3 or self.df.value < "2")
        self.assertEqual(99, self.df.filter(~(self.df.key == 1)).count())
        self.assertRaises(ValueError, lambda: not self.df.key == 1)

    def test_validate_column_types(self):
        from pyspark.sql.functions import udf, to_json
        from pyspark.sql.column import _to_java_column

        self.assertTrue("Column" in _to_java_column("a").getClass().toString())
        self.assertTrue("Column" in _to_java_column(u"a").getClass().toString())
        self.assertTrue("Column" in _to_java_column(self.spark.range(1).id).getClass().toString())

        self.assertRaisesRegex(
            TypeError,
            "Invalid argument, not a string or column",
            lambda: _to_java_column(1))

        class A():
            pass

        self.assertRaises(TypeError, lambda: _to_java_column(A()))
        self.assertRaises(TypeError, lambda: _to_java_column([]))

        self.assertRaisesRegex(
            TypeError,
            "Invalid argument, not a string or column",
            lambda: udf(lambda x: x)(None))
        self.assertRaises(TypeError, lambda: to_json(1))

    def test_column_operators(self):
        ci = self.df.key
        cs = self.df.value
        c = ci == cs
        self.assertTrue(isinstance((- ci - 1 - 2) % 3 * 2.5 / 3.5, Column))
        rcc = (1 + ci), (1 - ci), (1 * ci), (1 / ci), (1 % ci), (1 ** ci), (ci ** 1)
        self.assertTrue(all(isinstance(c, Column) for c in rcc))
        cb = [ci == 5, ci != 0, ci > 3, ci < 4, ci >= 0, ci <= 7]
        self.assertTrue(all(isinstance(c, Column) for c in cb))
        cbool = (ci & ci), (ci | ci), (~ci)
        self.assertTrue(all(isinstance(c, Column) for c in cbool))
        css = cs.contains('a'), cs.like('a'), cs.rlike('a'), cs.ilike('A'), cs.asc(), cs.desc(),\
            cs.startswith('a'), cs.endswith('a'), ci.eqNullSafe(cs)
        self.assertTrue(all(isinstance(c, Column) for c in css))
        self.assertTrue(isinstance(ci.cast(LongType()), Column))
        self.assertRaisesRegex(ValueError,
                               "Cannot apply 'in' operator against a column",
                               lambda: 1 in cs)

    def test_column_accessor(self):
        from pyspark.sql.functions import col

        self.assertIsInstance(col("foo")[1:3], Column)
        self.assertIsInstance(col("foo")[0], Column)
        self.assertIsInstance(col("foo")["bar"], Column)
        self.assertRaises(ValueError, lambda: col("foo")[0:10:2])

    def test_column_select(self):
        df = self.df
        self.assertEqual(self.testData, df.select("*").collect())
        self.assertEqual(self.testData, df.select(df.key, df.value).collect())
        self.assertEqual([Row(value='1')], df.where(df.key == 1).select(df.value).collect())

    def test_access_column(self):
        df = self.df
        self.assertTrue(isinstance(df.key, Column))
        self.assertTrue(isinstance(df['key'], Column))
        self.assertTrue(isinstance(df[0], Column))
        self.assertRaises(IndexError, lambda: df[2])
        self.assertRaises(AnalysisException, lambda: df["bad_key"])
        self.assertRaises(TypeError, lambda: df[{}])

    def test_column_name_with_non_ascii(self):
        columnName = "数量"
        self.assertTrue(isinstance(columnName, str))
        schema = StructType([StructField(columnName, LongType(), True)])
        df = self.spark.createDataFrame([(1,)], schema)
        self.assertEqual(schema, df.schema)
        self.assertEqual("DataFrame[数量: bigint]", str(df))
        self.assertEqual([("数量", 'bigint')], df.dtypes)
        self.assertEqual(1, df.select("数量").first()[0])
        self.assertEqual(1, df.select(df["数量"]).first()[0])
        self.assertTrue(columnName in repr(df[columnName]))

    def test_field_accessor(self):
        df = self.sc.parallelize([Row(l=[1], r=Row(a=1, b="b"), d={"k": "v"})]).toDF()
        self.assertEqual(1, df.select(df.l[0]).first()[0])
        self.assertEqual(1, df.select(df.r["a"]).first()[0])
        self.assertEqual(1, df.select(df["r.a"]).first()[0])
        self.assertEqual("b", df.select(df.r["b"]).first()[0])
        self.assertEqual("b", df.select(df["r.b"]).first()[0])
        self.assertEqual("v", df.select(df.d["k"]).first()[0])

    def test_bitwise_operations(self):
        from pyspark.sql import functions
        row = Row(a=170, b=75)
        df = self.spark.createDataFrame([row])
        result = df.select(df.a.bitwiseAND(df.b)).collect()[0].asDict()
        self.assertEqual(170 & 75, result['(a & b)'])
        result = df.select(df.a.bitwiseOR(df.b)).collect()[0].asDict()
        self.assertEqual(170 | 75, result['(a | b)'])
        result = df.select(df.a.bitwiseXOR(df.b)).collect()[0].asDict()
        self.assertEqual(170 ^ 75, result['(a ^ b)'])
        result = df.select(functions.bitwiseNOT(df.b)).collect()[0].asDict()
        self.assertEqual(~75, result['~b'])
        result = df.select(functions.bitwise_not(df.b)).collect()[0].asDict()
        self.assertEqual(~75, result['~b'])

    def test_with_field(self):
        from pyspark.sql.functions import lit, col
        df = self.spark.createDataFrame([Row(a=Row(b=1, c=2))])
        self.assertIsInstance(df['a'].withField('b', lit(3)), Column)
        self.assertIsInstance(df['a'].withField('d', lit(3)), Column)
        result = df.withColumn('a', df['a'].withField('d', lit(3))).collect()[0].asDict()
        self.assertEqual(3, result['a']['d'])
        result = df.withColumn('a', df['a'].withField('b', lit(3))).collect()[0].asDict()
        self.assertEqual(3, result['a']['b'])

        self.assertRaisesRegex(TypeError,
                               'col should be a Column',
                               lambda: df['a'].withField('b', 3))
        self.assertRaisesRegex(TypeError,
                               'fieldName should be a string',
                               lambda: df['a'].withField(col('b'), lit(3)))

    def test_drop_fields(self):
        df = self.spark.createDataFrame([Row(a=Row(b=1, c=2, d=Row(e=3, f=4)))])
        self.assertIsInstance(df["a"].dropFields("b"), Column)
        self.assertIsInstance(df["a"].dropFields("b", "c"), Column)
        self.assertIsInstance(df["a"].dropFields("d.e"), Column)

        result = df.select(
            df["a"].dropFields("b").alias("a1"),
            df["a"].dropFields("d.e").alias("a2"),
        ).first().asDict(True)

        self.assertTrue(
            "b" not in result["a1"] and
            "c" in result["a1"] and
            "d" in result["a1"]
        )

        self.assertTrue(
            "e" not in result["a2"]["d"] and
            "f" in result["a2"]["d"]
        )

if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.test_column import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
