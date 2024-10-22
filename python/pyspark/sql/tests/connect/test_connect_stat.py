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

from pyspark.errors import PySparkTypeError, PySparkValueError
from pyspark.testing.connectutils import should_test_connect
from pyspark.sql.tests.connect.test_connect_basic import SparkConnectSQLTestCase

if should_test_connect:
    from pyspark.sql import functions as SF
    from pyspark.sql.connect import functions as CF
    from pyspark.errors.exceptions.connect import (
        AnalysisException,
        SparkConnectException,
    )


class SparkConnectStatTests(SparkConnectSQLTestCase):
    def test_fill_na(self):
        # SPARK-41128: Test fill na
        query = """
            SELECT * FROM VALUES
            (false, 1, NULL), (false, NULL, 2.0), (NULL, 3, 3.0)
            AS tab(a, b, c)
            """
        # +-----+----+----+
        # |    a|   b|   c|
        # +-----+----+----+
        # |false|   1|NULL|
        # |false|NULL| 2.0|
        # | NULL|   3| 3.0|
        # +-----+----+----+

        self.assert_eq(
            self.connect.sql(query).fillna(True).toPandas(),
            self.spark.sql(query).fillna(True).toPandas(),
        )
        self.assert_eq(
            self.connect.sql(query).fillna(2).toPandas(),
            self.spark.sql(query).fillna(2).toPandas(),
        )
        self.assert_eq(
            self.connect.sql(query).fillna(2, ["a", "b"]).toPandas(),
            self.spark.sql(query).fillna(2, ["a", "b"]).toPandas(),
        )
        self.assert_eq(
            self.connect.sql(query).na.fill({"a": True, "b": 2}).toPandas(),
            self.spark.sql(query).na.fill({"a": True, "b": 2}).toPandas(),
        )

    def test_drop_na(self):
        # SPARK-41148: Test drop na
        query = """
            SELECT * FROM VALUES
            (false, 1, NULL), (false, NULL, 2.0), (NULL, 3, 3.0)
            AS tab(a, b, c)
            """
        # +-----+----+----+
        # |    a|   b|   c|
        # +-----+----+----+
        # |false|   1|NULL|
        # |false|NULL| 2.0|
        # | NULL|   3| 3.0|
        # +-----+----+----+

        self.assert_eq(
            self.connect.sql(query).dropna().toPandas(),
            self.spark.sql(query).dropna().toPandas(),
        )
        self.assert_eq(
            self.connect.sql(query).na.drop(how="all", thresh=1).toPandas(),
            self.spark.sql(query).na.drop(how="all", thresh=1).toPandas(),
        )
        self.assert_eq(
            self.connect.sql(query).dropna(thresh=1, subset=("a", "b")).toPandas(),
            self.spark.sql(query).dropna(thresh=1, subset=("a", "b")).toPandas(),
        )
        self.assert_eq(
            self.connect.sql(query).na.drop(how="any", thresh=2, subset="a").toPandas(),
            self.spark.sql(query).na.drop(how="any", thresh=2, subset="a").toPandas(),
        )

    def test_replace(self):
        # SPARK-41315: Test replace
        query = """
            SELECT * FROM VALUES
            (false, 1, NULL), (false, NULL, 2.0), (NULL, 3, 3.0)
            AS tab(a, b, c)
            """
        # +-----+----+----+
        # |    a|   b|   c|
        # +-----+----+----+
        # |false|   1|NULL|
        # |false|NULL| 2.0|
        # | NULL|   3| 3.0|
        # +-----+----+----+

        self.assert_eq(
            self.connect.sql(query).replace(2, 3).toPandas(),
            self.spark.sql(query).replace(2, 3).toPandas(),
        )
        self.assert_eq(
            self.connect.sql(query).na.replace(False, True).toPandas(),
            self.spark.sql(query).na.replace(False, True).toPandas(),
        )
        self.assert_eq(
            self.connect.sql(query).replace({1: 2, 3: -1}, subset=("a", "b")).toPandas(),
            self.spark.sql(query).replace({1: 2, 3: -1}, subset=("a", "b")).toPandas(),
        )
        self.assert_eq(
            self.connect.sql(query).na.replace((1, 2), (3, 1)).toPandas(),
            self.spark.sql(query).na.replace((1, 2), (3, 1)).toPandas(),
        )
        self.assert_eq(
            self.connect.sql(query).na.replace((1, 2), (3, 1), subset=("c", "b")).toPandas(),
            self.spark.sql(query).na.replace((1, 2), (3, 1), subset=("c", "b")).toPandas(),
        )

        with self.assertRaises(ValueError) as context:
            self.connect.sql(query).replace({None: 1}, subset="a").toPandas()
            self.assertTrue("Mixed type replacements are not supported" in str(context.exception))

        with self.assertRaises(AnalysisException) as context:
            self.connect.sql(query).replace({1: 2, 3: -1}, subset=("a", "x")).toPandas()
            self.assertIn(
                """Cannot resolve column name "x" among (a, b, c)""", str(context.exception)
            )

    def test_random_split(self):
        # SPARK-41440: test randomSplit(weights, seed).
        relations = (
            self.connect.read.table(self.tbl_name).filter("id > 3").randomSplit([1.0, 2.0, 3.0], 2)
        )
        datasets = (
            self.spark.read.table(self.tbl_name).filter("id > 3").randomSplit([1.0, 2.0, 3.0], 2)
        )

        self.assertTrue(len(relations) == len(datasets))
        i = 0
        while i < len(relations):
            self.assert_eq(relations[i].toPandas(), datasets[i].toPandas())
            i += 1

    def test_describe(self):
        # SPARK-41403: Test the describe method
        self.assert_eq(
            self.connect.read.table(self.tbl_name).describe("id").toPandas(),
            self.spark.read.table(self.tbl_name).describe("id").toPandas(),
        )
        self.assert_eq(
            self.connect.read.table(self.tbl_name).describe("id", "name").toPandas(),
            self.spark.read.table(self.tbl_name).describe("id", "name").toPandas(),
        )
        self.assert_eq(
            self.connect.read.table(self.tbl_name).describe(["id", "name"]).toPandas(),
            self.spark.read.table(self.tbl_name).describe(["id", "name"]).toPandas(),
        )

    def test_stat_cov(self):
        # SPARK-41067: Test the stat.cov method
        self.assertEqual(
            self.connect.read.table(self.tbl_name2).stat.cov("col1", "col3"),
            self.spark.read.table(self.tbl_name2).stat.cov("col1", "col3"),
        )

    def test_stat_corr(self):
        # SPARK-41068: Test the stat.corr method
        self.assertEqual(
            self.connect.read.table(self.tbl_name2).stat.corr("col1", "col3"),
            self.spark.read.table(self.tbl_name2).stat.corr("col1", "col3"),
        )

        self.assertEqual(
            self.connect.read.table(self.tbl_name2).stat.corr("col1", "col3", "pearson"),
            self.spark.read.table(self.tbl_name2).stat.corr("col1", "col3", "pearson"),
        )

        with self.assertRaises(PySparkTypeError) as pe:
            self.connect.read.table(self.tbl_name2).stat.corr(1, "col3", "pearson")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_STR",
            messageParameters={
                "arg_name": "col1",
                "arg_type": "int",
            },
        )

        with self.assertRaises(PySparkTypeError) as pe:
            self.connect.read.table(self.tbl_name).stat.corr("col1", 1, "pearson")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_STR",
            messageParameters={
                "arg_name": "col2",
                "arg_type": "int",
            },
        )
        with self.assertRaises(ValueError) as context:
            self.connect.read.table(self.tbl_name2).stat.corr("col1", "col3", "spearman"),
            self.assertTrue(
                "Currently only the calculation of the Pearson Correlation "
                + "coefficient is supported."
                in str(context.exception)
            )

    def test_stat_approx_quantile(self):
        # SPARK-41069: Test the stat.approxQuantile method
        result = self.connect.read.table(self.tbl_name2).stat.approxQuantile(
            ["col1", "col3"], [0.1, 0.5, 0.9], 0.1
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0]), 3)
        self.assertEqual(len(result[1]), 3)

        result = self.connect.read.table(self.tbl_name2).stat.approxQuantile(
            ["col1"], [0.1, 0.5, 0.9], 0.1
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]), 3)

        with self.assertRaises(PySparkTypeError) as pe:
            self.connect.read.table(self.tbl_name2).stat.approxQuantile(1, [0.1, 0.5, 0.9], 0.1)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_LIST_OR_STR_OR_TUPLE",
            messageParameters={
                "arg_name": "col",
                "arg_type": "int",
            },
        )

        with self.assertRaises(PySparkTypeError) as pe:
            self.connect.read.table(self.tbl_name2).stat.approxQuantile(["col1", "col3"], 0.1, 0.1)

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_LIST_OR_TUPLE",
            messageParameters={
                "arg_name": "probabilities",
                "arg_type": "float",
            },
        )
        with self.assertRaises(PySparkTypeError) as pe:
            self.connect.read.table(self.tbl_name2).stat.approxQuantile(
                ["col1", "col3"], [-0.1], 0.1
            )

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_LIST_OF_FLOAT_OR_INT",
            messageParameters={"arg_name": "probabilities", "arg_type": "float"},
        )
        with self.assertRaises(PySparkTypeError) as pe:
            self.connect.read.table(self.tbl_name2).stat.approxQuantile(
                ["col1", "col3"], [0.1, 0.5, 0.9], "str"
            )

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_FLOAT_OR_INT",
            messageParameters={
                "arg_name": "relativeError",
                "arg_type": "str",
            },
        )
        with self.assertRaises(PySparkValueError) as pe:
            self.connect.read.table(self.tbl_name2).stat.approxQuantile(
                ["col1", "col3"], [0.1, 0.5, 0.9], -0.1
            )

        self.check_error(
            exception=pe.exception,
            errorClass="NEGATIVE_VALUE",
            messageParameters={
                "arg_name": "relativeError",
                "arg_value": "-0.1",
            },
        )

    def test_stat_freq_items(self):
        # SPARK-41065: Test the stat.freqItems method
        self.assert_eq(
            self.connect.read.table(self.tbl_name2).stat.freqItems(["col1", "col3"]).toPandas(),
            self.spark.read.table(self.tbl_name2).stat.freqItems(["col1", "col3"]).toPandas(),
            check_exact=False,
        )

        self.assert_eq(
            self.connect.read.table(self.tbl_name2)
            .stat.freqItems(["col1", "col3"], 0.4)
            .toPandas(),
            self.spark.read.table(self.tbl_name2).stat.freqItems(["col1", "col3"], 0.4).toPandas(),
        )

        with self.assertRaises(PySparkTypeError) as pe:
            self.connect.read.table(self.tbl_name2).stat.freqItems("col1")

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_LIST_OR_TUPLE",
            messageParameters={
                "arg_name": "cols",
                "arg_type": "str",
            },
        )

    def test_stat_sample_by(self):
        # SPARK-41069: Test stat.sample_by

        cdf = self.connect.range(0, 100).select((CF.col("id") % 3).alias("key"))
        sdf = self.spark.range(0, 100).select((SF.col("id") % 3).alias("key"))

        self.assert_eq(
            cdf.sampleBy(cdf.key, fractions={0: 0.1, 1: 0.2}, seed=0)
            .groupBy("key")
            .agg(CF.count(CF.lit(1)))
            .orderBy("key")
            .toPandas(),
            sdf.sampleBy(sdf.key, fractions={0: 0.1, 1: 0.2}, seed=0)
            .groupBy("key")
            .agg(SF.count(SF.lit(1)))
            .orderBy("key")
            .toPandas(),
        )

        with self.assertRaises(PySparkTypeError) as pe:
            cdf.stat.sampleBy(cdf.key, fractions={0: 0.1, None: 0.2}, seed=0)

        self.check_error(
            exception=pe.exception,
            errorClass="DISALLOWED_TYPE_FOR_CONTAINER",
            messageParameters={
                "arg_name": "fractions",
                "arg_type": "dict",
                "allowed_types": "float, int, str",
                "item_type": "NoneType",
            },
        )

        with self.assertRaises(SparkConnectException):
            cdf.sampleBy(cdf.key, fractions={0: 0.1, 1: 1.2}, seed=0).show()

    def test_subtract(self):
        # SPARK-41453: test dataframe.subtract()
        ndf1 = self.connect.read.table(self.tbl_name)
        ndf2 = ndf1.filter("id > 3")
        df1 = self.spark.read.table(self.tbl_name)
        df2 = df1.filter("id > 3")

        self.assert_eq(
            ndf1.subtract(ndf2).toPandas(),
            df1.subtract(df2).toPandas(),
        )

    def test_agg_with_avg(self):
        # SPARK-41325: groupby.avg()
        df = (
            self.connect.range(10)
            .groupBy((CF.col("id") % CF.lit(2)).alias("moded"))
            .avg("id")
            .sort("moded")
        )
        res = df.collect()
        self.assertEqual(2, len(res))
        self.assertEqual(4.0, res[0][1])
        self.assertEqual(5.0, res[1][1])

        # Additional GroupBy tests with 3 rows

        df_a = self.connect.range(10).groupBy((CF.col("id") % CF.lit(3)).alias("moded"))
        df_b = self.spark.range(10).groupBy((SF.col("id") % SF.lit(3)).alias("moded"))
        self.assertEqual(
            set(df_b.agg(SF.sum("id")).collect()), set(df_a.agg(CF.sum("id")).collect())
        )

        # Dict agg
        measures = {"id": "sum"}
        self.assertEqual(
            set(df_a.agg(measures).select("sum(id)").collect()),
            set(df_b.agg(measures).select("sum(id)").collect()),
        )

    def test_agg_with_two_agg_exprs(self) -> None:
        # SPARK-41230: test dataframe.agg()
        self.assert_eq(
            self.connect.read.table(self.tbl_name).agg({"name": "min", "id": "max"}).toPandas(),
            self.spark.read.table(self.tbl_name).agg({"name": "min", "id": "max"}).toPandas(),
        )

    def test_grouped_data(self):
        query = """
            SELECT * FROM VALUES
                ('James', 'Sales', 3000, 2020),
                ('Michael', 'Sales', 4600, 2020),
                ('Robert', 'Sales', 4100, 2020),
                ('Maria', 'Finance', 3000, 2020),
                ('James', 'Sales', 3000, 2019),
                ('Scott', 'Finance', 3300, 2020),
                ('Jen', 'Finance', 3900, 2020),
                ('Jeff', 'Marketing', 3000, 2020),
                ('Kumar', 'Marketing', 2000, 2020),
                ('Saif', 'Sales', 4100, 2020)
            AS T(name, department, salary, year)
            """

        # +-------+----------+------+----+
        # |   name|department|salary|year|
        # +-------+----------+------+----+
        # |  James|     Sales|  3000|2020|
        # |Michael|     Sales|  4600|2020|
        # | Robert|     Sales|  4100|2020|
        # |  Maria|   Finance|  3000|2020|
        # |  James|     Sales|  3000|2019|
        # |  Scott|   Finance|  3300|2020|
        # |    Jen|   Finance|  3900|2020|
        # |   Jeff| Marketing|  3000|2020|
        # |  Kumar| Marketing|  2000|2020|
        # |   Saif|     Sales|  4100|2020|
        # +-------+----------+------+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        # test groupby
        self.assert_eq(
            cdf.groupBy("name").agg(CF.sum(cdf.salary)).toPandas(),
            sdf.groupBy("name").agg(SF.sum(sdf.salary)).toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("name", cdf.department).agg(CF.max("year"), CF.min(cdf.salary)).toPandas(),
            sdf.groupBy("name", sdf.department).agg(SF.max("year"), SF.min(sdf.salary)).toPandas(),
        )

        # test rollup
        self.assert_eq(
            cdf.rollup("name").agg(CF.sum(cdf.salary)).toPandas(),
            sdf.rollup("name").agg(SF.sum(sdf.salary)).toPandas(),
        )
        self.assert_eq(
            cdf.rollup("name", cdf.department).agg(CF.max("year"), CF.min(cdf.salary)).toPandas(),
            sdf.rollup("name", sdf.department).agg(SF.max("year"), SF.min(sdf.salary)).toPandas(),
        )

        # test cube
        self.assert_eq(
            cdf.cube("name").agg(CF.sum(cdf.salary)).toPandas(),
            sdf.cube("name").agg(SF.sum(sdf.salary)).toPandas(),
        )
        self.assert_eq(
            cdf.cube("name", cdf.department).agg(CF.max("year"), CF.min(cdf.salary)).toPandas(),
            sdf.cube("name", sdf.department).agg(SF.max("year"), SF.min(sdf.salary)).toPandas(),
        )

        # test pivot
        # pivot with values
        self.assert_eq(
            cdf.groupBy("name")
            .pivot("department", ["Sales", "Marketing"])
            .agg(CF.sum(cdf.salary))
            .toPandas(),
            sdf.groupBy("name")
            .pivot("department", ["Sales", "Marketing"])
            .agg(SF.sum(sdf.salary))
            .toPandas(),
        )
        self.assert_eq(
            cdf.groupBy(cdf.name)
            .pivot("department", ["Sales", "Finance", "Marketing"])
            .agg(CF.sum(cdf.salary))
            .toPandas(),
            sdf.groupBy(sdf.name)
            .pivot("department", ["Sales", "Finance", "Marketing"])
            .agg(SF.sum(sdf.salary))
            .toPandas(),
        )
        self.assert_eq(
            cdf.groupBy(cdf.name)
            .pivot("department", ["Sales", "Finance", "Unknown"])
            .agg(CF.sum(cdf.salary))
            .toPandas(),
            sdf.groupBy(sdf.name)
            .pivot("department", ["Sales", "Finance", "Unknown"])
            .agg(SF.sum(sdf.salary))
            .toPandas(),
        )

        # pivot without values
        self.assert_eq(
            cdf.groupBy("name").pivot("department").agg(CF.sum(cdf.salary)).toPandas(),
            sdf.groupBy("name").pivot("department").agg(SF.sum(sdf.salary)).toPandas(),
        )

        self.assert_eq(
            cdf.groupBy("name").pivot("year").agg(CF.sum(cdf.salary)).toPandas(),
            sdf.groupBy("name").pivot("year").agg(SF.sum(sdf.salary)).toPandas(),
        )

        # check error
        with self.assertRaisesRegex(
            Exception,
            "PIVOT after ROLLUP is not supported",
        ):
            cdf.rollup("name").pivot("department").agg(CF.sum(cdf.salary))

        with self.assertRaisesRegex(
            Exception,
            "PIVOT after CUBE is not supported",
        ):
            cdf.cube("name").pivot("department").agg(CF.sum(cdf.salary))

        with self.assertRaisesRegex(
            Exception,
            "Repeated PIVOT operation is not supported",
        ):
            cdf.groupBy("name").pivot("year").pivot("year").agg(CF.sum(cdf.salary))

        with self.assertRaises(PySparkTypeError) as pe:
            cdf.groupBy("name").pivot("department", ["Sales", b"Marketing"]).agg(CF.sum(cdf.salary))

        self.check_error(
            exception=pe.exception,
            errorClass="NOT_BOOL_OR_FLOAT_OR_INT_OR_STR",
            messageParameters={
                "arg_name": "value",
                "arg_type": "bytes",
            },
        )

    def test_numeric_aggregation(self):
        # SPARK-41737: test numeric aggregation
        query = """
                SELECT * FROM VALUES
                    ('James', 'Sales', 3000, 2020),
                    ('Michael', 'Sales', 4600, 2020),
                    ('Robert', 'Sales', 4100, 2020),
                    ('Maria', 'Finance', 3000, 2020),
                    ('James', 'Sales', 3000, 2019),
                    ('Scott', 'Finance', 3300, 2020),
                    ('Jen', 'Finance', 3900, 2020),
                    ('Jeff', 'Marketing', 3000, 2020),
                    ('Kumar', 'Marketing', 2000, 2020),
                    ('Saif', 'Sales', 4100, 2020)
                AS T(name, department, salary, year)
                """

        # +-------+----------+------+----+
        # |   name|department|salary|year|
        # +-------+----------+------+----+
        # |  James|     Sales|  3000|2020|
        # |Michael|     Sales|  4600|2020|
        # | Robert|     Sales|  4100|2020|
        # |  Maria|   Finance|  3000|2020|
        # |  James|     Sales|  3000|2019|
        # |  Scott|   Finance|  3300|2020|
        # |    Jen|   Finance|  3900|2020|
        # |   Jeff| Marketing|  3000|2020|
        # |  Kumar| Marketing|  2000|2020|
        # |   Saif|     Sales|  4100|2020|
        # +-------+----------+------+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        # test groupby
        self.assert_eq(
            cdf.groupBy("name").min().toPandas(),
            sdf.groupBy("name").min().toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("name").min("salary").toPandas(),
            sdf.groupBy("name").min("salary").toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("name").max("salary").toPandas(),
            sdf.groupBy("name").max("salary").toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("name", cdf.department).avg("salary", "year").toPandas(),
            sdf.groupBy("name", sdf.department).avg("salary", "year").toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("name", cdf.department).mean("salary", "year").toPandas(),
            sdf.groupBy("name", sdf.department).mean("salary", "year").toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("name", cdf.department).sum("salary", "year").toPandas(),
            sdf.groupBy("name", sdf.department).sum("salary", "year").toPandas(),
        )

        # test rollup
        self.assert_eq(
            cdf.rollup("name").max().toPandas(),
            sdf.rollup("name").max().toPandas(),
        )
        self.assert_eq(
            cdf.rollup("name").min("salary").toPandas(),
            sdf.rollup("name").min("salary").toPandas(),
        )
        self.assert_eq(
            cdf.rollup("name").max("salary").toPandas(),
            sdf.rollup("name").max("salary").toPandas(),
        )
        self.assert_eq(
            cdf.rollup("name", cdf.department).avg("salary", "year").toPandas(),
            sdf.rollup("name", sdf.department).avg("salary", "year").toPandas(),
        )
        self.assert_eq(
            cdf.rollup("name", cdf.department).mean("salary", "year").toPandas(),
            sdf.rollup("name", sdf.department).mean("salary", "year").toPandas(),
        )
        self.assert_eq(
            cdf.rollup("name", cdf.department).sum("salary", "year").toPandas(),
            sdf.rollup("name", sdf.department).sum("salary", "year").toPandas(),
        )

        # test cube
        self.assert_eq(
            cdf.cube("name").avg().toPandas(),
            sdf.cube("name").avg().toPandas(),
        )
        self.assert_eq(
            cdf.cube("name").mean().toPandas(),
            sdf.cube("name").mean().toPandas(),
        )
        self.assert_eq(
            cdf.cube("name").min("salary").toPandas(),
            sdf.cube("name").min("salary").toPandas(),
        )
        self.assert_eq(
            cdf.cube("name").max("salary").toPandas(),
            sdf.cube("name").max("salary").toPandas(),
        )
        self.assert_eq(
            cdf.cube("name", cdf.department).avg("salary", "year").toPandas(),
            sdf.cube("name", sdf.department).avg("salary", "year").toPandas(),
        )
        self.assert_eq(
            cdf.cube("name", cdf.department).sum("salary", "year").toPandas(),
            sdf.cube("name", sdf.department).sum("salary", "year").toPandas(),
        )

        # test pivot
        # pivot with values
        self.assert_eq(
            cdf.groupBy("name").pivot("department", ["Sales", "Marketing"]).sum().toPandas(),
            sdf.groupBy("name").pivot("department", ["Sales", "Marketing"]).sum().toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("name")
            .pivot("department", ["Sales", "Marketing"])
            .min("salary")
            .toPandas(),
            sdf.groupBy("name")
            .pivot("department", ["Sales", "Marketing"])
            .min("salary")
            .toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("name")
            .pivot("department", ["Sales", "Marketing"])
            .max("salary")
            .toPandas(),
            sdf.groupBy("name")
            .pivot("department", ["Sales", "Marketing"])
            .max("salary")
            .toPandas(),
        )
        self.assert_eq(
            cdf.groupBy(cdf.name)
            .pivot("department", ["Sales", "Finance", "Unknown"])
            .avg("salary", "year")
            .toPandas(),
            sdf.groupBy(sdf.name)
            .pivot("department", ["Sales", "Finance", "Unknown"])
            .avg("salary", "year")
            .toPandas(),
        )
        self.assert_eq(
            cdf.groupBy(cdf.name)
            .pivot("department", ["Sales", "Finance", "Unknown"])
            .sum("salary", "year")
            .toPandas(),
            sdf.groupBy(sdf.name)
            .pivot("department", ["Sales", "Finance", "Unknown"])
            .sum("salary", "year")
            .toPandas(),
        )

        # pivot without values
        self.assert_eq(
            cdf.groupBy("name").pivot("department").min().toPandas(),
            sdf.groupBy("name").pivot("department").min().toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("name").pivot("department").min("salary").toPandas(),
            sdf.groupBy("name").pivot("department").min("salary").toPandas(),
        )
        self.assert_eq(
            cdf.groupBy("name").pivot("department").max("salary").toPandas(),
            sdf.groupBy("name").pivot("department").max("salary").toPandas(),
        )
        self.assert_eq(
            cdf.groupBy(cdf.name).pivot("department").avg("salary", "year").toPandas(),
            sdf.groupBy(sdf.name).pivot("department").avg("salary", "year").toPandas(),
        )
        self.assert_eq(
            cdf.groupBy(cdf.name).pivot("department").sum("salary", "year").toPandas(),
            sdf.groupBy(sdf.name).pivot("department").sum("salary", "year").toPandas(),
        )

        # check error
        with self.assertRaisesRegex(
            TypeError,
            "Numeric aggregation function can only be applied on numeric columns",
        ):
            cdf.groupBy("name").min("department").show()

        with self.assertRaisesRegex(
            TypeError,
            "Numeric aggregation function can only be applied on numeric columns",
        ):
            cdf.groupBy("name").max("salary", "department").show()

        with self.assertRaisesRegex(
            TypeError,
            "Numeric aggregation function can only be applied on numeric columns",
        ):
            cdf.rollup("name").avg("department").show()

        with self.assertRaisesRegex(
            TypeError,
            "Numeric aggregation function can only be applied on numeric columns",
        ):
            cdf.rollup("name").sum("salary", "department").show()

        with self.assertRaisesRegex(
            TypeError,
            "Numeric aggregation function can only be applied on numeric columns",
        ):
            cdf.cube("name").min("department").show()

        with self.assertRaisesRegex(
            TypeError,
            "Numeric aggregation function can only be applied on numeric columns",
        ):
            cdf.cube("name").max("salary", "department").show()

        with self.assertRaisesRegex(
            TypeError,
            "Numeric aggregation function can only be applied on numeric columns",
        ):
            cdf.groupBy("name").pivot("department").avg("department").show()

        with self.assertRaisesRegex(
            TypeError,
            "Numeric aggregation function can only be applied on numeric columns",
        ):
            cdf.groupBy("name").pivot("department").sum("salary", "department").show()

    def test_unpivot(self):
        self.assert_eq(
            self.connect.read.table(self.tbl_name)
            .filter("id > 3")
            .unpivot(["id"], ["name"], "variable", "value")
            .toPandas(),
            self.spark.read.table(self.tbl_name)
            .filter("id > 3")
            .unpivot(["id"], ["name"], "variable", "value")
            .toPandas(),
        )

        self.assert_eq(
            self.connect.read.table(self.tbl_name)
            .filter("id > 3")
            .unpivot("id", None, "variable", "value")
            .toPandas(),
            self.spark.read.table(self.tbl_name)
            .filter("id > 3")
            .unpivot("id", None, "variable", "value")
            .toPandas(),
        )


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_stat import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
