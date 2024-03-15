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

import os
import unittest
import shutil
import tempfile

from pyspark.errors import (
    PySparkAttributeError,
    PySparkTypeError,
    PySparkValueError,
)
from pyspark.errors.exceptions.base import SessionNotSameException
from pyspark.sql import SparkSession as PySparkSession, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    IntegerType,
    MapType,
    ArrayType,
    Row,
)

from pyspark.testing.sqlutils import (
    SQLTestUtils,
    PythonOnlyUDT,
    ExamplePoint,
    PythonOnlyPoint,
)
from pyspark.testing.connectutils import (
    should_test_connect,
    ReusedConnectTestCase,
)
from pyspark.testing.pandasutils import PandasOnSparkTestUtils
from pyspark.errors.exceptions.connect import (
    AnalysisException,
    SparkConnectException,
)

if should_test_connect:
    from pyspark.sql.connect.proto import Expression as ProtoExpression
    from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
    from pyspark.sql.connect.column import Column
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.connect.dataframe import DataFrame as CDataFrame
    from pyspark.sql import functions as SF
    from pyspark.sql.connect import functions as CF


class SparkConnectSQLTestCase(ReusedConnectTestCase, SQLTestUtils, PandasOnSparkTestUtils):
    """Parent test fixture class for all Spark Connect related
    test cases."""

    @classmethod
    def setUpClass(cls):
        super(SparkConnectSQLTestCase, cls).setUpClass()
        # Disable the shared namespace so pyspark.sql.functions, etc point the regular
        # PySpark libraries.
        os.environ["PYSPARK_NO_NAMESPACE_SHARE"] = "1"

        cls.connect = cls.spark  # Switch Spark Connect session and regular PySpark session.
        cls.spark = PySparkSession._instantiatedSession
        assert cls.spark is not None

        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.testDataStr = [Row(key=str(i)) for i in range(100)]
        cls.df = cls.spark.sparkContext.parallelize(cls.testData).toDF()
        cls.df_text = cls.spark.sparkContext.parallelize(cls.testDataStr).toDF()

        cls.tbl_name = "test_connect_basic_table_1"
        cls.tbl_name2 = "test_connect_basic_table_2"
        cls.tbl_name3 = "test_connect_basic_table_3"
        cls.tbl_name4 = "test_connect_basic_table_4"
        cls.tbl_name_empty = "test_connect_basic_table_empty"

        # Cleanup test data
        cls.spark_connect_clean_up_test_data()
        # Load test data
        cls.spark_connect_load_test_data()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark_connect_clean_up_test_data()
            # Stopping Spark Connect closes the session in JVM at the server.
            cls.spark = cls.connect
            del os.environ["PYSPARK_NO_NAMESPACE_SHARE"]
        finally:
            super(SparkConnectSQLTestCase, cls).tearDownClass()

    @classmethod
    def spark_connect_load_test_data(cls):
        df = cls.spark.createDataFrame([(x, f"{x}") for x in range(100)], ["id", "name"])
        # Since we might create multiple Spark sessions, we need to create global temporary view
        # that is specifically maintained in the "global_temp" schema.
        df.write.saveAsTable(cls.tbl_name)
        df2 = cls.spark.createDataFrame(
            [(x, f"{x}", 2 * x) for x in range(100)], ["col1", "col2", "col3"]
        )
        df2.write.saveAsTable(cls.tbl_name2)
        df3 = cls.spark.createDataFrame([(x, f"{x}") for x in range(100)], ["id", "test\n_column"])
        df3.write.saveAsTable(cls.tbl_name3)
        df4 = cls.spark.createDataFrame(
            [(x, {"a": x}, [x, x * 2]) for x in range(100)], ["id", "map_column", "array_column"]
        )
        df4.write.saveAsTable(cls.tbl_name4)
        empty_table_schema = StructType(
            [
                StructField("firstname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True),
            ]
        )
        emptyRDD = cls.spark.sparkContext.emptyRDD()
        empty_df = cls.spark.createDataFrame(emptyRDD, empty_table_schema)
        empty_df.write.saveAsTable(cls.tbl_name_empty)

    @classmethod
    def spark_connect_clean_up_test_data(cls):
        cls.spark.sql("DROP TABLE IF EXISTS {}".format(cls.tbl_name))
        cls.spark.sql("DROP TABLE IF EXISTS {}".format(cls.tbl_name2))
        cls.spark.sql("DROP TABLE IF EXISTS {}".format(cls.tbl_name3))
        cls.spark.sql("DROP TABLE IF EXISTS {}".format(cls.tbl_name4))
        cls.spark.sql("DROP TABLE IF EXISTS {}".format(cls.tbl_name_empty))


class SparkConnectBasicTests(SparkConnectSQLTestCase):
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

    def test_df_getattr_behavior(self):
        cdf = self.connect.range(10)
        sdf = self.spark.range(10)

        sdf._simple_extension = 10
        cdf._simple_extension = 10

        self.assertEqual(sdf._simple_extension, cdf._simple_extension)
        self.assertEqual(type(sdf._simple_extension), type(cdf._simple_extension))

        self.assertTrue(hasattr(cdf, "_simple_extension"))
        self.assertFalse(hasattr(cdf, "_simple_extension_does_not_exsit"))

    def test_df_get_item(self):
        # SPARK-41779: test __getitem__

        query = """
            SELECT * FROM VALUES
            (true, 1, NULL), (false, NULL, 2.0), (NULL, 3, 3.0)
            AS tab(a, b, c)
            """

        # +-----+----+----+
        # |    a|   b|   c|
        # +-----+----+----+
        # | true|   1|NULL|
        # |false|NULL| 2.0|
        # | NULL|   3| 3.0|
        # +-----+----+----+

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)

        # filter
        self.assert_eq(
            cdf[cdf.a].toPandas(),
            sdf[sdf.a].toPandas(),
        )
        self.assert_eq(
            cdf[cdf.b.isin(2, 3)].toPandas(),
            sdf[sdf.b.isin(2, 3)].toPandas(),
        )
        self.assert_eq(
            cdf[cdf.c > 1.5].toPandas(),
            sdf[sdf.c > 1.5].toPandas(),
        )

        # select
        self.assert_eq(
            cdf[[cdf.a, "b", cdf.c]].toPandas(),
            sdf[[sdf.a, "b", sdf.c]].toPandas(),
        )
        self.assert_eq(
            cdf[(cdf.a, "b", cdf.c)].toPandas(),
            sdf[(sdf.a, "b", sdf.c)].toPandas(),
        )

        # select by index
        self.assertTrue(isinstance(cdf[0], Column))
        self.assertTrue(isinstance(cdf[1], Column))
        self.assertTrue(isinstance(cdf[2], Column))

        self.assert_eq(
            cdf[[cdf[0], cdf[1], cdf[2]]].toPandas(),
            sdf[[sdf[0], sdf[1], sdf[2]]].toPandas(),
        )

        # check error
        with self.assertRaises(PySparkTypeError) as pe:
            cdf[1.5]

        self.check_error(
            exception=pe.exception,
            error_class="NOT_COLUMN_OR_INT_OR_LIST_OR_STR_OR_TUPLE",
            message_parameters={
                "arg_name": "item",
                "arg_type": "float",
            },
        )

        with self.assertRaises(PySparkTypeError) as pe:
            cdf[None]

        self.check_error(
            exception=pe.exception,
            error_class="NOT_COLUMN_OR_INT_OR_LIST_OR_STR_OR_TUPLE",
            message_parameters={
                "arg_name": "item",
                "arg_type": "NoneType",
            },
        )

        with self.assertRaises(PySparkTypeError) as pe:
            cdf[cdf]

        self.check_error(
            exception=pe.exception,
            error_class="NOT_COLUMN_OR_INT_OR_LIST_OR_STR_OR_TUPLE",
            message_parameters={
                "arg_name": "item",
                "arg_type": "DataFrame",
            },
        )

    def test_error_handling(self):
        # SPARK-41533 Proper error handling for Spark Connect
        df = self.connect.range(10).select("id2")
        with self.assertRaises(AnalysisException):
            df.collect()

    def test_join_condition_column_list_columns(self):
        left_connect_df = self.connect.read.table(self.tbl_name)
        right_connect_df = self.connect.read.table(self.tbl_name2)
        left_spark_df = self.spark.read.table(self.tbl_name)
        right_spark_df = self.spark.read.table(self.tbl_name2)
        joined_plan = left_connect_df.join(
            other=right_connect_df, on=left_connect_df.id == right_connect_df.col1, how="inner"
        )
        joined_plan2 = left_spark_df.join(
            other=right_spark_df, on=left_spark_df.id == right_spark_df.col1, how="inner"
        )
        self.assert_eq(joined_plan.toPandas(), joined_plan2.toPandas())

        joined_plan3 = left_connect_df.join(
            other=right_connect_df,
            on=[
                left_connect_df.id == right_connect_df.col1,
                left_connect_df.name == right_connect_df.col2,
            ],
            how="inner",
        )
        joined_plan4 = left_spark_df.join(
            other=right_spark_df,
            on=[left_spark_df.id == right_spark_df.col1, left_spark_df.name == right_spark_df.col2],
            how="inner",
        )
        self.assert_eq(joined_plan3.toPandas(), joined_plan4.toPandas())

    def test_join_ambiguous_cols(self):
        # SPARK-41812: test join with ambiguous columns
        data1 = [Row(id=1, value="foo"), Row(id=2, value=None)]
        cdf1 = self.connect.createDataFrame(data1)
        sdf1 = self.spark.createDataFrame(data1)

        data2 = [Row(value="bar"), Row(value=None), Row(value="foo")]
        cdf2 = self.connect.createDataFrame(data2)
        sdf2 = self.spark.createDataFrame(data2)

        cdf3 = cdf1.join(cdf2, cdf1["value"] == cdf2["value"])
        sdf3 = sdf1.join(sdf2, sdf1["value"] == sdf2["value"])

        self.assertEqual(cdf3.schema, sdf3.schema)
        self.assertEqual(cdf3.collect(), sdf3.collect())

        cdf4 = cdf1.join(cdf2, cdf1["value"].eqNullSafe(cdf2["value"]))
        sdf4 = sdf1.join(sdf2, sdf1["value"].eqNullSafe(sdf2["value"]))

        self.assertEqual(cdf4.schema, sdf4.schema)
        self.assertEqual(cdf4.collect(), sdf4.collect())

        cdf5 = cdf1.join(
            cdf2, (cdf1["value"] == cdf2["value"]) & (cdf1["value"].eqNullSafe(cdf2["value"]))
        )
        sdf5 = sdf1.join(
            sdf2, (sdf1["value"] == sdf2["value"]) & (sdf1["value"].eqNullSafe(sdf2["value"]))
        )

        self.assertEqual(cdf5.schema, sdf5.schema)
        self.assertEqual(cdf5.collect(), sdf5.collect())

        cdf6 = cdf1.join(cdf2, cdf1["value"] == cdf2["value"]).select(cdf1.value)
        sdf6 = sdf1.join(sdf2, sdf1["value"] == sdf2["value"]).select(sdf1.value)

        self.assertEqual(cdf6.schema, sdf6.schema)
        self.assertEqual(cdf6.collect(), sdf6.collect())

        cdf7 = cdf1.join(cdf2, cdf1["value"] == cdf2["value"]).select(cdf2.value)
        sdf7 = sdf1.join(sdf2, sdf1["value"] == sdf2["value"]).select(sdf2.value)

        self.assertEqual(cdf7.schema, sdf7.schema)
        self.assertEqual(cdf7.collect(), sdf7.collect())

    def test_join_with_cte(self):
        cte_query = "with dt as (select 1 as ida) select ida as id from dt"

        sdf1 = self.spark.range(10)
        sdf2 = self.spark.sql(cte_query)
        sdf3 = sdf1.join(sdf2, sdf1.id == sdf2.id)

        cdf1 = self.connect.range(10)
        cdf2 = self.connect.sql(cte_query)
        cdf3 = cdf1.join(cdf2, cdf1.id == cdf2.id)

        self.assertEqual(sdf3.schema, cdf3.schema)
        self.assertEqual(sdf3.collect(), cdf3.collect())

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

    def test_with_columns_renamed(self):
        # SPARK-41312: test DataFrame.withColumnsRenamed()
        self.assertEqual(
            self.connect.read.table(self.tbl_name).withColumnRenamed("id", "id_new").schema,
            self.spark.read.table(self.tbl_name).withColumnRenamed("id", "id_new").schema,
        )
        self.assertEqual(
            self.connect.read.table(self.tbl_name)
            .withColumnsRenamed({"id": "id_new", "name": "name_new"})
            .schema,
            self.spark.read.table(self.tbl_name)
            .withColumnsRenamed({"id": "id_new", "name": "name_new"})
            .schema,
        )

    def test_simple_explain_string(self):
        df = self.connect.read.table(self.tbl_name).limit(10)
        result = df._explain_string()
        self.assertGreater(len(result), 0)

    def test_schema(self):
        schema = self.connect.read.table(self.tbl_name).schema
        self.assertEqual(
            StructType(
                [StructField("id", LongType(), True), StructField("name", StringType(), True)]
            ),
            schema,
        )

        # test FloatType, DoubleType, DecimalType, StringType, BooleanType, NullType
        query = """
            SELECT * FROM VALUES
            (float(1.0), double(1.0), 1.0, "1", true, NULL),
            (float(2.0), double(2.0), 2.0, "2", false, NULL),
            (float(3.0), double(3.0), NULL, "3", false, NULL)
            AS tab(a, b, c, d, e, f)
            """
        self.assertEqual(
            self.spark.sql(query).schema,
            self.connect.sql(query).schema,
        )

        # test TimestampType, DateType
        query = """
            SELECT * FROM VALUES
            (TIMESTAMP('2019-04-12 15:50:00'), DATE('2022-02-22')),
            (TIMESTAMP('2019-04-12 15:50:00'), NULL),
            (NULL, DATE('2022-02-22'))
            AS tab(a, b)
            """
        self.assertEqual(
            self.spark.sql(query).schema,
            self.connect.sql(query).schema,
        )

        # test DayTimeIntervalType
        query = """ SELECT INTERVAL '100 10:30' DAY TO MINUTE AS interval """
        self.assertEqual(
            self.spark.sql(query).schema,
            self.connect.sql(query).schema,
        )

        # test MapType
        query = """
            SELECT * FROM VALUES
            (MAP('a', 'ab'), MAP('a', 'ab'), MAP(1, 2, 3, 4)),
            (MAP('x', 'yz'), MAP('x', NULL), NULL),
            (MAP('c', 'de'), NULL, MAP(-1, NULL, -3, -4))
            AS tab(a, b, c)
            """
        self.assertEqual(
            self.spark.sql(query).schema,
            self.connect.sql(query).schema,
        )

        # test ArrayType
        query = """
            SELECT * FROM VALUES
            (ARRAY('a', 'ab'), ARRAY(1, 2, 3), ARRAY(1, NULL, 3)),
            (ARRAY('x', NULL), NULL, ARRAY(1, 3)),
            (NULL, ARRAY(-1, -2, -3), Array())
            AS tab(a, b, c)
            """
        self.assertEqual(
            self.spark.sql(query).schema,
            self.connect.sql(query).schema,
        )

        # test StructType
        query = """
            SELECT STRUCT(a, b, c, d), STRUCT(e, f, g), STRUCT(STRUCT(a, b), STRUCT(h)) FROM VALUES
            (float(1.0), double(1.0), 1.0, "1", true, NULL, ARRAY(1, NULL, 3), MAP(1, 2, 3, 4)),
            (float(2.0), double(2.0), 2.0, "2", false, NULL, ARRAY(1, 3), MAP(1, NULL, 3, 4)),
            (float(3.0), double(3.0), NULL, "3", false, NULL, ARRAY(NULL), NULL)
            AS tab(a, b, c, d, e, f, g, h)
            """
        self.assertEqual(
            self.spark.sql(query).schema,
            self.connect.sql(query).schema,
        )

    def test_to(self):
        # SPARK-41464: test DataFrame.to()

        cdf = self.connect.read.table(self.tbl_name)
        df = self.spark.read.table(self.tbl_name)

        def assert_eq_schema(cdf: CDataFrame, df: DataFrame, schema: StructType):
            cdf_to = cdf.to(schema)
            df_to = df.to(schema)
            self.assertEqual(cdf_to.schema, df_to.schema)
            self.assert_eq(cdf_to.toPandas(), df_to.toPandas())

        # The schema has not changed
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        assert_eq_schema(cdf, df, schema)

        # Change schema with struct
        schema2 = StructType([StructField("struct", schema, False)])

        cdf_to = cdf.select(CF.struct("id", "name").alias("struct")).to(schema2)
        df_to = df.select(SF.struct("id", "name").alias("struct")).to(schema2)

        self.assertEqual(cdf_to.schema, df_to.schema)

        # Change the column name
        schema = StructType(
            [
                StructField("col1", IntegerType(), True),
                StructField("col2", StringType(), True),
            ]
        )

        assert_eq_schema(cdf, df, schema)

        # Change the column data type
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )

        assert_eq_schema(cdf, df, schema)

        # Reduce the column quantity and change data type
        schema = StructType(
            [
                StructField("id", LongType(), True),
            ]
        )

        assert_eq_schema(cdf, df, schema)

        # incompatible field nullability
        schema = StructType([StructField("id", LongType(), False)])
        self.assertRaisesRegex(
            AnalysisException,
            "NULLABLE_COLUMN_OR_FIELD",
            lambda: cdf.to(schema).toPandas(),
        )

        # field cannot upcast
        schema = StructType([StructField("name", LongType())])
        self.assertRaisesRegex(
            AnalysisException,
            "INVALID_COLUMN_OR_FIELD_DATA_TYPE",
            lambda: cdf.to(schema).toPandas(),
        )

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", IntegerType(), True),
            ]
        )
        self.assertRaisesRegex(
            AnalysisException,
            "INVALID_COLUMN_OR_FIELD_DATA_TYPE",
            lambda: cdf.to(schema).toPandas(),
        )

        # Test map type and array type
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("my_map", MapType(StringType(), IntegerType(), False), True),
                StructField("my_array", ArrayType(IntegerType(), False), True),
            ]
        )
        cdf = self.connect.read.table(self.tbl_name4)
        df = self.spark.read.table(self.tbl_name4)

        assert_eq_schema(cdf, df, schema)

    def test_toDF(self):
        # SPARK-41310: test DataFrame.toDF()
        self.assertEqual(
            self.connect.read.table(self.tbl_name).toDF("col1", "col2").schema,
            self.spark.read.table(self.tbl_name).toDF("col1", "col2").schema,
        )

    def test_print_schema(self):
        # SPARK-41216: Test print schema
        tree_str = self.connect.sql("SELECT 1 AS X, 2 AS Y")._tree_string()
        # root
        #  |-- X: integer (nullable = false)
        #  |-- Y: integer (nullable = false)
        expected = "root\n |-- X: integer (nullable = false)\n |-- Y: integer (nullable = false)\n"
        self.assertEqual(tree_str, expected)

    def test_is_local(self):
        # SPARK-41216: Test is local
        self.assertTrue(self.connect.sql("SHOW DATABASES").isLocal())
        self.assertFalse(self.connect.read.table(self.tbl_name).isLocal())

    def test_is_streaming(self):
        # SPARK-41216: Test is streaming
        self.assertFalse(self.connect.read.table(self.tbl_name).isStreaming)
        self.assertFalse(self.connect.sql("SELECT 1 AS X LIMIT 0").isStreaming)

    def test_input_files(self):
        # SPARK-41216: Test input files
        tmpPath = tempfile.mkdtemp()
        shutil.rmtree(tmpPath)
        try:
            self.df_text.write.text(tmpPath)

            input_files_list1 = (
                self.spark.read.format("text").schema("id STRING").load(path=tmpPath).inputFiles()
            )
            input_files_list2 = (
                self.connect.read.format("text").schema("id STRING").load(path=tmpPath).inputFiles()
            )

            self.assertTrue(len(input_files_list1) > 0)
            self.assertEqual(len(input_files_list1), len(input_files_list2))
            for file_path in input_files_list2:
                self.assertTrue(file_path in input_files_list1)
        finally:
            shutil.rmtree(tmpPath)

    def test_limit_offset(self):
        df = self.connect.read.table(self.tbl_name)
        pd = df.limit(10).offset(1).toPandas()
        self.assertEqual(9, len(pd.index))
        pd2 = df.offset(98).limit(10).toPandas()
        self.assertEqual(2, len(pd2.index))

    def test_tail(self):
        df = self.connect.read.table(self.tbl_name)
        df2 = self.spark.read.table(self.tbl_name)
        self.assertEqual(df.tail(10), df2.tail(10))

    def test_sql(self):
        pdf = self.connect.sql("SELECT 1").toPandas()
        self.assertEqual(1, len(pdf.index))

    def test_sql_with_named_args(self):
        sqlText = "SELECT *, element_at(:m, 'a') FROM range(10) WHERE id > :minId"
        df = self.connect.sql(
            sqlText, args={"minId": 7, "m": CF.create_map(CF.lit("a"), CF.lit(1))}
        )
        df2 = self.spark.sql(sqlText, args={"minId": 7, "m": SF.create_map(SF.lit("a"), SF.lit(1))})
        self.assert_eq(df.toPandas(), df2.toPandas())

    def test_sql_with_pos_args(self):
        sqlText = "SELECT *, element_at(?, 1) FROM range(10) WHERE id > ?"
        df = self.connect.sql(sqlText, args=[CF.array(CF.lit(1)), 7])
        df2 = self.spark.sql(sqlText, args=[SF.array(SF.lit(1)), 7])
        self.assert_eq(df.toPandas(), df2.toPandas())

    def test_sql_with_invalid_args(self):
        sqlText = "SELECT ?, ?, ?"
        for session in [self.connect, self.spark]:
            with self.assertRaises(PySparkTypeError) as pe:
                session.sql(sqlText, args={1, 2, 3})

            self.check_error(
                exception=pe.exception,
                error_class="INVALID_TYPE",
                message_parameters={"arg_name": "args", "arg_type": "set"},
            )

    def test_deduplicate(self):
        # SPARK-41326: test distinct and dropDuplicates.
        df = self.connect.read.table(self.tbl_name)
        df2 = self.spark.read.table(self.tbl_name)
        self.assert_eq(df.distinct().toPandas(), df2.distinct().toPandas())
        self.assert_eq(df.dropDuplicates().toPandas(), df2.dropDuplicates().toPandas())
        self.assert_eq(
            df.dropDuplicates(["name"]).toPandas(), df2.dropDuplicates(["name"]).toPandas()
        )

    def test_deduplicate_within_watermark_in_batch(self):
        df = self.connect.read.table(self.tbl_name)
        with self.assertRaisesRegex(
            AnalysisException,
            "dropDuplicatesWithinWatermark is not supported with batch DataFrames/DataSets",
        ):
            df.dropDuplicatesWithinWatermark().toPandas()

    def test_drop(self):
        # SPARK-41169: test drop
        query = """
            SELECT * FROM VALUES
            (false, 1, NULL), (false, NULL, 2), (NULL, 3, 3)
            AS tab(a, b, c)
            """

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)
        self.assert_eq(
            cdf.drop("a").toPandas(),
            sdf.drop("a").toPandas(),
        )
        self.assert_eq(
            cdf.drop("a", "b").toPandas(),
            sdf.drop("a", "b").toPandas(),
        )
        self.assert_eq(
            cdf.drop("a", "x").toPandas(),
            sdf.drop("a", "x").toPandas(),
        )
        self.assert_eq(
            cdf.drop(cdf.a, "x").toPandas(),
            sdf.drop(sdf.a, "x").toPandas(),
        )

    def test_subquery_alias(self) -> None:
        # SPARK-40938: test subquery alias.
        plan_text = (
            self.connect.read.table(self.tbl_name)
            .alias("special_alias")
            ._explain_string(extended=True)
        )
        self.assertTrue("special_alias" in plan_text)

    def test_sort(self):
        # SPARK-41332: test sort
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

        cdf = self.connect.sql(query)
        sdf = self.spark.sql(query)
        self.assert_eq(
            cdf.sort("a").toPandas(),
            sdf.sort("a").toPandas(),
        )
        self.assert_eq(
            cdf.sort("c").toPandas(),
            sdf.sort("c").toPandas(),
        )
        self.assert_eq(
            cdf.sort("b").toPandas(),
            sdf.sort("b").toPandas(),
        )
        self.assert_eq(
            cdf.sort(cdf.c, "b").toPandas(),
            sdf.sort(sdf.c, "b").toPandas(),
        )
        self.assert_eq(
            cdf.sort(cdf.c.desc(), "b").toPandas(),
            sdf.sort(sdf.c.desc(), "b").toPandas(),
        )
        self.assert_eq(
            cdf.sort(cdf.c.desc(), cdf.a.asc()).toPandas(),
            sdf.sort(sdf.c.desc(), sdf.a.asc()).toPandas(),
        )

    def test_range(self):
        self.assert_eq(
            self.connect.range(start=0, end=10).toPandas(),
            self.spark.range(start=0, end=10).toPandas(),
        )
        self.assert_eq(
            self.connect.range(start=0, end=10, step=3).toPandas(),
            self.spark.range(start=0, end=10, step=3).toPandas(),
        )
        self.assert_eq(
            self.connect.range(start=0, end=10, step=3, numPartitions=2).toPandas(),
            self.spark.range(start=0, end=10, step=3, numPartitions=2).toPandas(),
        )
        # SPARK-41301
        self.assert_eq(
            self.connect.range(10).toPandas(), self.connect.range(start=0, end=10).toPandas()
        )

    def test_create_global_temp_view(self):
        # SPARK-41127: test global temp view creation.
        with self.tempView("view_1"):
            self.connect.sql("SELECT 1 AS X LIMIT 0").createGlobalTempView("view_1")
            self.connect.sql("SELECT 2 AS X LIMIT 1").createOrReplaceGlobalTempView("view_1")
            self.assertTrue(self.spark.catalog.tableExists("global_temp.view_1"))

            # Test when creating a view which is already exists but
            self.assertTrue(self.spark.catalog.tableExists("global_temp.view_1"))
            with self.assertRaises(AnalysisException):
                self.connect.sql("SELECT 1 AS X LIMIT 0").createGlobalTempView("view_1")

    def test_create_session_local_temp_view(self):
        # SPARK-41372: test session local temp view creation.
        with self.tempView("view_local_temp"):
            self.connect.sql("SELECT 1 AS X").createTempView("view_local_temp")
            self.assertEqual(self.connect.sql("SELECT * FROM view_local_temp").count(), 1)
            self.connect.sql("SELECT 1 AS X LIMIT 0").createOrReplaceTempView("view_local_temp")
            self.assertEqual(self.connect.sql("SELECT * FROM view_local_temp").count(), 0)

            # Test when creating a view which is already exists but
            with self.assertRaises(AnalysisException):
                self.connect.sql("SELECT 1 AS X LIMIT 0").createTempView("view_local_temp")

    def test_select_expr(self):
        # SPARK-41201: test selectExpr API.
        self.assert_eq(
            self.connect.read.table(self.tbl_name).selectExpr("id * 2").toPandas(),
            self.spark.read.table(self.tbl_name).selectExpr("id * 2").toPandas(),
        )
        self.assert_eq(
            self.connect.read.table(self.tbl_name)
            .selectExpr(["id * 2", "cast(name as long) as name"])
            .toPandas(),
            self.spark.read.table(self.tbl_name)
            .selectExpr(["id * 2", "cast(name as long) as name"])
            .toPandas(),
        )

        self.assert_eq(
            self.connect.read.table(self.tbl_name)
            .selectExpr("id * 2", "cast(name as long) as name")
            .toPandas(),
            self.spark.read.table(self.tbl_name)
            .selectExpr("id * 2", "cast(name as long) as name")
            .toPandas(),
        )

    def test_select_star(self):
        data = [Row(a=1, b=Row(c=2, d=Row(e=3)))]

        # +---+--------+
        # |  a|       b|
        # +---+--------+
        # |  1|{2, {3}}|
        # +---+--------+

        cdf = self.connect.createDataFrame(data=data)
        sdf = self.spark.createDataFrame(data=data)

        self.assertEqual(
            cdf.select("*").collect(),
            sdf.select("*").collect(),
        )
        self.assertEqual(
            cdf.select("a", "*").collect(),
            sdf.select("a", "*").collect(),
        )
        self.assertEqual(
            cdf.select("a", "b").collect(),
            sdf.select("a", "b").collect(),
        )
        self.assertEqual(
            cdf.select("a", "b.*").collect(),
            sdf.select("a", "b.*").collect(),
        )

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

    def test_union_by_name(self):
        # SPARK-41832: Test unionByName
        data1 = [(1, 2, 3)]
        data2 = [(6, 2, 5)]
        df1_connect = self.connect.createDataFrame(data1, ["a", "b", "c"])
        df2_connect = self.connect.createDataFrame(data2, ["a", "b", "c"])
        union_df_connect = df1_connect.unionByName(df2_connect)

        df1_spark = self.spark.createDataFrame(data1, ["a", "b", "c"])
        df2_spark = self.spark.createDataFrame(data2, ["a", "b", "c"])
        union_df_spark = df1_spark.unionByName(df2_spark)

        self.assert_eq(union_df_connect.toPandas(), union_df_spark.toPandas())

        df2_connect = self.connect.createDataFrame(data2, ["a", "B", "C"])
        union_df_connect = df1_connect.unionByName(df2_connect, allowMissingColumns=True)

        df2_spark = self.spark.createDataFrame(data2, ["a", "B", "C"])
        union_df_spark = df1_spark.unionByName(df2_spark, allowMissingColumns=True)

        self.assert_eq(union_df_connect.toPandas(), union_df_spark.toPandas())

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

    def test_observe(self):
        # SPARK-41527: test DataFrame.observe()
        observation_name = "my_metric"

        self.assert_eq(
            self.connect.read.table(self.tbl_name)
            .filter("id > 3")
            .observe(observation_name, CF.min("id"), CF.max("id"), CF.sum("id"))
            .toPandas(),
            self.spark.read.table(self.tbl_name)
            .filter("id > 3")
            .observe(observation_name, SF.min("id"), SF.max("id"), SF.sum("id"))
            .toPandas(),
        )

        from pyspark.sql.connect.observation import Observation as ConnectObservation
        from pyspark.sql.observation import Observation

        cobservation = ConnectObservation(observation_name)
        observation = Observation(observation_name)

        cdf = (
            self.connect.read.table(self.tbl_name)
            .filter("id > 3")
            .observe(cobservation, CF.min("id"), CF.max("id"), CF.sum("id"))
            .toPandas()
        )
        df = (
            self.spark.read.table(self.tbl_name)
            .filter("id > 3")
            .observe(observation, SF.min("id"), SF.max("id"), SF.sum("id"))
            .toPandas()
        )

        self.assert_eq(cdf, df)

        self.assertEqual(cobservation.get, observation.get)

        observed_metrics = cdf.attrs["observed_metrics"]
        self.assert_eq(len(observed_metrics), 1)
        self.assert_eq(observed_metrics[0].name, observation_name)
        self.assert_eq(len(observed_metrics[0].metrics), 3)
        for metric in observed_metrics[0].metrics:
            self.assertIsInstance(metric, ProtoExpression.Literal)
        values = list(map(lambda metric: metric.long, observed_metrics[0].metrics))
        self.assert_eq(values, [4, 99, 4944])

        with self.assertRaises(PySparkValueError) as pe:
            self.connect.read.table(self.tbl_name).observe(observation_name)

        self.check_error(
            exception=pe.exception,
            error_class="CANNOT_BE_EMPTY",
            message_parameters={"item": "exprs"},
        )

        with self.assertRaises(PySparkTypeError) as pe:
            self.connect.read.table(self.tbl_name).observe(observation_name, CF.lit(1), "id")

        self.check_error(
            exception=pe.exception,
            error_class="NOT_LIST_OF_COLUMN",
            message_parameters={"arg_name": "exprs"},
        )

    def test_with_columns(self):
        # SPARK-41256: test withColumn(s).
        self.assert_eq(
            self.connect.read.table(self.tbl_name).withColumn("id", CF.lit(False)).toPandas(),
            self.spark.read.table(self.tbl_name).withColumn("id", SF.lit(False)).toPandas(),
        )

        self.assert_eq(
            self.connect.read.table(self.tbl_name)
            .withColumns({"id": CF.lit(False), "col_not_exist": CF.lit(False)})
            .toPandas(),
            self.spark.read.table(self.tbl_name)
            .withColumns(
                {
                    "id": SF.lit(False),
                    "col_not_exist": SF.lit(False),
                }
            )
            .toPandas(),
        )

    def test_hint(self):
        # SPARK-41349: Test hint
        self.assert_eq(
            self.connect.read.table(self.tbl_name).hint("COALESCE", 3000).toPandas(),
            self.spark.read.table(self.tbl_name).hint("COALESCE", 3000).toPandas(),
        )

        # Hint with unsupported name will be ignored
        self.assert_eq(
            self.connect.read.table(self.tbl_name).hint("illegal").toPandas(),
            self.spark.read.table(self.tbl_name).hint("illegal").toPandas(),
        )

        # Hint with all supported parameter values
        such_a_nice_list = ["itworks1", "itworks2", "itworks3"]
        self.assert_eq(
            self.connect.read.table(self.tbl_name).hint("my awesome hint", 1.2345, 2).toPandas(),
            self.spark.read.table(self.tbl_name).hint("my awesome hint", 1.2345, 2).toPandas(),
        )

        # Hint with unsupported parameter values
        with self.assertRaises(AnalysisException):
            self.connect.read.table(self.tbl_name).hint("REPARTITION", "id+1").toPandas()

        # Hint with unsupported parameter types
        with self.assertRaises(TypeError):
            self.connect.read.table(self.tbl_name).hint("REPARTITION", range(5)).toPandas()

        # Hint with unsupported parameter types
        with self.assertRaises(TypeError):
            self.connect.read.table(self.tbl_name).hint(
                "my awesome hint", 1.2345, 2, such_a_nice_list, range(6)
            ).toPandas()

        # Hint with wrong combination
        with self.assertRaises(AnalysisException):
            self.connect.read.table(self.tbl_name).hint("REPARTITION", "id", 3).toPandas()

    def test_join_hint(self):
        cdf1 = self.connect.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        cdf2 = self.connect.createDataFrame(
            [Row(height=80, name="Tom"), Row(height=85, name="Bob")]
        )

        self.assertTrue(
            "BroadcastHashJoin" in cdf1.join(cdf2.hint("BROADCAST"), "name")._explain_string()
        )
        self.assertTrue("SortMergeJoin" in cdf1.join(cdf2.hint("MERGE"), "name")._explain_string())
        self.assertTrue(
            "ShuffledHashJoin" in cdf1.join(cdf2.hint("SHUFFLE_HASH"), "name")._explain_string()
        )

    def test_different_spark_session_join_or_union(self):
        df = self.connect.range(10).limit(3)

        spark2 = RemoteSparkSession(connection="sc://localhost")
        df2 = spark2.range(10).limit(3)

        with self.assertRaises(SessionNotSameException) as e1:
            df.union(df2).collect()
        self.check_error(
            exception=e1.exception,
            error_class="SESSION_NOT_SAME",
            message_parameters={},
        )

        with self.assertRaises(SessionNotSameException) as e2:
            df.unionByName(df2).collect()
        self.check_error(
            exception=e2.exception,
            error_class="SESSION_NOT_SAME",
            message_parameters={},
        )

        with self.assertRaises(SessionNotSameException) as e3:
            df.join(df2).collect()
        self.check_error(
            exception=e3.exception,
            error_class="SESSION_NOT_SAME",
            message_parameters={},
        )

    def test_extended_hint_types(self):
        cdf = self.connect.range(100).toDF("id")

        cdf.hint(
            "my awesome hint",
            1.2345,
            "what",
            ["itworks1", "itworks2", "itworks3"],
        ).show()

        with self.assertRaises(PySparkTypeError) as pe:
            cdf.hint(
                "my awesome hint",
                1.2345,
                "what",
                {"itworks1": "itworks2"},
            ).show()

        self.check_error(
            exception=pe.exception,
            error_class="INVALID_ITEM_FOR_CONTAINER",
            message_parameters={
                "arg_name": "parameters",
                "allowed_types": "str, float, int, Column, list[str], list[float], list[int]",
                "item_type": "dict",
            },
        )

    def test_empty_dataset(self):
        # SPARK-41005: Test arrow based collection with empty dataset.
        self.assertTrue(
            self.connect.sql("SELECT 1 AS X LIMIT 0")
            .toPandas()
            .equals(self.spark.sql("SELECT 1 AS X LIMIT 0").toPandas())
        )
        pdf = self.connect.sql("SELECT 1 AS X LIMIT 0").toPandas()
        self.assertEqual(0, len(pdf))  # empty dataset
        self.assertEqual(1, len(pdf.columns))  # one column
        self.assertEqual("X", pdf.columns[0])

    def test_is_empty(self):
        # SPARK-41212: Test is empty
        self.assertFalse(self.connect.sql("SELECT 1 AS X").isEmpty())
        self.assertTrue(self.connect.sql("SELECT 1 AS X LIMIT 0").isEmpty())

    def test_is_empty_with_unsupported_types(self):
        df = self.spark.sql("SELECT INTERVAL '10-8' YEAR TO MONTH AS interval")
        self.assertEqual(df.count(), 1)
        self.assertFalse(df.isEmpty())

    def test_session(self):
        self.assertEqual(self.connect, self.connect.sql("SELECT 1").sparkSession)

    def test_show(self):
        # SPARK-41111: Test the show method
        show_str = self.connect.sql("SELECT 1 AS X, 2 AS Y")._show_string()
        # +---+---+
        # |  X|  Y|
        # +---+---+
        # |  1|  2|
        # +---+---+
        expected = "+---+---+\n|  X|  Y|\n+---+---+\n|  1|  2|\n+---+---+\n"
        self.assertEqual(show_str, expected)

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
            error_class="NOT_STR",
            message_parameters={
                "arg_name": "col1",
                "arg_type": "int",
            },
        )

        with self.assertRaises(PySparkTypeError) as pe:
            self.connect.read.table(self.tbl_name).stat.corr("col1", 1, "pearson")

        self.check_error(
            exception=pe.exception,
            error_class="NOT_STR",
            message_parameters={
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
            error_class="NOT_LIST_OR_STR_OR_TUPLE",
            message_parameters={
                "arg_name": "col",
                "arg_type": "int",
            },
        )

        with self.assertRaises(PySparkTypeError) as pe:
            self.connect.read.table(self.tbl_name2).stat.approxQuantile(["col1", "col3"], 0.1, 0.1)

        self.check_error(
            exception=pe.exception,
            error_class="NOT_LIST_OR_TUPLE",
            message_parameters={
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
            error_class="NOT_LIST_OF_FLOAT_OR_INT",
            message_parameters={"arg_name": "probabilities", "arg_type": "float"},
        )
        with self.assertRaises(PySparkTypeError) as pe:
            self.connect.read.table(self.tbl_name2).stat.approxQuantile(
                ["col1", "col3"], [0.1, 0.5, 0.9], "str"
            )

        self.check_error(
            exception=pe.exception,
            error_class="NOT_FLOAT_OR_INT",
            message_parameters={
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
            error_class="NEGATIVE_VALUE",
            message_parameters={
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
            error_class="NOT_LIST_OR_TUPLE",
            message_parameters={
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
            error_class="DISALLOWED_TYPE_FOR_CONTAINER",
            message_parameters={
                "arg_name": "fractions",
                "arg_type": "dict",
                "allowed_types": "float, int, str",
                "item_type": "NoneType",
            },
        )

        with self.assertRaises(SparkConnectException):
            cdf.sampleBy(cdf.key, fractions={0: 0.1, 1: 1.2}, seed=0).show()

    def test_repr(self):
        # SPARK-41213: Test the __repr__ method
        query = """SELECT * FROM VALUES (1L, NULL), (3L, "Z") AS tab(a, b)"""
        self.assertEqual(
            self.connect.sql(query).__repr__(),
            self.spark.sql(query).__repr__(),
        )

    def test_explain_string(self):
        # SPARK-41122: test explain API.
        plan_str = self.connect.sql("SELECT 1")._explain_string(extended=True)
        self.assertTrue("Parsed Logical Plan" in plan_str)
        self.assertTrue("Analyzed Logical Plan" in plan_str)
        self.assertTrue("Optimized Logical Plan" in plan_str)
        self.assertTrue("Physical Plan" in plan_str)

        with self.assertRaises(PySparkValueError) as pe:
            self.connect.sql("SELECT 1")._explain_string(mode="unknown")
        self.check_error(
            exception=pe.exception,
            error_class="UNKNOWN_EXPLAIN_MODE",
            message_parameters={"explain_mode": "unknown"},
        )

    def test_count(self) -> None:
        # SPARK-41308: test count() API.
        self.assertEqual(
            self.connect.read.table(self.tbl_name).count(),
            self.spark.read.table(self.tbl_name).count(),
        )

    def test_simple_transform(self) -> None:
        """SPARK-41203: Support DF.transform"""

        def transform_df(input_df: CDataFrame) -> CDataFrame:
            return input_df.select((CF.col("id") + CF.lit(10)).alias("id"))

        df = self.connect.range(1, 100)
        result_left = df.transform(transform_df).collect()
        result_right = self.connect.range(11, 110).collect()
        self.assertEqual(result_right, result_left)

        # Check assertion.
        with self.assertRaises(AssertionError):
            df.transform(lambda x: 2)  # type: ignore

    def test_alias(self) -> None:
        """Testing supported and unsupported alias"""
        col0 = (
            self.connect.range(1, 10)
            .select(CF.col("id").alias("name", metadata={"max": 99}))
            .schema.names[0]
        )
        self.assertEqual("name", col0)

        with self.assertRaises(SparkConnectException) as exc:
            self.connect.range(1, 10).select(CF.col("id").alias("this", "is", "not")).collect()
        self.assertIn("(this, is, not)", str(exc.exception))

    def test_column_regexp(self) -> None:
        # SPARK-41438: test dataframe.colRegex()
        ndf = self.connect.read.table(self.tbl_name3)
        df = self.spark.read.table(self.tbl_name3)

        self.assert_eq(
            ndf.select(ndf.colRegex("`tes.*\n.*mn`")).toPandas(),
            df.select(df.colRegex("`tes.*\n.*mn`")).toPandas(),
        )

    def test_repartition(self) -> None:
        # SPARK-41354: test dataframe.repartition(numPartitions)
        self.assert_eq(
            self.connect.read.table(self.tbl_name).repartition(10).toPandas(),
            self.spark.read.table(self.tbl_name).repartition(10).toPandas(),
        )

        self.assert_eq(
            self.connect.read.table(self.tbl_name).coalesce(10).toPandas(),
            self.spark.read.table(self.tbl_name).coalesce(10).toPandas(),
        )

    def test_repartition_by_expression(self) -> None:
        # SPARK-41354: test dataframe.repartition(expressions)
        self.assert_eq(
            self.connect.read.table(self.tbl_name).repartition(10, "id").toPandas(),
            self.spark.read.table(self.tbl_name).repartition(10, "id").toPandas(),
        )

        self.assert_eq(
            self.connect.read.table(self.tbl_name).repartition("id").toPandas(),
            self.spark.read.table(self.tbl_name).repartition("id").toPandas(),
        )

        # repartition with unsupported parameter values
        with self.assertRaises(AnalysisException):
            self.connect.read.table(self.tbl_name).repartition("id+1").toPandas()

    def test_repartition_by_range(self) -> None:
        # SPARK-41354: test dataframe.repartitionByRange(expressions)
        cdf = self.connect.read.table(self.tbl_name)
        sdf = self.spark.read.table(self.tbl_name)

        self.assert_eq(
            cdf.repartitionByRange(10, "id").toPandas(),
            sdf.repartitionByRange(10, "id").toPandas(),
        )

        self.assert_eq(
            cdf.repartitionByRange("id").toPandas(),
            sdf.repartitionByRange("id").toPandas(),
        )

        self.assert_eq(
            cdf.repartitionByRange(cdf.id.desc()).toPandas(),
            sdf.repartitionByRange(sdf.id.desc()).toPandas(),
        )

        # repartitionByRange with unsupported parameter values
        with self.assertRaises(AnalysisException):
            self.connect.read.table(self.tbl_name).repartitionByRange("id+1").toPandas()

    def test_agg_with_two_agg_exprs(self) -> None:
        # SPARK-41230: test dataframe.agg()
        self.assert_eq(
            self.connect.read.table(self.tbl_name).agg({"name": "min", "id": "max"}).toPandas(),
            self.spark.read.table(self.tbl_name).agg({"name": "min", "id": "max"}).toPandas(),
        )

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

    def test_column_cannot_be_constructed_from_string(self):
        with self.assertRaises(TypeError):
            Column("col")

    def test_crossjoin(self):
        # SPARK-41227: Test CrossJoin
        connect_df = self.connect.read.table(self.tbl_name)
        spark_df = self.spark.read.table(self.tbl_name)
        self.assert_eq(
            set(
                connect_df.select("id")
                .join(other=connect_df.select("name"), how="cross")
                .toPandas()
            ),
            set(spark_df.select("id").join(other=spark_df.select("name"), how="cross").toPandas()),
        )
        self.assert_eq(
            set(connect_df.select("id").crossJoin(other=connect_df.select("name")).toPandas()),
            set(spark_df.select("id").crossJoin(other=spark_df.select("name")).toPandas()),
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
            error_class="NOT_BOOL_OR_FLOAT_OR_INT_OR_STR",
            message_parameters={
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

    def test_with_metadata(self):
        cdf = self.connect.createDataFrame(data=[(2, "Alice"), (5, "Bob")], schema=["age", "name"])
        self.assertEqual(cdf.schema["age"].metadata, {})
        self.assertEqual(cdf.schema["name"].metadata, {})

        cdf1 = cdf.withMetadata(columnName="age", metadata={"max_age": 5})
        self.assertEqual(cdf1.schema["age"].metadata, {"max_age": 5})

        cdf2 = cdf.withMetadata(columnName="name", metadata={"names": ["Alice", "Bob"]})
        self.assertEqual(cdf2.schema["name"].metadata, {"names": ["Alice", "Bob"]})

        with self.assertRaises(PySparkTypeError) as pe:
            cdf.withMetadata(columnName="name", metadata=["magic"])

        self.check_error(
            exception=pe.exception,
            error_class="NOT_DICT",
            message_parameters={
                "arg_name": "metadata",
                "arg_type": "list",
            },
        )

    def test_simple_udt(self):
        from pyspark.ml.linalg import MatrixUDT, VectorUDT

        for schema in [
            StructType().add("key", LongType()).add("val", PythonOnlyUDT()),
            StructType().add("key", LongType()).add("val", ArrayType(PythonOnlyUDT())),
            StructType().add("key", LongType()).add("val", MapType(LongType(), PythonOnlyUDT())),
            StructType().add("key", LongType()).add("val", PythonOnlyUDT()),
            StructType().add("key", LongType()).add("vec", VectorUDT()),
            StructType().add("key", LongType()).add("mat", MatrixUDT()),
        ]:
            cdf = self.connect.createDataFrame(data=[], schema=schema)
            sdf = self.spark.createDataFrame(data=[], schema=schema)

            self.assertEqual(cdf.schema, sdf.schema)

    def test_simple_udt_from_read(self):
        from pyspark.ml.linalg import Matrices, Vectors

        with tempfile.TemporaryDirectory(prefix="test_simple_udt_from_read") as d:
            path1 = f"{d}/df1.parquet"
            self.spark.createDataFrame(
                [(i % 3, PythonOnlyPoint(float(i), float(i))) for i in range(10)],
                schema=StructType().add("key", LongType()).add("val", PythonOnlyUDT()),
            ).write.parquet(path1)

            path2 = f"{d}/df2.parquet"
            self.spark.createDataFrame(
                [(i % 3, [PythonOnlyPoint(float(i), float(i))]) for i in range(10)],
                schema=StructType().add("key", LongType()).add("val", ArrayType(PythonOnlyUDT())),
            ).write.parquet(path2)

            path3 = f"{d}/df3.parquet"
            self.spark.createDataFrame(
                [(i % 3, {i % 3: PythonOnlyPoint(float(i + 1), float(i + 1))}) for i in range(10)],
                schema=StructType()
                .add("key", LongType())
                .add("val", MapType(LongType(), PythonOnlyUDT())),
            ).write.parquet(path3)

            path4 = f"{d}/df4.parquet"
            self.spark.createDataFrame(
                [(i % 3, PythonOnlyPoint(float(i), float(i))) for i in range(10)],
                schema=StructType().add("key", LongType()).add("val", PythonOnlyUDT()),
            ).write.parquet(path4)

            path5 = f"{d}/df5.parquet"
            self.spark.createDataFrame(
                [Row(label=1.0, point=ExamplePoint(1.0, 2.0))]
            ).write.parquet(path5)

            path6 = f"{d}/df6.parquet"
            self.spark.createDataFrame(
                [(Vectors.dense(1.0, 2.0, 3.0),), (Vectors.sparse(3, {1: 1.0, 2: 5.5}),)],
                ["vec"],
            ).write.parquet(path6)

            path7 = f"{d}/df7.parquet"
            self.spark.createDataFrame(
                [
                    (Matrices.dense(3, 2, [0, 1, 4, 5, 9, 10]),),
                    (Matrices.sparse(1, 1, [0, 1], [0], [2.0]),),
                ],
                ["mat"],
            ).write.parquet(path7)

            for path in [path1, path2, path3, path4, path5, path6, path7]:
                self.assertEqual(
                    self.connect.read.parquet(path).schema,
                    self.spark.read.parquet(path).schema,
                )

    def test_version(self):
        self.assertEqual(
            self.connect.version,
            self.spark.version,
        )

    def test_same_semantics(self):
        plan = self.connect.sql("SELECT 1")
        other = self.connect.sql("SELECT 1")
        self.assertTrue(plan.sameSemantics(other))

    def test_semantic_hash(self):
        plan = self.connect.sql("SELECT 1")
        other = self.connect.sql("SELECT 1")
        self.assertEqual(
            plan.semanticHash(),
            other.semanticHash(),
        )

    def test_unsupported_functions(self):
        # SPARK-41225: Disable unsupported functions.
        df = self.connect.read.table(self.tbl_name)
        for f in (
            "checkpoint",
            "localCheckpoint",
        ):
            with self.assertRaises(NotImplementedError):
                getattr(df, f)()

    def test_sql_with_command(self):
        # SPARK-42705: spark.sql should return values from the command.
        self.assertEqual(
            self.connect.sql("show functions").collect(), self.spark.sql("show functions").collect()
        )

    def test_unsupported_jvm_attribute(self):
        # Unsupported jvm attributes for Spark session.
        unsupported_attrs = ["_jsc", "_jconf", "_jvm", "_jsparkSession"]
        spark_session = self.connect
        for attr in unsupported_attrs:
            with self.assertRaises(PySparkAttributeError) as pe:
                getattr(spark_session, attr)

            self.check_error(
                exception=pe.exception,
                error_class="JVM_ATTRIBUTE_NOT_SUPPORTED",
                message_parameters={"attr_name": attr},
            )

        # Unsupported jvm attributes for DataFrame.
        unsupported_attrs = ["_jseq", "_jdf", "_jmap", "_jcols"]
        cdf = self.connect.range(10)
        for attr in unsupported_attrs:
            with self.assertRaises(PySparkAttributeError) as pe:
                getattr(cdf, attr)

            self.check_error(
                exception=pe.exception,
                error_class="JVM_ATTRIBUTE_NOT_SUPPORTED",
                message_parameters={"attr_name": attr},
            )

        # Unsupported jvm attributes for Column.
        with self.assertRaises(PySparkAttributeError) as pe:
            getattr(cdf.id, "_jc")

        self.check_error(
            exception=pe.exception,
            error_class="JVM_ATTRIBUTE_NOT_SUPPORTED",
            message_parameters={"attr_name": "_jc"},
        )

        # Unsupported jvm attributes for DataFrameReader.
        with self.assertRaises(PySparkAttributeError) as pe:
            getattr(spark_session.read, "_jreader")

        self.check_error(
            exception=pe.exception,
            error_class="JVM_ATTRIBUTE_NOT_SUPPORTED",
            message_parameters={"attr_name": "_jreader"},
        )

    def test_df_caache(self):
        df = self.connect.range(10)
        df.cache()
        self.assert_eq(10, df.count())
        self.assertTrue(df.is_cached)


if __name__ == "__main__":
    from pyspark.sql.tests.connect.test_connect_basic import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None

    unittest.main(testRunner=testRunner, verbosity=2)
