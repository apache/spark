/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.execution.BatchedDataSourceScanExec
import org.apache.spark.sql.execution.datasources.parquet.TestingUDT.{NestedStruct, NestedStructUDT}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * A test suite that tests various Parquet queries.
 */
class ParquetQuerySuite extends QueryTest with ParquetTest with SharedSQLContext {
  import testImplicits._

  test("simple select queries") {
    withParquetTable((0 until 10).map(i => (i, i.toString)), "t") {
      checkAnswer(sql("SELECT _1 FROM t where t._1 > 5"), (6 until 10).map(Row.apply(_)))
      checkAnswer(sql("SELECT _1 FROM t as tmp where tmp._1 < 5"), (0 until 5).map(Row.apply(_)))
    }
  }

  test("appending") {
    val data = (0 until 10).map(i => (i, i.toString))
    spark.createDataFrame(data).toDF("c1", "c2").createOrReplaceTempView("tmp")
    // Query appends, don't test with both read modes.
    withParquetTable(data, "t", false) {
      sql("INSERT INTO TABLE t SELECT * FROM tmp")
      checkAnswer(spark.table("t"), (data ++ data).map(Row.fromTuple))
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("tmp"), ignoreIfNotExists = true)
  }

  test("overwriting") {
    val data = (0 until 10).map(i => (i, i.toString))
    spark.createDataFrame(data).toDF("c1", "c2").createOrReplaceTempView("tmp")
    withParquetTable(data, "t") {
      sql("INSERT OVERWRITE TABLE t SELECT * FROM tmp")
      checkAnswer(spark.table("t"), data.map(Row.fromTuple))
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("tmp"), ignoreIfNotExists = true)
  }

  test("self-join") {
    // 4 rows, cells of column 1 of row 2 and row 4 are null
    val data = (1 to 4).map { i =>
      val maybeInt = if (i % 2 == 0) None else Some(i)
      (maybeInt, i.toString)
    }

    // TODO: vectorized doesn't work here because it requires UnsafeRows
    withParquetTable(data, "t", false) {
      val selfJoin = sql("SELECT * FROM t x JOIN t y WHERE x._1 = y._1")
      val queryOutput = selfJoin.queryExecution.analyzed.output

      assertResult(4, "Field count mismatches")(queryOutput.size)
      assertResult(2, "Duplicated expression ID in query plan:\n $selfJoin") {
        queryOutput.filter(_.name == "_1").map(_.exprId).size
      }

      checkAnswer(selfJoin, List(Row(1, "1", 1, "1"), Row(3, "3", 3, "3")))
    }
  }

  test("nested data - struct with array field") {
    val data = (1 to 10).map(i => Tuple1((i, Seq("val_$i"))))
    withParquetTable(data, "t") {
      checkAnswer(sql("SELECT _1._2[0] FROM t"), data.map {
        case Tuple1((_, Seq(string))) => Row(string)
      })
    }
  }

  test("nested data - array of struct") {
    val data = (1 to 10).map(i => Tuple1(Seq(i -> "val_$i")))
    withParquetTable(data, "t") {
      checkAnswer(sql("SELECT _1[0]._2 FROM t"), data.map {
        case Tuple1(Seq((_, string))) => Row(string)
      })
    }
  }

  test("SPARK-1913 regression: columns only referenced by pushed down filters should remain") {
    withParquetTable((1 to 10).map(Tuple1.apply), "t") {
      checkAnswer(sql("SELECT _1 FROM t WHERE _1 < 10"), (1 to 9).map(Row.apply(_)))
    }
  }

  test("SPARK-5309 strings stored using dictionary compression in parquet") {
    withParquetTable((0 until 1000).map(i => ("same", "run_" + i /100, 1)), "t") {

      checkAnswer(sql("SELECT _1, _2, SUM(_3) FROM t GROUP BY _1, _2"),
        (0 until 10).map(i => Row("same", "run_" + i, 100)))

      checkAnswer(sql("SELECT _1, _2, SUM(_3) FROM t WHERE _2 = 'run_5' GROUP BY _1, _2"),
        List(Row("same", "run_5", 100)))
    }
  }

  test("SPARK-6917 DecimalType should work with non-native types") {
    val data = (1 to 10).map(i => Row(Decimal(i, 18, 0), new java.sql.Timestamp(i)))
    val schema = StructType(List(StructField("d", DecimalType(18, 0), false),
      StructField("time", TimestampType, false)).toArray)
    withTempPath { file =>
      val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
      df.write.parquet(file.getCanonicalPath)
      val df2 = spark.read.parquet(file.getCanonicalPath)
      checkAnswer(df2, df.collect().toSeq)
    }
  }

  test("Enabling/disabling merging partfiles when merging parquet schema") {
    def testSchemaMerging(expectedColumnNumber: Int): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
        spark.range(0, 10).toDF("b").write.parquet(new Path(basePath, "foo=2").toString)
        // delete summary files, so if we don't merge part-files, one column will not be included.
        Utils.deleteRecursively(new File(basePath + "/foo=1/_metadata"))
        Utils.deleteRecursively(new File(basePath + "/foo=1/_common_metadata"))
        assert(spark.read.parquet(basePath).columns.length === expectedColumnNumber)
      }
    }

    withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true",
      SQLConf.PARQUET_SCHEMA_RESPECT_SUMMARIES.key -> "true") {
      testSchemaMerging(2)
    }

    withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true",
      SQLConf.PARQUET_SCHEMA_RESPECT_SUMMARIES.key -> "false") {
      testSchemaMerging(3)
    }
  }

  test("Enabling/disabling schema merging") {
    def testSchemaMerging(expectedColumnNumber: Int): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
        spark.range(0, 10).toDF("b").write.parquet(new Path(basePath, "foo=2").toString)
        assert(spark.read.parquet(basePath).columns.length === expectedColumnNumber)
      }
    }

    withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true") {
      testSchemaMerging(3)
    }

    withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "false") {
      testSchemaMerging(2)
    }
  }

  test("SPARK-8990 DataFrameReader.parquet() should respect user specified options") {
    withTempPath { dir =>
      val basePath = dir.getCanonicalPath
      spark.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
      spark.range(0, 10).toDF("b").write.parquet(new Path(basePath, "foo=a").toString)

      // Disables the global SQL option for schema merging
      withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "false") {
        assertResult(2) {
          // Disables schema merging via data source option
          spark.read.option("mergeSchema", "false").parquet(basePath).columns.length
        }

        assertResult(3) {
          // Enables schema merging via data source option
          spark.read.option("mergeSchema", "true").parquet(basePath).columns.length
        }
      }
    }
  }

  test("SPARK-9119 Decimal should be correctly written into parquet") {
    withTempPath { dir =>
      val basePath = dir.getCanonicalPath
      val schema = StructType(Array(StructField("name", DecimalType(10, 5), false)))
      val rowRDD = sparkContext.parallelize(Array(Row(Decimal("67123.45"))))
      val df = spark.createDataFrame(rowRDD, schema)
      df.write.parquet(basePath)

      val decimal = spark.read.parquet(basePath).first().getDecimal(0)
      assert(Decimal("67123.45") === Decimal(decimal))
    }
  }

  test("SPARK-10005 Schema merging for nested struct") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      def append(df: DataFrame): Unit = {
        df.write.mode(SaveMode.Append).parquet(path)
      }

      // Note that both the following two DataFrames contain a single struct column with multiple
      // nested fields.
      append((1 to 2).map(i => Tuple1((i, i))).toDF())
      append((1 to 2).map(i => Tuple1((i, i, i))).toDF())

      withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "true") {
        checkAnswer(
          spark.read.option("mergeSchema", "true").parquet(path),
          Seq(
            Row(Row(1, 1, null)),
            Row(Row(2, 2, null)),
            Row(Row(1, 1, 1)),
            Row(Row(2, 2, 2))))
      }
    }
  }

  test("SPARK-10301 requested schema clipping - same schema") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(1).selectExpr("NAMED_STRUCT('a', id, 'b', id + 1) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 1L)))
    }
  }

  test("SPARK-11997 parquet with null partition values") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(1, 3)
        .selectExpr("if(id % 2 = 0, null, id) AS n", "id")
        .write.partitionBy("n").parquet(path)

      checkAnswer(
        spark.read.parquet(path).filter("n is null"),
        Row(2, null))
    }
  }

  // This test case is ignored because of parquet-mr bug PARQUET-370
  ignore("SPARK-10301 requested schema clipping - schemas with disjoint sets of fields") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(1).selectExpr("NAMED_STRUCT('a', id, 'b', id + 1) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(null, null)))
    }
  }

  test("SPARK-10301 requested schema clipping - requested schema contains physical schema") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(1).selectExpr("NAMED_STRUCT('a', id, 'b', id + 1) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true)
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 1L, null, null)))
    }

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(1).selectExpr("NAMED_STRUCT('a', id, 'd', id + 3) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true)
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, null, null, 3L)))
    }
  }

  test("SPARK-10301 requested schema clipping - physical schema contains requested schema") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2, 'd', id + 3) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 1L)))
    }

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2, 'd', id + 3) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 3L)))
    }
  }

  test("SPARK-10301 requested schema clipping - schemas overlap but don't contain each other") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("b", LongType, nullable = true)
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(1L, 2L, null)))
    }
  }

  test("SPARK-10301 requested schema clipping - deeply nested struct") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', ARRAY(NAMED_STRUCT('b', id, 'c', id))) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema = new StructType()
        .add("s",
          new StructType()
            .add(
              "a",
              ArrayType(
                new StructType()
                  .add("b", LongType, nullable = true)
                  .add("d", StringType, nullable = true),
                containsNull = true),
              nullable = true),
          nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(Seq(Row(0, null)))))
    }
  }

  test("SPARK-10301 requested schema clipping - out of order") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df1 = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2) AS s")
        .coalesce(1)

      val df2 = spark
        .range(1, 2)
        .selectExpr("NAMED_STRUCT('c', id + 2, 'b', id + 1, 'd', id + 3) AS s")
        .coalesce(1)

      df1.write.parquet(path)
      df2.write.mode(SaveMode.Append).parquet(path)

      val userDefinedSchema = new StructType()
        .add("s",
          new StructType()
            .add("a", LongType, nullable = true)
            .add("b", LongType, nullable = true)
            .add("d", LongType, nullable = true),
          nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Seq(
          Row(Row(0, 1, null)),
          Row(Row(null, 2, 4))))
    }
  }

  test("SPARK-10301 requested schema clipping - schema merging") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df1 = spark
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'c', id + 2) AS s")
        .coalesce(1)

      val df2 = spark
        .range(1, 2)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2) AS s")
        .coalesce(1)

      df1.write.mode(SaveMode.Append).parquet(path)
      df2.write.mode(SaveMode.Append).parquet(path)

      checkAnswer(
        spark
          .read
          .option("mergeSchema", "true")
          .parquet(path)
          .selectExpr("s.a", "s.b", "s.c"),
        Seq(
          Row(0, null, 2),
          Row(1, 2, 3)))
    }
  }

  testStandardAndLegacyModes("SPARK-10301 requested schema clipping - UDT") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df = spark
        .range(1)
        .selectExpr(
          """NAMED_STRUCT(
            |  'f0', CAST(id AS STRING),
            |  'f1', NAMED_STRUCT(
            |    'a', CAST(id + 1 AS INT),
            |    'b', CAST(id + 2 AS LONG),
            |    'c', CAST(id + 3.5 AS DOUBLE)
            |  )
            |) AS s
          """.stripMargin)
        .coalesce(1)

      df.write.mode(SaveMode.Append).parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("f1", new NestedStructUDT, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(NestedStruct(1, 2L, 3.5D))))
    }
  }

  test("expand UDT in StructType") {
    val schema = new StructType().add("n", new NestedStructUDT, nullable = true)
    val expected = new StructType().add("n", new NestedStructUDT().sqlType, nullable = true)
    assert(CatalystReadSupport.expandUDT(schema) === expected)
  }

  test("expand UDT in ArrayType") {
    val schema = new StructType().add(
      "n",
      ArrayType(
        elementType = new NestedStructUDT,
        containsNull = false),
      nullable = true)

    val expected = new StructType().add(
      "n",
      ArrayType(
        elementType = new NestedStructUDT().sqlType,
        containsNull = false),
      nullable = true)

    assert(CatalystReadSupport.expandUDT(schema) === expected)
  }

  test("expand UDT in MapType") {
    val schema = new StructType().add(
      "n",
      MapType(
        keyType = IntegerType,
        valueType = new NestedStructUDT,
        valueContainsNull = false),
      nullable = true)

    val expected = new StructType().add(
      "n",
      MapType(
        keyType = IntegerType,
        valueType = new NestedStructUDT().sqlType,
        valueContainsNull = false),
      nullable = true)

    assert(CatalystReadSupport.expandUDT(schema) === expected)
  }

  test("returning batch for wide table") {
    withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "10") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        val df = spark.range(10).select(Seq.tabulate(11) {i => ('id + i).as(s"c$i")} : _*)
        df.write.mode(SaveMode.Overwrite).parquet(path)

        // donot return batch, because whole stage codegen is disabled for wide table (>200 columns)
        val df2 = spark.read.parquet(path)
        assert(df2.queryExecution.sparkPlan.find(_.isInstanceOf[BatchedDataSourceScanExec]).isEmpty,
          "Should not return batch")
        checkAnswer(df2, df)

        // return batch
        val columns = Seq.tabulate(9) {i => s"c$i"}
        val df3 = df2.selectExpr(columns : _*)
        assert(
          df3.queryExecution.sparkPlan.find(_.isInstanceOf[BatchedDataSourceScanExec]).isDefined,
          "Should return batch")
        checkAnswer(df3, df.selectExpr(columns : _*))
      }
    }
  }
}

object TestingUDT {
  @SQLUserDefinedType(udt = classOf[NestedStructUDT])
  case class NestedStruct(a: Integer, b: Long, c: Double)

  class NestedStructUDT extends UserDefinedType[NestedStruct] {
    override def sqlType: DataType =
      new StructType()
        .add("a", IntegerType, nullable = true)
        .add("b", LongType, nullable = false)
        .add("c", DoubleType, nullable = false)

    override def serialize(n: NestedStruct): Any = {
      val row = new SpecificMutableRow(sqlType.asInstanceOf[StructType].map(_.dataType))
      row.setInt(0, n.a)
      row.setLong(1, n.b)
      row.setDouble(2, n.c)
    }

    override def userClass: Class[NestedStruct] = classOf[NestedStruct]

    override def deserialize(datum: Any): NestedStruct = {
      datum match {
        case row: InternalRow =>
          NestedStruct(row.getInt(0), row.getLong(1), row.getDouble(2))
      }
    }
  }
}
