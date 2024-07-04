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
import java.math.BigDecimal
import java.time.{Duration, LocalDateTime, Period, ZoneOffset}
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.{DebugFilesystem, SparkConf, SparkException}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{SchemaColumnConvertNotSupportedException, SQLHadoopMapReduceCommitProtocol}
import org.apache.spark.sql.execution.datasources.parquet.TestingUDT._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * A test suite that tests various Parquet queries.
 */
abstract class ParquetQuerySuite extends QueryTest with ParquetTest with SharedSparkSession {
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
      TableIdentifier("tmp"), ignoreIfNotExists = true, purge = false)
  }

  test("overwriting") {
    val data = (0 until 10).map(i => (i, i.toString))
    spark.createDataFrame(data).toDF("c1", "c2").createOrReplaceTempView("tmp")
    withParquetTable(data, "t") {
      sql("INSERT OVERWRITE TABLE t SELECT * FROM tmp")
      checkAnswer(spark.table("t"), data.map(Row.fromTuple))
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("tmp"), ignoreIfNotExists = true, purge = false)
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
      assertResult(2, s"Duplicated expression ID in query plan:\n $selfJoin") {
        queryOutput.filter(_.name == "_1").map(_.exprId).size
      }

      checkAnswer(selfJoin, List(Row(1, "1", 1, "1"), Row(3, "3", 3, "3")))
    }
  }

  test("nested data - struct with array field") {
    val data = (1 to 10).map(i => Tuple1((i, Seq(s"val_$i"))))
    withParquetTable(data, "t") {
      checkAnswer(sql("SELECT _1._2[0] FROM t"), data.map {
        case Tuple1((_, Seq(string))) => Row(string)
      })
    }
  }

  test("nested data - array of struct") {
    val data = (1 to 10).map(i => Tuple1(Seq(i -> s"val_$i")))
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

  test("SPARK-10634 timestamp written and read as INT64 - truncation") {
    withTable("ts") {
      sql("create table ts (c1 int, c2 timestamp) using parquet")
      sql("insert into ts values (1, timestamp'2016-01-01 10:11:12.123456')")
      sql("insert into ts values (2, null)")
      sql("insert into ts values (3, timestamp'1965-01-01 10:11:12.123456')")
      val expected = Seq(
        (1, "2016-01-01 10:11:12.123456"),
        (2, null),
        (3, "1965-01-01 10:11:12.123456"))
        .toDS().select($"_1", $"_2".cast("timestamp"))
      checkAnswer(sql("select * from ts"), expected)
    }
  }

  test("SPARK-36182, SPARK-47368: writing and reading TimestampNTZType column") {
    Seq("true", "false").foreach { inferNTZ =>
      // The SQL Conf PARQUET_INFER_TIMESTAMP_NTZ_ENABLED should not affect the file written
      // by Spark.
      withSQLConf(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key -> inferNTZ) {
        withTable("ts") {
          sql("create table ts (c1 timestamp_ntz) using parquet")
          sql("insert into ts values (timestamp_ntz'2016-01-01 10:11:12.123456')")
          sql("insert into ts values (null)")
          sql("insert into ts values (timestamp_ntz'1965-01-01 10:11:12.123456')")
          val expectedSchema = new StructType().add(StructField("c1", TimestampNTZType))
          assert(spark.table("ts").schema == expectedSchema)
          val expected = Seq(
            ("2016-01-01 10:11:12.123456"),
            (null),
            ("1965-01-01 10:11:12.123456"))
            .toDS().select($"value".cast("timestamp_ntz"))
          withAllParquetReaders {
            checkAnswer(sql("select * from ts"), expected)
          }
        }
      }
    }
  }

  test("SPARK-47447: read TimestampLTZ as TimestampNTZ") {
    val providedSchema = StructType(Seq(StructField("time", TimestampNTZType, false)))

    Seq("INT96", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS").foreach { tsType =>
      Seq(true, false).foreach { dictionaryEnabled =>
        withSQLConf(
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> tsType,
          ParquetOutputFormat.ENABLE_DICTIONARY -> dictionaryEnabled.toString,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
          withTempPath { file =>
            val df = sql("select timestamp'2021-02-02 16:00:00' as time")
            df.write.parquet(file.getCanonicalPath)
            withAllParquetReaders {
              val df2 = spark.read.schema(providedSchema).parquet(file.getCanonicalPath)
              checkAnswer(df2, Row(LocalDateTime.parse("2021-02-03T00:00:00")))
            }
          }
        }
      }
    }
  }

  test("SPARK-36182: read TimestampNTZ as TimestampLTZ") {
    val data = (1 to 1000).map { i =>
      // The second parameter is `nanoOfSecond`, while java.sql.Timestamp accepts milliseconds
      // as input. So here we multiple the `nanoOfSecond` by NANOS_PER_MILLIS
      val ts = LocalDateTime.ofEpochSecond(i / 1000, (i % 1000) * 1000000, ZoneOffset.UTC)
      Row(ts)
    }
    val answer = (1 to 1000).map { i =>
      val ts = new java.sql.Timestamp(i)
      Row(ts)
    }
    val actualSchema = StructType(Seq(StructField("time", TimestampNTZType, false)))
    val providedSchema = StructType(Seq(StructField("time", TimestampType, false)))

    withTempPath { file =>
      val df = spark.createDataFrame(sparkContext.parallelize(data), actualSchema)
      df.write.parquet(file.getCanonicalPath)
      withAllParquetReaders {
        Seq(true, false).foreach { dictionaryEnabled =>
          withSQLConf(ParquetOutputFormat.ENABLE_DICTIONARY -> dictionaryEnabled.toString) {
            checkAnswer(spark.read.schema(providedSchema).parquet(file.getCanonicalPath), answer)
          }
        }
      }
    }
  }

  test("SPARK-10365 timestamp written and read as INT64 - TIMESTAMP_MICROS") {
    val data = (1 to 10).map { i =>
      val ts = new java.sql.Timestamp(i)
      ts.setNanos(2000)
      Row(i, ts)
    }
    val schema = StructType(List(StructField("d", IntegerType, false),
      StructField("time", TimestampType, false)).toArray)
    withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS") {
      withTempPath { file =>
        val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
        df.write.parquet(file.getCanonicalPath)
        withAllParquetReaders {
          val df2 = spark.read.parquet(file.getCanonicalPath)
          checkAnswer(df2, df.collect().toSeq)
        }
      }
    }
  }

  test("SPARK-46466: write and read TimestampNTZ with legacy rebase mode") {
    withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "LEGACY") {
      withTable("ts") {
        sql("create table ts (c1 timestamp_ntz) using parquet")
        sql("insert into ts values (timestamp_ntz'0900-01-01 01:10:10')")
        withAllParquetReaders {
          checkAnswer(spark.table("ts"), sql("select timestamp_ntz'0900-01-01 01:10:10'"))
        }
      }
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

    withSQLConf(
      SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
        classOf[SQLHadoopMapReduceCommitProtocol].getCanonicalName,
      SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true",
      SQLConf.PARQUET_SCHEMA_RESPECT_SUMMARIES.key -> "true",
      ParquetOutputFormat.JOB_SUMMARY_LEVEL -> "ALL"
    ) {
      testSchemaMerging(2)
    }

    withSQLConf(
      SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
        classOf[SQLHadoopMapReduceCommitProtocol].getCanonicalName,
      SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true",
      SQLConf.PARQUET_SCHEMA_RESPECT_SUMMARIES.key -> "false"
    ) {
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

  test("Enabling/disabling ignoreCorruptFiles") {
    def testIgnoreCorruptFiles(options: Map[String, String]): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(1).toDF("a").write.parquet(new Path(basePath, "first").toString)
        spark.range(1, 2).toDF("a").write.parquet(new Path(basePath, "second").toString)
        spark.range(2, 3).toDF("a").write.json(new Path(basePath, "third").toString)
        val df = spark.read.options(options).parquet(
          new Path(basePath, "first").toString,
          new Path(basePath, "second").toString,
          new Path(basePath, "third").toString)
        checkAnswer(df, Seq(Row(0), Row(1)))
      }
    }

    def testIgnoreCorruptFilesWithoutSchemaInfer(options: Map[String, String]): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(1).toDF("a").write.parquet(new Path(basePath, "first").toString)
        spark.range(1, 2).toDF("a").write.parquet(new Path(basePath, "second").toString)
        spark.range(2, 3).toDF("a").write.json(new Path(basePath, "third").toString)
        val df = spark.read.options(options).schema("a long").parquet(
          new Path(basePath, "first").toString,
          new Path(basePath, "second").toString,
          new Path(basePath, "third").toString)
        checkAnswer(df, Seq(Row(0), Row(1)))
      }
    }

    // Test ignoreCorruptFiles = true
    Seq("SQLConf", "FormatOption").foreach { by =>
      val (sqlConf, options) = by match {
        case "SQLConf" => ("true", Map.empty[String, String])
        // Explicitly set SQLConf to false but still should ignore corrupt files
        case "FormatOption" => ("false", Map("ignoreCorruptFiles" -> "true"))
      }
      withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> sqlConf) {
        testIgnoreCorruptFiles(options)
        testIgnoreCorruptFilesWithoutSchemaInfer(options)
      }
    }

    // Test ignoreCorruptFiles = false
    Seq("SQLConf", "FormatOption").foreach { by =>
      val (sqlConf, options) = by match {
        case "SQLConf" => ("false", Map.empty[String, String])
        // Explicitly set SQLConf to true but still should not ignore corrupt files
        case "FormatOption" => ("true", Map("ignoreCorruptFiles" -> "false"))
      }

      withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> sqlConf) {
        val exception = intercept[SparkException] {
          testIgnoreCorruptFiles(options)
        }.getCause
        assert(exception.getMessage().contains("is not a Parquet file"))
        val exception2 = intercept[SparkException] {
          testIgnoreCorruptFilesWithoutSchemaInfer(options)
        }.getCause
        assert(exception2.getMessage().contains("is not a Parquet file"))
      }
    }
  }

  /**
   * this is part of test 'Enabling/disabling ignoreCorruptFiles' but run in a loop
   * to increase the chance of failure
    */
  ignore("SPARK-20407 ParquetQuerySuite 'Enabling/disabling ignoreCorruptFiles' flaky test") {
    def testIgnoreCorruptFiles(): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(1).toDF("a").write.parquet(new Path(basePath, "first").toString)
        spark.range(1, 2).toDF("a").write.parquet(new Path(basePath, "second").toString)
        spark.range(2, 3).toDF("a").write.json(new Path(basePath, "third").toString)
        val df = spark.read.parquet(
          new Path(basePath, "first").toString,
          new Path(basePath, "second").toString,
          new Path(basePath, "third").toString)
        checkAnswer(
          df,
          Seq(Row(0), Row(1)))
      }
    }

    for (i <- 1 to 100) {
      DebugFilesystem.clearOpenStreams()
      withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "false") {
        val exception = intercept[SparkException] {
          testIgnoreCorruptFiles()
        }
        assert(exception.getMessage().contains("is not a Parquet file"))
      }
      DebugFilesystem.assertNoOpenStreams()
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
      val rowRDD = sparkContext.parallelize(Seq(Row(Decimal("67123.45"))))
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
              .add("f1", new TestNestedStructUDT, nullable = true),
            nullable = true)

      checkAnswer(
        spark.read.schema(userDefinedSchema).parquet(path),
        Row(Row(TestNestedStruct(1, 2L, 3.5D))))
    }
  }

  testStandardAndLegacyModes("SPARK-39086: UDT read support in vectorized reader") {
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
            |  ),
            |  'f2', CAST(id + 4 AS INT),
            |  'f3', ARRAY(id + 5, id + 6)
            |) AS s
          """.stripMargin
        )
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("f0", StringType)
              .add("f1", new TestNestedStructUDT())
              .add("f2", new TestPrimitiveUDT())
              .add("f3", new TestArrayUDT())
          )

      Seq(true, false).foreach { enabled =>
        withSQLConf(
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
            SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> s"$enabled") {
          checkAnswer(
            spark.read.schema(userDefinedSchema).parquet(path),
            Row(Row("0", TestNestedStruct(1, 2L, 3.5D), TestPrimitive(4), TestArray(Seq(5, 6)))))
        }
      }
    }
  }

  test("SPARK-39086: support UDT type in ColumnVector") {
    val schema = StructType(
      StructField("col1", ArrayType(new TestPrimitiveUDT())) ::
      StructField("col2", ArrayType(new TestArrayUDT())) ::
      StructField("col3", ArrayType(new TestNestedStructUDT())) ::
      Nil)

    withTempPath { dir =>
      val rows = sparkContext.parallelize(0 until 2).map { _ =>
        Row(
          Seq(new TestPrimitive(1)),
          Seq(new TestArray(Seq(1L, 2L, 3L))),
          Seq(new TestNestedStruct(1, 2L, 3.0)))
      }
      val df = spark.createDataFrame(rows, schema)
      df.write.parquet(dir.getCanonicalPath)

      for (offHeapEnabled <- Seq(true, false)) {
        withSQLConf(SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offHeapEnabled.toString) {
          withAllParquetReaders {
            val res = spark.read.parquet(dir.getCanonicalPath)
            checkAnswer(res, df)
          }
        }
      }
    }
  }

  test("expand UDT in StructType") {
    val schema = new StructType().add("n", new TestNestedStructUDT, nullable = true)
    val expected = new StructType().add("n", new TestNestedStructUDT().sqlType, nullable = true)
    assert(ParquetReadSupport.expandUDT(schema) === expected)
  }

  test("expand UDT in ArrayType") {
    val schema = new StructType().add(
      "n",
      ArrayType(
        elementType = new TestNestedStructUDT,
        containsNull = false),
      nullable = true)

    val expected = new StructType().add(
      "n",
      ArrayType(
        elementType = new TestNestedStructUDT().sqlType,
        containsNull = false),
      nullable = true)

    assert(ParquetReadSupport.expandUDT(schema) === expected)
  }

  test("expand UDT in MapType") {
    val schema = new StructType().add(
      "n",
      MapType(
        keyType = IntegerType,
        valueType = new TestNestedStructUDT,
        valueContainsNull = false),
      nullable = true)

    val expected = new StructType().add(
      "n",
      MapType(
        keyType = IntegerType,
        valueType = new TestNestedStructUDT().sqlType,
        valueContainsNull = false),
      nullable = true)

    assert(ParquetReadSupport.expandUDT(schema) === expected)
  }

  test("SPARK-15719: disable writing summary files by default") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(3).write.parquet(path)

      val fs = FileSystem.get(spark.sessionState.newHadoopConf())
      val files = fs.listFiles(new Path(path), true)

      while (files.hasNext) {
        val file = files.next
        assert(!file.getPath.getName.contains("_metadata"))
      }
    }
  }

  test("SPARK-15804: write out the metadata to parquet file") {
    val df = Seq((1, "abc"), (2, "hello")).toDF("a", "b")
    val md = new MetadataBuilder().putString("key", "value").build()
    val dfWithmeta = df.select($"a", $"b".as("b", md))

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      dfWithmeta.write.parquet(path)

      readParquetFile(path) { df =>
        assert(df.schema.last.metadata.getString("key") == "value")
      }
    }
  }

  test("SPARK-16344: array of struct with a single field named 'element'") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      Seq(Tuple1(Array(SingleElement(42)))).toDF("f").write.parquet(path)

      checkAnswer(
        sqlContext.read.parquet(path),
        Row(Array(Row(42)))
      )
    }
  }

  test("SPARK-16632: read Parquet int32 as ByteType and ShortType") {
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        // When being written to Parquet, `TINYINT` and `SMALLINT` should be converted into
        // `int32 (INT_8)` and `int32 (INT_16)` respectively. However, Hive doesn't add the `INT_8`
        // and `INT_16` annotation properly (HIVE-14294). Thus, when reading files written by Hive
        // using Spark with the vectorized Parquet reader enabled, we may hit error due to type
        // mismatch.
        //
        // Here we are simulating Hive's behavior by writing a single `INT` field and then read it
        // back as `TINYINT` and `SMALLINT` in Spark to verify this issue.
        Seq(1).toDF("f").write.parquet(path)

        val withByteField = new StructType().add("f", ByteType)
        checkAnswer(spark.read.schema(withByteField).parquet(path), Row(1: Byte))

        val withShortField = new StructType().add("f", ShortType)
        checkAnswer(spark.read.schema(withShortField).parquet(path), Row(1: Short))
      }
    }
  }

  test("SPARK-24230: filter row group using dictionary") {
    withSQLConf(("parquet.filter.dictionary.enabled", "true")) {
      // create a table with values from 0, 2, ..., 18 that will be dictionary-encoded
      withParquetTable((0 until 100).map(i => ((i * 2) % 20, s"data-$i")), "t") {
        // search for a key that is not present so the dictionary filter eliminates all row groups
        // Fails without SPARK-24230:
        //   java.io.IOException: expecting more rows but reached last block. Read 0 out of 50
        checkAnswer(sql("SELECT _2 FROM t WHERE t._1 = 5"), Seq.empty)
      }
    }
  }

  test("SPARK-26677: negated null-safe equality comparison should not filter matched row groups") {
    withAllParquetReaders {
      withTempPath { path =>
        // Repeated values for dictionary encoding.
        Seq(Some("A"), Some("A"), None).toDF().repartition(1)
          .write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        checkAnswer(stripSparkFilter(df.where("NOT (value <=> 'A')")), df)
      }
    }
  }

  test("Migration from INT96 to TIMESTAMP_MICROS timestamp type") {
    def testMigration(fromTsType: String, toTsType: String): Unit = {
      def checkAppend(write: DataFrameWriter[_] => Unit, readback: => DataFrame): Unit = {
        def data(start: Int, end: Int): Seq[Row] = (start to end).map { i =>
          val ts = new java.sql.Timestamp(TimeUnit.SECONDS.toMillis(i))
          ts.setNanos(123456000)
          Row(ts)
        }
        val schema = new StructType().add("time", TimestampType)
        val df1 = spark.createDataFrame(sparkContext.parallelize(data(0, 1)), schema)
        withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> fromTsType) {
          write(df1.write)
        }
        val df2 = spark.createDataFrame(sparkContext.parallelize(data(2, 10)), schema)
        withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> toTsType) {
          write(df2.write.mode(SaveMode.Append))
        }
        withAllParquetReaders {
          checkAnswer(readback, df1.unionAll(df2))
        }
      }

      Seq(false, true).foreach { mergeSchema =>
        withTempPath { file =>
          checkAppend(_.parquet(file.getCanonicalPath),
            spark.read.option("mergeSchema", mergeSchema).parquet(file.getCanonicalPath))
        }

        withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> mergeSchema.toString) {
          val tableName = "parquet_timestamp_migration"
          withTable(tableName) {
            checkAppend(_.saveAsTable(tableName), spark.table(tableName))
          }
        }
      }
    }

    testMigration(fromTsType = "INT96", toTsType = "TIMESTAMP_MICROS")
    testMigration(fromTsType = "TIMESTAMP_MICROS", toTsType = "INT96")
  }

  test("SPARK-34212 Parquet should read decimals correctly") {
    def readParquet(schema: String, path: File): DataFrame = {
      spark.read.schema(schema).parquet(path.toString)
    }

    withTempPath { path =>
      // a is int-decimal (4 bytes), b is long-decimal (8 bytes), c is binary-decimal (16 bytes)
      val df = sql("SELECT 1.0 a, CAST(1.23 AS DECIMAL(17, 2)) b, CAST(1.23 AS DECIMAL(36, 2)) c")
      df.write.parquet(path.toString)

      withAllParquetReaders {
        // We can read the decimal parquet field with a larger precision, if scale is the same.
        val schema1 = "a DECIMAL(9, 1), b DECIMAL(18, 2), c DECIMAL(38, 2)"
        checkAnswer(readParquet(schema1, path), df)
        val schema2 = "a DECIMAL(18, 1), b DECIMAL(38, 2), c DECIMAL(38, 2)"
        checkAnswer(readParquet(schema2, path), df)
      }

      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        val schema1 = "a DECIMAL(3, 2), b DECIMAL(18, 3), c DECIMAL(37, 3)"
        checkAnswer(readParquet(schema1, path), df)
        val schema2 = "a DECIMAL(3, 0), b DECIMAL(18, 1), c DECIMAL(37, 1)"
        checkAnswer(readParquet(schema2, path), Row(1, 1.2, 1.2))
      }

      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
       val schema1 = "a DECIMAL(3, 2), b DECIMAL(18, 3), c DECIMAL(37, 3)"
        checkAnswer(readParquet(schema1, path), df)
        Seq("a DECIMAL(3, 0)", "b DECIMAL(18, 1)", "c DECIMAL(37, 1)").foreach { schema =>
          val e = intercept[SparkException] {
            readParquet(schema, path).collect()
          }.getCause
          assert(e.isInstanceOf[SchemaColumnConvertNotSupportedException])
        }
      }
    }

    // tests for parquet types without decimal metadata.
    withTempPath { path =>
      val df = sql(s"SELECT 1 a, 123456 b, ${Int.MaxValue.toLong * 10} c, CAST('1.2' AS BINARY) d")
      df.write.parquet(path.toString)

      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        checkAnswer(readParquet("a DECIMAL(3, 2)", path), sql("SELECT 1.00"))
        checkAnswer(readParquet("a DECIMAL(11, 2)", path), sql("SELECT 1.00"))
        checkAnswer(readParquet("b DECIMAL(3, 2)", path), Row(null))
        checkAnswer(readParquet("b DECIMAL(11, 1)", path), sql("SELECT 123456.0"))
        checkAnswer(readParquet("c DECIMAL(11, 1)", path), Row(null))
        checkAnswer(readParquet("c DECIMAL(13, 0)", path), df.select("c"))
        checkAnswer(readParquet("c DECIMAL(22, 0)", path), df.select("c"))
        val e = intercept[SparkException] {
          readParquet("d DECIMAL(3, 2)", path).collect()
        }
        assert(e.getErrorClass.startsWith("FAILED_READ_FILE"))
        assert(e.getCause.getMessage.contains("Please read this column/field as Spark BINARY type"))
      }

      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
        Seq("a DECIMAL(3, 2)", "c DECIMAL(18, 1)", "d DECIMAL(37, 1)").foreach { schema =>
          val e = intercept[SparkException] {
            readParquet(schema, path).collect()
          }.getCause
          assert(e.isInstanceOf[SchemaColumnConvertNotSupportedException])
        }
      }
    }
  }

  test("SPARK-37191: Merge schema for DecimalType with different precision") {
    withTempPath { path =>
      val data1 = Seq(Row(new BigDecimal("123456789.11")))
      val schema1 = StructType(StructField("col", DecimalType(12, 2)) :: Nil)

      val data2 = Seq(Row(new BigDecimal("1234567890000.11")))
      val schema2 = StructType(StructField("col", DecimalType(17, 2)) :: Nil)

      spark.createDataFrame(sparkContext.parallelize(data1, 1), schema1)
        .write.parquet(path.toString)
      spark.createDataFrame(sparkContext.parallelize(data2, 1), schema2)
        .write.mode("append").parquet(path.toString)

      withAllParquetReaders {
        val res = spark.read.option("mergeSchema", "true").parquet(path.toString)
        assert(res.schema("col").dataType == DecimalType(17, 2))
        checkAnswer(res, data1 ++ data2)
      }
    }
  }

  test("row group skipping doesn't overflow when reading into larger type") {
    withTempPath { path =>
      Seq(0).toDF("a").write.parquet(path.toString)
      withAllParquetReaders {
        val result =
          spark.read
            .schema("a LONG")
            .parquet(path.toString)
            .where(s"a < ${Long.MaxValue}")
        checkAnswer(result, Row(0))
      }
    }
  }

  test("SPARK-36825, SPARK-36852: create table with ANSI intervals") {
    withTable("tbl") {
      sql("create table tbl (c1 interval day, c2 interval year to month) using parquet")
      sql("insert into tbl values (interval '100' day, interval '1-11' year to month)")
      sql("insert into tbl values (null, null)")
      sql("insert into tbl values (interval '-100' day, interval -'1-11' year to month)")
      val expected = Seq(
        (Duration.ofDays(100), Period.ofYears(1).plusMonths(11)),
        (null, null),
        (Duration.ofDays(100).negated(), Period.ofYears(1).plusMonths(11).negated())).toDF()
      checkAnswer(sql("select * from tbl"), expected)
    }
  }

  test("SPARK-44805: cast of struct with two arrays") {
    withTable("tbl") {
      sql("create table tbl (value struct<f1:array<int>,f2:array<int>>) using parquet")
      sql("insert into tbl values (named_struct('f1', array(1, 2, 3), 'f2', array(1, 1, 2)))")
      val df = sql("select cast(value as struct<f1:array<double>,f2:array<int>>) AS value from tbl")
      val expected = Row(Row(Array(1.0d, 2.0d, 3.0d), Array(1, 1, 2))) :: Nil
      checkAnswer(df, expected)
    }
  }
}

class ParquetV1QuerySuite extends ParquetQuerySuite {
  import testImplicits._

  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "parquet")

  test("returning batch for wide table") {
    withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "10") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        val df = spark.range(10).select(Seq.tabulate(11) {i => ($"id" + i).as(s"c$i")} : _*)
        df.write.mode(SaveMode.Overwrite).parquet(path)

        // do not return batch - whole stage codegen is disabled for wide table (>200 columns)
        val df2 = spark.read.parquet(path)
        val fileScan2 = df2.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
        assert(!fileScan2.asInstanceOf[FileSourceScanExec].supportsColumnar)
        checkAnswer(df2, df)

        // return batch
        val columns = Seq.tabulate(9) {i => s"c$i"}
        val df3 = df2.selectExpr(columns : _*)
        val fileScan3 = df3.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
        assert(fileScan3.asInstanceOf[FileSourceScanExec].supportsColumnar)
        checkAnswer(df3, df.selectExpr(columns : _*))

        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true") {
          val df4 = spark.range(10).select(struct(
            Seq.tabulate(11) {i => ($"id" + i).as(s"c$i")} : _*).as("nested"))
          df4.write.mode(SaveMode.Overwrite).parquet(path)

          // do not return batch - whole stage codegen is disabled for wide table (>200 columns)
          val df5 = spark.read.parquet(path)
          val fileScan5 = df5.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
          assert(!fileScan5.asInstanceOf[FileSourceScanExec].supportsColumnar)
          checkAnswer(df5, df4)

          // return batch
          val columns2 = Seq.tabulate(9) {i => s"nested.c$i"}
          val df6 = df5.selectExpr(columns2 : _*)
          val fileScan6 = df6.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
          assert(fileScan6.asInstanceOf[FileSourceScanExec].supportsColumnar)
          checkAnswer(df6, df4.selectExpr(columns2 : _*))
        }
      }
    }
  }

  test("SPARK-39833: pushed filters with count()") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        withTempPath { path =>
          val p = s"${path.getCanonicalPath}${File.separator}col=0${File.separator}"
          Seq(0).toDF("COL").coalesce(1).write.save(p)
          val df = spark.read.parquet(path.getCanonicalPath)
          val expected = if (caseSensitive) Seq(Row(0, 0)) else Seq(Row(0))
          checkAnswer(df.filter("col = 0"), expected)
          assert(df.filter("col = 0").count() == 1, "col")
          assert(df.filter("COL = 0").count() == 1, "COL")
        }
      }
    }
  }

  test("SPARK-39833: pushed filters with project without filter columns") {
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        withTempPath { path =>
          val p = s"${path.getCanonicalPath}${File.separator}col=0${File.separator}"
          Seq((0, 1)).toDF("COL", "a").coalesce(1).write.save(p)
          val df = spark.read.parquet(path.getCanonicalPath)
          val expected = if (caseSensitive) Seq(Row(0, 1, 0)) else Seq(Row(0, 1))
          checkAnswer(df.filter("col = 0"), expected)
          assert(df.filter("col = 0").select("a").collect().toSeq == Row(1) :: Nil)
          assert(df.filter("col = 0 and a = 1").select("a").collect().toSeq == Row(1) :: Nil)
        }
      }
    }
  }
}

class ParquetV2QuerySuite extends ParquetQuerySuite {
  import testImplicits._

  // TODO: enable Parquet V2 write path after file source V2 writers are workable.
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("returning batch for wide table") {
    withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "10") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        val df = spark.range(10).select(Seq.tabulate(11) {i => ($"id" + i).as(s"c$i")} : _*)
        df.write.mode(SaveMode.Overwrite).parquet(path)

        // do not return batch - whole stage codegen is disabled for wide table (>200 columns)
        val df2 = spark.read.parquet(path)
        val fileScan2 = df2.queryExecution.sparkPlan.find(_.isInstanceOf[BatchScanExec]).get
        val parquetScan2 = fileScan2.asInstanceOf[BatchScanExec].scan.asInstanceOf[ParquetScan]
        // The method `supportColumnarReads` in Parquet doesn't depends on the input partition.
        // Here we can pass null input partition to the method for testing propose.
        assert(!parquetScan2.createReaderFactory().supportColumnarReads(null))
        checkAnswer(df2, df)

        // return batch
        val columns = Seq.tabulate(9) {i => s"c$i"}
        val df3 = df2.selectExpr(columns : _*)
        val fileScan3 = df3.queryExecution.sparkPlan.find(_.isInstanceOf[BatchScanExec]).get
        val parquetScan3 = fileScan3.asInstanceOf[BatchScanExec].scan.asInstanceOf[ParquetScan]
        assert(parquetScan3.createReaderFactory().supportColumnarReads(null))
        checkAnswer(df3, df.selectExpr(columns : _*))

        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true") {
          val df4 = spark.range(10).select(struct(
            Seq.tabulate(11) {i => ($"id" + i).as(s"c$i")} : _*).as("nested"))
          df4.write.mode(SaveMode.Overwrite).parquet(path)

          // do not return batch - whole stage codegen is disabled for wide table (>200 columns)
          val df5 = spark.read.parquet(path)
          val fileScan5 = df5.queryExecution.sparkPlan.find(_.isInstanceOf[BatchScanExec]).get
          val parquetScan5 = fileScan5.asInstanceOf[BatchScanExec].scan.asInstanceOf[ParquetScan]
          // The method `supportColumnarReads` in Parquet doesn't depends on the input partition.
          // Here we can pass null input partition to the method for testing propose.
          assert(!parquetScan5.createReaderFactory().supportColumnarReads(null))
          checkAnswer(df5, df4)

          // return batch
          val columns2 = Seq.tabulate(9) {i => s"nested.c$i"}
          val df6 = df5.selectExpr(columns2 : _*)
          val fileScan6 = df6.queryExecution.sparkPlan.find(_.isInstanceOf[BatchScanExec]).get
          val parquetScan6 = fileScan6.asInstanceOf[BatchScanExec].scan.asInstanceOf[ParquetScan]
          assert(parquetScan6.createReaderFactory().supportColumnarReads(null))
          checkAnswer(df6, df4.selectExpr(columns2 : _*))
        }
      }
    }
  }
}

object TestingUDT {
  case class SingleElement(element: Long)

  @SQLUserDefinedType(udt = classOf[TestNestedStructUDT])
  case class TestNestedStruct(a: Integer, b: Long, c: Double)

  class TestNestedStructUDT extends UserDefinedType[TestNestedStruct] {
    override def sqlType: DataType =
      new StructType()
        .add("a", IntegerType, nullable = true)
        .add("b", LongType, nullable = false)
        .add("c", DoubleType, nullable = false)

    override def serialize(n: TestNestedStruct): Any = {
      val row = new SpecificInternalRow(sqlType.asInstanceOf[StructType].map(_.dataType))
      row.setInt(0, n.a)
      row.setLong(1, n.b)
      row.setDouble(2, n.c)
      row
    }

    override def userClass: Class[TestNestedStruct] = classOf[TestNestedStruct]

    override def deserialize(datum: Any): TestNestedStruct = {
      datum match {
        case row: InternalRow =>
          TestNestedStruct(row.getInt(0), row.getLong(1), row.getDouble(2))
      }
    }
  }

  @SQLUserDefinedType(udt = classOf[TestArrayUDT])
  case class TestArray(value: Seq[Long])

  class TestArrayUDT extends UserDefinedType[TestArray] {
    override def sqlType: DataType = ArrayType(LongType)

    override def serialize(obj: TestArray): Any = ArrayData.toArrayData(obj.value.toArray)

    override def userClass: Class[TestArray] = classOf[TestArray]

    override def deserialize(datum: Any): TestArray = datum match {
      case value: ArrayData => TestArray(value.toLongArray().toSeq)
    }
  }

  @SQLUserDefinedType(udt = classOf[TestPrimitiveUDT])
  case class TestPrimitive(value: Int)

  class TestPrimitiveUDT extends UserDefinedType[TestPrimitive] {
    override def sqlType: DataType = IntegerType

    override def serialize(obj: TestPrimitive): Any = obj.value

    override def userClass: Class[TestPrimitive] = classOf[TestPrimitive]

    override def deserialize(datum: Any): TestPrimitive = datum match {
      case value: Int => TestPrimitive(value)
    }
  }
}
