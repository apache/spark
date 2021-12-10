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
import java.math.BigInteger
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.util.Locale

import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.localDateTimeToMicros
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.{PartitionPath => Partition}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, FileTable}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.TimestampTypes
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// The data where the partitioning key exists only in the directory structure.
case class ParquetData(intField: Int, stringField: String)

// The data that also includes the partitioning key
case class ParquetDataWithKey(intField: Int, pi: Int, stringField: String, ps: String)

abstract class ParquetPartitionDiscoverySuite
  extends QueryTest with ParquetTest with SharedSparkSession {
  import PartitioningUtils._
  import testImplicits._

  val defaultPartitionName = ExternalCatalogUtils.DEFAULT_PARTITION_NAME

  val timeZoneId = ZoneId.systemDefault()
  val df = DateFormatter()
  val tf = TimestampFormatter(
    timestampPartitionPattern, timeZoneId, isParsing = true)

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.DEFAULT_DATA_SOURCE_NAME.key, "parquet")
  }

  protected override def afterAll(): Unit = {
    spark.conf.unset(SQLConf.DEFAULT_DATA_SOURCE_NAME.key)
    super.afterAll()
  }

  test("column type inference") {
    def check(raw: String, dataType: DataType, zoneId: ZoneId = timeZoneId): Unit = {
      assert(inferPartitionColumnValue(raw, true, zoneId, df, tf) === dataType)
    }

    check("10", IntegerType)
    check("1000000000000000", LongType)
    val decimal = Decimal("1" * 20)
    check("1" * 20, DecimalType(decimal.precision, decimal.scale))
    check("1.5", DoubleType)
    check("hello", StringType)
    check("1990-02-24", DateType)
    // The inferred timestmap type is consistent with the value of `SQLConf.TIMESTAMP_TYPE`
    Seq(TimestampTypes.TIMESTAMP_LTZ, TimestampTypes.TIMESTAMP_NTZ).foreach { tsType =>
      withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> tsType.toString) {
        check("1990-02-24 12:00:30", SQLConf.get.timestampType)
        check("1990-02-24 12:00:30", SQLConf.get.timestampType, ZoneOffset.UTC)
      }
    }

    check(defaultPartitionName, NullType)
  }

  test("parse invalid partitioned directories") {
    // Invalid
    var paths = Seq(
      "hdfs://host:9000/invalidPath",
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/a=10.5/b=hello")

    var exception = intercept[AssertionError] {
      parsePartitions(paths.map(new Path(_)), true, Set.empty[Path], None, true, true, timeZoneId)
    }
    assert(exception.getMessage().contains("Conflicting directory structures detected"))

    // Valid
    paths = Seq(
      "hdfs://host:9000/path/_temporary",
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/_temporary/path")

    parsePartitions(
      paths.map(new Path(_)),
      true,
      Set(new Path("hdfs://host:9000/path/")),
      None,
      true,
      true,
      timeZoneId)

    // Valid
    paths = Seq(
      "hdfs://host:9000/path/something=true/table/",
      "hdfs://host:9000/path/something=true/table/_temporary",
      "hdfs://host:9000/path/something=true/table/a=10/b=20",
      "hdfs://host:9000/path/something=true/table/_temporary/path")

    parsePartitions(
      paths.map(new Path(_)),
      true,
      Set(new Path("hdfs://host:9000/path/something=true/table")),
      None,
      true,
      true,
      timeZoneId)

    // Valid
    paths = Seq(
      "hdfs://host:9000/path/table=true/",
      "hdfs://host:9000/path/table=true/_temporary",
      "hdfs://host:9000/path/table=true/a=10/b=20",
      "hdfs://host:9000/path/table=true/_temporary/path")

    parsePartitions(
      paths.map(new Path(_)),
      true,
      Set(new Path("hdfs://host:9000/path/table=true")),
      None,
      true,
      true,
      timeZoneId)

    // Invalid
    paths = Seq(
      "hdfs://host:9000/path/_temporary",
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/path1")

    exception = intercept[AssertionError] {
      parsePartitions(
        paths.map(new Path(_)),
        true,
        Set(new Path("hdfs://host:9000/path/")),
        None,
        true,
        true,
        timeZoneId)
    }
    assert(exception.getMessage().contains("Conflicting directory structures detected"))

    // Invalid
    // Conflicting directory structure:
    // "hdfs://host:9000/tmp/tables/partitionedTable"
    // "hdfs://host:9000/tmp/tables/nonPartitionedTable1"
    // "hdfs://host:9000/tmp/tables/nonPartitionedTable2"
    paths = Seq(
      "hdfs://host:9000/tmp/tables/partitionedTable",
      "hdfs://host:9000/tmp/tables/partitionedTable/p=1/",
      "hdfs://host:9000/tmp/tables/nonPartitionedTable1",
      "hdfs://host:9000/tmp/tables/nonPartitionedTable2")

    exception = intercept[AssertionError] {
      parsePartitions(
        paths.map(new Path(_)),
        true,
        Set(new Path("hdfs://host:9000/tmp/tables/")),
        None,
        true,
        true,
        timeZoneId)
    }
    assert(exception.getMessage().contains("Conflicting directory structures detected"))
  }

  test("parse partition") {
    def check(path: String, expected: Option[PartitionValues]): Unit = {
      val actual = parsePartition(new Path(path), true, Set.empty[Path],
        Map.empty, true, timeZoneId, df, tf)._1
      assert(expected === actual)
    }

    def checkThrows[T <: Throwable: Manifest](path: String, expected: String): Unit = {
      val message = intercept[T] {
        parsePartition(new Path(path), true, Set.empty[Path], Map.empty, true, timeZoneId, df, tf)
      }.getMessage

      assert(message.contains(expected))
    }

    check("file://path/a=10", Some {
      PartitionValues(
        Seq("a"),
        Seq(TypedPartValue("10", IntegerType)))
    })

    check("file://path/a=10/b=hello/c=1.5", Some {
      PartitionValues(
        Seq("a", "b", "c"),
        Seq(
          TypedPartValue("10", IntegerType),
          TypedPartValue("hello", StringType),
          TypedPartValue("1.5", DoubleType)))
    })

    check("file://path/a=10/b_hello/c=1.5", Some {
      PartitionValues(
        Seq("c"),
        Seq(TypedPartValue("1.5", DoubleType)))
    })

    check("file:///", None)
    check("file:///path/_temporary", None)
    check("file:///path/_temporary/c=1.5", None)
    check("file:///path/_temporary/path", None)
    check("file://path/a=10/_temporary/c=1.5", None)
    check("file://path/a=10/c=1.5/_temporary", None)

    checkThrows[AssertionError]("file://path/=10", "Empty partition column name")
    checkThrows[AssertionError]("file://path/a=", "Empty partition column value")
  }

  test("parse partition with base paths") {
    // when the basePaths is the same as the path to a leaf directory
    val partitionSpec1: Option[PartitionValues] = parsePartition(
      path = new Path("file://path/a=10"),
      typeInference = true,
      basePaths = Set(new Path("file://path/a=10")),
      Map.empty,
      true,
      zoneId = timeZoneId,
      df,
      tf)._1

    assert(partitionSpec1.isEmpty)

    // when the basePaths is the path to a base directory of leaf directories
    val partitionSpec2: Option[PartitionValues] = parsePartition(
      path = new Path("file://path/a=10"),
      typeInference = true,
      basePaths = Set(new Path("file://path")),
      Map.empty,
      true,
      zoneId = timeZoneId,
      df,
      tf)._1

    assert(partitionSpec2 ==
      Option(PartitionValues(
        Seq("a"),
        Seq(TypedPartValue("10", IntegerType)))))
  }

  test("parse partitions") {
    def check(
        paths: Seq[String],
        spec: PartitionSpec,
        rootPaths: Set[Path] = Set.empty[Path]): Unit = {
      val actualSpec =
        parsePartitions(
          paths.map(new Path(_)),
          true,
          rootPaths,
          None,
          true,
          true,
          timeZoneId)
      assert(actualSpec.partitionColumns === spec.partitionColumns)
      assert(actualSpec.partitions.length === spec.partitions.length)
      actualSpec.partitions.zip(spec.partitions).foreach { case (actual, expected) =>
        assert(actual === expected)
      }
      assert(actualSpec === spec)
    }

    check(Seq(
      "hdfs://host:9000/path/a=10/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType))),
        Seq(Partition(InternalRow(10, UTF8String.fromString("hello")),
          "hdfs://host:9000/path/a=10/b=hello"))))

    check(Seq(
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/a=10.5/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", DoubleType),
          StructField("b", StringType))),
        Seq(
          Partition(InternalRow(10, UTF8String.fromString("20")),
            "hdfs://host:9000/path/a=10/b=20"),
          Partition(InternalRow(10.5, UTF8String.fromString("hello")),
            "hdfs://host:9000/path/a=10.5/b=hello"))))

    check(Seq(
      "hdfs://host:9000/path/_temporary",
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/a=10.5/b=hello",
      "hdfs://host:9000/path/a=10.5/_temporary",
      "hdfs://host:9000/path/a=10.5/_TeMpOrArY",
      "hdfs://host:9000/path/a=10.5/b=hello/_temporary",
      "hdfs://host:9000/path/a=10.5/b=hello/_TEMPORARY",
      "hdfs://host:9000/path/_temporary/path",
      "hdfs://host:9000/path/a=11/_temporary/path",
      "hdfs://host:9000/path/a=10.5/b=world/_temporary/path"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", DoubleType),
          StructField("b", StringType))),
        Seq(
          Partition(InternalRow(10, UTF8String.fromString("20")),
            "hdfs://host:9000/path/a=10/b=20"),
          Partition(InternalRow(10.5, UTF8String.fromString("hello")),
            "hdfs://host:9000/path/a=10.5/b=hello"))))

    check(Seq(
      s"hdfs://host:9000/path/a=10/b=20",
      s"hdfs://host:9000/path/a=$defaultPartitionName/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType))),
        Seq(
          Partition(InternalRow(10, UTF8String.fromString("20")),
            s"hdfs://host:9000/path/a=10/b=20"),
          Partition(InternalRow(null, UTF8String.fromString("hello")),
            s"hdfs://host:9000/path/a=$defaultPartitionName/b=hello"))))

    check(Seq(
      s"hdfs://host:9000/path/a=10/b=$defaultPartitionName",
      s"hdfs://host:9000/path/a=10.5/b=$defaultPartitionName"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", DoubleType),
          StructField("b", NullType))),
        Seq(
          Partition(InternalRow(10, null), s"hdfs://host:9000/path/a=10/b=$defaultPartitionName"),
          Partition(InternalRow(10.5, null),
            s"hdfs://host:9000/path/a=10.5/b=$defaultPartitionName"))))

    check(Seq(
      s"hdfs://host:9000/path1",
      s"hdfs://host:9000/path2"),
      PartitionSpec.emptySpec)

    // The inferred timestmap type is consistent with the value of `SQLConf.TIMESTAMP_TYPE`
    Seq(TimestampTypes.TIMESTAMP_LTZ, TimestampTypes.TIMESTAMP_NTZ).foreach { tsType =>
      withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> tsType.toString) {
        // The cases below check the resolution for type conflicts.
        val t1 = if (tsType == TimestampTypes.TIMESTAMP_LTZ) {
          Timestamp.valueOf("2014-01-01 00:00:00.0").getTime * 1000
        } else {
          localDateTimeToMicros(LocalDateTime.parse("2014-01-01T00:00:00"))
        }
        val t2 = if (tsType == TimestampTypes.TIMESTAMP_LTZ) {
          Timestamp.valueOf("2014-01-01 00:01:00.0").getTime * 1000
        } else {
          localDateTimeToMicros(LocalDateTime.parse("2014-01-01T00:01:00"))
        }
        // Values in column 'a' are inferred as null, date and timestamp each, and timestamp is set
        // as a common type.
        // Values in column 'b' are inferred as integer, decimal(22, 0) and null, and decimal(22, 0)
        // is set as a common type.
        check(Seq(
          s"hdfs://host:9000/path/a=$defaultPartitionName/b=0",
          s"hdfs://host:9000/path/a=2014-01-01/b=${Long.MaxValue}111",
          s"hdfs://host:9000/path/a=2014-01-01 00%3A01%3A00.0/b=$defaultPartitionName"),
          PartitionSpec(
            StructType(Seq(
              StructField("a", SQLConf.get.timestampType),
              StructField("b", DecimalType(22, 0)))),
            Seq(
              Partition(
                InternalRow(null, Decimal(0)),
                s"hdfs://host:9000/path/a=$defaultPartitionName/b=0"),
              Partition(
                InternalRow(t1, Decimal(s"${Long.MaxValue}111")),
                s"hdfs://host:9000/path/a=2014-01-01/b=${Long.MaxValue}111"),
              Partition(
                InternalRow(t2, null),
                s"hdfs://host:9000/path/a=2014-01-01 00%3A01%3A00.0/b=$defaultPartitionName"))))
      }
    }
  }

  test("parse partitions with type inference disabled") {
    def check(paths: Seq[String], spec: PartitionSpec): Unit = {
      val actualSpec =
        parsePartitions(paths.map(new Path(_)), false, Set.empty[Path], None,
          true, true, timeZoneId)
      assert(actualSpec === spec)
    }

    check(Seq(
      "hdfs://host:9000/path/a=10/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", StringType),
          StructField("b", StringType))),
        Seq(Partition(InternalRow(UTF8String.fromString("10"), UTF8String.fromString("hello")),
          "hdfs://host:9000/path/a=10/b=hello"))))

    check(Seq(
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/a=10.5/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", StringType),
          StructField("b", StringType))),
        Seq(
          Partition(InternalRow(UTF8String.fromString("10"), UTF8String.fromString("20")),
            "hdfs://host:9000/path/a=10/b=20"),
          Partition(InternalRow(UTF8String.fromString("10.5"), UTF8String.fromString("hello")),
            "hdfs://host:9000/path/a=10.5/b=hello"))))

    check(Seq(
      "hdfs://host:9000/path/_temporary",
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/a=10.5/b=hello",
      "hdfs://host:9000/path/a=10.5/_temporary",
      "hdfs://host:9000/path/a=10.5/_TeMpOrArY",
      "hdfs://host:9000/path/a=10.5/b=hello/_temporary",
      "hdfs://host:9000/path/a=10.5/b=hello/_TEMPORARY",
      "hdfs://host:9000/path/_temporary/path",
      "hdfs://host:9000/path/a=11/_temporary/path",
      "hdfs://host:9000/path/a=10.5/b=world/_temporary/path"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", StringType),
          StructField("b", StringType))),
        Seq(
          Partition(InternalRow(UTF8String.fromString("10"), UTF8String.fromString("20")),
            "hdfs://host:9000/path/a=10/b=20"),
          Partition(InternalRow(UTF8String.fromString("10.5"), UTF8String.fromString("hello")),
            "hdfs://host:9000/path/a=10.5/b=hello"))))

    check(Seq(
      s"hdfs://host:9000/path/a=10/b=20",
      s"hdfs://host:9000/path/a=$defaultPartitionName/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", StringType),
          StructField("b", StringType))),
        Seq(
          Partition(InternalRow(UTF8String.fromString("10"), UTF8String.fromString("20")),
            s"hdfs://host:9000/path/a=10/b=20"),
          Partition(InternalRow(null, UTF8String.fromString("hello")),
            s"hdfs://host:9000/path/a=$defaultPartitionName/b=hello"))))

    check(Seq(
      s"hdfs://host:9000/path/a=10/b=$defaultPartitionName",
      s"hdfs://host:9000/path/a=10.5/b=$defaultPartitionName"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", StringType),
          StructField("b", NullType))),
        Seq(
          Partition(InternalRow(UTF8String.fromString("10"), null),
            s"hdfs://host:9000/path/a=10/b=$defaultPartitionName"),
          Partition(InternalRow(UTF8String.fromString("10.5"), null),
            s"hdfs://host:9000/path/a=10.5/b=$defaultPartitionName"))))

    check(Seq(
      s"hdfs://host:9000/path1",
      s"hdfs://host:9000/path2"),
      PartitionSpec.emptySpec)
  }

  test("read partitioned table - normal case") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", "bar")
      } {
        val dir = makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps)
        makeParquetFile(
          (1 to 10).map(i => ParquetData(i, i.toString)),
          dir)
        // Introduce _temporary dir to test the robustness of the schema discovery process.
        new File(dir.toString, "_temporary").mkdir()
      }
      // Introduce _temporary dir to the base dir the robustness of the schema discovery process.
      new File(base.getCanonicalPath, "_temporary").mkdir()

      spark.read.parquet(base.getCanonicalPath).createOrReplaceTempView("t")

      withTempView("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT intField, pi FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            _ <- Seq("foo", "bar")
          } yield Row(i, pi))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi = 1"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, 1, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps = 'foo'"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, i.toString, pi, "foo"))
      }
    }
  }

  test("read partitioned table using different path options") {
    withTempDir { base =>
      val pi = 1
      val ps = "foo"
      val path = makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps)
      makeParquetFile(
        (1 to 10).map(i => ParquetData(i, i.toString)), path)

      // when the input is the base path containing partitioning directories
      val baseDf = spark.read.parquet(base.getCanonicalPath)
      assert(baseDf.schema.map(_.name) === Seq("intField", "stringField", "pi", "ps"))

      // when the input is a path to the leaf directory containing a parquet file
      val partDf = spark.read.parquet(path.getCanonicalPath)
      assert(partDf.schema.map(_.name) === Seq("intField", "stringField"))

      path.listFiles().foreach { f =>
        if (!f.getName.startsWith("_") &&
            f.getName.toLowerCase(Locale.ROOT).endsWith(".parquet")) {
          // when the input is a path to a parquet file
          val df = spark.read.parquet(f.getCanonicalPath)
          assert(df.schema.map(_.name) === Seq("intField", "stringField"))
        }
      }

      path.listFiles().foreach { f =>
        if (!f.getName.startsWith("_") &&
            f.getName.toLowerCase(Locale.ROOT).endsWith(".parquet")) {
          // when the input is a path to a parquet file but `basePath` is overridden to
          // the base path containing partitioning directories
          val df = spark
            .read.option("basePath", base.getCanonicalPath)
            .parquet(f.getCanonicalPath)
          assert(df.schema.map(_.name) === Seq("intField", "stringField", "pi", "ps"))
        }
      }
    }
  }

  test("read partitioned table - with nulls") {
    withTempDir { base =>
      for {
        // Must be `Integer` rather than `Int` here. `null.asInstanceOf[Int]` results in a zero...
        pi <- Seq(1, null.asInstanceOf[Integer])
        ps <- Seq("foo", null.asInstanceOf[String])
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetData(i, i.toString)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      val parquetRelation = spark.read.format("parquet").load(base.getCanonicalPath)
      parquetRelation.createOrReplaceTempView("t")

      withTempView("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, null.asInstanceOf[Integer])
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi IS NULL"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, i.toString, null, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps IS NULL"),
          for {
            i <- 1 to 10
            pi <- Seq(1, null.asInstanceOf[Integer])
          } yield Row(i, i.toString, pi, null))
      }
    }
  }

  test("read partitioned table - merging compatible schemas") {
    withTempDir { base =>
      makeParquetFile(
        (1 to 10).map(i => Tuple1(i)).toDF("intField"),
        makePartitionDir(base, defaultPartitionName, "pi" -> 1))

      makeParquetFile(
        (1 to 10).map(i => (i, i.toString)).toDF("intField", "stringField"),
        makePartitionDir(base, defaultPartitionName, "pi" -> 2))

      spark
        .read
        .option("mergeSchema", "true")
        .format("parquet")
        .load(base.getCanonicalPath)
        .createOrReplaceTempView("t")

      withTempView("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          (1 to 10).map(i => Row(i, null, 1)) ++ (1 to 10).map(i => Row(i, i.toString, 2)))
      }
    }
  }

  test("SPARK-7847: Dynamic partition directory path escaping and unescaping") {
    withTempPath { dir =>
      val df = Seq("/", "[]", "?").zipWithIndex.map(_.swap).toDF("i", "s")
      df.write.format("parquet").partitionBy("s").save(dir.getCanonicalPath)
      checkAnswer(spark.read.parquet(dir.getCanonicalPath), df.collect())
    }
  }

  test("Various partition value types") {
    Seq(TimestampTypes.TIMESTAMP_LTZ, TimestampTypes.TIMESTAMP_NTZ).foreach { tsType =>
      withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> tsType.toString) {
        val ts = if (tsType == TimestampTypes.TIMESTAMP_LTZ) {
          new Timestamp(0)
        } else {
          LocalDateTime.parse("1970-01-01T00:00:00")
        }
        val row =
          Row(
            100.toByte,
            40000.toShort,
            Int.MaxValue,
            Long.MaxValue,
            1.5.toFloat,
            4.5,
            new java.math.BigDecimal(new BigInteger("212500"), 5),
            new java.math.BigDecimal("2.125"),
            java.sql.Date.valueOf("2015-05-23"),
            ts,
            "This is a string, /[]?=:",
            "This is not a partition column")

        // BooleanType is not supported yet
        val partitionColumnTypes =
          Seq(
            ByteType,
            ShortType,
            IntegerType,
            LongType,
            FloatType,
            DoubleType,
            DecimalType(10, 5),
            DecimalType.SYSTEM_DEFAULT,
            DateType,
            SQLConf.get.timestampType,
            StringType)

        val partitionColumns = partitionColumnTypes.zipWithIndex.map {
          case (t, index) => StructField(s"p_$index", t)
        }

        val schema = StructType(partitionColumns :+ StructField(s"i", StringType))
        val df = spark.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

        withTempPath { dir =>
          df.write
            .format("parquet")
            .partitionBy(partitionColumns.map(_.name): _*)
            .save(dir.toString)
          val fields = schema.map(f => Column(f.name).cast(f.dataType))
          checkAnswer(spark.read.load(dir.toString).select(fields: _*), row)
        }

        withTempPath { dir =>
          df.write.option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
            .format("parquet").partitionBy(partitionColumns.map(_.name): _*).save(dir.toString)
          val fields = schema.map(f => Column(f.name).cast(f.dataType))
          checkAnswer(spark.read.option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
            .load(dir.toString).select(fields: _*), row)
        }
      }
    }
  }

  test("Various inferred partition value types") {
    Seq(TimestampTypes.TIMESTAMP_LTZ, TimestampTypes.TIMESTAMP_NTZ).foreach { tsType =>
      withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> tsType.toString) {
        val ts = if (tsType == TimestampTypes.TIMESTAMP_LTZ) {
          Timestamp.valueOf("1990-02-24 12:00:30")
        } else {
          LocalDateTime.parse("1990-02-24T12:00:30")
        }
        val row =
          Row(
            Long.MaxValue,
            4.5,
            new java.math.BigDecimal(new BigInteger("1" * 20)),
            java.sql.Date.valueOf("2015-05-23"),
            ts,
            "This is a string, /[]?=:",
            "This is not a partition column")

        val partitionColumnTypes =
          Seq(
            LongType,
            DoubleType,
            DecimalType(20, 0),
            DateType,
            SQLConf.get.timestampType,
            StringType)

        val partitionColumns = partitionColumnTypes.zipWithIndex.map {
          case (t, index) => StructField(s"p_$index", t)
        }

        val schema = StructType(partitionColumns :+ StructField(s"i", StringType))
        val df = spark.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

        withTempPath { dir =>
          df.write
            .format("parquet")
            .partitionBy(partitionColumns.map(_.name): _*)
            .save(dir.toString)
          val fields = schema.map(f => Column(f.name))
          checkAnswer(spark.read.load(dir.toString).select(fields: _*), row)
        }

        withTempPath { dir =>
          df.write.option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
            .format("parquet").partitionBy(partitionColumns.map(_.name): _*).save(dir.toString)
          val fields = schema.map(f => Column(f.name))
          checkAnswer(spark.read.option(DateTimeUtils.TIMEZONE_OPTION, "UTC")
            .load(dir.toString).select(fields: _*), row)
        }
      }
    }
  }

  test("SPARK-8037: Ignores files whose name starts with dot") {
    withTempPath { dir =>
      val df = (1 to 3).map(i => (i, i, i, i)).toDF("a", "b", "c", "d")

      df.write
        .format("parquet")
        .partitionBy("b", "c", "d")
        .save(dir.getCanonicalPath)

      Files.touch(new File(s"${dir.getCanonicalPath}/b=1", ".DS_Store"))
      Files.createParentDirs(new File(s"${dir.getCanonicalPath}/b=1/c=1/.foo/bar"))

      checkAnswer(spark.read.format("parquet").load(dir.getCanonicalPath), df)
    }
  }

  test("SPARK-11678: Partition discovery stops at the root path of the dataset") {
    withTempPath { dir =>
      val tablePath = new File(dir, "key=value")
      val df = (1 to 3).map(i => (i, i, i, i)).toDF("a", "b", "c", "d")

      df.write
        .format("parquet")
        .partitionBy("b", "c", "d")
        .save(tablePath.getCanonicalPath)

      Files.touch(new File(s"${tablePath.getCanonicalPath}/", "_SUCCESS"))
      Files.createParentDirs(new File(s"${dir.getCanonicalPath}/b=1/c=1/.foo/bar"))

      checkAnswer(spark.read.format("parquet").load(tablePath.getCanonicalPath), df)
    }

    withTempPath { dir =>
      val path = new File(dir, "key=value")
      val tablePath = new File(path, "table")

      val df = (1 to 3).map(i => (i, i, i, i)).toDF("a", "b", "c", "d")

      df.write
        .format("parquet")
        .partitionBy("b", "c", "d")
        .save(tablePath.getCanonicalPath)

      Files.touch(new File(s"${tablePath.getCanonicalPath}/", "_SUCCESS"))
      Files.createParentDirs(new File(s"${dir.getCanonicalPath}/b=1/c=1/.foo/bar"))

      checkAnswer(spark.read.format("parquet").load(tablePath.getCanonicalPath), df)
    }
  }

  test("use basePath to specify the root dir of a partitioned table.") {
    withTempPath { dir =>
      val tablePath = new File(dir, "table")
      val df = (1 to 3).map(i => (i, i, i, i)).toDF("a", "b", "c", "d")

      df.write
        .format("parquet")
        .partitionBy("b", "c", "d")
        .save(tablePath.getCanonicalPath)

      val twoPartitionsDF =
        spark
          .read
          .option("basePath", tablePath.getCanonicalPath)
          .parquet(
            s"${tablePath.getCanonicalPath}/b=1",
            s"${tablePath.getCanonicalPath}/b=2")

      checkAnswer(twoPartitionsDF, df.filter("b != 3"))

      intercept[AssertionError] {
        spark
          .read
          .parquet(
            s"${tablePath.getCanonicalPath}/b=1",
            s"${tablePath.getCanonicalPath}/b=2")
      }
    }
  }

  test("use basePath and file globbing to selectively load partitioned table") {
    withTempPath { dir =>

      val df = Seq(
        (1, "foo", 100),
        (1, "bar", 200),
        (2, "foo", 300),
        (2, "bar", 400)
      ).toDF("p1", "p2", "v")
      df.write
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .parquet(dir.getCanonicalPath)

      def check(path: String, basePath: String, expectedDf: DataFrame): Unit = {
        val testDf = spark.read
          .option("basePath", basePath)
          .parquet(path)
        checkAnswer(testDf, expectedDf)
      }

      // Should find all the data with partitioning columns when base path is set to the root
      val resultDf = df.select("v", "p1", "p2")
      check(path = s"$dir", basePath = s"$dir", resultDf)
      check(path = s"$dir/*", basePath = s"$dir", resultDf)
      check(path = s"$dir/*/*", basePath = s"$dir", resultDf)
      check(path = s"$dir/*/*/*", basePath = s"$dir", resultDf)

      // Should find selective partitions of the data if the base path is not set to root

      check(          // read from ../p1=1 with base ../p1=1, should not infer p1 col
        path = s"$dir/p1=1/*",
        basePath = s"$dir/p1=1/",
        resultDf.filter("p1 = 1").drop("p1"))

      check(          // red from ../p1=1/p2=foo with base ../p1=1/ should not infer p1
        path = s"$dir/p1=1/p2=foo/*",
        basePath = s"$dir/p1=1/",
        resultDf.filter("p1 = 1").filter("p2 = 'foo'").drop("p1"))

      check(          // red from ../p1=1/p2=foo with base ../p1=1/p2=foo, should not infer p1, p2
        path = s"$dir/p1=1/p2=foo/*",
        basePath = s"$dir/p1=1/p2=foo/",
        resultDf.filter("p1 = 1").filter("p2 = 'foo'").drop("p1", "p2"))
    }
  }

  test("_SUCCESS should not break partitioning discovery") {
    Seq(1, 32).foreach { threshold =>
      // We have two paths to list files, one at driver side, another one that we use
      // a Spark job. We need to test both ways.
      withSQLConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> threshold.toString) {
        withTempPath { dir =>
          val tablePath = new File(dir, "table")
          val df = (1 to 3).map(i => (i, i, i, i)).toDF("a", "b", "c", "d")

          df.write
            .format("parquet")
            .partitionBy("b", "c", "d")
            .save(tablePath.getCanonicalPath)

          Files.touch(new File(s"${tablePath.getCanonicalPath}/b=1", "_SUCCESS"))
          Files.touch(new File(s"${tablePath.getCanonicalPath}/b=1/c=1", "_SUCCESS"))
          Files.touch(new File(s"${tablePath.getCanonicalPath}/b=1/c=1/d=1", "_SUCCESS"))
          checkAnswer(spark.read.format("parquet").load(tablePath.getCanonicalPath), df)
        }
      }
    }
  }

  test("listConflictingPartitionColumns") {
    def makeExpectedMessage(colNameLists: Seq[String], paths: Seq[String]): String = {
      val conflictingColNameLists = colNameLists.zipWithIndex.map { case (list, index) =>
        s"\tPartition column name list #$index: $list"
      }.mkString("\n", "\n", "\n")

      // scalastyle:off
      s"""Conflicting partition column names detected:
         |$conflictingColNameLists
         |For partitioned table directories, data files should only live in leaf directories.
         |And directories at the same level should have the same partition column name.
         |Please check the following directories for unexpected files or inconsistent partition column names:
         |${paths.map("\t" + _).mkString("\n", "\n", "")}
       """.stripMargin.trim
      // scalastyle:on
    }

    assert(
      listConflictingPartitionColumns(
        Seq(
          (new Path("file:/tmp/foo/a=1"),
            PartitionValues(Seq("a"), Seq(TypedPartValue("1", IntegerType)))),
          (new Path("file:/tmp/foo/b=1"),
            PartitionValues(Seq("b"), Seq(TypedPartValue("1", IntegerType)))))).trim ===
        makeExpectedMessage(Seq("a", "b"), Seq("file:/tmp/foo/a=1", "file:/tmp/foo/b=1")))

    assert(
      listConflictingPartitionColumns(
        Seq(
          (new Path("file:/tmp/foo/a=1/_temporary"),
            PartitionValues(Seq("a"), Seq(TypedPartValue("1", IntegerType)))),
          (new Path("file:/tmp/foo/a=1"),
            PartitionValues(Seq("a"), Seq(TypedPartValue("1", IntegerType)))))).trim ===
        makeExpectedMessage(
          Seq("a"),
          Seq("file:/tmp/foo/a=1/_temporary", "file:/tmp/foo/a=1")))

    assert(
      listConflictingPartitionColumns(
        Seq(
          (new Path("file:/tmp/foo/a=1"),
            PartitionValues(Seq("a"), Seq(TypedPartValue("1", IntegerType)))),
          (new Path("file:/tmp/foo/a=1/b=foo"),
            PartitionValues(Seq("a", "b"),
              Seq(TypedPartValue("1", IntegerType), TypedPartValue("foo", StringType)))))).trim ===
        makeExpectedMessage(
          Seq("a", "a, b"),
          Seq("file:/tmp/foo/a=1", "file:/tmp/foo/a=1/b=foo")))
  }

  test("Parallel partition discovery") {
    withTempPath { dir =>
      withSQLConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> "1") {
        val path = dir.getCanonicalPath
        val df = spark.range(5).select('id as 'a, 'id as 'b, 'id as 'c).coalesce(1)
        df.write.partitionBy("b", "c").parquet(path)
        checkAnswer(spark.read.parquet(path), df)
      }
    }
  }

  test("SPARK-15895 summary files in non-leaf partition directories") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withSQLConf(
          ParquetOutputFormat.JOB_SUMMARY_LEVEL -> "ALL",
          SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
            classOf[SQLHadoopMapReduceCommitProtocol].getCanonicalName) {
        spark.range(3).write.parquet(s"$path/p0=0/p1=0")
      }

      val p0 = new File(path, "p0=0")
      val p1 = new File(p0, "p1=0")

      // Builds the following directory layout by:
      //
      //  1. copying Parquet summary files we just wrote into `p0=0`, and
      //  2. touching a dot-file `.dummy` under `p0=0`.
      //
      // <base>
      // +- p0=0
      //    |- _metadata
      //    |- _common_metadata
      //    |- .dummy
      //    +- p1=0
      //       |- _metadata
      //       |- _common_metadata
      //       |- part-00000.parquet
      //       |- part-00001.parquet
      //       +- ...
      //
      // The summary files and the dot-file under `p0=0` should not fail partition discovery.

      Files.copy(new File(p1, "_metadata"), new File(p0, "_metadata"))
      Files.copy(new File(p1, "_common_metadata"), new File(p0, "_common_metadata"))
      Files.touch(new File(p0, ".dummy"))

      checkAnswer(spark.read.parquet(s"$path"), Seq(
        Row(0, 0, 0),
        Row(1, 0, 0),
        Row(2, 0, 0)
      ))
    }
  }

  test("SPARK-22109: Resolve type conflicts between strings and timestamps in partition column") {
    val df = Seq(
      (1, "2015-01-01 00:00:00"),
      (2, "2014-01-01 00:00:00"),
      (3, "blah")).toDF("i", "str")

    withTempPath { path =>
      df.write.format("parquet").partitionBy("str").save(path.getAbsolutePath)
      checkAnswer(spark.read.load(path.getAbsolutePath), df)
    }
  }

  test("Resolve type conflicts - decimals, dates and timestamps in partition column") {
    withTempPath { path =>
      val df = Seq((1, "2014-01-01"), (2, "2016-01-01"), (3, "2015-01-01 00:01:00")).toDF("i", "ts")
      df.write.format("parquet").partitionBy("ts").save(path.getAbsolutePath)
      checkAnswer(
        spark.read.load(path.getAbsolutePath),
        Row(1, Timestamp.valueOf("2014-01-01 00:00:00")) ::
          Row(2, Timestamp.valueOf("2016-01-01 00:00:00")) ::
          Row(3, Timestamp.valueOf("2015-01-01 00:01:00")) :: Nil)
    }

    withTempPath { path =>
      val df = Seq((1, "1"), (2, "3"), (3, "2" * 30)).toDF("i", "decimal")
      df.write.format("parquet").partitionBy("decimal").save(path.getAbsolutePath)
      checkAnswer(
        spark.read.load(path.getAbsolutePath),
        Row(1, BigDecimal("1")) ::
          Row(2, BigDecimal("3")) ::
          Row(3, BigDecimal("2" * 30)) :: Nil)
    }
  }

  test(
    "SPARK-23436, SPARK-36861: invalid Dates should be inferred as String in partition inference") {
    withTempPath { path =>
      val data = Seq(("1", "2018-41", "2018-01-01-04", "2021-01-01T00", "test"))
        .toDF("id", "date_month", "date_hour", "date_t_hour", "data")

      data.write.partitionBy("date_month", "date_hour", "date_t_hour").parquet(path.getAbsolutePath)
      val input = spark.read.parquet(path.getAbsolutePath).select("id",
        "date_month", "date_hour", "date_t_hour", "data")

      assert(input.schema.sameType(input.schema))
      checkAnswer(input, data)
    }
  }

  test("SPARK-34314: preserve partition values of the string type") {
    import testImplicits._
    withTempPath { file =>
      val path = file.getCanonicalPath
      val df = Seq((0, "AA"), (1, "-0")).toDF("id", "part")
      df.write
        .partitionBy("part")
        .format("parquet")
        .save(path)
      val readback = spark.read.parquet(path)
      assert(readback.schema("part").dataType === StringType)
      checkAnswer(readback, Row(0, "AA") :: Row(1, "-0") :: Nil)
    }
  }
}

class ParquetV1PartitionDiscoverySuite extends ParquetPartitionDiscoverySuite {
  import testImplicits._

  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "parquet")

  test("read partitioned table - partition key included in Parquet file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", "bar")
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      spark.read.parquet(base.getCanonicalPath).createOrReplaceTempView("t")

      withTempView("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", "bar")
          } yield Row(i, pi, i.toString, ps))

        checkAnswer(
          sql("SELECT intField, pi FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            _ <- Seq("foo", "bar")
          } yield Row(i, pi))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi = 1"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", "bar")
          } yield Row(i, 1, i.toString, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps = 'foo'"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, pi, i.toString, "foo"))
      }
    }
  }

  test("read partitioned table - with nulls and partition keys are included in Parquet file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", null.asInstanceOf[String])
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      val parquetRelation = spark.read.format("parquet").load(base.getCanonicalPath)
      parquetRelation.createOrReplaceTempView("t")

      withTempView("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, pi, i.toString, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps IS NULL"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, pi, i.toString, null))
      }
    }
  }

  test("SPARK-7749 Non-partitioned table should have empty partition spec") {
    withTempPath { dir =>
      (1 to 10).map(i => (i, i.toString)).toDF("a", "b").write.parquet(dir.getCanonicalPath)
      val queryExecution = spark.read.parquet(dir.getCanonicalPath).queryExecution
      queryExecution.analyzed.collectFirst {
        case LogicalRelation(
        HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _), _, _, _) =>
          assert(location.partitionSpec() === PartitionSpec.emptySpec)
      }.getOrElse {
        fail(s"Expecting a matching HadoopFsRelation, but got:\n$queryExecution")
      }
    }
  }

  test("SPARK-18108 Parquet reader fails when data column types conflict with partition ones") {
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        val df = Seq((1L, 2.0)).toDF("a", "b")
        df.write.parquet(s"$path/a=1")
        checkAnswer(spark.read.parquet(s"$path"), Seq(Row(1, 2.0)))
      }
    }
  }

  test("SPARK-21463: MetadataLogFileIndex should respect userSpecifiedSchema for partition cols") {
    withTempDir { tempDir =>
      val output = new File(tempDir, "output").toString
      val checkpoint = new File(tempDir, "checkpoint").toString
      try {
        val stream = MemoryStream[(String, Int)]
        val df = stream.toDS().toDF("time", "value")
        val sq = df.writeStream
          .option("checkpointLocation", checkpoint)
          .format("parquet")
          .partitionBy("time")
          .start(output)

        stream.addData(("2017-01-01-00", 1), ("2017-01-01-01", 2))
        sq.processAllAvailable()

        val schema = new StructType()
          .add("time", StringType)
          .add("value", IntegerType)
        val readBack = spark.read.schema(schema).parquet(output)
        assert(readBack.schema.toSet === schema.toSet)

        checkAnswer(
          readBack,
          Seq(Row("2017-01-01-00", 1), Row("2017-01-01-01", 2))
        )
      } finally {
        spark.streams.active.foreach(_.stop())
      }
    }
  }
}

class ParquetV2PartitionDiscoverySuite extends ParquetPartitionDiscoverySuite {
  import testImplicits._

  // TODO: enable Parquet V2 write path after file source V2 writers are workable.
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  test("SPARK-35561: remove leading zeros from empty static number type partition") {
    val spec = Map("p_int"-> "010", "p_float"-> "01.00")
    val schema = new StructType().add("p_int", "int").add("p_float", "float")
    val path = PartitioningUtils.getPathFragment(spec, schema)
    assert("p_int=10/p_float=1.0" === path)
  }

  test("read partitioned table - partition key included in Parquet file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", "bar")
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      spark.read.parquet(base.getCanonicalPath).createOrReplaceTempView("t")

      withTempView("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT intField, pi FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            _ <- Seq("foo", "bar")
          } yield Row(i, pi))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi = 1"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, 1, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps = 'foo'"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, i.toString, pi, "foo"))
      }
    }
  }

  test("read partitioned table - with nulls and partition keys are included in Parquet file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", null.asInstanceOf[String])
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      val parquetRelation = spark.read.format("parquet").load(base.getCanonicalPath)
      parquetRelation.createOrReplaceTempView("t")

      withTempView("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps IS NULL"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, i.toString, pi, null))
      }
    }
  }

  test("SPARK-7749 Non-partitioned table should have empty partition spec") {
    withTempPath { dir =>
      (1 to 10).map(i => (i, i.toString)).toDF("a", "b").write.parquet(dir.getCanonicalPath)
      val queryExecution = spark.read.parquet(dir.getCanonicalPath).queryExecution
      queryExecution.analyzed.collectFirst {
        case DataSourceV2Relation(fileTable: FileTable, _, _, _, _) =>
          assert(fileTable.fileIndex.partitionSpec() === PartitionSpec.emptySpec)
      }.getOrElse {
        fail(s"Expecting a matching DataSourceV2Relation, but got:\n$queryExecution")
      }
    }
  }

  test("SPARK-18108 Parquet reader fails when data column types conflict with partition ones") {
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        val df = Seq((1L, 2.0)).toDF("a", "b")
        df.write.parquet(s"$path/a=1")
        checkAnswer(spark.read.parquet(s"$path"), Seq(Row(2.0, 1)))
      }
    }
  }

  test("SPARK-21463: MetadataLogFileIndex should respect userSpecifiedSchema for partition cols") {
    withTempDir { tempDir =>
      val output = new File(tempDir, "output").toString
      val checkpoint = new File(tempDir, "checkpoint").toString
      try {
        val stream = MemoryStream[(String, Int)]
        val df = stream.toDS().toDF("time", "value")
        val sq = df.writeStream
          .option("checkpointLocation", checkpoint)
          .format("parquet")
          .partitionBy("time")
          .start(output)

        stream.addData(("2017-01-01-00", 1), ("2017-01-01-01", 2))
        sq.processAllAvailable()

        val schema = new StructType()
          .add("time", StringType)
          .add("value", IntegerType)
        val readBack = spark.read.schema(schema).parquet(output)
        assert(readBack.schema.toSet === schema.toSet)

        checkAnswer(
          readBack,
          Seq(Row(1, "2017-01-01-00"), Row(2, "2017-01-01-01"))
        )
      } finally {
        spark.streams.active.foreach(_.stop())
      }
    }
  }
}
