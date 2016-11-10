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

import scala.collection.mutable.ArrayBuffer

import com.google.common.io.Files
import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.datasources.{LogicalRelation, PartitionSpec, Partition, PartitioningUtils}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// The data where the partitioning key exists only in the directory structure.
case class ParquetData(intField: Int, stringField: String)

// The data that also includes the partitioning key
case class ParquetDataWithKey(intField: Int, pi: Int, stringField: String, ps: String)

class ParquetPartitionDiscoverySuite extends QueryTest with ParquetTest with SharedSQLContext {
  import PartitioningUtils._
  import testImplicits._

  val defaultPartitionName = "__HIVE_DEFAULT_PARTITION__"

  test("column type inference") {
    def check(raw: String, literal: Literal): Unit = {
      assert(inferPartitionColumnValue(raw, defaultPartitionName, true) === literal)
    }

    check("10", Literal.create(10, IntegerType))
    check("1000000000000000", Literal.create(1000000000000000L, LongType))
    check("1.5", Literal.create(1.5, DoubleType))
    check("hello", Literal.create("hello", StringType))
    check(defaultPartitionName, Literal.create(null, NullType))
  }

  test("parse invalid partitioned directories") {
    // Invalid
    var paths = Seq(
      "hdfs://host:9000/invalidPath",
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/a=10.5/b=hello")

    var exception = intercept[AssertionError] {
      parsePartitions(paths.map(new Path(_)), defaultPartitionName, true, Set.empty[Path])
    }
    assert(exception.getMessage().contains("Conflicting directory structures detected"))

    // Valid
    paths = Seq(
      "hdfs://host:9000/path/_temporary",
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/_temporary/path")

    parsePartitions(
      paths.map(new Path(_)),
      defaultPartitionName,
      true,
      Set(new Path("hdfs://host:9000/path/")))

    // Valid
    paths = Seq(
      "hdfs://host:9000/path/something=true/table/",
      "hdfs://host:9000/path/something=true/table/_temporary",
      "hdfs://host:9000/path/something=true/table/a=10/b=20",
      "hdfs://host:9000/path/something=true/table/_temporary/path")

    parsePartitions(
      paths.map(new Path(_)),
      defaultPartitionName,
      true,
      Set(new Path("hdfs://host:9000/path/something=true/table")))

    // Valid
    paths = Seq(
      "hdfs://host:9000/path/table=true/",
      "hdfs://host:9000/path/table=true/_temporary",
      "hdfs://host:9000/path/table=true/a=10/b=20",
      "hdfs://host:9000/path/table=true/_temporary/path")

    parsePartitions(
      paths.map(new Path(_)),
      defaultPartitionName,
      true,
      Set(new Path("hdfs://host:9000/path/table=true")))

    // Invalid
    paths = Seq(
      "hdfs://host:9000/path/_temporary",
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/path1")

    exception = intercept[AssertionError] {
      parsePartitions(
        paths.map(new Path(_)),
        defaultPartitionName,
        true,
        Set(new Path("hdfs://host:9000/path/")))
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
        defaultPartitionName,
        true,
        Set(new Path("hdfs://host:9000/tmp/tables/")))
    }
    assert(exception.getMessage().contains("Conflicting directory structures detected"))
  }

  test("parse partition") {
    def check(path: String, expected: Option[PartitionValues]): Unit = {
      val actual = parsePartition(new Path(path), defaultPartitionName, true, Set.empty[Path])._1
      assert(expected === actual)
    }

    def checkThrows[T <: Throwable: Manifest](path: String, expected: String): Unit = {
      val message = intercept[T] {
        parsePartition(new Path(path), defaultPartitionName, true, Set.empty[Path])
      }.getMessage

      assert(message.contains(expected))
    }

    check("file://path/a=10", Some {
      PartitionValues(
        ArrayBuffer("a"),
        ArrayBuffer(Literal.create(10, IntegerType)))
    })

    check("file://path/a=10/b=hello/c=1.5", Some {
      PartitionValues(
        ArrayBuffer("a", "b", "c"),
        ArrayBuffer(
          Literal.create(10, IntegerType),
          Literal.create("hello", StringType),
          Literal.create(1.5, DoubleType)))
    })

    check("file://path/a=10/b_hello/c=1.5", Some {
      PartitionValues(
        ArrayBuffer("c"),
        ArrayBuffer(Literal.create(1.5, DoubleType)))
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

  test("parse partitions") {
    def check(
        paths: Seq[String],
        spec: PartitionSpec,
        rootPaths: Set[Path] = Set.empty[Path]): Unit = {
      val actualSpec =
        parsePartitions(
          paths.map(new Path(_)),
          defaultPartitionName,
          true,
          rootPaths)
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
          StructField("b", StringType))),
        Seq(
          Partition(InternalRow(10, null), s"hdfs://host:9000/path/a=10/b=$defaultPartitionName"),
          Partition(InternalRow(10.5, null),
            s"hdfs://host:9000/path/a=10.5/b=$defaultPartitionName"))))

    check(Seq(
      s"hdfs://host:9000/path1",
      s"hdfs://host:9000/path2"),
      PartitionSpec.emptySpec)
  }

  test("parse partitions with type inference disabled") {
    def check(paths: Seq[String], spec: PartitionSpec): Unit = {
      val actualSpec =
        parsePartitions(paths.map(new Path(_)), defaultPartitionName, false, Set.empty[Path])
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
          StructField("b", StringType))),
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

      sqlContext.read.parquet(base.getCanonicalPath).registerTempTable("t")

      withTempTable("t") {
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

      sqlContext.read.parquet(base.getCanonicalPath).registerTempTable("t")

      withTempTable("t") {
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

      val parquetRelation = sqlContext.read.format("parquet").load(base.getCanonicalPath)
      parquetRelation.registerTempTable("t")

      withTempTable("t") {
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

      val parquetRelation = sqlContext.read.format("parquet").load(base.getCanonicalPath)
      parquetRelation.registerTempTable("t")

      withTempTable("t") {
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

  test("read partitioned table - merging compatible schemas") {
    withTempDir { base =>
      makeParquetFile(
        (1 to 10).map(i => Tuple1(i)).toDF("intField"),
        makePartitionDir(base, defaultPartitionName, "pi" -> 1))

      makeParquetFile(
        (1 to 10).map(i => (i, i.toString)).toDF("intField", "stringField"),
        makePartitionDir(base, defaultPartitionName, "pi" -> 2))

      sqlContext
        .read
        .option("mergeSchema", "true")
        .format("parquet")
        .load(base.getCanonicalPath)
        .registerTempTable("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          (1 to 10).map(i => Row(i, null, 1)) ++ (1 to 10).map(i => Row(i, i.toString, 2)))
      }
    }
  }

  test("SPARK-7749 Non-partitioned table should have empty partition spec") {
    withTempPath { dir =>
      (1 to 10).map(i => (i, i.toString)).toDF("a", "b").write.parquet(dir.getCanonicalPath)
      val queryExecution = sqlContext.read.parquet(dir.getCanonicalPath).queryExecution
      queryExecution.analyzed.collectFirst {
        case LogicalRelation(relation: ParquetRelation, _) =>
          assert(relation.partitionSpec === PartitionSpec.emptySpec)
      }.getOrElse {
        fail(s"Expecting a ParquetRelation2, but got:\n$queryExecution")
      }
    }
  }

  test("SPARK-7847: Dynamic partition directory path escaping and unescaping") {
    withTempPath { dir =>
      val df = Seq("/", "[]", "?").zipWithIndex.map(_.swap).toDF("i", "s")
      df.write.format("parquet").partitionBy("s").save(dir.getCanonicalPath)
      checkAnswer(sqlContext.read.parquet(dir.getCanonicalPath), df.collect())
    }
  }

  test("Various partition value types") {
    val row =
      Row(
        100.toByte,
        40000.toShort,
        Int.MaxValue,
        Long.MaxValue,
        1.5.toFloat,
        4.5,
        new java.math.BigDecimal(new BigInteger("212500"), 5),
        new java.math.BigDecimal(2.125),
        java.sql.Date.valueOf("2015-05-23"),
        new Timestamp(0),
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
        TimestampType,
        StringType)

    val partitionColumns = partitionColumnTypes.zipWithIndex.map {
      case (t, index) => StructField(s"p_$index", t)
    }

    val schema = StructType(partitionColumns :+ StructField(s"i", StringType))
    val df = sqlContext.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

    withTempPath { dir =>
      df.write.format("parquet").partitionBy(partitionColumns.map(_.name): _*).save(dir.toString)
      val fields = schema.map(f => Column(f.name).cast(f.dataType))
      checkAnswer(sqlContext.read.load(dir.toString).select(fields: _*), row)
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

      checkAnswer(sqlContext.read.format("parquet").load(dir.getCanonicalPath), df)
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

      checkAnswer(sqlContext.read.format("parquet").load(tablePath.getCanonicalPath), df)
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

      checkAnswer(sqlContext.read.format("parquet").load(tablePath.getCanonicalPath), df)
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
        sqlContext
          .read
          .option("basePath", tablePath.getCanonicalPath)
          .parquet(
            s"${tablePath.getCanonicalPath}/b=1",
            s"${tablePath.getCanonicalPath}/b=2")

      checkAnswer(twoPartitionsDF, df.filter("b != 3"))

      intercept[AssertionError] {
        sqlContext
          .read
          .parquet(
            s"${tablePath.getCanonicalPath}/b=1",
            s"${tablePath.getCanonicalPath}/b=2")
      }
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
          checkAnswer(sqlContext.read.format("parquet").load(tablePath.getCanonicalPath), df)
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
          (new Path("file:/tmp/foo/a=1"), PartitionValues(Seq("a"), Seq(Literal(1)))),
          (new Path("file:/tmp/foo/b=1"), PartitionValues(Seq("b"), Seq(Literal(1)))))).trim ===
        makeExpectedMessage(Seq("a", "b"), Seq("file:/tmp/foo/a=1", "file:/tmp/foo/b=1")))

    assert(
      listConflictingPartitionColumns(
        Seq(
          (new Path("file:/tmp/foo/a=1/_temporary"), PartitionValues(Seq("a"), Seq(Literal(1)))),
          (new Path("file:/tmp/foo/a=1"), PartitionValues(Seq("a"), Seq(Literal(1)))))).trim ===
        makeExpectedMessage(
          Seq("a"),
          Seq("file:/tmp/foo/a=1/_temporary", "file:/tmp/foo/a=1")))

    assert(
      listConflictingPartitionColumns(
        Seq(
          (new Path("file:/tmp/foo/a=1"),
            PartitionValues(Seq("a"), Seq(Literal(1)))),
          (new Path("file:/tmp/foo/a=1/b=foo"),
            PartitionValues(Seq("a", "b"), Seq(Literal(1), Literal("foo")))))).trim ===
        makeExpectedMessage(
          Seq("a", "a, b"),
          Seq("file:/tmp/foo/a=1", "file:/tmp/foo/a=1/b=foo")))
  }

  test("Parallel partition discovery") {
    withTempPath { dir =>
      withSQLConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> "1") {
        val path = dir.getCanonicalPath
        val df = sqlContext.range(5).select('id as 'a, 'id as 'b, 'id as 'c).coalesce(1)
        df.write.partitionBy("b", "c").parquet(path)
        checkAnswer(sqlContext.read.parquet(path), df)
      }
    }
  }
}
