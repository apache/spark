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

package org.apache.spark.sql.sources

import scala.collection.mutable

import com.google.common.base.Objects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{TaskAttemptContext, OutputFormat}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.scalatest.BeforeAndAfter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class SimpleFSBasedSource extends FSBasedRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      schema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): BaseRelation = {
    val path = parameters("path")

    // Uses data sources options to simulate data schema
    val dataSchema = DataType.fromJson(parameters("schema")).asInstanceOf[StructType]

    // Uses data sources options to mock partition discovery
    val maybePartitionSpec =
      parameters.get("partitionColumns").map { json =>
        new PartitionSpec(
          DataType.fromJson(json).asInstanceOf[StructType],
          Array.empty[Partition])
      }

    new SimpleFSBasedRelation(path, dataSchema, maybePartitionSpec, parameters)(sqlContext)
  }
}

class SimpleOutputWriter extends OutputWriter {
  override def init(path: String, dataSchema: StructType, context: TaskAttemptContext): Unit = {
    TestResult.synchronized {
      TestResult.writerPaths += path
    }
  }

  override def write(row: Row): Unit = {
    TestResult.synchronized {
      TestResult.writtenRows += row
    }
  }
}

class SimpleFSBasedRelation
    (val path: String,
     val dataSchema: StructType,
     val maybePartitionSpec: Option[PartitionSpec],
     val parameter: Map[String, String])
    (@transient val sqlContext: SQLContext)
  extends FSBasedRelation(Array(path), maybePartitionSpec) {

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: SimpleFSBasedRelation =>
      this.path == that.path &&
        this.dataSchema == that.dataSchema &&
        this.maybePartitionSpec == that.maybePartitionSpec
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hashCode(path, dataSchema, maybePartitionSpec)

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[String]): RDD[Row] = {
    val sqlContext = this.sqlContext
    val basePath = new Path(path)
    val fs = basePath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    // Used to test queries like "INSERT OVERWRITE tbl SELECT * FROM tbl". Source directory
    // shouldn't be removed before scanning the data.
    assert(inputPaths.map(new Path(_)).forall(fs.exists))

    TestResult.synchronized {
      TestResult.requiredColumns = requiredColumns
      TestResult.filters = filters
      TestResult.inputPaths = inputPaths
      Option(TestResult.rowsToRead).getOrElse(sqlContext.emptyResult)
    }
  }

  override def outputWriterClass: Class[_ <: OutputWriter] = classOf[SimpleOutputWriter]

  override def outputFormatClass: Class[_ <: OutputFormat[Void, Row]] = {
    // This is just a mock, not used within this test suite.
    classOf[TextOutputFormat[Void, Row]]
  }
}

object TestResult {
  var requiredColumns: Array[String] = _
  var filters: Array[Filter] = _
  var inputPaths: Array[String] = _
  var rowsToRead: RDD[Row] = _
  var writerPaths: mutable.Set[String] = mutable.Set.empty[String]
  var writtenRows: mutable.Set[Row] = mutable.Set.empty[Row]

  def reset(): Unit = this.synchronized {
    requiredColumns = null
    filters = null
    inputPaths = null
    rowsToRead = null
    writerPaths.clear()
    writtenRows.clear()
  }
}

class FSBasedRelationSuite extends QueryTest with BeforeAndAfter {
  import TestHive._
  import TestHive.implicits._

  var basePath: Path = _

  var fs: FileSystem = _

  val dataSchema =
    StructType(
      Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", StringType, nullable = false)))

  val singlePartitionColumn = StructType(
    Seq(
      StructField("p1", IntegerType, nullable = true)))

  val doublePartitionColumns = StructType(
    Seq(
      StructField("p1", IntegerType, nullable = true),
      StructField("p2", StringType, nullable = true)))

  val testDF = (for {
    i <- 1 to 3
    p <- 1 to 2
  } yield (i, s"val_$i", p)).toDF("a", "b", "p1")

  before {
    basePath = new Path(Utils.createTempDir().getCanonicalPath)
    fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    basePath = fs.makeQualified(basePath)
    TestResult.reset()
  }

  test("load() - partitioned table - partition column not included in data files") {
    // Mocks partition directory layout.
    val fakeData = sparkContext.parallelize(Seq("placeholder"))
    fakeData.saveAsTextFile(new Path(basePath, "p1=1/p2=hello").toString)
    fakeData.saveAsTextFile(new Path(basePath, "p1=2/p2=world").toString)

    val df = load(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json))

    df.queryExecution.analyzed.collect {
      case LogicalRelation(relation: SimpleFSBasedRelation) =>
        assert(relation.dataSchema === dataSchema)
      case _ =>
        fail("Couldn't find expected SimpleFSBasedRelation instance")
    }

    val expectedSchema =
      StructType(
        dataSchema ++ Seq(
          StructField("p1", IntegerType, nullable = true),
          StructField("p2", StringType, nullable = true)))

    assert(df.schema === expectedSchema)

    df.where($"a" > 0 && $"p1" === 1).select("b").collect()

    // Check for column pruning, filter push-down, and partition pruning
    assert(TestResult.requiredColumns.toSet === Set("a", "b"))
    assert(TestResult.filters === Seq(GreaterThan("a", 0)))
    assertResult(Array(new Path(basePath, "p1=1/p2=hello"))) {
      TestResult.inputPaths.map(new Path(_).getParent)
    }
  }

  test("load() - partitioned table - partition column included in data files") {
    // Mocks partition directory layout.
    val fakeData = sparkContext.parallelize(Seq("placeholder"))
    fakeData.saveAsTextFile(new Path(basePath, "p1=1/p2=hello").toString)
    fakeData.saveAsTextFile(new Path(basePath, "p1=2/p2=world").toString)

    val dataSchema =
      StructType(
        Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("p1", IntegerType, nullable = true),
          StructField("b", StringType, nullable = false)))

    val df = load(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json))

    df.queryExecution.analyzed.collect {
      case LogicalRelation(relation: SimpleFSBasedRelation) =>
        assert(relation.dataSchema === dataSchema)
      case _ =>
        fail("Couldn't find expected SimpleFSBasedRelation instance")
    }

    val expectedSchema =
      StructType(
        Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("p1", IntegerType, nullable = true),
          StructField("b", StringType, nullable = false),
          StructField("p2", StringType, nullable = true)))

    assert(df.schema === expectedSchema)

    df.where($"a" > 0 && $"p1" === 1).select("b").collect()

    // Check for column pruning, filter push-down, and partition pruning
    assert(TestResult.requiredColumns.toSet === Set("a", "b"))
    assert(TestResult.filters === Seq(GreaterThan("a", 0)))
    assertResult(Array(new Path(basePath, "p1=1/p2=hello"))) {
      TestResult.inputPaths.map(new Path(_).getParent)
    }
  }

  test("save() - partitioned table - Overwrite") {
    testDF.save(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json,
        "partitionColumns" -> singlePartitionColumn.json),
      partitionColumns = Seq("p1"))

    Thread.sleep(500)

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 2) yield Row(i, s"val_$i")

    TestResult.synchronized {
      assert(TestResult.writerPaths.size === 2)
      assert(TestResult.writtenRows === expectedRows.toSet)
    }
  }

  ignore("save() - partitioned table - Overwrite - select and overwrite the same table") {
    TestResult.rowsToRead = testDF.rdd

    val df = load(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json))

    df.save(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json,
        "partitionColumns" -> singlePartitionColumn.json),
      partitionColumns = Seq("p1"))

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 2) yield Row(i, s"val_$i")

    TestResult.synchronized {
      assert(TestResult.writerPaths.size === 2)
      assert(TestResult.writtenRows === expectedRows.toSet)
    }
  }

  test("save() - partitioned table - Append - new partition values") {
    testDF.save(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json,
        "partitionColumns" -> singlePartitionColumn.json),
      partitionColumns = Seq("p1"))

    val moreData = (for {
      i <- 1 to 3
      p <- 3 to 4
    } yield (i, s"val_$i", p)).toDF("a", "b", "p1")

    moreData.save(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Append,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json,
        "partitionColumns" -> singlePartitionColumn.json),
      partitionColumns = Seq("p1"))

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 4) yield Row(i, s"val_$i")

    TestResult.synchronized {
      assert(TestResult.writerPaths.size === 4)
      assert(TestResult.writtenRows === expectedRows.toSet)
    }
  }

  test("save() - partitioned table - Append - mismatched partition columns") {
    val data = (for {
      i <- 1 to 3
      p1 <- 1 to 2
      p2 <- Array("hello", "world")
    } yield (i, s"val_$i", p1, p2)).toDF("a", "b", "p1", "p2")

    // Using only a subset of all partition columns
    intercept[IllegalArgumentException] {
      data.save(
        source = classOf[SimpleFSBasedSource].getCanonicalName,
        mode = SaveMode.Append,
        options = Map(
          "path" -> basePath.toString,
          "schema" -> dataSchema.json,
          "partitionColumns" -> doublePartitionColumns.json),
        partitionColumns = Seq("p1"))
    }

    // Using different order of partition columns
    intercept[IllegalArgumentException] {
      data.save(
        source = classOf[SimpleFSBasedSource].getCanonicalName,
        mode = SaveMode.Append,
        options = Map(
          "path" -> basePath.toString,
          "schema" -> dataSchema.json,
          "partitionColumns" -> doublePartitionColumns.json),
        partitionColumns = Seq("p2", "p1"))
    }
  }

  test("save() - partitioned table - ErrorIfExists") {
    fs.delete(basePath, true)

    testDF.save(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json,
        "partitionColumns" -> singlePartitionColumn.json),
      partitionColumns = Seq("p1"))

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 2) yield Row(i, s"val_$i")

    TestResult.synchronized {
      assert(TestResult.writerPaths.size === 2)
      assert(TestResult.writtenRows === expectedRows.toSet)
    }

    intercept[RuntimeException] {
      testDF.save(
        source = classOf[SimpleFSBasedSource].getCanonicalName,
        mode = SaveMode.ErrorIfExists,
        options = Map(
          "path" -> basePath.toString,
          "schema" -> dataSchema.json,
          "partitionColumns" -> singlePartitionColumn.json),
        partitionColumns = Seq("p1"))
    }
  }

  test("save() - partitioned table - Ignore") {
    testDF.save(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Ignore,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    assert(TestResult.writtenRows.isEmpty)
  }

  test("save() - data sources other than FSBasedRelation") {
    intercept[RuntimeException] {
      testDF.save(
        source = classOf[FilteredScanSource].getCanonicalName,
        mode = SaveMode.Overwrite,
        options = Map("path" -> basePath.toString),
        partitionColumns = Seq("p1"))
    }
  }

  def saveToPartitionedTable(
      df: DataFrame,
      tableName: String,
      relationPartitionColumns: StructType,
      partitionedBy: Seq[String],
      mode: SaveMode): Unit = {
    df.saveAsTable(
      tableName = tableName,
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = mode,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json,
        "partitionColumns" -> relationPartitionColumns.json),
      partitionColumns = partitionedBy)
  }

  test("saveAsTable() - partitioned table - Overwrite") {
    saveToPartitionedTable(testDF, "t", singlePartitionColumn, Array("p1"), SaveMode.Overwrite)

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 2) yield Row(i, s"val_$i")

    TestResult.synchronized {
      assert(TestResult.writerPaths.size === 2)
      assert(TestResult.writtenRows === expectedRows.toSet)
    }

    assertResult(table("t").schema) {
      StructType(
        dataSchema ++ Seq(
          StructField("p1", IntegerType, nullable = true)))
    }

    sql("DROP TABLE t")
  }

  ignore("saveAsTable() - partitioned table - Overwrite - select and overwrite the same table") {
    TestResult.rowsToRead = testDF.rdd

    val df = load(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json,
        "partitionColumns" -> singlePartitionColumn.json))

    saveToPartitionedTable(df, "t", singlePartitionColumn, Seq("p1"), SaveMode.Overwrite)

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 2) yield Row(i, s"val_$i")

    TestResult.synchronized {
      assert(TestResult.writerPaths.size === 2)
      assert(TestResult.writtenRows === expectedRows.toSet)
    }

    assertResult(table("t").schema) {
      StructType(
        dataSchema ++ Seq(
          StructField("p1", IntegerType, nullable = true)))
    }

    sql("DROP TABLE t")
  }

  test("saveAsTable() - partitioned table - Append") {
    saveToPartitionedTable(testDF, "t", singlePartitionColumn, Seq("p1"), SaveMode.Overwrite)
    saveToPartitionedTable(testDF, "t", singlePartitionColumn, Seq("p1"), SaveMode.Append)

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 2) yield Row(i, s"val_$i")

    TestResult.synchronized {
      assert(TestResult.writerPaths.size === 2)
      assert(TestResult.writtenRows === expectedRows.toSet)
    }

    assertResult(table("t").schema) {
      StructType(
        dataSchema ++ Seq(
          StructField("p1", IntegerType, nullable = true)))
    }

    sql("DROP TABLE t")
  }

  test("saveAsTable() - partitioned table - Append - mismatched partition columns") {
    val data = (for {
      i <- 1 to 3
      p1 <- 1 to 2
      p2 <- Array("hello", "world")
    } yield (i, s"val_$i", p1, p2)).toDF("a", "b", "p1", "p2")

    // Using only a subset of all partition columns
    intercept[IllegalArgumentException] {
      saveToPartitionedTable(data, "t", doublePartitionColumns, Seq("p1"), SaveMode.Append)
    }

    // Using different order of partition columns
    intercept[IllegalArgumentException] {
      saveToPartitionedTable(data, "t", doublePartitionColumns, Seq("p2", "p1"), SaveMode.Append)
    }

    sql("DROP TABLE t")
  }

  test("saveAsTable() - partitioned table - ErrorIfExists") {
    fs.delete(basePath, true)

    saveToPartitionedTable(testDF, "t", singlePartitionColumn, Seq("p1"), SaveMode.ErrorIfExists)

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 2) yield Row(i, s"val_$i")

    TestResult.synchronized {
      assert(TestResult.writerPaths.size === 2)
      assert(TestResult.writtenRows === expectedRows.toSet)
    }

    assertResult(table("t").schema) {
      StructType(
        dataSchema ++ Seq(
          StructField("p1", IntegerType, nullable = true)))
    }

    intercept[AnalysisException] {
      saveToPartitionedTable(testDF, "t", singlePartitionColumn, Seq("p1"), SaveMode.ErrorIfExists)
    }

    sql("DROP TABLE t")
  }

  test("saveAsTable() - partitioned table - Ignore") {
    saveToPartitionedTable(testDF, "t", singlePartitionColumn, Seq("p1"), SaveMode.Ignore)

    assert(TestResult.writtenRows.isEmpty)
    assertResult(table("t").schema) {
      StructType(
        dataSchema ++ Seq(
          StructField("p1", IntegerType, nullable = true)))
    }

    sql("DROP TABLE t")
  }

  test("saveAsTable() - data sources other than FSBasedRelation") {
    intercept[RuntimeException] {
      testDF.saveAsTable(
        tableName = "t",
        source = classOf[FilteredScanSource].getCanonicalName,
        mode = SaveMode.Overwrite,
        options = Map("path" -> basePath.toString),
        partitionColumns = Seq("p1"))
    }
  }
}
