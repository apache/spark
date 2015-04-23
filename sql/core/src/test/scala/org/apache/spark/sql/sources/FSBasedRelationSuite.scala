package org.apache.spark.sql.sources

import java.io.IOException

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.util.Utils

class SimpleFSBasedSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SimpleFSBasedRelation(parameters)(sqlContext)
  }
}

case class SimpleFSBasedRelation
    (parameter: Map[String, String])
    (val sqlContext: SQLContext)
  extends FSBasedRelation {

  override val path = parameter("path")

  override def dataSchema: StructType =
    DataType.fromJson(parameter("schema")).asInstanceOf[StructType]

  override def newOutputWriter(path: String): OutputWriter = new OutputWriter {
    override def write(row: Row): Unit = TestResult.writtenRows += row
  }

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[String]): RDD[Row] = {
    val basePath = new Path(path)
    val fs = basePath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    // Used to test queries like "INSERT OVERWRITE tbl SELECT * FROM tbl". Source directory
    // shouldn't be removed before scanning the data.
    assert(inputPaths.map(new Path(_)).forall(fs.exists))

    TestResult.requiredColumns = requiredColumns
    TestResult.filters = filters
    TestResult.inputPaths = inputPaths

    Option(TestResult.rowsToRead).getOrElse(sqlContext.emptyResult)
  }
}

object TestResult {
  var requiredColumns: Array[String] = _
  var filters: Array[Filter] = _
  var inputPaths: Array[String] = _
  var rowsToRead: RDD[Row] = _
  var writtenRows: ArrayBuffer[Row] = ArrayBuffer.empty[Row]

  def reset(): Unit = {
    requiredColumns = null
    filters = null
    inputPaths = null
    rowsToRead = null
    writtenRows.clear()
  }
}

class FSBasedRelationSuite extends DataSourceTest {
  import caseInsensitiveContext._
  import caseInsensitiveContext.implicits._

  var basePath: Path = _

  var fs: FileSystem = _

  val dataSchema =
    StructType(
      Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", StringType, nullable = false)))

  val testDF = (for {
    i <- 1 to 3
    p <- 1 to 2
  } yield (i, s"val_$i", p)).toDF("a", "b", "p1")

  before {
    basePath = new Path(Utils.createTempDir().getCanonicalPath)
    fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    TestResult.reset()
  }

  ignore("load() - partitioned table - partition column not included in data files") {
    fs.mkdirs(new Path(basePath, "p1=1/p2=hello"))
    fs.mkdirs(new Path(basePath, "p1=2/p2=world"))

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

    df.select("b").where($"a" > 0 && $"p1" === 1).collect()

    // Check for column pruning, filter push-down, and partition pruning
    assert(TestResult.requiredColumns.toSet === Set("a", "b"))
    assert(TestResult.filters === Seq(GreaterThan("a", 0)))
    assert(TestResult.inputPaths === Seq(new Path(basePath, "p1=1").toString))
  }

  ignore("load() - partitioned table - partition column included in data files") {
    val data = sparkContext.parallelize(Seq.empty[String])
    data.saveAsTextFile(new Path(basePath, "p1=1/p2=hello").toString)
    data.saveAsTextFile(new Path(basePath, "p1=2/p2=world").toString)

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

    df.select("b").where($"a" > 0 && $"p1" === 1).collect()

    // Check for column pruning, filter push-down, and partition pruning
    assert(TestResult.requiredColumns.toSet === Set("a", "b"))
    assert(TestResult.filters === Seq(GreaterThan("a", 0)))
    assert(TestResult.inputPaths === Seq(new Path(basePath, "p1=1").toString))
  }

  ignore("save() - partitioned table - Overwrite") {
    testDF.save(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 2) yield Row(i, s"val_$i")
    assert(TestResult.writtenRows.sameElements(expectedRows))
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
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 2) yield Row(i, s"val_$i")
    assert(TestResult.writtenRows.sameElements(expectedRows))
  }

  ignore("save() - partitioned table - Append") {
    testDF.save(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    testDF.save(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Append,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 4) yield Row(i, s"val_$i")
    assert(TestResult.writtenRows.sameElements(expectedRows))
  }

  ignore("save() - partitioned table - ErrorIfExists") {
    fs.delete(basePath, true)

    testDF.save(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    assert(TestResult.writtenRows.sameElements(testDF.collect()))

    intercept[IOException] {
      testDF.save(
        source = classOf[SimpleFSBasedSource].getCanonicalName,
        mode = SaveMode.Overwrite,
        options = Map(
          "path" -> basePath.toString,
          "schema" -> dataSchema.json),
        partitionColumns = Seq("p1"))
    }
  }

  ignore("save() - partitioned table - Ignore") {
    testDF.save(
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    assert(TestResult.writtenRows.isEmpty)
  }

  ignore("saveAsTable() - partitioned table - Overwrite") {
    testDF.saveAsTable(
      tableName = "t",
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 2) yield Row(i, s"val_$i")
    assert(TestResult.writtenRows.sameElements(expectedRows))

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
        "schema" -> dataSchema.json))

    df.saveAsTable(
      tableName = "t",
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 2) yield Row(i, s"val_$i")
    assert(TestResult.writtenRows.sameElements(expectedRows))

    assertResult(table("t").schema) {
      StructType(
        dataSchema ++ Seq(
          StructField("p1", IntegerType, nullable = true)))
    }

    sql("DROP TABLE t")
  }

  ignore("saveAsTable() - partitioned table - Append") {
    testDF.saveAsTable(
      tableName = "t",
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    testDF.saveAsTable(
      tableName = "t",
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Append,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    // Written rows shouldn't contain dynamic partition column
    val expectedRows = for (i <- 1 to 3; _ <- 1 to 4) yield Row(i, s"val_$i")
    assert(TestResult.writtenRows.sameElements(expectedRows))

    assertResult(table("t").schema) {
      StructType(
        dataSchema ++ Seq(
          StructField("p1", IntegerType, nullable = true)))
    }

    sql("DROP TABLE t")
  }

  ignore("saveAsTable() - partitioned table - ErrorIfExists") {
    fs.delete(basePath, true)

    testDF.saveAsTable(
      tableName = "t",
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    assert(TestResult.writtenRows.sameElements(testDF.collect()))

    assertResult(table("t").schema) {
      StructType(
        dataSchema ++ Seq(
          StructField("p1", IntegerType, nullable = true)))
    }

    intercept[IOException] {
      testDF.saveAsTable(
        tableName = "t",
        source = classOf[SimpleFSBasedSource].getCanonicalName,
        mode = SaveMode.Overwrite,
        options = Map(
          "path" -> basePath.toString,
          "schema" -> dataSchema.json),
        partitionColumns = Seq("p1"))
    }

    sql("DROP TABLE t")
  }

  ignore("saveAsTable() - partitioned table - Ignore") {
    testDF.saveAsTable(
      tableName = "t",
      source = classOf[SimpleFSBasedSource].getCanonicalName,
      mode = SaveMode.Overwrite,
      options = Map(
        "path" -> basePath.toString,
        "schema" -> dataSchema.json),
      partitionColumns = Seq("p1"))

    assert(TestResult.writtenRows.isEmpty)

    assertResult(table("t").schema) {
      StructType(
        dataSchema ++ Seq(
          StructField("p1", IntegerType, nullable = true)))
    }

    sql("DROP TABLE t")
  }
}
