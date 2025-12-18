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

package org.apache.spark.sql.test

import java.io.File
import java.util.{Locale, Random}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkContext, SparkIllegalArgumentException, TestUtils}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.noop.NoopDataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.Utils


object LastOptions {

  var parameters: Map[String, String] = null
  var schema: Option[StructType] = null
  var saveMode: SaveMode = null

  def clear(): Unit = {
    parameters = null
    schema = null
    saveMode = null
  }
}

/** Dummy provider. */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

  case class FakeRelation(sqlContext: SQLContext) extends BaseRelation {
    override def schema: StructType = StructType(Seq(StructField("a", StringType)))
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType
    ): BaseRelation = {
    LastOptions.parameters = parameters
    LastOptions.schema = Some(schema)
    FakeRelation(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
    ): BaseRelation = {
    LastOptions.parameters = parameters
    LastOptions.schema = None
    FakeRelation(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    LastOptions.parameters = parameters
    LastOptions.schema = None
    LastOptions.saveMode = mode
    FakeRelation(sqlContext)
  }
}

/** Dummy provider with only RelationProvider and CreatableRelationProvider. */
class DefaultSourceWithoutUserSpecifiedSchema
  extends RelationProvider
  with CreatableRelationProvider {

  case class FakeRelation(sqlContext: SQLContext) extends BaseRelation {
    override def schema: StructType = StructType(Seq(StructField("a", StringType)))
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    FakeRelation(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    FakeRelation(sqlContext)
  }
}

object MessageCapturingCommitProtocol {
  val commitMessages = new ConcurrentLinkedQueue[TaskCommitMessage]()
}

class MessageCapturingCommitProtocol(jobId: String, path: String)
    extends HadoopMapReduceCommitProtocol(jobId, path) {

  // captures commit messages for testing
  override def onTaskCommit(msg: TaskCommitMessage): Unit = {
    MessageCapturingCommitProtocol.commitMessages.offer(msg)
  }
}


class DataFrameReaderWriterSuite extends QueryTest with SharedSparkSession with BeforeAndAfter {
  import testImplicits._

  private val userSchema = new StructType().add("s", StringType)
  private val userSchemaString = "s STRING"
  private val textSchema = new StructType().add("value", StringType)
  private val data = Seq("1", "2", "3")
  private val dir = Utils.createTempDir(namePrefix = "input").getCanonicalPath

  before {
    Utils.deleteRecursively(new File(dir))
  }

  test("writeStream cannot be called on non-streaming datasets") {
    val e = intercept[AnalysisException] {
      spark.read
        .format("org.apache.spark.sql.test")
        .load()
        .writeStream
        .start()
    }
    checkError(
      exception = e,
      condition = "WRITE_STREAM_NOT_ALLOWED",
      parameters = Map.empty
    )
  }

  test("resolve default source") {
    spark.read
      .format("org.apache.spark.sql.test")
      .load()
      .write
      .format("org.apache.spark.sql.test")
      .save()
  }

  test("resolve default source without extending SchemaRelationProvider") {
    spark.read
      .format("org.apache.spark.sql.test.DefaultSourceWithoutUserSpecifiedSchema")
      .load()
      .write
      .format("org.apache.spark.sql.test.DefaultSourceWithoutUserSpecifiedSchema")
      .save()
  }

  test("resolve full class") {
    spark.read
      .format("org.apache.spark.sql.test.DefaultSource")
      .load()
      .write
      .format("org.apache.spark.sql.test")
      .save()
  }

  test("options") {
    val map = new java.util.HashMap[String, String]
    map.put("opt3", "3")

    val df = spark.read
        .format("org.apache.spark.sql.test")
        .option("opt1", "1")
        .options(Map("opt2" -> "2"))
        .options(map)
        .load()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")

    LastOptions.clear()

    df.write
      .format("org.apache.spark.sql.test")
      .option("opt1", "1")
      .options(Map("opt2" -> "2"))
      .options(map)
      .save()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")
  }

  test("SPARK-32364: later option should override earlier options for load()") {
    spark.read
      .format("org.apache.spark.sql.test")
      .option("paTh", "1")
      .option("PATH", "2")
      .option("Path", "3")
      .option("patH", "4")
      .option("path", "5")
      .load()
    assert(LastOptions.parameters("path") == "5")

    withClue("SPARK-32516: legacy path option behavior") {
      withSQLConf(SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key -> "true") {
        spark.read
          .format("org.apache.spark.sql.test")
          .option("paTh", "1")
          .option("PATH", "2")
          .option("Path", "3")
          .option("patH", "4")
          .load("5")
        assert(LastOptions.parameters("path") == "5")
      }
    }
  }

  test("SPARK-32364: later option should override earlier options for save()") {
    Seq(1).toDF().write
      .format("org.apache.spark.sql.test")
      .option("paTh", "1")
      .option("PATH", "2")
      .option("Path", "3")
      .option("patH", "4")
      .option("path", "5")
      .save()
    assert(LastOptions.parameters("path") == "5")

    withClue("SPARK-32516: legacy path option behavior") {
      withSQLConf(SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key -> "true") {
        Seq(1).toDF().write
          .format("org.apache.spark.sql.test")
          .option("paTh", "1")
          .option("PATH", "2")
          .option("Path", "3")
          .option("patH", "4")
          .save("5")
        assert(LastOptions.parameters("path") == "5")
      }
    }
  }

  test("pass partitionBy as options") {
    Seq(1).toDF().write
      .format("org.apache.spark.sql.test")
      .partitionBy("col1", "col2")
      .save()

    val partColumns = LastOptions.parameters(DataSourceUtils.PARTITIONING_COLUMNS_KEY)
    assert(DataSourceUtils.decodePartitioningColumns(partColumns) === Seq("col1", "col2"))
  }

  test("pass clusterBy as options") {
    Seq(1).toDF().write
      .format("org.apache.spark.sql.test")
      .clusterBy("col1", "col2")
      .save()

    val clusteringColumns = LastOptions.parameters(DataSourceUtils.CLUSTERING_COLUMNS_KEY)
    assert(DataSourceUtils.decodePartitioningColumns(clusteringColumns) === Seq("col1", "col2"))
  }

  test("Clustering columns should match when appending to existing data source tables") {
    import testImplicits._
    val df = Seq((1, 2, 3)).toDF("a", "b", "c")
    withTable("clusteredTable") {
      df.write.mode("overwrite").clusterBy("a", "b").saveAsTable("clusteredTable")
      // Misses some clustering columns
      checkError(
        exception = intercept[AnalysisException] {
          df.write.mode("append").clusterBy("a").saveAsTable("clusteredTable")
        },
        condition = "CLUSTERING_COLUMNS_MISMATCH",
        parameters = Map(
          "tableName" -> "spark_catalog.default.clusteredtable",
          "specifiedClusteringString" -> """[["a"]]""",
          "existingClusteringString" -> """[["a"],["b"]]""")
      )
      // Wrong order
      checkError(
        exception = intercept[AnalysisException] {
          df.write.mode("append").clusterBy("b", "a").saveAsTable("clusteredTable")
        },
        condition = "CLUSTERING_COLUMNS_MISMATCH",
        parameters = Map(
          "tableName" -> "spark_catalog.default.clusteredtable",
          "specifiedClusteringString" -> """[["b"],["a"]]""",
          "existingClusteringString" -> """[["a"],["b"]]""")
      )
      // Clustering columns not specified
      checkError(
        exception = intercept[AnalysisException] {
          df.write.mode("append").saveAsTable("clusteredTable")
        },
        condition = "CLUSTERING_COLUMNS_MISMATCH",
        parameters = Map(
          "tableName" -> "spark_catalog.default.clusteredtable",
          "specifiedClusteringString" -> "", "existingClusteringString" -> """[["a"],["b"]]""")
      )
      assert(sql("select * from clusteredTable").collect().length == 1)
      // Inserts new data successfully when clustering columns are correctly specified in
      // clusterBy(...).
      Seq((4, 5, 6)).toDF("a", "b", "c")
        .write
        .mode("append")
        .clusterBy("a", "b")
        .saveAsTable("clusteredTable")

      Seq((7, 8, 9)).toDF("a", "b", "c")
        .write
        .mode("append")
        .clusterBy("a", "b")
        .saveAsTable("clusteredTable")

      checkAnswer(
        sql("select a, b, c from clusteredTable"),
        Row(1, 2, 3) :: Row(4, 5, 6) :: Row(7, 8, 9) :: Nil
      )
    }
  }

  test ("SPARK-29537: throw exception when user defined a wrong base path") {
    withTempPath { p =>
      val path = new Path(p.toURI).toString
      Seq((1, 1), (2, 2)).toDF("c1", "c2")
        .write.partitionBy("c1").mode(SaveMode.Overwrite).parquet(path)
      val wrongBasePath = new File(p, "unknown")
      // basePath must be a directory
      wrongBasePath.mkdir()
      val msg = intercept[IllegalArgumentException] {
        spark.read.option("basePath", wrongBasePath.getCanonicalPath).parquet(path)
      }.getMessage
      assert(msg === s"Wrong basePath ${wrongBasePath.getCanonicalPath} for the root path: $path")
    }
  }

  test("save mode") {
    spark.range(10).write
      .format("org.apache.spark.sql.test")
      .mode(SaveMode.ErrorIfExists)
      .save()
    assert(LastOptions.saveMode === SaveMode.ErrorIfExists)

    spark.range(10).write
      .format("org.apache.spark.sql.test")
      .mode(SaveMode.Append)
      .save()
    assert(LastOptions.saveMode === SaveMode.Append)

    // By default the save mode is `ErrorIfExists` for data source v1.
    spark.range(10).write
      .format("org.apache.spark.sql.test")
      .save()
    assert(LastOptions.saveMode === SaveMode.ErrorIfExists)

    spark.range(10).write
      .format("org.apache.spark.sql.test")
      .mode("default")
      .save()
    assert(LastOptions.saveMode === SaveMode.ErrorIfExists)
  }

  test("save mode for data source v2") {
    var plan: LogicalPlan = null
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plan = qe.analyzed

      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    spark.listenerManager.register(listener)
    try {
      // append mode creates `AppendData`
      spark.range(10).write
        .format(classOf[NoopDataSource].getName)
        .mode(SaveMode.Append)
        .save()
      sparkContext.listenerBus.waitUntilEmpty()
      assert(plan.isInstanceOf[AppendData])

      // overwrite mode creates `OverwriteByExpression`
      spark.range(10).write
        .format(classOf[NoopDataSource].getName)
        .mode(SaveMode.Overwrite)
        .save()
      sparkContext.listenerBus.waitUntilEmpty()
      assert(plan.isInstanceOf[OverwriteByExpression])

      // By default the save mode is `ErrorIfExists` for data source v2.
      val e = intercept[AnalysisException] {
        spark.range(10).write
          .format(classOf[NoopDataSource].getName)
          .save()
      }
      assert(e.getMessage.contains("ErrorIfExists"))

      val e2 = intercept[AnalysisException] {
        spark.range(10).write
          .format(classOf[NoopDataSource].getName)
          .mode("default")
          .save()
      }
      assert(e2.getMessage.contains("ErrorIfExists"))
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  test("Throw exception on unsafe table insertion with strict casting policy") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.STRICT.toString) {
      withTable("t") {
        sql("create table t(i int, d double) using parquet")
        // Calling `saveAsTable` to an existing table with append mode results in table insertion.
        checkError(
          exception = intercept[AnalysisException] {
            Seq((1L, 2.0)).toDF("i", "d").write.mode("append").saveAsTable("t")
          },
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
          parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`t`",
            "colName" -> "`i`",
            "srcType" -> "\"BIGINT\"",
            "targetType" -> "\"INT\"")
        )

        // Insert into table successfully.
        Seq((1, 2.0)).toDF("i", "d").write.mode("append").saveAsTable("t")
        // The API `saveAsTable` matches the fields by name.
        Seq((4.0, 3)).toDF("d", "i").write.mode("append").saveAsTable("t")
        checkAnswer(sql("select * from t"), Seq(Row(1, 2.0), Row(3, 4.0)))
      }
    }
  }

  test("Throw exception on unsafe cast with ANSI casting policy") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.ANSI.toString) {
      withTable("t") {
        sql("create table t(i int, d double) using parquet")
        // Calling `saveAsTable` to an existing table with append mode results in table insertion.
        checkError(
          exception = intercept[AnalysisException] {
            Seq(("a", "b")).toDF("i", "d").write.mode("append").saveAsTable("t")
          },
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
          parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`t`",
            "colName" -> "`i`",
            "srcType" -> "\"STRING\"",
            "targetType" -> "\"INT\"")
        )

        checkError(
          exception = intercept[AnalysisException] {
            Seq((true, false)).toDF("i", "d").write.mode("append").saveAsTable("t")
          },
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
          parameters = Map(
            "tableName" -> "`spark_catalog`.`default`.`t`",
            "colName" -> "`i`",
            "srcType" -> "\"BOOLEAN\"",
            "targetType" -> "\"INT\"")
        )
      }
    }
  }

  test("test path option in load") {
    spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 56)
      .load("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("path") == "/test")

    LastOptions.clear()
    spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 55)
      .load()

    assert(LastOptions.parameters("intOpt") == "55")
    assert(!LastOptions.parameters.contains("path"))

    LastOptions.clear()
    spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 54)
      .load("/test", "/test1", "/test2")

    assert(LastOptions.parameters("intOpt") == "54")
    assert(!LastOptions.parameters.contains("path"))
  }

  test("test different data types for options") {
    val df = spark.read
      .format("org.apache.spark.sql.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .load("/test")

    assert(
      !df.queryExecution.logical.resolved,
      "DataFrameReader should create an unresolved plan")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")

    LastOptions.clear()
    df.write
      .format("org.apache.spark.sql.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .save("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")
  }

  test("check jdbc() does not support partitioning, bucketBy, clusterBy or sortBy") {
    val df = spark.read.text(Utils.createTempDir(namePrefix = "text").getCanonicalPath)

    var w = df.write.partitionBy("value")
    var e = intercept[AnalysisException](w.jdbc(null, null, null))
    Seq("jdbc", "partitioning").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }

    w = df.write.bucketBy(2, "value")
    e = intercept[AnalysisException](w.jdbc(null, null, null))
    Seq("jdbc", "does not support bucketBy right now").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }

    w = df.write.clusterBy("value")
    e = intercept[AnalysisException](w.jdbc(null, null, null))
    Seq("jdbc", "clustering").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }

    w = df.write.sortBy("value")
    e = intercept[AnalysisException](w.jdbc(null, null, null))
    Seq("sortBy must be used together with bucketBy").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }

    w = df.write.bucketBy(2, "value").sortBy("value")
    e = intercept[AnalysisException](w.jdbc(null, null, null))
    Seq("jdbc", "does not support bucketBy and sortBy right now").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }
  }

  test("prevent all column partitioning") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      intercept[AnalysisException] {
        spark.range(10).write.format("parquet").mode("overwrite").partitionBy("id").save(path)
      }
      intercept[AnalysisException] {
        spark.range(10).write.format("csv").mode("overwrite").partitionBy("id").save(path)
      }
    }
  }

  test("load API") {
    def assertFirstUnresolved(df: DataFrame): Unit = {
      assert(!df.queryExecution.logical.resolved)
    }
    assertFirstUnresolved(spark.read.format("org.apache.spark.sql.test").load())
    assertFirstUnresolved(spark.read.format("org.apache.spark.sql.test").load(dir))
    assertFirstUnresolved(spark.read.format("org.apache.spark.sql.test").load(dir, dir, dir))
    assertFirstUnresolved(spark.read.format("org.apache.spark.sql.test").load(Seq(dir, dir): _*))
    Option(dir).map(spark.read.format("org.apache.spark.sql.test").load)
  }

  test("write path implements onTaskCommit API correctly") {
    withSQLConf(
        SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
          classOf[MessageCapturingCommitProtocol].getCanonicalName) {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        MessageCapturingCommitProtocol.commitMessages.clear()
        spark.range(10).repartition(10).write.mode("overwrite").parquet(path)
        assert(MessageCapturingCommitProtocol.commitMessages.size() == 10)
      }
    }
  }

  test("read a data source that does not extend SchemaRelationProvider") {
    val dfReader = spark.read
      .option("from", "1")
      .option("TO", "10")
      .format("org.apache.spark.sql.sources.SimpleScanSource")

    val answerDf = spark.range(1, 11).toDF()

    // when users do not specify the schema
    checkAnswer(dfReader.load(), answerDf)

    // same base schema, differing metadata and nullability
    val fooBarMetadata = new MetadataBuilder().putString("foo", "bar").build()
    val nullableAndMetadataCases = Seq(
      (false, fooBarMetadata),
      (false, Metadata.empty),
      (true, fooBarMetadata),
      (true, Metadata.empty))
    nullableAndMetadataCases.foreach { case (nullable, metadata) =>
      val inputSchema = new StructType()
        .add("i", IntegerType, nullable = nullable, metadata = metadata)
      checkAnswer(dfReader.schema(inputSchema).load(), answerDf)
    }

    // when users specify a wrong schema
    var inputSchema = new StructType().add("s", IntegerType, nullable = false)
    var e = intercept[AnalysisException] { dfReader.schema(inputSchema).load() }
    assert(e.getMessage.contains("The user-specified schema doesn't match the actual schema"))

    inputSchema = new StructType().add("i", StringType, nullable = true)
    e = intercept[AnalysisException] { dfReader.schema(inputSchema).load() }
    assert(e.getMessage.contains("The user-specified schema doesn't match the actual schema"))
  }

  test("read a data source that does not extend RelationProvider") {
    val dfReader = spark.read
      .option("from", "1")
      .option("TO", "10")
      .option("option_with_underscores", "someval")
      .option("option.with.dots", "someval")
      .format("org.apache.spark.sql.sources.AllDataTypesScanSource")

    // when users do not specify the schema
    val e = intercept[AnalysisException] { dfReader.load() }
    assert(e.getMessage.contains("A schema needs to be specified when using"))

    // when users specify the schema
    val inputSchema = new StructType().add("s", StringType, nullable = false)
    assert(dfReader.schema(inputSchema).load().count() == 10)
  }

  test("text - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).write.mode(SaveMode.Overwrite).text(dir)
    testRead(spark.read.text(dir), data, textSchema)

    // Reader, without user specified schema
    testRead(spark.read.text(), Seq.empty, textSchema)
    testRead(spark.read.text(dir, dir, dir), data ++ data ++ data, textSchema)
    testRead(spark.read.text(Seq(dir, dir): _*), data ++ data, textSchema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.text).get, data, textSchema)

    // Reader, with user specified schema, should just apply user schema on the file data
    testRead(spark.read.schema(userSchema).text(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).text(dir), data, userSchema)
    testRead(spark.read.schema(userSchema).text(dir, dir), data ++ data, userSchema)
    testRead(spark.read.schema(userSchema).text(Seq(dir, dir): _*), data ++ data, userSchema)
  }

  test("textFile - API and behavior regarding schema") {
    spark.createDataset(data).write.mode(SaveMode.Overwrite).text(dir)

    // Reader, without user specified schema
    testRead(spark.read.text().toDF(), Seq.empty, textSchema)
    testRead(spark.read.text(dir).toDF(), data, textSchema)
    testRead(spark.read.text(dir, dir).toDF(), data ++ data, textSchema)
    testRead(spark.read.text(Seq(dir, dir): _*).toDF(), data ++ data, textSchema)
    testRead(spark.read.textFile().toDF(), Seq.empty, textSchema, checkLogicalPlan = false)
    testRead(spark.read.textFile(dir).toDF(), data, textSchema, checkLogicalPlan = false)
    testRead(
      spark.read.textFile(dir, dir).toDF(), data ++ data, textSchema, checkLogicalPlan = false)
    testRead(
      spark.read.textFile(Seq(dir, dir): _*).toDF(),
      data ++ data,
      textSchema,
      checkLogicalPlan = false)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.text).get, data, textSchema)

    // Reader, with user specified schema, should just apply user schema on the file data
    val e = intercept[AnalysisException] { spark.read.schema(userSchema).textFile() }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains(
      "user specified schema not supported"))
    intercept[AnalysisException] { spark.read.schema(userSchema).textFile(dir) }
    intercept[AnalysisException] { spark.read.schema(userSchema).textFile(dir, dir) }
    intercept[AnalysisException] { spark.read.schema(userSchema).textFile(Seq(dir, dir): _*) }
  }

  test("csv - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).csv(dir)
    val df = spark.read.csv(dir)
    checkAnswer(df, spark.createDataset(data).toDF())
    val schema = df.schema

    // Reader, without user specified schema
    checkError(
      exception = intercept[AnalysisException] {
        testRead(spark.read.csv(), Seq.empty, schema)
      },
      condition = "UNABLE_TO_INFER_SCHEMA",
      parameters = Map("format" -> "CSV")
    )

    testRead(spark.read.csv(dir), data, schema)
    testRead(spark.read.csv(dir, dir), data ++ data, schema)
    testRead(spark.read.csv(Seq(dir, dir): _*), data ++ data, schema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.csv).get, data, schema)

    // Reader, with user specified schema, should just apply user schema on the file data
    testRead(spark.read.schema(userSchema).csv(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).csv(dir), data, userSchema)
    testRead(spark.read.schema(userSchema).csv(dir, dir), data ++ data, userSchema)
    testRead(spark.read.schema(userSchema).csv(Seq(dir, dir): _*), data ++ data, userSchema)
  }

  test("json - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).json(dir)
    val df = spark.read.json(dir)
    checkAnswer(df, spark.createDataset(data).toDF())
    val schema = df.schema

    // Reader, without user specified schema
    intercept[AnalysisException] {
      testRead(spark.read.json(), Seq.empty, schema)
    }
    testRead(spark.read.json(dir), data, schema)
    testRead(spark.read.json(dir, dir), data ++ data, schema)
    testRead(spark.read.json(Seq(dir, dir): _*), data ++ data, schema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.json).get, data, schema)

    // Reader, with user specified schema, data should be nulls as schema in file different
    // from user schema
    val expData = Seq[String](null, null, null)
    testRead(spark.read.schema(userSchema).json(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).json(dir), expData, userSchema)
    testRead(spark.read.schema(userSchema).json(dir, dir), expData ++ expData, userSchema)
    testRead(spark.read.schema(userSchema).json(Seq(dir, dir): _*), expData ++ expData, userSchema)
  }

  test("parquet - API and behavior regarding schema") {
    // Writer
    spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).parquet(dir)
    val df = spark.read.parquet(dir)
    checkAnswer(df, spark.createDataset(data).toDF())
    val schema = df.schema

    // Reader, without user specified schema
    intercept[AnalysisException] {
      testRead(spark.read.parquet(), Seq.empty, schema)
    }
    testRead(spark.read.parquet(dir), data, schema)
    testRead(spark.read.parquet(dir, dir), data ++ data, schema)
    testRead(spark.read.parquet(Seq(dir, dir): _*), data ++ data, schema)
    // Test explicit calls to single arg method - SPARK-16009
    testRead(Option(dir).map(spark.read.parquet).get, data, schema)

    // Reader, with user specified schema, data should be nulls as schema in file different
    // from user schema
    val expData = Seq[String](null, null, null)
    testRead(spark.read.schema(userSchema).parquet(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchema).parquet(dir), expData, userSchema)
    testRead(spark.read.schema(userSchema).parquet(dir, dir), expData ++ expData, userSchema)
    testRead(
      spark.read.schema(userSchema).parquet(Seq(dir, dir): _*), expData ++ expData, userSchema)
  }

  test("orc - API and behavior regarding schema") {
    withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> "native") {
      // Writer
      spark.createDataset(data).toDF("str").write.mode(SaveMode.Overwrite).orc(dir)
      val df = spark.read.orc(dir)
      checkAnswer(df, spark.createDataset(data).toDF())
      val schema = df.schema

      // Reader, without user specified schema
      intercept[AnalysisException] {
        testRead(spark.read.orc(), Seq.empty, schema)
      }
      testRead(spark.read.orc(dir), data, schema)
      testRead(spark.read.orc(dir, dir), data ++ data, schema)
      testRead(spark.read.orc(Seq(dir, dir): _*), data ++ data, schema)
      // Test explicit calls to single arg method - SPARK-16009
      testRead(Option(dir).map(spark.read.orc).get, data, schema)

      // Reader, with user specified schema, data should be nulls as schema in file different
      // from user schema
      val expData = Seq[String](null, null, null)
      testRead(spark.read.schema(userSchema).orc(), Seq.empty, userSchema)
      testRead(spark.read.schema(userSchema).orc(dir), expData, userSchema)
      testRead(spark.read.schema(userSchema).orc(dir, dir), expData ++ expData, userSchema)
      testRead(
        spark.read.schema(userSchema).orc(Seq(dir, dir): _*), expData ++ expData, userSchema)
    }
  }

  test("column nullability and comment - write and then read") {
    withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> "native") {
      Seq("json", "orc", "parquet", "csv").foreach { format =>
        val schema = StructType(
          StructField("cl1", IntegerType, nullable = false).withComment("test") ::
          StructField("cl2", IntegerType, nullable = true) ::
          StructField("cl3", IntegerType, nullable = true) :: Nil)
        val row = Row(3, null, 4)
        val df = spark.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

        // if we write and then read, the read will enforce schema to be nullable
        val tableName = "tab"
        withTable(tableName) {
          df.write.format(format).mode("overwrite").saveAsTable(tableName)
          // Verify the DDL command result: DESCRIBE TABLE
          checkAnswer(
            sql(s"desc $tableName").select("col_name", "comment").where($"comment" === "test"),
            Row("cl1", "test") :: Nil)
          // Verify the schema
          val expectedFields = schema.fields.map(f => f.copy(nullable = true))
          assert(spark.table(tableName).schema === schema.copy(fields = expectedFields))
        }
      }
    }
  }

  test("parquet - column nullability -- write only") {
    val schema = StructType(
      StructField("cl1", IntegerType, nullable = false) ::
      StructField("cl2", IntegerType, nullable = true) :: Nil)
    val row = Row(3, 4)
    val df = spark.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

    withTempPath { dir =>
      val path = dir.getAbsolutePath
      df.write.mode("overwrite").parquet(path)
      val file = TestUtils.listDirectory(dir).head

      val hadoopInputFile = HadoopInputFile.fromPath(new Path(file), new Configuration())
      val f = ParquetFileReader.open(hadoopInputFile)
      val parquetSchema = f.getFileMetaData.getSchema.getColumns.asScala
                          .map(_.getPrimitiveType)
      f.close()

      // the write keeps nullable info from the schema
      val expectedParquetSchema = Seq(
        new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cl1"),
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "cl2")
      )

      assert (expectedParquetSchema === parquetSchema)
    }

  }

  test("SPARK-17230: write out results of decimal calculation") {
    val df = spark.range(99, 101)
      .selectExpr("id", "cast(id as long) * cast('1.0' as decimal(38, 18)) as num")
    df.write.mode(SaveMode.Overwrite).parquet(dir)
    val df2 = spark.read.parquet(dir)
    checkAnswer(df2, df)
  }

  private def testRead(
      df: => DataFrame,
      expectedResult: Seq[String],
      expectedSchema: StructType,
      checkLogicalPlan: Boolean = true): Unit = {
    checkAnswer(df, spark.createDataset(expectedResult).toDF())
    assert(df.schema === expectedSchema)
    if (checkLogicalPlan) {
      // While the `textfile` API also leverages our UnresolvedDataSource plan, it later overrides
      // the plan with encoders. Therefore this check fails
      assert(!df.queryExecution.logical.resolved, "Should've created an unresolved plan")
    }
  }

  test("saveAsTable with mode Append should not fail if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.Append).saveAsTable("same_name")
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("saveAsTable with mode Append should not fail if the table already exists " +
    "and a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        val format = spark.sessionState.conf.defaultDataSourceName
        sql(s"CREATE TABLE same_name(id LONG) USING $format")
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.Append).saveAsTable("same_name")
        checkAnswer(spark.table("same_name"), spark.range(10).toDF())
        checkAnswer(spark.table("default.same_name"), spark.range(20).toDF())
      }
    }
  }

  test("saveAsTable with mode ErrorIfExists should not fail if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.ErrorIfExists).saveAsTable("same_name")
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("saveAsTable with mode Overwrite should not drop the temp view if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.Overwrite).saveAsTable("same_name")
        assert(spark.sessionState.catalog.getTempView("same_name").isDefined)
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("saveAsTable with mode Overwrite should not fail if the table already exists " +
    "and a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        sql("CREATE TABLE same_name(id LONG) USING parquet")
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.Overwrite).saveAsTable("same_name")
        checkAnswer(spark.table("same_name"), spark.range(10).toDF())
        checkAnswer(spark.table("default.same_name"), spark.range(20).toDF())
      }
    }
  }

  test("saveAsTable with mode Ignore should create the table if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.mode(SaveMode.Ignore).saveAsTable("same_name")
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("SPARK-18510: use user specified types for partition columns in file sources") {
    import org.apache.spark.sql.functions.udf
    withTempDir { src =>
      val createArray = udf { (length: Long) =>
        for (i <- 1 to length.toInt) yield i.toString
      }
      spark.range(4).select(createArray($"id" + 1) as Symbol("ex"),
        $"id", $"id" % 4 as Symbol("part")).coalesce(1).write
        .partitionBy("part", "id")
        .mode("overwrite")
        .parquet(src.toString)
      // Specify a random ordering of the schema, partition column in the middle, etc.
      // Also let's say that the partition columns are Strings instead of Longs.
      // partition columns should go to the end
      val schema = new StructType()
        .add("id", StringType)
        .add("ex", ArrayType(StringType))
      val df = spark.read
        .schema(schema)
        .format("parquet")
        .load(src.toString)

      assert(df.schema.toList === List(
        StructField("ex", ArrayType(StringType)),
        StructField("part", IntegerType), // inferred partitionColumn dataType
        StructField("id", StringType))) // used user provided partitionColumn dataType

      checkAnswer(
        df,
        // notice how `part` is ordered before `id`
        Row(Array("1"), 0, "0") :: Row(Array("1", "2"), 1, "1") ::
          Row(Array("1", "2", "3"), 2, "2") :: Row(Array("1", "2", "3", "4"), 3, "3") :: Nil
      )
    }
  }

  test("SPARK-18899: append to a bucketed table using DataFrameWriter with mismatched bucketing") {
    withTable("t") {
      Seq(1 -> "a", 2 -> "b").toDF("i", "j").write.bucketBy(2, "i").saveAsTable("t")
      val e = intercept[AnalysisException] {
        Seq(3 -> "c").toDF("i", "j").write.bucketBy(3, "i").mode("append").saveAsTable("t")
      }
      assert(e.message.contains("Specified bucketing does not match that of the existing table"))
    }
  }

  test("SPARK-18912: number of columns mismatch for non-file-based data source table") {
    withTable("t") {
      sql("CREATE TABLE t USING org.apache.spark.sql.test.DefaultSource")

      val e = intercept[AnalysisException] {
        Seq(1 -> "a").toDF("a", "b").write
          .format("org.apache.spark.sql.test.DefaultSource")
          .mode("append").saveAsTable("t")
      }
      assert(e.message.contains("The column number of the existing table"))
    }
  }

  test("SPARK-18913: append to a table with special column names") {
    withTable("t") {
      Seq(1 -> "a").toDF("x.x", "y.y").write.saveAsTable("t")
      Seq(2 -> "b").toDF("x.x", "y.y").write.mode("append").saveAsTable("t")
      checkAnswer(spark.table("t"), Row(1, "a") :: Row(2, "b") :: Nil)
    }
  }

  test("SPARK-16848: table API throws an exception for user specified schema") {
    withTable("t") {
      val schema = StructType(StructField("a", StringType) :: Nil)
      val e = intercept[AnalysisException] {
        spark.read.schema(schema).table("t")
      }.getMessage
      assert(e.contains("User specified schema not supported with `table`"))
    }
  }

  test("SPARK-20431: Specify a schema by using a DDL-formatted string") {
    spark.createDataset(data).write.mode(SaveMode.Overwrite).text(dir)
    testRead(spark.read.schema(userSchemaString).text(), Seq.empty, userSchema)
    testRead(spark.read.schema(userSchemaString).text(dir), data, userSchema)
    testRead(spark.read.schema(userSchemaString).text(dir, dir), data ++ data, userSchema)
    testRead(spark.read.schema(userSchemaString).text(Seq(dir, dir): _*), data ++ data, userSchema)
  }

  test("SPARK-20460 Check name duplication in buckets") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        checkError(
          exception = intercept[AnalysisException] {
            Seq((1, 1)).toDF("col", c0).write.bucketBy(2, c0, c1).saveAsTable("t")
          },
          condition = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c1.toLowerCase(Locale.ROOT)}`"))
        checkError(
          exception = intercept[AnalysisException] {
            Seq((1, 1)).toDF("col", c0).write.bucketBy(2, "col").sortBy(c0, c1).saveAsTable("t")
          },
          condition = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c1.toLowerCase(Locale.ROOT)}`"))
      }
    }
  }

  test("SPARK-20460 Check name duplication in schema") {
    def checkWriteDataColumnDuplication(
        format: String, colName0: String, colName1: String, tempDir: File): Unit = {
      checkError(
        exception = intercept[AnalysisException] {
          Seq((1, 1)).toDF(colName0, colName1).write.format(format).mode("overwrite")
            .save(tempDir.getAbsolutePath)
        },
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> s"`${colName1.toLowerCase(Locale.ROOT)}`"))
    }

    def checkReadUserSpecifiedDataColumnDuplication(
        df: DataFrame, format: String, colName0: String, colName1: String, tempDir: File): Unit = {
      val testDir = Utils.createTempDir(tempDir.getAbsolutePath)
      df.write.format(format).mode("overwrite").save(testDir.getAbsolutePath)
      checkError(
        exception = intercept[AnalysisException] {
          spark.read.format(format).schema(s"$colName0 INT, $colName1 INT")
            .load(testDir.getAbsolutePath)
        },
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> s"`${colName1.toLowerCase(Locale.ROOT)}`"))
    }

    def checkReadPartitionColumnDuplication(
        format: String, colName0: String, colName1: String, tempDir: File): Unit = {
      val testDir = Utils.createTempDir(tempDir.getAbsolutePath)
      Seq(1).toDF("col").write.format(format).mode("overwrite")
        .save(s"${testDir.getAbsolutePath}/$colName0=1/$colName1=1")
      checkError(
        exception = intercept[AnalysisException] {
          spark.read.format(format).load(testDir.getAbsolutePath)
        },
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> s"`${colName1.toLowerCase(Locale.ROOT)}`"))
    }

    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        withTempDir { src =>
          // Check CSV format
          checkReadUserSpecifiedDataColumnDuplication(
            Seq((1, 1)).toDF("c0", "c1"), "csv", c0, c1, src)
          // If `inferSchema` is true, a CSV format is duplicate-safe (See SPARK-16896)
          var testDir = Utils.createTempDir(src.getAbsolutePath)
          Seq("a,a", "1,1").toDF().coalesce(1).write.mode("overwrite").text(testDir.getAbsolutePath)
          val df = spark.read.format("csv").option("inferSchema", true).option("header", true)
            .load(testDir.getAbsolutePath)
          checkAnswer(df, Row(1, 1))
          checkReadPartitionColumnDuplication("csv", c0, c1, src)

          // Check JSON format
          checkWriteDataColumnDuplication("json", c0, c1, src)
          checkReadUserSpecifiedDataColumnDuplication(
            Seq((1, 1)).toDF("c0", "c1"), "json", c0, c1, src)
          // Inferred schema cases
          testDir = Utils.createTempDir(src.getAbsolutePath)
          Seq(s"""{"$c0":3, "$c1":5}""").toDF().write.mode("overwrite")
            .text(testDir.getAbsolutePath)
          checkError(
            exception = intercept[AnalysisException] {
              spark.read.format("json").option("inferSchema", true).load(testDir.getAbsolutePath)
            },
            condition = "COLUMN_ALREADY_EXISTS",
            parameters = Map("columnName" -> s"`${c1.toLowerCase(Locale.ROOT)}`"))
          checkReadPartitionColumnDuplication("json", c0, c1, src)

          // Check Parquet format
          checkWriteDataColumnDuplication("parquet", c0, c1, src)
          checkReadUserSpecifiedDataColumnDuplication(
            Seq((1, 1)).toDF("c0", "c1"), "parquet", c0, c1, src)
          checkReadPartitionColumnDuplication("parquet", c0, c1, src)

          // Check ORC format
          checkWriteDataColumnDuplication("orc", c0, c1, src)
          checkReadUserSpecifiedDataColumnDuplication(
            Seq((1, 1)).toDF("c0", "c1"), "orc", c0, c1, src)
          checkReadPartitionColumnDuplication("orc", c0, c1, src)
        }
      }
    }
  }

  test("Insert overwrite table command should output correct schema: basic") {
    withTable("tbl", "tbl2") {
      withView("view1") {
        val df = spark.range(10).toDF("id")
        df.write.format("parquet").saveAsTable("tbl")
        spark.sql("CREATE VIEW view1 AS SELECT id FROM tbl")
        spark.sql("CREATE TABLE tbl2(ID long) USING parquet")
        spark.sql("INSERT OVERWRITE TABLE tbl2 SELECT ID FROM view1")
        val identifier = TableIdentifier("tbl2")
        val location = spark.sessionState.catalog.getTableMetadata(identifier).location.toString
        val expectedSchema = StructType(Seq(StructField("ID", LongType, true)))
        assert(spark.read.parquet(location).schema == expectedSchema)
        checkAnswer(spark.table("tbl2"), df)
      }
    }
  }

  test("Insert overwrite table command should output correct schema: complex") {
    withTable("tbl", "tbl2") {
      withView("view1") {
        val df = spark.range(10).map(x => (x, x.toInt, x.toInt)).toDF("col1", "col2", "col3")
        df.write.format("parquet").saveAsTable("tbl")
        spark.sql("CREATE VIEW view1 AS SELECT * FROM tbl")
        spark.sql("CREATE TABLE tbl2(COL1 long, COL2 int, COL3 int) USING parquet PARTITIONED " +
          "BY (COL2) CLUSTERED BY (COL3) INTO 3 BUCKETS")
        spark.sql("INSERT OVERWRITE TABLE tbl2 SELECT COL1, COL2, COL3 FROM view1")
        val identifier = TableIdentifier("tbl2")
        val location = spark.sessionState.catalog.getTableMetadata(identifier).location.toString
        val expectedSchema = StructType(Seq(
          StructField("COL1", LongType, true),
          StructField("COL3", IntegerType, true),
          StructField("COL2", IntegerType, true)))
        assert(spark.read.parquet(location).schema == expectedSchema)
        checkAnswer(spark.table("tbl2"), df)
      }
    }
  }

  test("Create table as select command should output correct schema: basic") {
    withTable("tbl", "tbl2") {
      withView("view1") {
        val df = spark.range(10).toDF("id")
        df.write.format("parquet").saveAsTable("tbl")
        spark.sql("CREATE VIEW view1 AS SELECT id FROM tbl")
        spark.sql("CREATE TABLE tbl2 USING parquet AS SELECT ID FROM view1")
        val identifier = TableIdentifier("tbl2")
        val location = spark.sessionState.catalog.getTableMetadata(identifier).location.toString
        val expectedSchema = StructType(Seq(StructField("ID", LongType, true)))
        assert(spark.read.parquet(location).schema == expectedSchema)
        checkAnswer(spark.table("tbl2"), df)
      }
    }
  }

  test("Create table as select command should output correct schema: complex") {
    withTable("tbl", "tbl2") {
      withView("view1") {
        val df = spark.range(10).map(x => (x, x.toInt, x.toInt)).toDF("col1", "col2", "col3")
        df.write.format("parquet").saveAsTable("tbl")
        spark.sql("CREATE VIEW view1 AS SELECT * FROM tbl")
        spark.sql("CREATE TABLE tbl2 USING parquet PARTITIONED BY (COL2) " +
          "CLUSTERED BY (COL3) INTO 3 BUCKETS AS SELECT COL1, COL2, COL3 FROM view1")
        val identifier = TableIdentifier("tbl2")
        val location = spark.sessionState.catalog.getTableMetadata(identifier).location.toString
        val expectedSchema = StructType(Seq(
          StructField("COL1", LongType, true),
          StructField("COL3", IntegerType, true),
          StructField("COL2", IntegerType, true)))
        assert(spark.read.parquet(location).schema == expectedSchema)
        checkAnswer(spark.table("tbl2"), df)
      }
    }
  }

  test("use Spark jobs to list files") {
    withSQLConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> "1") {
      withTempDir { dir =>
        val jobDescriptions = new ConcurrentLinkedQueue[String]()
        val jobListener = new SparkListener {
          override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
            val desc = jobStart.properties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION)
            if (desc != null) jobDescriptions.add(desc)
          }
        }
        sparkContext.addSparkListener(jobListener)
        try {
          spark.range(0, 3).map(i => (i, i))
            .write.partitionBy("_1").mode("overwrite").parquet(dir.getCanonicalPath)
          // normal file paths
          checkDatasetUnorderly(
            spark.read.parquet(dir.getCanonicalPath).as[(Long, Long)],
            0L -> 0L, 1L -> 1L, 2L -> 2L)
          sparkContext.listenerBus.waitUntilEmpty()
          assert(jobDescriptions.asScala.toList.exists(
            _.contains("Listing leaf files and directories for 3 paths")))
        } finally {
          sparkContext.removeSparkListener(jobListener)
        }
      }
    }
  }

  test("SPARK-32516: 'path' or 'paths' option cannot coexist with load()'s path parameters") {
    def verifyLoadFails(f: => DataFrame): Unit = {
      val e = intercept[AnalysisException](f)
      assert(e.getMessage.contains(
        "Either remove the path option if it's the same as the path parameter"))
    }

    val path = "/tmp"
    verifyLoadFails(spark.read.option("path", path).parquet(path))
    verifyLoadFails(spark.read.option("path", path).parquet(""))
    verifyLoadFails(spark.read.option("path", path).format("parquet").load(path))
    verifyLoadFails(spark.read.option("path", path).format("parquet").load(""))
    verifyLoadFails(spark.read.option("paths", path).parquet(path))
    verifyLoadFails(spark.read.option("paths", path).parquet(""))
    verifyLoadFails(spark.read.option("paths", path).format("parquet").load(path))
    verifyLoadFails(spark.read.option("paths", path).format("parquet").load(""))
  }

  test("SPARK-32516: legacy path option behavior in load()") {
    withSQLConf(SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key -> "true") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        Seq(1).toDF().write.mode("overwrite").parquet(path)

        // When there is one path parameter to load(), "path" option is overwritten.
        checkAnswer(spark.read.format("parquet").option("path", path).load(path), Row(1))

        // When there are multiple path parameters to load(), "path" option is added.
        checkAnswer(
          spark.read.format("parquet").option("path", path).load(path, path),
          Seq(Row(1), Row(1), Row(1)))

        // When built-in datasource functions are invoked (e.g, `csv`, `parquet`, etc.),
        // the path option is always added regardless of the number of path parameters.
        checkAnswer(spark.read.option("path", path).parquet(path), Seq(Row(1), Row(1)))
        checkAnswer(
          spark.read.option("path", path).parquet(path, path),
          Seq(Row(1), Row(1), Row(1)))
      }
    }
  }

  test("SPARK-32516: 'path' option cannot coexist with save()'s path parameter") {
    def verifyLoadFails(f: => Unit): Unit = {
      val e = intercept[AnalysisException](f)
      assert(e.getMessage.contains(
        "Either remove the path option, or call save() without the parameter"))
    }

    val df = Seq(1).toDF()
    val path = "tmp"
    verifyLoadFails(df.write.option("path", path).parquet(path))
    verifyLoadFails(df.write.option("path", path).parquet(""))
    verifyLoadFails(df.write.option("path", path).format("parquet").save(path))
    verifyLoadFails(df.write.option("path", path).format("parquet").save(""))
  }

  test("SPARK-32853: consecutive load/save calls should be allowed") {
    val dfr = spark.read.format(classOf[FakeSourceOne].getName)
    dfr.load("1")
    dfr.load("2")
    val dfw = spark.range(10).write.format(classOf[DefaultSource].getName)
    dfw.save("1")
    dfw.save("2")
  }

  test("SPARK-32844: DataFrameReader.table take the specified options for V1 relation") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTable("t") {
        sql("CREATE TABLE t(i int, d double) USING parquet OPTIONS ('p1'='v1', 'p2'='v2')")

        val msg = intercept[AnalysisException] {
          spark.read.option("P1", "v3").table("t").count()
        }.getMessage
        assert(msg.contains("duplicated key"))

        val df = spark.read.option("P2", "v2").option("p3", "v3").table("t")
        val options = df.queryExecution.analyzed.collectFirst {
          case r: LogicalRelation => r.relation.asInstanceOf[HadoopFsRelation].options
        }.get
        assert(options("p2") == "v2")
        assert(options("p3") == "v3")
      }
    }
  }

  test("SPARK-26164: Allow concurrent writers for multiple partitions and buckets") {
    withTable("t1", "t2") {
      // Uses fixed seed to ensure reproducible test execution
      val r = new Random(31)
      val df = spark.range(200).map(_ => {
        val n = r.nextInt()
        (n, n.toString, n % 5)
      }).toDF("k1", "k2", "part")
      df.write.format("parquet").saveAsTable("t1")
      spark.sql("CREATE TABLE t2(k1 int, k2 string, part int) USING parquet PARTITIONED " +
        "BY (part) CLUSTERED BY (k1) INTO 3 BUCKETS")
      val queryToInsertTable = "INSERT OVERWRITE TABLE t2 SELECT k1, k2, part FROM t1"

      Seq(
        // Single writer
        0,
        // Concurrent writers without fallback
        200,
        // concurrent writers with fallback
        3
      ).foreach { maxWriters =>
        withSQLConf(SQLConf.MAX_CONCURRENT_OUTPUT_FILE_WRITERS.key -> maxWriters.toString) {
          spark.sql(queryToInsertTable).collect()
          checkAnswer(spark.table("t2").orderBy("k1"),
            spark.table("t1").orderBy("k1"))

          withSQLConf(SQLConf.MAX_RECORDS_PER_FILE.key -> "1") {
            spark.sql(queryToInsertTable).collect()
            checkAnswer(spark.table("t2").orderBy("k1"),
              spark.table("t1").orderBy("k1"))
          }
        }
      }
    }
  }

  test("SPARK-43281: Fix concurrent writer does not update file metrics") {
    withTable("t") {
      withSQLConf(SQLConf.MAX_CONCURRENT_OUTPUT_FILE_WRITERS.key -> "3",
          SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1") {
        spark.sql("CREATE TABLE t(c int) USING parquet PARTITIONED BY (p String)")
        var dataWriting: DataWritingCommandExec = null
        val listener = new QueryExecutionListener {
          override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {}
          override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
            qe.executedPlan match {
              case dataWritingCommandExec: DataWritingCommandExec =>
                dataWriting = dataWritingCommandExec
              case _ =>
            }
          }
        }
        spark.listenerManager.register(listener)

        def checkMetrics(sqlStr: String, numFiles: Int, numOutputRows: Long): Unit = {
          sql(sqlStr)
          sparkContext.listenerBus.waitUntilEmpty()
          assert(dataWriting != null)
          val metrics = dataWriting.cmd.metrics
          assert(metrics.contains("numFiles"))
          assert(metrics("numFiles").value == numFiles)
          assert(metrics.contains("numOutputBytes"))
          assert(metrics("numOutputBytes").value > 0)
          assert(metrics.contains("numOutputRows"))
          assert(metrics("numOutputRows").value == numOutputRows)
        }

        try {
          // without fallback
          checkMetrics(
            "INSERT INTO TABLE t PARTITION(p) SELECT * FROM VALUES(1, 'a'),(2, 'a'),(1, 'b')",
            numFiles = 2,
            numOutputRows = 3)

          // with fallback
          checkMetrics(
            """
              |INSERT INTO TABLE t PARTITION(p)
              |SELECT * FROM VALUES(1, 'a'),(2, 'b'),(1, 'c'),(2, 'd')""".stripMargin,
            numFiles = 4,
            numOutputRows = 4)
        } finally {
          spark.listenerManager.unregister(listener)
        }
      }
    }
  }

  test("SPARK-39910: read files from Hadoop archives") {
    val fileSchema = new StructType().add("str", StringType)
    val harPath = testFile("test-data/test-archive.har")
      .replaceFirst("file:/", "har:/")

    testRead(spark.read.schema(fileSchema).csv(s"$harPath/test.csv"), data, fileSchema)
  }

  test("SPARK-51182: DataFrameWriter should throw dataPathNotSpecifiedError when path is not " +
    "specified") {
    val dataFrameWriter = spark.range(0).write
    checkError(
      exception = intercept[SparkIllegalArgumentException](dataFrameWriter.save()),
      condition = "_LEGACY_ERROR_TEMP_2047")
  }
}
