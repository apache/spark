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

package org.apache.spark.sql.streaming.test

import java.io.File
import java.util.ConcurrentModificationException
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import org.apache.hadoop.fs.Path
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryException, StreamTest}
import org.apache.spark.sql.streaming.Trigger._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

object LastOptions {

  var mockStreamSourceProvider = mock(classOf[StreamSourceProvider])
  var mockStreamSinkProvider = mock(classOf[StreamSinkProvider])
  var parameters: Map[String, String] = null
  var sinkParameters: Map[String, String] = null
  var schema: Option[StructType] = null
  var partitionColumns: Seq[String] = Nil

  def clear(): Unit = {
    parameters = null
    sinkParameters = null
    schema = null
    partitionColumns = null
    reset(mockStreamSourceProvider)
    reset(mockStreamSinkProvider)
  }
}

/** Dummy provider: returns no-op source/sink and records options in [[LastOptions]]. */
class DefaultSource extends StreamSourceProvider with StreamSinkProvider {

  private val fakeSchema = StructType(StructField("a", IntegerType) :: Nil)

  override def sourceSchema(
      spark: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    LastOptions.parameters = parameters
    LastOptions.schema = schema
    LastOptions.mockStreamSourceProvider.sourceSchema(spark, schema, providerName, parameters)
    ("dummySource", fakeSchema)
  }

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    LastOptions.parameters = parameters
    LastOptions.schema = schema
    LastOptions.mockStreamSourceProvider.createSource(
      spark, metadataPath, schema, providerName, parameters)
    new Source {
      override def schema: StructType = fakeSchema

      override def getOffset: Option[Offset] = Some(new LongOffset(0))

      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        spark.internalCreateDataFrame(spark.sparkContext.emptyRDD, schema, isStreaming = true)
      }

      override def stop(): Unit = {}
    }
  }

  override def createSink(
      spark: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    LastOptions.sinkParameters = parameters
    LastOptions.partitionColumns = partitionColumns
    LastOptions.mockStreamSinkProvider.createSink(spark, parameters, partitionColumns, outputMode)
    (_: Long, _: DataFrame) => {}
  }
}

class DataStreamReaderWriterSuite extends StreamTest with BeforeAndAfter {
  import testImplicits._

  private def newMetadataDir =
    Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath

  after {
    spark.streams.active.foreach(_.stop())
  }

  test("write cannot be called on streaming datasets") {
    val e = intercept[AnalysisException] {
      spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load()
        .write
        .save()
    }
    Seq("'write'", "not", "streaming Dataset/DataFrame").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }
  }

  test("resolve default source") {
    spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()
      .writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .start()
      .stop()
  }

  test("resolve full class") {
    spark.readStream
      .format("org.apache.spark.sql.streaming.test.DefaultSource")
      .load()
      .writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .start()
      .stop()
  }

  test("options") {
    val map = new java.util.HashMap[String, String]
    map.put("opt3", "3")

    val df = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .option("opt1", "1")
        .options(Map("opt2" -> "2"))
        .options(map)
        .load()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")

    LastOptions.clear()

    df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("opt1", "5")
      .options(Map("opt2" -> "4"))
      .options(map)
      .option("checkpointLocation", newMetadataDir)
      .start()
      .stop()

    assert(LastOptions.sinkParameters("opt1") == "5")
    assert(LastOptions.sinkParameters("opt2") == "4")
    assert(LastOptions.sinkParameters("opt3") == "3")
    assert(LastOptions.sinkParameters.contains("checkpointLocation"))
  }

  test("SPARK-32832: later option should override earlier options for load()") {
    spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .option("paTh", "1")
      .option("PATH", "2")
      .option("Path", "3")
      .option("patH", "4")
      .option("path", "5")
      .load()
    assert(LastOptions.parameters("path") == "5")
  }

  test("SPARK-32832: later option should override earlier options for start()") {
    val ds = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()
    assert(LastOptions.parameters.isEmpty)

    ds.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .option("paTh", "1")
      .option("PATH", "2")
      .option("Path", "3")
      .option("patH", "4")
      .option("path", "5")
      .start()
      .stop()
    assert(LastOptions.sinkParameters("path") == "5")
  }

  test("partitioning") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .start()
      .stop()
    assert(LastOptions.partitionColumns == Nil)

    df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .partitionBy("a")
      .start()
      .stop()
    assert(LastOptions.partitionColumns == Seq("a"))

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      df.writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        .partitionBy("A")
        .start()
        .stop()
      assert(LastOptions.partitionColumns == Seq("a"))
    }

    intercept[AnalysisException] {
      df.writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        .partitionBy("b")
        .start()
        .stop()
    }
  }

  test("stream paths") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .load("/test")

    assert(LastOptions.parameters("path") == "/test")

    LastOptions.clear()

    df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .start("/test")
      .stop()

    assert(LastOptions.parameters("path") == "/test")
  }

  test("test different data types for options") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .load("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")

    LastOptions.clear()
    df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .option("checkpointLocation", newMetadataDir)
      .start("/test")
      .stop()

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")
  }

  test("unique query names") {

    /** Start a query with a specific name */
    def startQueryWithName(name: String = ""): StreamingQuery = {
      spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load("/test")
        .writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        .queryName(name)
        .start()
    }

    /** Start a query without specifying a name */
    def startQueryWithoutName(): StreamingQuery = {
      spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load("/test")
        .writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        .start()
    }

    /** Get the names of active streams */
    def activeStreamNames: Set[String] = {
      val streams = spark.streams.active
      val names = streams.map(_.name).toSet
      assert(streams.length === names.size, s"names of active queries are not unique: $names")
      names
    }

    val q1 = startQueryWithName("name")

    // Should not be able to start another query with the same name
    intercept[IllegalArgumentException] {
      startQueryWithName("name")
    }
    assert(activeStreamNames === Set("name"))

    // Should be able to start queries with other names
    val q3 = startQueryWithName("another-name")
    assert(activeStreamNames === Set("name", "another-name"))

    // Should be able to start queries with auto-generated names
    val q4 = startQueryWithoutName()
    assert(activeStreamNames.contains(q4.name))

    // Should not be able to start a query with same auto-generated name
    intercept[IllegalArgumentException] {
      startQueryWithName(q4.name)
    }

    // Should be able to start query with that name after stopping the previous query
    q1.stop()
    val q5 = startQueryWithName("name")
    assert(activeStreamNames.contains("name"))
    spark.streams.active.foreach(_.stop())
  }

  test("trigger") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load("/test")

    var q = df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q.stop()

    assert(q.asInstanceOf[StreamingQueryWrapper].streamingQuery.trigger == ProcessingTime(10000))

    q = df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .trigger(ProcessingTime(100, TimeUnit.SECONDS))
      .start()
    q.stop()

    assert(q.asInstanceOf[StreamingQueryWrapper].streamingQuery.trigger == ProcessingTime(100000))
  }

  test("source metadataPath") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    val q = df1.union(df2).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q.processAllAvailable()
    q.stop()

    verify(LastOptions.mockStreamSourceProvider).createSource(
      any(),
      meq(s"${new Path(makeQualifiedPath(checkpointLocation.toString)).toString}/sources/0"),
      meq(None),
      meq("org.apache.spark.sql.streaming.test"),
      meq(Map.empty))

    verify(LastOptions.mockStreamSourceProvider).createSource(
      any(),
      meq(s"${new Path(makeQualifiedPath(checkpointLocation.toString)).toString}/sources/1"),
      meq(None),
      meq("org.apache.spark.sql.streaming.test"),
      meq(Map.empty))
  }

  private def newTextInput = Utils.createTempDir(namePrefix = "text").getCanonicalPath

  test("check foreach() catches null writers") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    var w = df.writeStream
    var e = intercept[IllegalArgumentException](w.foreach(null))
    Seq("foreach", "null").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }
  }


  test("check foreach() does not support partitioning") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()
    val foreachWriter = new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long): Boolean = false
      override def process(value: Row): Unit = {}
      override def close(errorOrNull: Throwable): Unit = {}
    }
    var w = df.writeStream.partitionBy("value")
    var e = intercept[AnalysisException](w.foreach(foreachWriter).start())
    Seq("foreach", "partitioning").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }
  }

  test("prevent all column partitioning") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      intercept[AnalysisException] {
        spark.range(10).writeStream
          .outputMode("append")
          .partitionBy("id")
          .format("parquet")
          .start(path)
      }
    }
  }

  private def testMemorySinkCheckpointRecovery(chkLoc: String, provideInWriter: Boolean): Unit = {
    val ms = new MemoryStream[Int](0, sqlContext)
    val df = ms.toDF().toDF("a")
    val tableName = "test"
    def startQuery: StreamingQuery = {
      val writer = df.groupBy("a")
        .count()
        .writeStream
        .format("memory")
        .queryName(tableName)
        .outputMode("complete")
      if (provideInWriter) {
        writer.option("checkpointLocation", chkLoc)
      }
      writer.start()
    }
    // no exception here
    val q = startQuery
    ms.addData(0, 1)
    q.processAllAvailable()
    q.stop()

    checkAnswer(
      spark.table(tableName),
      Seq(Row(0, 1), Row(1, 1))
    )
    spark.sql(s"drop table $tableName")
    // verify table is dropped
    intercept[AnalysisException](spark.table(tableName).collect())
    val q2 = startQuery
    ms.addData(0)
    q2.processAllAvailable()
    checkAnswer(
      spark.table(tableName),
      Seq(Row(0, 2), Row(1, 1))
    )

    q2.stop()
  }

  test("MemorySink can recover from a checkpoint in Complete Mode") {
    val checkpointLoc = newMetadataDir
    val checkpointDir = new File(checkpointLoc, "offsets")
    checkpointDir.mkdirs()
    assert(checkpointDir.exists())
    testMemorySinkCheckpointRecovery(checkpointLoc, provideInWriter = true)
  }

  test("SPARK-18927: MemorySink can recover from a checkpoint provided in conf in Complete Mode") {
    val checkpointLoc = newMetadataDir
    val checkpointDir = new File(checkpointLoc, "offsets")
    checkpointDir.mkdirs()
    assert(checkpointDir.exists())
    withSQLConf(SQLConf.CHECKPOINT_LOCATION.key -> checkpointLoc) {
      testMemorySinkCheckpointRecovery(checkpointLoc, provideInWriter = false)
    }
  }

  test("append mode memory sink's do not support checkpoint recovery") {
    import testImplicits._
    val ms = new MemoryStream[Int](0, sqlContext)
    val df = ms.toDF().toDF("a")
    val checkpointLoc = newMetadataDir
    val checkpointDir = new File(checkpointLoc, "offsets")
    checkpointDir.mkdirs()
    assert(checkpointDir.exists())

    val e = intercept[AnalysisException] {
      df.writeStream
        .format("memory")
        .queryName("test")
        .option("checkpointLocation", checkpointLoc)
        .outputMode("append")
        .start()
    }
    assert(e.getMessage.contains("does not support recovering"))
    assert(e.getMessage.contains("checkpoint location"))
  }

  test("SPARK-18510: use user specified types for partition columns in file sources") {
    import org.apache.spark.sql.functions.udf
    import testImplicits._
    withTempDir { src =>
      val createArray = udf { (length: Long) =>
        for (i <- 1 to length.toInt) yield i.toString
      }
      spark.range(4).select(createArray('id + 1) as 'ex, 'id, 'id % 4 as 'part).coalesce(1).write
        .partitionBy("part", "id")
        .mode("overwrite")
        .parquet(src.toString)
      // Specify a random ordering of the schema, partition column in the middle, etc.
      // Also let's say that the partition columns are Strings instead of Longs.
      // partition columns should go to the end
      val schema = new StructType()
        .add("id", StringType)
        .add("ex", ArrayType(StringType))

      val sdf = spark.readStream
        .schema(schema)
        .format("parquet")
        .load(src.toString)

      assert(sdf.schema.toList === List(
        StructField("ex", ArrayType(StringType)),
        StructField("part", IntegerType), // inferred partitionColumn dataType
        StructField("id", StringType))) // used user provided partitionColumn dataType

      val sq = sdf.writeStream
        .queryName("corruption_test")
        .format("memory")
        .start()
      sq.processAllAvailable()
      checkAnswer(
        spark.table("corruption_test"),
        // notice how `part` is ordered before `id`
        Row(Array("1"), 0, "0") :: Row(Array("1", "2"), 1, "1") ::
          Row(Array("1", "2", "3"), 2, "2") :: Row(Array("1", "2", "3", "4"), 3, "3") :: Nil
      )
      sq.stop()
    }
  }

  test("user specified checkpointLocation precedes SQLConf") {
    import testImplicits._
    withTempDir { checkpointPath =>
      withTempPath { userCheckpointPath =>
        assert(!userCheckpointPath.exists(), s"$userCheckpointPath should not exist")
        withSQLConf(SQLConf.CHECKPOINT_LOCATION.key -> checkpointPath.getAbsolutePath) {
          val queryName = "test_query"
          val ds = MemoryStream[Int].toDS
          ds.writeStream
            .format("memory")
            .queryName(queryName)
            .option("checkpointLocation", userCheckpointPath.getAbsolutePath)
            .start()
            .stop()
          assert(checkpointPath.listFiles().isEmpty,
            "SQLConf path is used even if user specified checkpointLoc: " +
              s"${checkpointPath.listFiles()} is not empty")
          assert(userCheckpointPath.exists(),
            s"The user specified checkpointLoc (userCheckpointPath) is not created")
        }
      }
    }
  }

  test("use SQLConf checkpoint dir when checkpointLocation is not specified") {
    import testImplicits._
    withTempDir { checkpointPath =>
      withSQLConf(SQLConf.CHECKPOINT_LOCATION.key -> checkpointPath.getAbsolutePath) {
        val queryName = "test_query"
        val ds = MemoryStream[Int].toDS
        ds.writeStream.format("memory").queryName(queryName).start().stop()
        // Should use query name to create a folder in `checkpointPath`
        val queryCheckpointDir = new File(checkpointPath, queryName)
        assert(queryCheckpointDir.exists(), s"$queryCheckpointDir doesn't exist")
        assert(
          checkpointPath.listFiles().size === 1,
          s"${checkpointPath.listFiles().toList} has 0 or more than 1 files ")
      }
    }
  }

  test("use SQLConf checkpoint dir when checkpointLocation is not specified without query name") {
    import testImplicits._
    withTempDir { checkpointPath =>
      withSQLConf(SQLConf.CHECKPOINT_LOCATION.key -> checkpointPath.getAbsolutePath) {
        val ds = MemoryStream[Int].toDS
        ds.writeStream.format("console").start().stop()
        // Should create a random folder in `checkpointPath`
        assert(
          checkpointPath.listFiles().size === 1,
          s"${checkpointPath.listFiles().toList} has 0 or more than 1 files ")
      }
    }
  }

  test("configured checkpoint dir should not be deleted if a query is stopped without errors and" +
    " force temp checkpoint deletion enabled") {
    import testImplicits._
    withTempDir { checkpointPath =>
      withSQLConf(SQLConf.CHECKPOINT_LOCATION.key -> checkpointPath.getAbsolutePath,
        SQLConf.FORCE_DELETE_TEMP_CHECKPOINT_LOCATION.key -> "true") {
        val ds = MemoryStream[Int].toDS
        val query = ds.writeStream.format("console").start()
        assert(checkpointPath.exists())
        query.stop()
        assert(checkpointPath.exists())
      }
    }
  }

  test("temp checkpoint dir should be deleted if a query is stopped without errors") {
    import testImplicits._
    val query = MemoryStream[Int].toDS.writeStream.format("console").start()
    query.processAllAvailable()
    val checkpointDir = new Path(
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.resolvedCheckpointRoot)
    val fs = checkpointDir.getFileSystem(spark.sessionState.newHadoopConf())
    assert(fs.exists(checkpointDir))
    query.stop()
    assert(!fs.exists(checkpointDir))
  }

  testQuietly("temp checkpoint dir should not be deleted if a query is stopped with an error") {
    testTempCheckpointWithFailedQuery(false)
  }

  testQuietly("temp checkpoint should be deleted if a query is stopped with an error and force" +
    " temp checkpoint deletion enabled") {
    withSQLConf(SQLConf.FORCE_DELETE_TEMP_CHECKPOINT_LOCATION.key -> "true") {
      testTempCheckpointWithFailedQuery(true)
    }
  }

  private def testTempCheckpointWithFailedQuery(checkpointMustBeDeleted: Boolean): Unit = {
    import testImplicits._
    val input = MemoryStream[Int]
    val query = input.toDS.map(_ / 0).writeStream.format("console").start()
    val checkpointDir = new Path(
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.resolvedCheckpointRoot)
    val fs = checkpointDir.getFileSystem(spark.sessionState.newHadoopConf())
    assert(fs.exists(checkpointDir))
    input.addData(1)
    intercept[StreamingQueryException] {
      query.awaitTermination()
    }
    if (!checkpointMustBeDeleted) {
      assert(fs.exists(checkpointDir))
    } else {
      assert(!fs.exists(checkpointDir))
    }
  }

  test("SPARK-20431: Specify a schema by using a DDL-formatted string") {
    spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .schema("aa INT")
      .load()

    assert(LastOptions.schema.isDefined)
    assert(LastOptions.schema.get === StructType(StructField("aa", IntegerType) :: Nil))

    LastOptions.clear()
  }

  test("SPARK-26586: Streams should have isolated confs") {
    import testImplicits._
    val input = MemoryStream[Int]
    input.addData(1 to 10)
    spark.conf.set("testKey1", 0)
    val queries = (1 to 10).map { i =>
      spark.conf.set("testKey1", i)
      input.toDF().writeStream
        .foreachBatch { (df: Dataset[Row], id: Long) =>
          val v = df.sparkSession.conf.get("testKey1").toInt
          if (i != v) {
            throw new ConcurrentModificationException(s"Stream $i has the wrong conf value $v")
          }
        }
        .start()
    }
    try {
      queries.foreach(_.processAllAvailable())
    } finally {
      queries.foreach(_.stop())
    }
  }

  test("SPARK-32516: 'path' cannot coexist with load()'s path parameter") {
    def verifyLoadFails(f: => DataFrame): Unit = {
      val e = intercept[AnalysisException](f)
      assert(e.getMessage.contains(
        "Either remove the path option, or call load() without the parameter"))
    }

    verifyLoadFails(spark.readStream.option("path", "tmp1").parquet("tmp2"))
    verifyLoadFails(spark.readStream.option("path", "tmp1").parquet(""))
    verifyLoadFails(spark.readStream.option("path", "tmp1").format("parquet").load("tmp2"))
    verifyLoadFails(spark.readStream.option("path", "tmp1").format("parquet").load(""))

    withClue("SPARK-32516: legacy behavior") {
      withSQLConf(SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key -> "true") {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .option("path", "tmp1")
          .load("tmp2")
        // The legacy behavior overwrites the path option.
        assert(LastOptions.parameters("path") == "tmp2")
      }
    }
  }

  test("SPARK-32516: 'path' cannot coexist with start()'s path parameter") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load("tmp1")

    def verifyStartFails(f: => StreamingQuery): Unit = {
      val e = intercept[AnalysisException](f)
      assert(e.getMessage.contains(
        "Either remove the path option, or call start() without the parameter"))
    }

    verifyStartFails(
      df.writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("path", "tmp2")
        .start("tmp3"))
    verifyStartFails(
      df.writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("path", "tmp2")
        .start(""))

    withClue("SPARK-32516: legacy behavior") {
      withTempDir { checkpointPath =>
        withSQLConf(SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key -> "true",
          SQLConf.CHECKPOINT_LOCATION.key -> checkpointPath.getAbsolutePath) {
          df.writeStream
            .format("org.apache.spark.sql.streaming.test")
            .option("path", "tmp4")
            .start("tmp5")
            .stop()
          // The legacy behavior overwrites the path option.
          assert(LastOptions.sinkParameters("path") == "tmp5")
        }
      }
    }
  }

  test("SPARK-32853: consecutive load/start calls should be allowed") {
    val dfr = spark.readStream.format(classOf[DefaultSource].getName)
    var df = dfr.load("1")
    df = dfr.load("2")
    withTempDir { checkpointPath =>
      val dfw = df.writeStream
        .option("checkpointLocation", checkpointPath.getCanonicalPath)
        .format(classOf[DefaultSource].getName)
      var query = dfw.start("1")
      query.stop()
      query = dfw.start("2")
      query.stop()
    }
  }
}
