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

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.sources.{StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQuery, StreamTest}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.Utils

object LastOptions {

  var mockStreamSourceProvider = mock(classOf[StreamSourceProvider])
  var mockStreamSinkProvider = mock(classOf[StreamSinkProvider])
  var parameters: Map[String, String] = null
  var schema: Option[StructType] = null
  var partitionColumns: Seq[String] = Nil

  def clear(): Unit = {
    parameters = null
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
        import spark.implicits._

        Seq[Int]().toDS().toDF()
      }

      override def stop() {}
    }
  }

  override def createSink(
      spark: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    LastOptions.parameters = parameters
    LastOptions.partitionColumns = partitionColumns
    LastOptions.mockStreamSinkProvider.createSink(spark, parameters, partitionColumns, outputMode)
    new Sink {
      override def addBatch(batchId: Long, data: DataFrame): Unit = {}
    }
  }
}

class DataStreamReaderWriterSuite extends StreamTest with BeforeAndAfter {

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
      assert(e.getMessage.toLowerCase.contains(s.toLowerCase))
    }
  }

  test("memory streams don't accept checkpoint location") {
    val e = intercept[AnalysisException] {
      spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load()
        .writeStream
        .format("memory")
        .option("checkpointLocation", "some/place")
        .start("over/the/rainbow")
    }
    Seq("Memory", "do not recover", "checkpoints", "'checkpointLocation'").foreach { s =>
      assert(e.getMessage.toLowerCase.contains(s.toLowerCase))
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
      .option("opt1", "1")
      .options(Map("opt2" -> "2"))
      .options(map)
      .option("checkpointLocation", newMetadataDir)
      .start()
      .stop()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")
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

    withSQLConf("spark.sql.caseSensitive" -> "false") {
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

    assert(q.asInstanceOf[StreamExecution].trigger == ProcessingTime(10000))

    q = df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .trigger(ProcessingTime.create(100, TimeUnit.SECONDS))
      .start()
    q.stop()

    assert(q.asInstanceOf[StreamExecution].trigger == ProcessingTime(100000))
  }

  test("source metadataPath") {
    LastOptions.clear()

    val checkpointLocation = newMetadataDir

    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    val q = df1.union(df2).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q.stop()

    verify(LastOptions.mockStreamSourceProvider).createSource(
      spark.sqlContext,
      checkpointLocation + "/sources/0",
      None,
      "org.apache.spark.sql.streaming.test",
      Map.empty)

    verify(LastOptions.mockStreamSourceProvider).createSource(
      spark.sqlContext,
      checkpointLocation + "/sources/1",
      None,
      "org.apache.spark.sql.streaming.test",
      Map.empty)
  }

  private def newTextInput = Utils.createTempDir(namePrefix = "text").getCanonicalPath

  test("check outputMode(string) throws exception on unsupported modes") {
    def testError(outputMode: String): Unit = {
      val df = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load()
      val w = df.writeStream
      val e = intercept[IllegalArgumentException](w.outputMode(outputMode))
      Seq("output mode", "unknown", outputMode).foreach { s =>
        assert(e.getMessage.toLowerCase.contains(s.toLowerCase))
      }
    }
    testError("Update")
    testError("Xyz")
  }

  test("check foreach() catches null writers") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    var w = df.writeStream
    var e = intercept[IllegalArgumentException](w.foreach(null))
    Seq("foreach", "null").foreach { s =>
      assert(e.getMessage.toLowerCase.contains(s.toLowerCase))
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
      assert(e.getMessage.toLowerCase.contains(s.toLowerCase))
    }
  }

  test("ConsoleSink can be correctly loaded") {
    LastOptions.clear()
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    val sq = df.writeStream
      .format("console")
      .option("checkpointLocation", newMetadataDir)
      .trigger(ProcessingTime(2.seconds))
      .start()

    sq.awaitTermination(2000L)
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

  test("ConsoleSink should not require checkpointLocation") {
    LastOptions.clear()
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    val sq = df.writeStream.format("console").start()
    sq.stop()
  }
}
