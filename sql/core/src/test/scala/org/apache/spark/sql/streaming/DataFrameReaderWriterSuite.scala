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
import org.apache.spark.sql.test.SharedSQLContext
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
    }
  }

  override def createSink(
      spark: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String]): Sink = {
    LastOptions.parameters = parameters
    LastOptions.partitionColumns = partitionColumns
    LastOptions.mockStreamSinkProvider.createSink(spark, parameters, partitionColumns)
    new Sink {
      override def addBatch(batchId: Long, data: DataFrame): Unit = {}
    }
  }
}

class DataFrameReaderWriterSuite extends StreamTest with SharedSQLContext with BeforeAndAfter {
  import testImplicits._

  private def newMetadataDir =
    Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath

  after {
    spark.streams.active.foreach(_.stop())
  }

  test("resolve default source") {
    spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
      .write
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .startStream()
      .stop()
  }

  test("resolve full class") {
    spark.read
      .format("org.apache.spark.sql.streaming.test.DefaultSource")
      .stream()
      .write
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .startStream()
      .stop()
  }

  test("options") {
    val map = new java.util.HashMap[String, String]
    map.put("opt3", "3")

    val df = spark.read
        .format("org.apache.spark.sql.streaming.test")
        .option("opt1", "1")
        .options(Map("opt2" -> "2"))
        .options(map)
        .stream()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")

    LastOptions.clear()

    df.write
      .format("org.apache.spark.sql.streaming.test")
      .option("opt1", "1")
      .options(Map("opt2" -> "2"))
      .options(map)
      .option("checkpointLocation", newMetadataDir)
      .startStream()
      .stop()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")
  }

  test("partitioning") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()

    df.write
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .startStream()
      .stop()
    assert(LastOptions.partitionColumns == Nil)

    df.write
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .partitionBy("a")
      .startStream()
      .stop()
    assert(LastOptions.partitionColumns == Seq("a"))

    withSQLConf("spark.sql.caseSensitive" -> "false") {
      df.write
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        .partitionBy("A")
        .startStream()
        .stop()
      assert(LastOptions.partitionColumns == Seq("a"))
    }

    intercept[AnalysisException] {
      df.write
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        .partitionBy("b")
        .startStream()
        .stop()
    }
  }

  test("stream paths") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .stream("/test")

    assert(LastOptions.parameters("path") == "/test")

    LastOptions.clear()

    df.write
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .startStream("/test")
      .stop()

    assert(LastOptions.parameters("path") == "/test")
  }

  test("test different data types for options") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .stream("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")

    LastOptions.clear()
    df.write
      .format("org.apache.spark.sql.streaming.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .option("checkpointLocation", newMetadataDir)
      .startStream("/test")
      .stop()

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")
  }

  test("unique query names") {

    /** Start a query with a specific name */
    def startQueryWithName(name: String = ""): ContinuousQuery = {
      spark.read
        .format("org.apache.spark.sql.streaming.test")
        .stream("/test")
        .write
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        .queryName(name)
        .startStream()
    }

    /** Start a query without specifying a name */
    def startQueryWithoutName(): ContinuousQuery = {
      spark.read
        .format("org.apache.spark.sql.streaming.test")
        .stream("/test")
        .write
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        .startStream()
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
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream("/test")

    var q = df.write
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .trigger(ProcessingTime(10.seconds))
      .startStream()
    q.stop()

    assert(q.asInstanceOf[StreamExecution].trigger == ProcessingTime(10000))

    q = df.write
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .trigger(ProcessingTime.create(100, TimeUnit.SECONDS))
      .startStream()
    q.stop()

    assert(q.asInstanceOf[StreamExecution].trigger == ProcessingTime(100000))
  }

  test("source metadataPath") {
    LastOptions.clear()

    val checkpointLocation = newMetadataDir

    val df1 = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()

    val df2 = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()

    val q = df1.union(df2).write
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation)
      .trigger(ProcessingTime(10.seconds))
      .startStream()
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

  test("check trigger() can only be called on continuous queries") {
    val df = spark.read.text(newTextInput)
    val w = df.write.option("checkpointLocation", newMetadataDir)
    val e = intercept[AnalysisException](w.trigger(ProcessingTime("10 seconds")))
    assert(e.getMessage == "trigger() can only be called on continuous queries;")
  }

  test("check queryName() can only be called on continuous queries") {
    val df = spark.read.text(newTextInput)
    val w = df.write.option("checkpointLocation", newMetadataDir)
    val e = intercept[AnalysisException](w.queryName("queryName"))
    assert(e.getMessage == "queryName() can only be called on continuous queries;")
  }

  test("check startStream() can only be called on continuous queries") {
    val df = spark.read.text(newTextInput)
    val w = df.write.option("checkpointLocation", newMetadataDir)
    val e = intercept[AnalysisException](w.startStream())
    assert(e.getMessage == "startStream() can only be called on continuous queries;")
  }

  test("check startStream(path) can only be called on continuous queries") {
    val df = spark.read.text(newTextInput)
    val w = df.write.option("checkpointLocation", newMetadataDir)
    val e = intercept[AnalysisException](w.startStream("non_exist_path"))
    assert(e.getMessage == "startStream() can only be called on continuous queries;")
  }

  test("check mode(SaveMode) can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.mode(SaveMode.Append))
    assert(e.getMessage == "mode() can only be called on non-continuous queries;")
  }

  test("check mode(string) can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.mode("append"))
    assert(e.getMessage == "mode() can only be called on non-continuous queries;")
  }

  test("check bucketBy() can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[IllegalArgumentException](w.bucketBy(1, "text").startStream())
    assert(e.getMessage == "Currently we don't support writing bucketed data to this data source.")
  }

  test("check sortBy() can only be called on non-continuous queries;") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[IllegalArgumentException](w.sortBy("text").startStream())
    assert(e.getMessage == "Currently we don't support writing bucketed data to this data source.")
  }

  test("check save(path) can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.save("non_exist_path"))
    assert(e.getMessage == "save() can only be called on non-continuous queries;")
  }

  test("check save() can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.save())
    assert(e.getMessage == "save() can only be called on non-continuous queries;")
  }

  test("check insertInto() can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.insertInto("non_exsit_table"))
    assert(e.getMessage == "insertInto() can only be called on non-continuous queries;")
  }

  test("check saveAsTable() can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.saveAsTable("non_exsit_table"))
    assert(e.getMessage == "saveAsTable() can only be called on non-continuous queries;")
  }

  test("check jdbc() can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.jdbc(null, null, null))
    assert(e.getMessage == "jdbc() can only be called on non-continuous queries;")
  }

  test("check json() can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.json("non_exist_path"))
    assert(e.getMessage == "json() can only be called on non-continuous queries;")
  }

  test("check parquet() can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.parquet("non_exist_path"))
    assert(e.getMessage == "parquet() can only be called on non-continuous queries;")
  }

  test("check orc() can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.orc("non_exist_path"))
    assert(e.getMessage == "orc() can only be called on non-continuous queries;")
  }

  test("check text() can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.text("non_exist_path"))
    assert(e.getMessage == "text() can only be called on non-continuous queries;")
  }

  test("check csv() can only be called on non-continuous queries") {
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
    val w = df.write
    val e = intercept[AnalysisException](w.csv("non_exist_path"))
    assert(e.getMessage == "csv() can only be called on non-continuous queries;")
  }

  test("ConsoleSink can be correctly loaded") {
    LastOptions.clear()
    val df = spark.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()

    val cq = df.write
      .format("console")
      .option("checkpointLocation", newMetadataDir)
      .trigger(ProcessingTime(2.seconds))
      .startStream()

    cq.awaitTermination(2000L)
  }
}
