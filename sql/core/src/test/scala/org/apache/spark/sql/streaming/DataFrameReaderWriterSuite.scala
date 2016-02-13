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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, ContinuousQuery, SQLContext, StreamTest}
import org.apache.spark.sql.execution.streaming.{Batch, Offset, Sink, Source}
import org.apache.spark.sql.sources.{StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object LastOptions {
  var parameters: Map[String, String] = null
  var schema: Option[StructType] = null
  var partitionColumns: Seq[String] = Nil
}

/** Dummy provider: returns no-op source/sink and records options in [[LastOptions]]. */
class DefaultSource extends StreamSourceProvider with StreamSinkProvider {
  override def createSource(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    LastOptions.parameters = parameters
    LastOptions.schema = schema
    new Source {
      override def getNextBatch(start: Option[Offset]): Option[Batch] = None
      override def schema: StructType = StructType(StructField("a", IntegerType) :: Nil)
    }
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String]): Sink = {
    LastOptions.parameters = parameters
    LastOptions.partitionColumns = partitionColumns
    new Sink {
      override def addBatch(batch: Batch): Unit = {}
      override def currentOffset: Option[Offset] = None
    }
  }
}

class DataFrameReaderWriterSuite extends StreamTest with SharedSQLContext with BeforeAndAfter {
  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("resolve default source") {
    sqlContext.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()
      .write
      .format("org.apache.spark.sql.streaming.test")
      .stream()
      .stop()
  }

  test("resolve full class") {
    sqlContext.read
      .format("org.apache.spark.sql.streaming.test.DefaultSource")
      .stream()
      .write
      .format("org.apache.spark.sql.streaming.test")
      .stream()
      .stop()
  }

  test("options") {
    val map = new java.util.HashMap[String, String]
    map.put("opt3", "3")

    val df = sqlContext.read
        .format("org.apache.spark.sql.streaming.test")
        .option("opt1", "1")
        .options(Map("opt2" -> "2"))
        .options(map)
        .stream()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")

    LastOptions.parameters = null

    df.write
      .format("org.apache.spark.sql.streaming.test")
      .option("opt1", "1")
      .options(Map("opt2" -> "2"))
      .options(map)
      .stream()
      .stop()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")
  }

  test("partitioning") {
    val df = sqlContext.read
      .format("org.apache.spark.sql.streaming.test")
      .stream()

    df.write
      .format("org.apache.spark.sql.streaming.test")
      .stream()
      .stop()
    assert(LastOptions.partitionColumns == Nil)

    df.write
      .format("org.apache.spark.sql.streaming.test")
      .partitionBy("a")
      .stream()
      .stop()
    assert(LastOptions.partitionColumns == Seq("a"))

    withSQLConf("spark.sql.caseSensitive" -> "false") {
      df.write
        .format("org.apache.spark.sql.streaming.test")
        .partitionBy("A")
        .stream()
        .stop()
      assert(LastOptions.partitionColumns == Seq("a"))
    }

    intercept[AnalysisException] {
      df.write
        .format("org.apache.spark.sql.streaming.test")
        .partitionBy("b")
        .stream()
        .stop()
    }
  }

  test("stream paths") {
    val df = sqlContext.read
      .format("org.apache.spark.sql.streaming.test")
      .stream("/test")

    assert(LastOptions.parameters("path") == "/test")

    LastOptions.parameters = null

    df.write
      .format("org.apache.spark.sql.streaming.test")
      .stream("/test")
      .stop()

    assert(LastOptions.parameters("path") == "/test")
  }

  test("test different data types for options") {
    val df = sqlContext.read
      .format("org.apache.spark.sql.streaming.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .stream("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")

    LastOptions.parameters = null
    df.write
      .format("org.apache.spark.sql.streaming.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .stream("/test")
      .stop()

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")
  }

  test("unique query names") {

    /** Start a query with a specific name */
    def startQueryWithName(name: String = ""): ContinuousQuery = {
      sqlContext.read
        .format("org.apache.spark.sql.streaming.test")
        .stream("/test")
        .write
        .format("org.apache.spark.sql.streaming.test")
        .queryName(name)
        .stream()
    }

    /** Start a query without specifying a name */
    def startQueryWithoutName(): ContinuousQuery = {
      sqlContext.read
        .format("org.apache.spark.sql.streaming.test")
        .stream("/test")
        .write
        .format("org.apache.spark.sql.streaming.test")
        .stream()
    }

    /** Get the names of active streams */
    def activeStreamNames: Set[String] = {
      val streams = sqlContext.streams.active
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
    sqlContext.streams.active.foreach(_.stop())
  }
}
