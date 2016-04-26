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

package org.apache.spark.sql.streaming

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class StreamSuite extends StreamTest with SharedSQLContext {

  import testImplicits._

  test("map with recovery") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped)(
      AddData(inputData, 1, 2, 3),
      StartStream,
      CheckAnswer(2, 3, 4),
      StopStream,
      AddData(inputData, 4, 5, 6),
      StartStream,
      CheckAnswer(2, 3, 4, 5, 6, 7))
  }

  test("join") {
    // Make a table and ensure it will be broadcast.
    val smallTable = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")

    // Join the input stream with a table.
    val inputData = MemoryStream[Int]
    val joined = inputData.toDS().toDF().join(smallTable, $"value" === $"number")

    testStream(joined)(
      AddData(inputData, 1, 2, 3),
      CheckAnswer(Row(1, 1, "one"), Row(2, 2, "two")),
      AddData(inputData, 4),
      CheckAnswer(Row(1, 1, "one"), Row(2, 2, "two"), Row(4, 4, "four")))
  }

  test("union two streams") {
    val inputData1 = MemoryStream[Int]
    val inputData2 = MemoryStream[Int]

    val unioned = inputData1.toDS().union(inputData2.toDS())

    testStream(unioned)(
      AddData(inputData1, 1, 3, 5),
      CheckAnswer(1, 3, 5),
      AddData(inputData2, 2, 4, 6),
      CheckAnswer(1, 2, 3, 4, 5, 6),
      StopStream,
      AddData(inputData1, 7),
      StartStream,
      AddData(inputData2, 8),
      CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8))
  }

  test("sql queries") {
    val inputData = MemoryStream[Int]
    inputData.toDF().registerTempTable("stream")
    val evens = sql("SELECT * FROM stream WHERE value % 2 = 0")

    testStream(evens)(
      AddData(inputData, 1, 2, 3, 4),
      CheckAnswer(2, 4))
  }

  test("DataFrame reuse") {
    def assertDF(df: DataFrame) {
      withTempDir { outputDir =>
        withTempDir { checkpointDir =>
          val query = df.write.format("parquet")
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .startStream(outputDir.getAbsolutePath)
          try {
            query.processAllAvailable()
            val outputDf = sqlContext.read.parquet(outputDir.getAbsolutePath).as[Long]
            checkDataset[Long](outputDf, (0L to 10L).toArray: _*)
          } finally {
            query.stop()
          }
        }
      }
    }

    val df = sqlContext.read.format(classOf[FakeDefaultSource].getName).stream()
    assertDF(df)
    assertDF(df)
  }

  test("unsupported queries") {
    val streamInput = MemoryStream[Int]
    val batchInput = Seq(1, 2, 3).toDS()

    def assertError(expectedMsgs: Seq[String])(body: => Unit): Unit = {
      val e = intercept[AnalysisException] {
        body
      }
      expectedMsgs.foreach { s => assert(e.getMessage.contains(s)) }
    }

    // Running streaming plan as a batch query
    assertError("startStream" :: Nil) {
      streamInput.toDS.map { i => i }.count()
    }

    // Running non-streaming plan with as a streaming query
    assertError("without streaming sources" :: "startStream" :: Nil) {
      val ds = batchInput.map { i => i }
      testStream(ds)()
    }

    // Running streaming plan that cannot be incrementalized
    assertError("not supported" :: "streaming" :: Nil) {
      val ds = streamInput.toDS.map { i => i }.sort()
      testStream(ds)()
    }
  }
}

/**
 * A fake StreamSourceProvider thats creates a fake Source that cannot be reused.
 */
class FakeDefaultSource extends StreamSourceProvider {

  private val fakeSchema = StructType(StructField("a", IntegerType) :: Nil)

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = ("fakeSource", fakeSchema)

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    // Create a fake Source that emits 0 to 10.
    new Source {
      private var offset = -1L

      override def schema: StructType = StructType(StructField("a", IntegerType) :: Nil)

      override def getOffset: Option[Offset] = {
        if (offset >= 10) {
          None
        } else {
          offset += 1
          Some(LongOffset(offset))
        }
      }

      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        val startOffset = start.map(_.asInstanceOf[LongOffset].offset).getOrElse(-1L) + 1
        sqlContext.range(startOffset, end.asInstanceOf[LongOffset].offset + 1).toDF("a")
      }
    }
  }
}
