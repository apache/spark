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

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.test.{InMemoryStreamTable, InMemoryStreamTableCatalog}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class StreamingSelfUnionSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  before {
    spark.conf.set("spark.sql.catalog.teststream", classOf[InMemoryStreamTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
    sqlContext.streams.active.foreach(_.stop())
  }

  test("self-union, DSv1, read via DataStreamReader API") {
    withTempPath { dir =>
      val dataLocation = dir.getAbsolutePath
      spark.range(1, 4).write.format("parquet").save(dataLocation)

      val streamDf = spark.readStream.format("parquet")
        .schema(StructType(Seq(StructField("id", LongType)))).load(dataLocation)
      val unionedDf = streamDf.union(streamDf)

      testStream(unionedDf)(
        ProcessAllAvailable(),
        CheckLastBatch(1, 2, 3, 1, 2, 3),
        AssertOnQuery { q =>
          val lastProgress = getLastProgressWithData(q)
          assert(lastProgress.nonEmpty)
          assert(lastProgress.get.numInputRows == 6)
          assert(lastProgress.get.sources.length == 1)
          assert(lastProgress.get.sources(0).numInputRows == 6)
          true
        }
      )
    }
  }

  test("self-union, DSv1, read via table API") {
    withTable("parquet_streaming_tbl") {
      spark.sql("CREATE TABLE parquet_streaming_tbl (key integer) USING parquet")

      val streamDf = spark.readStream.table("parquet_streaming_tbl")
      val unionedDf = streamDf.union(streamDf)

      val clock = new StreamManualClock()
      testStream(unionedDf)(
        StartStream(triggerClock = clock, trigger = Trigger.ProcessingTime(100)),
        Execute { _ =>
          spark.range(1, 4).selectExpr("id AS key")
            .write.format("parquet").mode(SaveMode.Append).saveAsTable("parquet_streaming_tbl")
        },
        AdvanceManualClock(150),
        waitUntilBatchProcessed(clock),
        CheckLastBatch(1, 2, 3, 1, 2, 3),
        AssertOnQuery { q =>
          val lastProgress = getLastProgressWithData(q)
          assert(lastProgress.nonEmpty)
          assert(lastProgress.get.numInputRows == 6)
          assert(lastProgress.get.sources.length == 1)
          assert(lastProgress.get.sources(0).numInputRows == 6)
          true
        }
      )
    }
  }

  test("self-union, DSv2, read via DataStreamReader API") {
    val inputData = MemoryStream[Int]

    val streamDf = inputData.toDF()
    val unionedDf = streamDf.union(streamDf)

    testStream(unionedDf)(
      AddData(inputData, 1, 2, 3),
      CheckLastBatch(1, 2, 3, 1, 2, 3),
      AssertOnQuery { q =>
        val lastProgress = getLastProgressWithData(q)
        assert(lastProgress.nonEmpty)
        assert(lastProgress.get.numInputRows == 6)
        assert(lastProgress.get.sources.length == 1)
        assert(lastProgress.get.sources(0).numInputRows == 6)
        true
      }
    )
  }

  test("self-union, DSv2, read via table API") {
    val tblName = "teststream.table_name"
    withTable(tblName) {
      spark.sql(s"CREATE TABLE $tblName (data int) USING foo")
      val stream = MemoryStream[Int]
      val testCatalog = spark.sessionState.catalogManager.catalog("teststream").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
      table.asInstanceOf[InMemoryStreamTable].setStream(stream)

      val streamDf = spark.readStream.table(tblName)
      val unionedDf = streamDf.union(streamDf)

      testStream(unionedDf) (
        AddData(stream, 1, 2, 3),
        CheckLastBatch(1, 2, 3, 1, 2, 3),
        AssertOnQuery { q =>
          val lastProgress = getLastProgressWithData(q)
          assert(lastProgress.nonEmpty)
          assert(lastProgress.get.numInputRows == 6)
          assert(lastProgress.get.sources.length == 1)
          assert(lastProgress.get.sources(0).numInputRows == 6)
          true
        }
      )
    }
  }

  private def waitUntilBatchProcessed(clock: StreamManualClock) = AssertOnQuery { q =>
    eventually(Timeout(streamingTimeout)) {
      if (q.exception.isEmpty) {
        assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
      }
    }
    if (q.exception.isDefined) {
      throw q.exception.get
    }
    true
  }

  private def getLastProgressWithData(q: StreamingQuery): Option[StreamingQueryProgress] = {
    q.recentProgress.filter(_.numInputRows > 0).lastOption
  }
}
