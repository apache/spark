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

package org.apache.spark.sql.execution.streaming

import java.util
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._
import scala.concurrent.TimeoutException

import org.scalatest.BeforeAndAfter
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.CONTINUOUS_READ
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MicroBatchExecutionSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("SPARK-24156: do not plan a no-data batch again after it has already been planned") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(df)(
      AddData(inputData, 10, 11, 12, 13, 14, 15), // Set watermark to 5
      CheckAnswer(),
      AddData(inputData, 25), // Set watermark to 15 to make MicroBatchExecution run no-data batch
      CheckAnswer((10, 5)),   // Last batch should be a no-data batch
      StopStream,
      Execute { q =>
        // Delete the last committed batch from the commit log to signify that the last batch
        // (a no-data batch) never completed
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        q.commitLog.purgeAfter(commit - 1)
      },
      // Add data before start so that MicroBatchExecution can plan a batch. It should not,
      // it should first re-run the incomplete no-data batch and then run a new batch to process
      // new data.
      AddData(inputData, 30),
      StartStream(),
      CheckNewAnswer((15, 1)),   // This should not throw the error reported in SPARK-24156
      StopStream,
      Execute { q =>
        // Delete the entire commit log
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        q.commitLog.purge(commit + 1)
      },
      AddData(inputData, 50),
      StartStream(),
      CheckNewAnswer((25, 1), (30, 1))   // This should not throw the error reported in SPARK-24156
    )
  }

  test("SPARK-30143: stop waits until timeout if blocked") {
    val latch = new CountDownLatch(1)
    BlockOnStopSourceProvider.setLatch(latch)
    val sq = spark.readStream.format(BlockOnStopSourceProvider.getClass.getName)
      .load()
      .writeStream
      .format("console")
      .start()
    failAfter(60.seconds) {
      val startTime = System.nanoTime()
      withSQLConf(SQLConf.STREAMING_STOP_TIMEOUT.key -> "2000") {
        intercept[TimeoutException] {
          sq.stop()
        }
      }
      val duration = (System.nanoTime() - startTime) / 1e6
      assert(duration >= 2000 * 1e6,
        s"Should have waiter more than 2000 millis, but waited $duration millis")

      latch.countDown()
      withSQLConf(SQLConf.STREAMING_STOP_TIMEOUT.key -> "0") {
        sq.stop()
      }
    }
  }
}

object BlockOnStopSourceProvider extends StreamSourceProvider with TableProvider {

  private var _latch: CountDownLatch = _

  val schema: StructType = new StructType().add("id", LongType)

  def setLatch(latch: CountDownLatch): Unit = {
    _latch = latch
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new BlockOnStopSourceTable(_latch)
  }

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    "blockingSource" -> BlockOnStopSourceProvider.schema
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    new BlockOnStopSource(sqlContext.sparkSession, _latch)
  }
}

class BlockOnStopSource(spark: SparkSession, latch: CountDownLatch) extends Source {
  // Blocks until latch countdowns
  override def stop(): Unit = latch.await()

  // Boiler-plate
  override val schema: StructType = BlockOnStopSourceProvider.schema
  override def getOffset: Option[Offset] = Some(LongOffset(0))
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }
}
