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

package org.apache.spark.sql.execution.streaming.sources

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{count, timestamp_seconds, window}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException, StreamTest}
import org.apache.spark.sql.test.SharedSparkSession

class ForeachWriterSuite extends StreamTest with SharedSparkSession with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("foreach() with `append` output mode") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(2).writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .outputMode(OutputMode.Append)
        .foreach(new TestForeachWriter())
        .start()

      def verifyOutput(expectedVersion: Int, expectedData: Seq[Int]): Unit = {
        import ForeachWriterSuite._

        val events = ForeachWriterSuite.allEvents()
        assert(events.size === 2) // one seq of events for each of the 2 partitions

        // Verify both seq of events have an Open event as the first event
        assert(events.map(_.head).toSet === Set(0, 1).map(p => Open(p, expectedVersion)))

        // Verify all the Process event correspond to the expected data
        val allProcessEvents = events.flatMap(_.filter(_.isInstanceOf[Process[_]]))
        assert(allProcessEvents.toSet === expectedData.map { data => Process(data) }.toSet)

        // Verify both seq of events have a Close event as the last event
        assert(events.map(_.last).toSet === Set(Close(None), Close(None)))
      }

      // -- batch 0 ---------------------------------------
      ForeachWriterSuite.clear()
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()
      verifyOutput(expectedVersion = 0, expectedData = 1 to 4)

      // -- batch 1 ---------------------------------------
      ForeachWriterSuite.clear()
      input.addData(5, 6, 7, 8)
      query.processAllAvailable()
      verifyOutput(expectedVersion = 1, expectedData = 5 to 8)

      query.stop()
    }
  }

  test("foreach() with `complete` output mode") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]

      val query = input.toDS()
        .groupBy().count().as[Long].map(_.toInt)
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .outputMode(OutputMode.Complete)
        .foreach(new TestForeachWriter())
        .start()

      // -- batch 0 ---------------------------------------
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      var allEvents = ForeachWriterSuite.allEvents()
      assert(allEvents.size === 1)
      var expectedEvents = Seq(
        ForeachWriterSuite.Open(partition = 0, version = 0),
        ForeachWriterSuite.Process(value = 4),
        ForeachWriterSuite.Close(None)
      )
      assert(allEvents === Seq(expectedEvents))

      ForeachWriterSuite.clear()

      // -- batch 1 ---------------------------------------
      input.addData(5, 6, 7, 8)
      query.processAllAvailable()

      allEvents = ForeachWriterSuite.allEvents()
      assert(allEvents.size === 1)
      expectedEvents = Seq(
        ForeachWriterSuite.Open(partition = 0, version = 1),
        ForeachWriterSuite.Process(value = 8),
        ForeachWriterSuite.Close(None)
      )
      assert(allEvents === Seq(expectedEvents))

      query.stop()
    }
  }

  testQuietly("foreach with error") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(1).writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter() {
          override def process(value: Int): Unit = {
            super.process(value)
            throw new RuntimeException("ForeachSinkSuite error")
          }
        }).start()
      input.addData(1, 2, 3, 4)

      // Error in `process` should fail the Spark job
      val e = intercept[StreamingQueryException] {
        query.processAllAvailable()
      }
      assert(e.getCause.isInstanceOf[SparkException])
      assert(e.getCause.getCause.getMessage === "ForeachSinkSuite error")
      assert(query.isActive === false)

      val allEvents = ForeachWriterSuite.allEvents()
      assert(allEvents.size === 1)
      assert(allEvents(0)(0) === ForeachWriterSuite.Open(partition = 0, version = 0))
      assert(allEvents(0)(1) === ForeachWriterSuite.Process(value = 1))

      // `close` should be called with the error
      val errorEvent = allEvents(0)(2).asInstanceOf[ForeachWriterSuite.Close]
      assert(errorEvent.error.get.isInstanceOf[RuntimeException])
      assert(errorEvent.error.get.getMessage === "ForeachSinkSuite error")
      // 'close' shouldn't be called with abort message if close with error has been called
      assert(allEvents(0).size == 3)
    }
  }

  test("foreach with watermark: complete") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"count".as[Long])
      .map(_.toInt)
      .repartition(1)

    val query = windowedAggregation
      .writeStream
      .outputMode(OutputMode.Complete)
      .foreach(new TestForeachWriter())
      .start()
    try {
      inputData.addData(10, 11, 12)
      query.processAllAvailable()

      val allEvents = ForeachWriterSuite.allEvents()
      assert(allEvents.size === 1)
      val expectedEvents = Seq(
        ForeachWriterSuite.Open(partition = 0, version = 0),
        ForeachWriterSuite.Process(value = 3),
        ForeachWriterSuite.Close(None)
      )
      assert(allEvents === Seq(expectedEvents))
    } finally {
      query.stop()
    }
  }

  test("foreach with watermark: append") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"count".as[Long])
      .map(_.toInt)
      .repartition(1)

    val query = windowedAggregation
      .writeStream
      .outputMode(OutputMode.Append)
      .foreach(new TestForeachWriter())
      .start()
    try {
      inputData.addData(10, 11, 12)
      query.processAllAvailable()
      inputData.addData(25) // Evict items less than previous watermark
      query.processAllAvailable()

      // There should be 3 batches and only does the last batch contain a value.
      val allEvents = ForeachWriterSuite.allEvents()
      assert(allEvents.size === 4)
      val expectedEvents = Seq(
        Seq(
          ForeachWriterSuite.Open(partition = 0, version = 0),
          ForeachWriterSuite.Close(None)
        ),
        Seq(
          ForeachWriterSuite.Open(partition = 0, version = 1),
          ForeachWriterSuite.Close(None)
        ),
        Seq(
          ForeachWriterSuite.Open(partition = 0, version = 2),
          ForeachWriterSuite.Close(None)
        ),
        Seq(
          ForeachWriterSuite.Open(partition = 0, version = 3),
          ForeachWriterSuite.Process(value = 3),
          ForeachWriterSuite.Close(None)
        )
      )
      assert(allEvents === expectedEvents)
    } finally {
      query.stop()
    }
  }

  test("foreach sink should support metrics") {
    val inputData = MemoryStream[Int]
    val query = inputData.toDS()
      .writeStream
      .foreach(new TestForeachWriter())
      .start()
    try {
      inputData.addData(10, 11, 12)
      query.processAllAvailable()
      val recentProgress = query.recentProgress.filter(_.numInputRows != 0).headOption
      assert(recentProgress.isDefined && recentProgress.get.numInputRows === 3,
        s"recentProgress[${query.recentProgress.toList}] doesn't contain correct metrics")
    } finally {
      query.stop()
    }
  }

  testQuietly("foreach with error not caused by ForeachWriter") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(1).map(_ / 0).writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter)
        .start()
      input.addData(1, 2, 3, 4)

      val e = intercept[StreamingQueryException] {
        query.processAllAvailable()
      }

      assert(e.getCause.isInstanceOf[SparkException])
      assert(e.getCause.getCause.getMessage === "/ by zero")
      assert(query.isActive === false)

      val allEvents = ForeachWriterSuite.allEvents()
      assert(allEvents.size === 1)
      assert(allEvents(0)(0) === ForeachWriterSuite.Open(partition = 0, version = 0))
      // `close` should be called with the error
      val errorEvent = allEvents(0)(1).asInstanceOf[ForeachWriterSuite.Close]
      assert(errorEvent.error.get.isInstanceOf[SparkException])
      assert(errorEvent.error.get.getMessage ===
        "Foreach writer has been aborted due to a task failure")
    }
  }
}

/** A global object to collect events in the executor */
object ForeachWriterSuite {

  trait Event

  case class Open(partition: Long, version: Long) extends Event

  case class Process[T](value: T) extends Event

  case class Close(error: Option[Throwable]) extends Event

  private val _allEvents = new ConcurrentLinkedQueue[Seq[Event]]()

  def addEvents(events: Seq[Event]): Unit = {
    _allEvents.add(events)
  }

  def allEvents(): Seq[Seq[Event]] = {
    _allEvents.toArray(new Array[Seq[Event]](_allEvents.size()))
  }

  def clear(): Unit = {
    _allEvents.clear()
  }
}

/** A [[ForeachWriter]] that writes collected events to ForeachSinkSuite */
class TestForeachWriter extends ForeachWriter[Int] {
  ForeachWriterSuite.clear()

  private val events = mutable.ArrayBuffer[ForeachWriterSuite.Event]()

  override def open(partitionId: Long, version: Long): Boolean = {
    events += ForeachWriterSuite.Open(partition = partitionId, version = version)
    true
  }

  override def process(value: Int): Unit = {
    events += ForeachWriterSuite.Process(value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    events += ForeachWriterSuite.Close(error = Option(errorOrNull))
    ForeachWriterSuite.addEvents(events.toSeq)
  }
}
