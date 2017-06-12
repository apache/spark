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

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamTest}
import org.apache.spark.util.ManualClock

class RateSourceSuite extends StreamTest {

  import testImplicits._

  case class AdvanceRateManualClock(seconds: Long) extends AddData {
    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      assert(query.nonEmpty)
      val rateSource = query.get.logicalPlan.collect {
        case StreamingExecutionRelation(source, _) if source.isInstanceOf[RateStreamSource] =>
          source.asInstanceOf[RateStreamSource]
      }.head
      rateSource.clock.asInstanceOf[ManualClock].advance(TimeUnit.SECONDS.toMillis(seconds))
      (rateSource, rateSource.getOffset.get)
    }
  }

  test("basic") {
    val input = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("useManualClock", "true")
      .load()
    testStream(input)(
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch((0 until 10).map(v => new java.sql.Timestamp(v * 100L) -> v): _*),
      StopStream,
      StartStream(),
      // Advance 2 seconds because creating a new RateSource will also create a new ManualClock
      AdvanceRateManualClock(seconds = 2),
      CheckLastBatch((10 until 20).map(v => new java.sql.Timestamp(v * 100L) -> v): _*)
    )
  }

  test("uniform distribution of event timestamps") {
    val input = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "1500")
      .option("useManualClock", "true")
      .load()
      .as[(java.sql.Timestamp, Long)]
      .map(v => (v._1.getTime, v._2))
    val expectedAnswer = (0 until 1500).map { v =>
      (math.round(v * (1000.0 / 1500)), v)
    }
    testStream(input)(
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(expectedAnswer: _*)
    )
  }

  test("valueAtSecond") {
    import RateStreamSource._

    assert(valueAtSecond(seconds = 0, rowsPerSecond = 5, rampUpTimeSeconds = 0) === 0)
    assert(valueAtSecond(seconds = 1, rowsPerSecond = 5, rampUpTimeSeconds = 0) === 5)

    assert(valueAtSecond(seconds = 0, rowsPerSecond = 5, rampUpTimeSeconds = 2) === 0)
    assert(valueAtSecond(seconds = 1, rowsPerSecond = 5, rampUpTimeSeconds = 2) === 1)
    assert(valueAtSecond(seconds = 2, rowsPerSecond = 5, rampUpTimeSeconds = 2) === 3)
    assert(valueAtSecond(seconds = 3, rowsPerSecond = 5, rampUpTimeSeconds = 2) === 8)

    assert(valueAtSecond(seconds = 0, rowsPerSecond = 10, rampUpTimeSeconds = 4) === 0)
    assert(valueAtSecond(seconds = 1, rowsPerSecond = 10, rampUpTimeSeconds = 4) === 2)
    assert(valueAtSecond(seconds = 2, rowsPerSecond = 10, rampUpTimeSeconds = 4) === 6)
    assert(valueAtSecond(seconds = 3, rowsPerSecond = 10, rampUpTimeSeconds = 4) === 12)
    assert(valueAtSecond(seconds = 4, rowsPerSecond = 10, rampUpTimeSeconds = 4) === 20)
    assert(valueAtSecond(seconds = 5, rowsPerSecond = 10, rampUpTimeSeconds = 4) === 30)
  }

  test("rampUpTime") {
    val input = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("rampUpTime", "4s")
      .option("useManualClock", "true")
      .load()
      .as[(java.sql.Timestamp, Long)]
      .map(v => (v._1.getTime, v._2))
    testStream(input)(
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch((0 until 2).map(v => v * 500 -> v): _*), // speed = 2
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch((2 until 6).map(v => 1000 + (v - 2) * 250 -> v): _*), // speed = 4
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch({
        Seq(2000 -> 6, 2167 -> 7, 2333 -> 8, 2500 -> 9, 2667 -> 10, 2833 -> 11)
      }: _*), // speed = 6
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch((12 until 20).map(v => 3000 + (v - 12) * 125 -> v): _*), // speed = 8
      AdvanceRateManualClock(seconds = 1),
      // Now we should reach full speed
      CheckLastBatch((20 until 30).map(v => 4000 + (v - 20) * 100 -> v): _*), // speed = 10
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch((30 until 40).map(v => 5000 + (v - 30) * 100 -> v): _*), // speed = 10
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch((40 until 50).map(v => 6000 + (v - 40) * 100 -> v): _*) // speed = 10
    )
  }

  test("numPartitions") {
    val input = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("numPartitions", "6")
      .option("useManualClock", "true")
      .load()
      .select(spark_partition_id())
      .distinct()
    testStream(input)(
      AdvanceRateManualClock(1),
      CheckLastBatch((0 until 6): _*)
    )
  }

  testQuietly("overflow") {
    val input = spark.readStream
      .format("rate")
      .option("rowsPerSecond", Long.MaxValue.toString)
      .option("useManualClock", "true")
      .load()
      .select(spark_partition_id())
      .distinct()
    testStream(input)(
      AdvanceRateManualClock(2),
      ExpectFailure[ArithmeticException](t => {
        Seq("overflow", "rowsPerSecond").foreach { msg =>
          assert(t.getMessage.contains(msg))
        }
      })
    )
  }

  testQuietly("illegal option values") {
    def testIllegalOptionValue(
        option: String,
        value: String,
        expectedMessages: Seq[String]): Unit = {
      val e = intercept[StreamingQueryException] {
        spark.readStream
          .format("rate")
          .option(option, value)
          .load()
          .writeStream
          .format("console")
          .start()
          .awaitTermination()
      }
      assert(e.getCause.isInstanceOf[IllegalArgumentException])
      for (msg <- expectedMessages) {
        assert(e.getCause.getMessage.contains(msg))
      }
    }

    testIllegalOptionValue("rowsPerSecond", "-1", Seq("-1", "rowsPerSecond", "positive"))
    testIllegalOptionValue("numPartitions", "-1", Seq("-1", "numPartitions", "positive"))
  }
}
