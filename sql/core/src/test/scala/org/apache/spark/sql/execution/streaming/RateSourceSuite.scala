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

  private def getManualClockFromQuery(query: StreamExecution): ManualClock = {
    val rateSource = query.logicalPlan.collect {
      case StreamingExecutionRelation(source, _) if source.isInstanceOf[RateStreamSource] =>
        source.asInstanceOf[RateStreamSource]
    }.head
    rateSource.clock.asInstanceOf[ManualClock]
  }

  test("basic") {
    val input = spark.readStream
      .format("rate")
      .option("tuplesPerSecond", "10")
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

  test("valueAtSecond") {
    import RateStreamSource._

    assert(valueAtSecond(seconds = 0, tuplesPerSecond = 5, rampUpTimeSeconds = 2) === 0)
    assert(valueAtSecond(seconds = 1, tuplesPerSecond = 5, rampUpTimeSeconds = 2) === 1)
    assert(valueAtSecond(seconds = 2, tuplesPerSecond = 5, rampUpTimeSeconds = 2) === 3)
    assert(valueAtSecond(seconds = 3, tuplesPerSecond = 5, rampUpTimeSeconds = 2) === 8)

    assert(valueAtSecond(seconds = 0, tuplesPerSecond = 10, rampUpTimeSeconds = 4) === 0)
    assert(valueAtSecond(seconds = 1, tuplesPerSecond = 10, rampUpTimeSeconds = 4) === 2)
    assert(valueAtSecond(seconds = 2, tuplesPerSecond = 10, rampUpTimeSeconds = 4) === 6)
    assert(valueAtSecond(seconds = 3, tuplesPerSecond = 10, rampUpTimeSeconds = 4) === 12)
    assert(valueAtSecond(seconds = 4, tuplesPerSecond = 10, rampUpTimeSeconds = 4) === 20)
    assert(valueAtSecond(seconds = 5, tuplesPerSecond = 10, rampUpTimeSeconds = 4) === 30)
  }

  test("rampUpTimeSeconds") {
    val input = spark.readStream
      .format("rate")
      .option("tuplesPerSecond", "10")
      .option("rampUpTimeSeconds", "4")
      .option("useManualClock", "true")
      .load()
      .select($"value")
    testStream(input)(
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(0 until 2: _*), // speed = 2
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(2 until 6: _*), // speed = 4
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(6 until 12: _*), // speed = 6
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(12 until 20: _*), // speed = 8
      AdvanceRateManualClock(seconds = 1),
      // Now we should reach full speed
      CheckLastBatch(20 until 30: _*), // speed = 10
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(30 until 40: _*), // speed = 10
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(40 until 50: _*) // speed = 10
    )
  }

  test("numPartitions") {
    val input = spark.readStream
      .format("rate")
      .option("tuplesPerSecond", "10")
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
      .option("tuplesPerSecond", Long.MaxValue.toString)
      .option("useManualClock", "true")
      .load()
      .select(spark_partition_id())
      .distinct()
    testStream(input)(
      AdvanceRateManualClock(2),
      ExpectFailure[ArithmeticException](t => {
        Seq("overflow", "tuplesPerSecond").foreach { msg =>
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

    testIllegalOptionValue("tuplesPerSecond", "-1", Seq("-1", "tuplesPerSecond", "positive"))
    testIllegalOptionValue("numPartitions", "-1", Seq("-1", "numPartitions", "positive"))
  }
}
