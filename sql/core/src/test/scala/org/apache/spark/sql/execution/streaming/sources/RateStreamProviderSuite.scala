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

import java.nio.file.Files
import java.util.Optional
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions, MicroBatchReadSupport}
import org.apache.spark.sql.sources.v2.reader.streaming.Offset
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.util.ManualClock

class RateSourceSuite extends StreamTest {

  import testImplicits._

  case class AdvanceRateManualClock(seconds: Long) extends AddData {
    override def addData(query: Option[StreamExecution]): (BaseStreamingSource, Offset) = {
      assert(query.nonEmpty)
      val rateSource = query.get.logicalPlan.collect {
        case StreamingExecutionRelation(source: RateStreamMicroBatchReader, _) => source
      }.head

      rateSource.clock.asInstanceOf[ManualClock].advance(TimeUnit.SECONDS.toMillis(seconds))
      val offset = LongOffset(TimeUnit.MILLISECONDS.toSeconds(
        rateSource.clock.getTimeMillis() - rateSource.creationTimeMs))
      (rateSource, offset)
    }
  }

  test("microbatch in registry") {
    DataSource.lookupDataSource("rate", spark.sqlContext.conf).newInstance() match {
      case ds: MicroBatchReadSupport =>
        val reader = ds.createMicroBatchReader(Optional.empty(), "dummy", DataSourceOptions.empty())
        assert(reader.isInstanceOf[RateStreamMicroBatchReader])
      case _ =>
        throw new IllegalStateException("Could not find read support for rate")
    }
  }

  test("compatible with old path in registry") {
    DataSource.lookupDataSource("org.apache.spark.sql.execution.streaming.RateSourceProvider",
      spark.sqlContext.conf).newInstance() match {
      case ds: MicroBatchReadSupport =>
        assert(ds.isInstanceOf[RateStreamProvider])
      case _ =>
        throw new IllegalStateException("Could not find read support for rate")
    }
  }

  test("microbatch - basic") {
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

  test("microbatch - uniform distribution of event timestamps") {
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

  test("microbatch - set offset") {
    val temp = Files.createTempDirectory("dummy").toString
    val reader = new RateStreamMicroBatchReader(DataSourceOptions.empty(), temp)
    val startOffset = LongOffset(0L)
    val endOffset = LongOffset(1L)
    reader.setOffsetRange(Optional.of(startOffset), Optional.of(endOffset))
    assert(reader.getStartOffset() == startOffset)
    assert(reader.getEndOffset() == endOffset)
  }

  test("microbatch - infer offsets") {
    val tempFolder = Files.createTempDirectory("dummy").toString
    val reader = new RateStreamMicroBatchReader(
      new DataSourceOptions(
        Map("numPartitions" -> "1", "rowsPerSecond" -> "100", "useManualClock" -> "true").asJava),
      tempFolder)
    reader.clock.asInstanceOf[ManualClock].advance(100000)
    reader.setOffsetRange(Optional.empty(), Optional.empty())
    reader.getStartOffset() match {
      case r: LongOffset => assert(r.offset === 0L)
      case _ => throw new IllegalStateException("unexpected offset type")
    }
    reader.getEndOffset() match {
      case r: LongOffset => assert(r.offset >= 100)
      case _ => throw new IllegalStateException("unexpected offset type")
    }
  }

  test("microbatch - predetermined batch size") {
    val temp = Files.createTempDirectory("dummy").toString
    val reader = new RateStreamMicroBatchReader(
      new DataSourceOptions(Map("numPartitions" -> "1", "rowsPerSecond" -> "20").asJava), temp)
    val startOffset = LongOffset(0L)
    val endOffset = LongOffset(1L)
    reader.setOffsetRange(Optional.of(startOffset), Optional.of(endOffset))
    val tasks = reader.createDataReaderFactories()
    assert(tasks.size == 1)
    val dataReader = tasks.get(0).createDataReader()
    val data = ArrayBuffer[Row]()
    while (dataReader.next()) {
      data.append(dataReader.get())
    }
    assert(data.size === 20)
  }

  test("microbatch - data read") {
    val temp = Files.createTempDirectory("dummy").toString
    val reader = new RateStreamMicroBatchReader(
      new DataSourceOptions(Map("numPartitions" -> "11", "rowsPerSecond" -> "33").asJava), temp)
    val startOffset = LongOffset(0L)
    val endOffset = LongOffset(1L)
    reader.setOffsetRange(Optional.of(startOffset), Optional.of(endOffset))
    val tasks = reader.createDataReaderFactories()
    assert(tasks.size == 11)

    val readData = tasks.asScala
      .map(_.createDataReader())
      .flatMap { reader =>
        val buf = scala.collection.mutable.ListBuffer[Row]()
        while (reader.next()) buf.append(reader.get())
        buf
      }

    assert(readData.map(_.getLong(1)).sorted == Range(0, 33))
  }

  test("valueAtSecond without ramp-up") {
    import RateStreamProvider._
    val rowsPerSec = Seq(1,10,50,100,1000,10000)
    val secs = Seq(1, 10, 100, 1000, 10000, 100000)
    for {
      sec <- secs
      rps <- rowsPerSec
    } yield {
      assert(valueAtSecond(seconds = sec, rowsPerSecond = rps, rampUpTimeSeconds = 0) === sec * rps)
    }
  }

  test("valueAtSecond with ramp-up") {
    import RateStreamProvider._
    val rowsPerSec = Seq(1, 5, 10, 50, 100, 1000, 10000)
    val rampUpSec = Seq(10, 100, 1000)

    // for any combination, value at zero = 0
    for {
      rps <- rowsPerSec
      rampUp <- rampUpSec
    } yield {
      assert(valueAtSecond(seconds = 0, rowsPerSecond = rps, rampUpTimeSeconds = rampUp) === 0)
    }

    // for any combination, the value at half-way between (0, rampUpSeconds) > 0
    for {
      rps <- rowsPerSec
      rampUp <- rampUpSec
      if rampUp/2 > 0
    } yield {
      assert(
        valueAtSecond(seconds = rampUp/2 , rowsPerSecond = rps, rampUpTimeSeconds = rampUp) > 0
      )
    }

    // The rate increases as the time gets to the ramp-up value and stabilizes to rowsPerSecond
    for {
      rps <- rowsPerSec
      rampUp <- rampUpSec
    } yield {
      val valueAtSec: Int => Long = i =>
        valueAtSecond(i , rowsPerSecond = rps, rampUpTimeSeconds = rampUp)

      val valuePerSecond = (0 to rampUp).map(i => valueAtSec(i))
      // calculate the actual rate
      val diffs = valuePerSecond.zip(valuePerSecond.tail).map{case (x,x1) => x1-x}
      // there should be values
      assert(diffs.sum > 0)
      // Rate values should be increasing
      assert(diffs.forall(x => x >= 0 ))

      // The rate after ramp up is the configured rate per second
      assert(valueAtSec(rampUp + 1) - valueAtSec(rampUp) ===  rps )
    }
  }

  // evenly distributes numValues over a second in milisecond intervals starting at startValue
  private def distributeValues(startValue: Int, numValues: Int, atSecond:Long): Seq[(Long, Long)]= {
    val offset = atSecond * 1000
    (0 until numValues).map{v =>
      val timeMills = offset + Math.round(v*1000.0/numValues)
      timeMills -> (v + startValue).toLong}
  }

  test("rampUpTime when rowsPerSecond > rampUpTime") {
    val input = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("rampUpTime", "4s")
      .option("useManualClock", "true")
      .load()
      .as[(java.sql.Timestamp, Long)]
      .map{case (ts, value) => (ts.getTime, value)}
    testStream(input)(
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(Seq((0,0)): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 1, numValues = 4, atSecond = 1): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 5, numValues = 6, atSecond = 2): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 11, numValues = 9, atSecond = 3): _*),
      AdvanceRateManualClock(seconds = 1),
      // Now we should reach full rate
      CheckLastBatch(distributeValues(startValue = 20, numValues = 10, atSecond = 4): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 30, numValues = 10, atSecond = 5): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 40, numValues = 10, atSecond = 6): _*)
    )
  }

  test("rampUpTime when rowsPerSecond < rampUpTime") {
    val input = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "4")
      .option("rampUpTime", "5s")
      .option("useManualClock", "true")
      .load()
      .as[(java.sql.Timestamp, Long)]
      .map{case (ts, value) => (ts.getTime, value)}
    testStream(input)(
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch((1000,0)),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 1, numValues = 2, atSecond = 2): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 3, numValues = 3, atSecond = 3): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 6, numValues = 4, atSecond = 4): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 10, numValues = 4, atSecond = 5): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 14, numValues = 4, atSecond = 6): _*)
    )
  }

  test("rampUpTime when rowsPerSecond == rampUpTime") {
    val input = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "5")
      .option("rampUpTime", "5s")
      .option("useManualClock", "true")
      .load()
      .as[(java.sql.Timestamp, Long)]
      .map{case (ts, value) => (ts.getTime, value)}
    testStream(input)(
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 0, numValues = 2, atSecond = 1): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 2, numValues = 2, atSecond = 2): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 4, numValues = 4, atSecond = 3): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 8, numValues = 4, atSecond = 4): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 12, numValues = 5, atSecond = 5): _*),
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch(distributeValues(startValue = 17, numValues = 5, atSecond = 6): _*)
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
      val e = intercept[IllegalArgumentException] {
        spark.readStream
          .format("rate")
          .option(option, value)
          .load()
          .writeStream
          .format("console")
          .start()
          .awaitTermination()
      }
      for (msg <- expectedMessages) {
        assert(e.getMessage.contains(msg))
      }
    }

    testIllegalOptionValue("rowsPerSecond", "-1", Seq("-1", "rowsPerSecond", "positive"))
    testIllegalOptionValue("numPartitions", "-1", Seq("-1", "numPartitions", "positive"))
  }

  test("user-specified schema given") {
    val exception = intercept[AnalysisException] {
      spark.readStream
        .format("rate")
        .schema(spark.range(1).schema)
        .load()
    }
    assert(exception.getMessage.contains(
      "rate source does not support a user-specified schema"))
  }

  test("continuous in registry") {
    DataSource.lookupDataSource("rate", spark.sqlContext.conf).newInstance() match {
      case ds: ContinuousReadSupport =>
        val reader = ds.createContinuousReader(Optional.empty(), "", DataSourceOptions.empty())
        assert(reader.isInstanceOf[RateStreamContinuousReader])
      case _ =>
        throw new IllegalStateException("Could not find read support for continuous rate")
    }
  }

  test("continuous data") {
    val reader = new RateStreamContinuousReader(
      new DataSourceOptions(Map("numPartitions" -> "2", "rowsPerSecond" -> "20").asJava))
    reader.setStartOffset(Optional.empty())
    val tasks = reader.createDataReaderFactories()
    assert(tasks.size == 2)

    val data = scala.collection.mutable.ListBuffer[Row]()
    tasks.asScala.foreach {
      case t: RateStreamContinuousDataReaderFactory =>
        val startTimeMs = reader.getStartOffset()
          .asInstanceOf[RateStreamOffset]
          .partitionToValueAndRunTimeMs(t.partitionIndex)
          .runTimeMs
        val r = t.createDataReader().asInstanceOf[RateStreamContinuousDataReader]
        for (rowIndex <- 0 to 9) {
          r.next()
          data.append(r.get())
          assert(r.getOffset() ==
            RateStreamPartitionOffset(
              t.partitionIndex,
              t.partitionIndex + rowIndex * 2,
              startTimeMs + (rowIndex + 1) * 100))
        }
        assert(System.currentTimeMillis() >= startTimeMs + 1000)

      case _ => throw new IllegalStateException("Unexpected task type")
    }

    assert(data.map(_.getLong(1)).toSeq.sorted == Range(0, 20))
  }
}
