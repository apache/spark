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

import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.continuous.{ContinuousRateStreamPartitionOffset, ContinuousRateStreamReader, RateStreamDataReader, RateStreamReadTask}
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceV2Options, MicroBatchReadSupport}
import org.apache.spark.sql.streaming.StreamTest

class RateSourceV2Suite extends StreamTest {
  test("microbatch in registry") {
    DataSource.lookupDataSource("rate").newInstance() match {
      case ds: MicroBatchReadSupport =>
        val reader = ds.createMicroBatchReader(Optional.empty(), "", DataSourceV2Options.empty())
        assert(reader.isInstanceOf[RateStreamV2Reader])
      case _ =>
        throw new IllegalStateException("Could not find v2 read support for rate")
    }
  }

  test("microbatch - options propagated") {
    val reader = new RateStreamV2Reader(
      new DataSourceV2Options(Map("numPartitions" -> "11", "rowsPerSecond" -> "33").asJava))
    reader.setOffsetRange(Optional.empty(),
      Optional.of(LongOffset(System.currentTimeMillis() + 1001)))
    val tasks = reader.createReadTasks()
    assert(tasks.size == 11)
    tasks.asScala.foreach {
      // for 1 second, size of each task is (rowsPerSecond / numPartitions)
      case RateStreamBatchTask(vals) => vals.size == 3
      case _ => throw new IllegalStateException("Unexpected task type")
    }
  }

  test("microbatch - set offset") {
    val reader = new RateStreamV2Reader(DataSourceV2Options.empty())
    reader.setOffsetRange(Optional.of(LongOffset(12345)), Optional.of(LongOffset(54321)))
    assert(reader.getStartOffset() == LongOffset(12345))
    assert(reader.getEndOffset() == LongOffset(54321))
  }

  test("microbatch - infer offsets") {
    val reader = new RateStreamV2Reader(DataSourceV2Options.empty())
    reader.clock.waitTillTime(reader.clock.getTimeMillis() + 100)
    reader.setOffsetRange(Optional.empty(), Optional.empty())
    assert(reader.getStartOffset() == LongOffset(reader.creationTimeMs))
    assert(reader.getEndOffset().asInstanceOf[LongOffset].offset >= reader.creationTimeMs + 100)
  }


  test("microbatch - data read") {
    val reader = new RateStreamV2Reader(
      new DataSourceV2Options(Map("numPartitions" -> "11", "rowsPerSecond" -> "33").asJava))
    reader.setOffsetRange(Optional.empty(),
      Optional.of(LongOffset(System.currentTimeMillis() + 1001)))
    val tasks = reader.createReadTasks()
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

  test("continuous in registry") {
    DataSource.lookupDataSource("rate").newInstance() match {
      case ds: ContinuousReadSupport =>
        val reader = ds.createContinuousReader(Optional.empty(), "", DataSourceV2Options.empty())
        assert(reader.isInstanceOf[ContinuousRateStreamReader])
      case _ =>
        throw new IllegalStateException("Could not find v2 read support for rate")
    }
  }

  test("continuous data") {
    val reader = new ContinuousRateStreamReader(
      new DataSourceV2Options(Map("numPartitions" -> "2", "rowsPerSecond" -> "10").asJava))
    reader.setOffset(Optional.empty())
    val tasks = reader.createReadTasks()
    assert(tasks.size == 2)

    val data = scala.collection.mutable.ListBuffer[Row]()
    tasks.asScala.foreach {
      case t: RateStreamReadTask =>
        val startTime = System.currentTimeMillis()
        val r = t.createDataReader().asInstanceOf[RateStreamDataReader]
        // The first set of (rowsPerSecond / numPartitions) should come ~immediately, but the
        // next should only come after 1 second.
        for (i <- 1 to 5) {
          r.next()
          data.append(r.get())
          assert(r.getOffset() ==
            ContinuousRateStreamPartitionOffset(t.partitionIndex, r.get.getLong(1)))
        }
        assert(System.currentTimeMillis() < startTime + 100)
        for (i <- 1 to 5) {
          r.next()
          data.append(r.get())
          assert(r.getOffset() ==
            ContinuousRateStreamPartitionOffset(t.partitionIndex, r.get.getLong(1)))
        }
        assert(System.currentTimeMillis() > startTime + 1000)

      case _ => throw new IllegalStateException("Unexpected task type")
    }

    assert(data.map(_.getLong(1)).toSeq.sorted == Range(0, 20))
  }
}
