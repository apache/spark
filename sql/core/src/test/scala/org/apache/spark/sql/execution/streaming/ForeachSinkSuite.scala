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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSQLContext

class ForeachSinkSuite extends StreamTest with SharedSQLContext with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("foreach") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(2).writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter())
        .start()
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      val expectedEventsForPartition0 = Seq(
        ForeachSinkSuite.Open(partition = 0, version = 0),
        ForeachSinkSuite.Process(value = 1),
        ForeachSinkSuite.Process(value = 3),
        ForeachSinkSuite.Close(None)
      )
      val expectedEventsForPartition1 = Seq(
        ForeachSinkSuite.Open(partition = 1, version = 0),
        ForeachSinkSuite.Process(value = 2),
        ForeachSinkSuite.Process(value = 4),
        ForeachSinkSuite.Close(None)
      )

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 2)
      assert {
        allEvents === Seq(expectedEventsForPartition0, expectedEventsForPartition1) ||
          allEvents === Seq(expectedEventsForPartition1, expectedEventsForPartition0)
      }
      query.stop()
    }
  }

  test("foreach with error") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(1).writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter() {
          override def process(value: Int): Unit = {
            super.process(value)
            throw new RuntimeException("error")
          }
        }).start()
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      assert(allEvents(0)(0) === ForeachSinkSuite.Open(partition = 0, version = 0))
      assert(allEvents(0)(1) ===  ForeachSinkSuite.Process(value = 1))
      val errorEvent = allEvents(0)(2).asInstanceOf[ForeachSinkSuite.Close]
      assert(errorEvent.error.get.isInstanceOf[RuntimeException])
      assert(errorEvent.error.get.getMessage === "error")
      query.stop()
    }
  }
}

/** A global object to collect events in the executor */
object ForeachSinkSuite {

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
  ForeachSinkSuite.clear()

  private val events = mutable.ArrayBuffer[ForeachSinkSuite.Event]()

  override def open(partitionId: Long, version: Long): Boolean = {
    events += ForeachSinkSuite.Open(partition = partitionId, version = version)
    true
  }

  override def process(value: Int): Unit = {
    events += ForeachSinkSuite.Process(value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    events += ForeachSinkSuite.Close(error = Option(errorOrNull))
    ForeachSinkSuite.addEvents(events)
  }
}
