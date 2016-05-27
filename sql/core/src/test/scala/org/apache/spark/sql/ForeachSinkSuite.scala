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

package org.apache.spark.sql

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.test.SharedSQLContext

class ForeachSinkSuite extends StreamTest with SharedSQLContext {

  import testImplicits._

  test("foreach") {
    ForeachWriterEvent.clear()
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(2).write
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new ForeachWriter[Int] {

          private val events = mutable.ArrayBuffer[ForeachWriterEvent.Event]()

          override def open(version: Long): Unit = {
            events += ForeachWriterEvent.Open(
              partition = TaskContext.getPartitionId(),
              version = version)
          }

          override def process(value: Int): Unit = {
            events += ForeachWriterEvent.Process(
              partition = TaskContext.getPartitionId(),
              value = value)
          }

          override def close(): Unit = {
            events += ForeachWriterEvent.Close(partition = TaskContext.getPartitionId())
            ForeachWriterEvent.addEvents(events)
          }
        })
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      val expectedEventsForPartition0 = Seq(
        ForeachWriterEvent.Open(partition = 0, version = 0),
        ForeachWriterEvent.Process(partition = 0, value = 1),
        ForeachWriterEvent.Process(partition = 0, value = 3),
        ForeachWriterEvent.Close(partition = 0)
      )
      val expectedEventsForPartition1 = Seq(
        ForeachWriterEvent.Open(partition = 1, version = 0),
        ForeachWriterEvent.Process(partition = 1, value = 2),
        ForeachWriterEvent.Process(partition = 1, value = 4),
        ForeachWriterEvent.Close(partition = 1)
      )

      val allEvents = ForeachWriterEvent.allEvents()
      assert(allEvents.size === 2)
      assert {
        allEvents === Seq(expectedEventsForPartition0, expectedEventsForPartition1) ||
          allEvents === Seq(expectedEventsForPartition1, expectedEventsForPartition0)
      }
      query.stop()
    }
  }
}

/** A global object to collect events in the executor side */
object ForeachWriterEvent {

  trait Event

  case class Open(partition: Int, version: Long) extends Event

  case class Process[T](partition: Int, value: T) extends Event

  case class Close(partition: Int) extends Event

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
