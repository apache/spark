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

package org.apache.spark.sql.util

import java.util.concurrent.ConcurrentLinkedQueue

import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfter
import org.scalatest.PrivateMethodTester._
import org.scalatest.concurrent.AsyncAssertions.Waiter
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.util.ContinuousQueryListener.{QueryProgress, QueryStarted, QueryTerminated}

class ContinuousQueryListenerSuite extends StreamTest with SharedSQLContext with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
    assert(sqlContext.streams.active.isEmpty)
    assert(addedListeners.isEmpty)
  }

  test("single listener") {
    val listener = new QueryStatusCollector
    val input = MemoryStream[Int]
    withListenerAdded(listener) {
      testStream(input.toDS)(
        StartStream,
        Assert("Incorrect query status in onQueryStarted") {
          val status = listener.startStatus
          assert(status != null)
          assert(status.active == true)
          assert(status.sourceStatuses.size === 1)
          assert(status.sourceStatuses(0).description.contains("Memory"))

          // The source and sink offsets must be None as this must be called before the
          // batches have started
          assert(status.sourceStatuses(0).offset === None)
          assert(status.sinkStatus.offset === None)

          // No progress events or termination events
          assert(listener.progressStatuses.isEmpty)
          assert(listener.terminationStatus === null)
        },
        AddDataMemory(input, Seq(1, 2, 3)),
        CheckAnswer(1, 2, 3),
        Assert("Incorrect query status in onQueryProgress") {
          eventually(Timeout(streamingTimeout)) {

            // There should be only on progress event as batch has been processed
            assert(listener.progressStatuses.size === 1)
            val status = listener.progressStatuses.peek()
            assert(status != null)
            assert(status.active == true)
            assert(status.sourceStatuses(0).offset === Some(LongOffset(0)))
            assert(status.sinkStatus.offset === Some(CompositeOffset.fill(LongOffset(0))))

            // No termination events
            assert(listener.terminationStatus === null)
          }
        },
        StopStream,
        Assert("Incorrect query status in onQueryTerminated") {
          eventually(Timeout(streamingTimeout)) {
            val status = listener.terminationStatus
            assert(status != null)

            assert(status.active === false) // must be inactive by the time onQueryTerm is called
            assert(status.sourceStatuses(0).offset === Some(LongOffset(0)))
            assert(status.sinkStatus.offset === Some(CompositeOffset.fill(LongOffset(0))))
          }
          listener.checkAsyncErrors()
        }
      )
    }
  }

  test("adding and removing listener") {
    def isListenerActive(listener: QueryStatusCollector): Boolean = {
      listener.reset()
      testStream(MemoryStream[Int].toDS)(
        StartStream,
        StopStream
      )
      listener.startStatus != null
    }

    try {
      val listener1 = new QueryStatusCollector
      val listener2 = new QueryStatusCollector

      sqlContext.streams.addListener(listener1)
      assert(isListenerActive(listener1) === true)
      assert(isListenerActive(listener2) === false)
      sqlContext.streams.addListener(listener2)
      assert(isListenerActive(listener1) === true)
      assert(isListenerActive(listener2) === true)
      sqlContext.streams.removeListener(listener1)
      assert(isListenerActive(listener1) === false)
      assert(isListenerActive(listener2) === true)
    } finally {
      addedListeners.foreach(sqlContext.streams.removeListener)
    }
  }

  test("event ordering") {
    val listener = new QueryStatusCollector
    withListenerAdded(listener) {
      for (i <- 1 to 100) {
        listener.reset()
        require(listener.startStatus === null)
        testStream(MemoryStream[Int].toDS)(
          StartStream,
          Assert(listener.startStatus !== null, "onQueryStarted not called before query returned"),
          StopStream,
          Assert { listener.checkAsyncErrors() }
        )
      }
    }
  }


  private def withListenerAdded(listener: ContinuousQueryListener)(body: => Unit): Unit = {
    @volatile var query: StreamExecution = null
    try {
      failAfter(1 minute) {
        sqlContext.streams.addListener(listener)
        body
      }
    } finally {
      sqlContext.streams.removeListener(listener)
    }
  }

  private def addedListeners(): Array[ContinuousQueryListener] = {
    val listenerBusMethod =
      PrivateMethod[ContinuousQueryListenerBus]('listenerBus)
    val listenerBus = sqlContext.streams invokePrivate listenerBusMethod()
    listenerBus.listeners.toArray.map(_.asInstanceOf[ContinuousQueryListener])
  }

  class QueryStatusCollector extends ContinuousQueryListener {

    private val asyncTestWaiter = new Waiter  // to catch errors in the async listener events

    @volatile var startStatus: QueryStatus = null
    @volatile var terminationStatus: QueryStatus = null
    val progressStatuses = new ConcurrentLinkedQueue[QueryStatus]

    def reset(): Unit = {
      startStatus = null
      terminationStatus = null
      progressStatuses.clear()

      // To reset the waiter
      try asyncTestWaiter.await(timeout(1 milliseconds)) catch {
        case NonFatal(e) =>
      }
    }

    def checkAsyncErrors(): Unit = {
      asyncTestWaiter.await(timeout(streamingTimeout))
    }


    override def onQueryStarted(queryStarted: QueryStarted): Unit = {
      asyncTestWaiter {
        startStatus = QueryStatus(queryStarted.query)
      }
    }

    override def onQueryProgress(queryProgress: QueryProgress): Unit = {
      asyncTestWaiter {
        assert(startStatus != null, "onQueryProgress called before onQueryStarted")
        progressStatuses.add(QueryStatus(queryProgress.query))
      }
    }

    override def onQueryTerminated(queryTerminated: QueryTerminated): Unit = {
      asyncTestWaiter {
        assert(startStatus != null, "onQueryTerminated called before onQueryStarted")
        terminationStatus = QueryStatus(queryTerminated.query)
      }
      asyncTestWaiter.dismiss()
    }
  }

  case class QueryStatus(
    active: Boolean,
    expection: Option[Exception],
    sourceStatuses: Array[SourceStatus],
    sinkStatus: SinkStatus)

  object QueryStatus {
    def apply(query: ContinuousQuery): QueryStatus = {
      QueryStatus(query.isActive, query.exception, query.sourceStatuses, query.sinkStatus)
    }
  }
}
