package org.apache.spark.sql.util

import scala.collection.mutable
import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfter
import org.scalatest.PrivateMethodTester._
import org.scalatest.concurrent.AsyncAssertions.Waiter
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{ContinuousQueryListenerBus, MemoryStream, StreamExecution}
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

    withListenerAdded(listener) {
      testStream(MemoryStream[Int].toDS)(
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
        },
        StopStream,
        Assert("Incorrect query status in onQueryTerminated") {
          eventually(Timeout(streamingTimout)) {
            val status = listener.terminationStatus
            assert(status != null)
            assert(status.active === false) // must be inactive by the time onQueryTerm is called
            assert(status.sourceStatuses(0).offset === None)
            assert(status.sinkStatus.offset === None)
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


  private def withListenerAdded(listeners: ContinuousQueryListener*)(body: => Unit): Unit = {
    @volatile var query: StreamExecution = null
    try {
      failAfter(streamingTimout) {
        listeners.foreach(sqlContext.streams.addListener)
        body
      }
    } finally {
      listeners.foreach(sqlContext.streams.removeListener)
    }
  }

  private def addedListeners(): Array[ContinuousQueryListener] = {
    val listenerBusMethod =
      PrivateMethod[ContinuousQueryListenerBus]('listenerBus)
    val listenerBus = sqlContext.streams invokePrivate listenerBusMethod()
    listenerBus.listeners.toArray.map(_.asInstanceOf[ContinuousQueryListener])
  }

  class QueryStatusCollector extends ContinuousQueryListener {

    private val asyncTestWaiter = new Waiter  // to catch errors in the listener thread

    @volatile var startStatus: QueryStatus = null
    @volatile var terminationStatus: QueryStatus = null
    val progressStatuses = new mutable.ArrayBuffer[QueryStatus]
      with mutable.SynchronizedBuffer[QueryStatus]

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
      asyncTestWaiter.await(timeout(streamingTimout))
    }


    override def onQueryStarted(queryStarted: QueryStarted): Unit = {
      asyncTestWaiter {
        startStatus = QueryStatus(queryStarted.query)
      }
    }


    override def onQueryProgress(queryProgress: QueryProgress): Unit = {
      asyncTestWaiter {
        assert(startStatus != null, "onQueryProgress called before onQueryStarted")
        progressStatuses += QueryStatus(queryProgress.query)
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
