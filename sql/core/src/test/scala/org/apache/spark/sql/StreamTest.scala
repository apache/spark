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

import java.lang.Thread.UncaughtExceptionHandler

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

import org.scalatest.Assertions
import org.scalatest.concurrent.{Eventually, Timeouts}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.streaming._

/**
 * A framework for implementing tests for streaming queries and sources.
 *
 * A test consists of a set of steps (expressed as a `StreamAction`) that are executed in order,
 * blocking as necessary to let the stream catch up.  For example, the following adds some data to
 * a stream, blocking until it can verify that the correct values are eventually produced.
 *
 * {{{
 *  val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped)(
      AddData(inputData, 1, 2, 3),
      CheckAnswer(2, 3, 4))
 * }}}
 *
 * Note that while we do sleep to allow the other thread to progress without spinning,
 * `StreamAction` checks should not depend on the amount of time spent sleeping.  Instead they
 * should check the actual progress of the stream before verifying the required test condition.
 *
 * Currently it is assumed that all streaming queries will eventually complete in 10 seconds to
 * avoid hanging forever in the case of failures. However, individual suites can change this
 * by overriding `streamingTimeout`.
 */
trait StreamTest extends QueryTest with Timeouts {

  implicit class RichSource(s: Source) {
    def toDF(): DataFrame = new DataFrame(sqlContext, StreamingRelation(s))

    def toDS[A: Encoder](): Dataset[A] = new Dataset(sqlContext, StreamingRelation(s))
  }

  /** How long to wait for an active stream to catch up when checking a result. */
  val streamingTimeout = 10.seconds

  /** A trait for actions that can be performed while testing a streaming DataFrame. */
  trait StreamAction

  /** A trait to mark actions that require the stream to be actively running. */
  trait StreamMustBeRunning

  /**
   * Adds the given data to the stream.  Subsuquent check answers will block until this data has
   * been processed.
   */
  object AddData {
    def apply[A](source: MemoryStream[A], data: A*): AddDataMemory[A] =
      AddDataMemory(source, data)
  }

  /** A trait that can be extended when testing other sources. */
  trait AddData extends StreamAction {
    def source: Source

    /**
     * Called to trigger adding the data.  Should return the offset that will denote when this
     * new data has been processed.
     */
    def addData(): Offset
  }

  case class AddDataMemory[A](source: MemoryStream[A], data: Seq[A]) extends AddData {
    override def toString: String = s"AddData to $source: ${data.mkString(",")}"

    override def addData(): Offset = {
      source.addData(data)
    }
  }

  /**
   * Checks to make sure that the current data stored in the sink matches the `expectedAnswer`.
   * This operation automatically blocks untill all added data has been processed.
   */
  object CheckAnswer {
    def apply[A : Encoder](data: A*): CheckAnswerRows = {
      val encoder = encoderFor[A]
      val toExternalRow = RowEncoder(encoder.schema)
      CheckAnswerRows(data.map(d => toExternalRow.fromRow(encoder.toRow(d))))
    }

    def apply(rows: Row*): CheckAnswerRows = CheckAnswerRows(rows)
  }

  case class CheckAnswerRows(expectedAnswer: Seq[Row])
      extends StreamAction with StreamMustBeRunning {
    override def toString: String = s"CheckAnswer: ${expectedAnswer.mkString(",")}"
  }

  case class DropBatches(num: Int) extends StreamAction

  /** Stops the stream.  It must currently be running. */
  case object StopStream extends StreamAction with StreamMustBeRunning

  /** Starts the stream, resuming if data has already been processed.  It must not be running. */
  case object StartStream extends StreamAction

  /** Signals that a failure is expected and should not kill the test. */
  case class ExpectFailure[T <: Throwable : ClassTag]() extends StreamAction {
    val causeClass: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    override def toString(): String = s"ExpectFailure[${causeClass.getCanonicalName}]"
  }

  /** Assert that a body is true */
  class Assert(condition: => Boolean, val message: String = "") extends StreamAction {
    def run(): Unit = { Assertions.assert(condition) }
    override def toString: String = s"Assert(<condition>, $message)"
  }

  object Assert {
    def apply(condition: => Boolean, message: String = ""): Assert = new Assert(condition, message)
    def apply(message: String)(body: => Unit): Assert = new Assert( { body; true }, message)
    def apply(body: => Unit): Assert = new Assert( { body; true }, "")
  }

  /** Assert that a condition on the active query is true */
  class AssertOnQuery(val condition: StreamExecution => Boolean, val message: String)
    extends StreamAction {
    override def toString: String = s"AssertOnQuery(<condition>, $message)"
  }

  object AssertOnQuery {
    def apply(condition: StreamExecution => Boolean, message: String = ""): AssertOnQuery = {
      new AssertOnQuery(condition, message)
    }

    def apply(message: String)(condition: StreamExecution => Boolean): AssertOnQuery = {
      new AssertOnQuery(condition, message)
    }
  }

  /** A helper for running actions on a Streaming Dataset. See `checkAnswer(DataFrame)`. */
  def testStream(stream: Dataset[_])(actions: StreamAction*): Unit =
    testStream(stream.toDF())(actions: _*)

  /**
   * Executes the specified actions on the the given streaming DataFrame and provides helpful
   * error messages in the case of failures or incorrect answers.
   *
   * Note that if the stream is not explictly started before an action that requires it to be
   * running then it will be automatically started before performing any other actions.
   */
  def testStream(stream: DataFrame)(actions: StreamAction*): Unit = {
    var pos = 0
    var currentPlan: LogicalPlan = stream.logicalPlan
    var currentStream: StreamExecution = null
    var lastStream: StreamExecution = null
    val awaiting = new mutable.HashMap[Source, Offset]()
    val sink = new MemorySink(stream.schema)

    @volatile
    var streamDeathCause: Throwable = null

    // If the test doesn't manually start the stream, we do it automatically at the beginning.
    val startedManually =
      actions.takeWhile(!_.isInstanceOf[StreamMustBeRunning]).contains(StartStream)
    val startedTest = if (startedManually) actions else StartStream +: actions

    def testActions = actions.zipWithIndex.map {
      case (a, i) =>
        if ((pos == i && startedManually) || (pos == (i + 1) && !startedManually)) {
          "=> " + a.toString
        } else {
          "   " + a.toString
        }
    }.mkString("\n")

    def currentOffsets =
      if (currentStream != null) currentStream.streamProgress.toString else "not started"

    def threadState =
      if (currentStream != null && currentStream.microBatchThread.isAlive) "alive" else "dead"

    def testState =
      s"""
         |== Progress ==
         |$testActions
         |
         |== Stream ==
         |Stream state: $currentOffsets
         |Thread state: $threadState
         |${if (streamDeathCause != null) stackTraceToString(streamDeathCause) else ""}
         |
         |== Sink ==
         |${sink.toDebugString}
         |
         |== Plan ==
         |${if (currentStream != null) currentStream.lastExecution else ""}
         """.stripMargin

    def verify(condition: => Boolean, message: String): Unit = {
      try {
        Assertions.assert(condition)
      } catch {
        case NonFatal(e) =>
          failTest(message, e)
      }
    }

    def eventually[T](message: String)(func: => T): T = {
      try {
        Eventually.eventually(Timeout(streamingTimeout)) {
          func
        }
      } catch {
        case NonFatal(e) =>
          failTest(message, e)
      }
    }

    def failTest(message: String, cause: Throwable = null) = {

      // Recursively pretty print a exception with truncated stacktrace and internal cause
      def exceptionToString(e: Throwable, prefix: String = ""): String = {
        val base = s"$prefix${e.getMessage}" +
          e.getStackTrace.take(10).mkString(s"\n$prefix", s"\n$prefix\t", "\n")
        if (e.getCause != null) {
          base + s"\n$prefix\tCaused by: " + exceptionToString(e.getCause, s"$prefix\t")
        } else {
          base
        }
      }
      val c = Option(cause).map(exceptionToString(_))
      val m = if (message != null && message.size > 0) Some(message) else None
      fail(
        s"""
           |${(m ++ c).mkString(": ")}
           |$testState
         """.stripMargin)
    }

    val testThread = Thread.currentThread()

    try {
      startedTest.foreach { action =>
        action match {
          case StartStream =>
            verify(currentStream == null, "stream already running")
            lastStream = currentStream
            currentStream =
              sqlContext
                .streams
                .startQuery(StreamExecution.nextName, stream, sink)
                .asInstanceOf[StreamExecution]
            currentStream.microBatchThread.setUncaughtExceptionHandler(
              new UncaughtExceptionHandler {
                override def uncaughtException(t: Thread, e: Throwable): Unit = {
                  streamDeathCause = e
                  testThread.interrupt()
                }
              })

          case StopStream =>
            verify(currentStream != null, "can not stop a stream that is not running")
            try failAfter(streamingTimeout) {
              currentStream.stop()
              verify(!currentStream.microBatchThread.isAlive,
                s"microbatch thread not stopped")
              verify(!currentStream.isActive,
                "query.isActive() is false even after stopping")
              verify(currentStream.exception.isEmpty,
                s"query.exception() is not empty after clean stop: " +
                  currentStream.exception.map(_.toString()).getOrElse(""))
            } catch {
              case _: InterruptedException =>
              case _: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
                failTest("Timed out while stopping and waiting for microbatchthread to terminate.")
              case t: Throwable =>
                failTest("Error while stopping stream", t)
            } finally {
              lastStream = currentStream
              currentStream = null
            }

          case DropBatches(num) =>
            verify(currentStream == null, "dropping batches while running leads to corruption")
            sink.dropBatches(num)

          case ef: ExpectFailure[_] =>
            verify(currentStream != null, "can not expect failure when stream is not running")
            try failAfter(streamingTimeout) {
              val thrownException = intercept[ContinuousQueryException] {
                currentStream.awaitTermination()
              }
              eventually("microbatch thread not stopped after termination with failure") {
                assert(!currentStream.microBatchThread.isAlive)
              }
              verify(thrownException.query.eq(currentStream),
                s"incorrect query reference in exception")
              verify(currentStream.exception === Some(thrownException),
                s"incorrect exception returned by query.exception()")

              val exception = currentStream.exception.get
              verify(exception.cause.getClass === ef.causeClass,
                "incorrect cause in exception returned by query.exception()\n" +
                  s"\tExpected: ${ef.causeClass}\n\tReturned: ${exception.cause.getClass}")
            } catch {
              case _: InterruptedException =>
              case _: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
                failTest("Timed out while waiting for failure")
              case t: Throwable =>
                failTest("Error while checking stream failure", t)
            } finally {
              lastStream = currentStream
              currentStream = null
              streamDeathCause = null
            }

          case a: AssertOnQuery =>
            verify(currentStream != null || lastStream != null,
              "cannot assert when not stream has been started")
            val streamToAssert = Option(currentStream).getOrElse(lastStream)
            verify(a.condition(streamToAssert), s"Assert on query failed: ${a.message}")

          case a: Assert =>
            val streamToAssert = Option(currentStream).getOrElse(lastStream)
            verify({ a.run(); true }, s"Assert failed: ${a.message}")

          case a: AddData =>
            awaiting.put(a.source, a.addData())

          case CheckAnswerRows(expectedAnswer) =>
            verify(currentStream != null, "stream not running")

            // Block until all data added has been processed
            awaiting.foreach { case (source, offset) =>
              failAfter(streamingTimeout) {
                currentStream.awaitOffset(source, offset)
              }
            }

            val allData = try sink.allData catch {
              case e: Exception =>
                failTest("Exception while getting data from sink", e)
            }

            QueryTest.sameRows(expectedAnswer, allData).foreach {
              error => failTest(error)
            }
        }
        pos += 1
      }
    } catch {
      case _: InterruptedException if streamDeathCause != null =>
        failTest("Stream Thread Died")
      case _: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
        failTest("Timed out waiting for stream")
    } finally {
      if (currentStream != null && currentStream.microBatchThread.isAlive) {
        currentStream.stop()
      }
    }
  }

  /**
   * Creates a stress test that randomly starts/stops/adds data/checks the result.
   *
   * @param ds a dataframe that executes + 1 on a stream of integers, returning the result.
   * @param addData and add data action that adds the given numbers to the stream, encoding them
   *                as needed
   */
  def runStressTest(
      ds: Dataset[Int],
      addData: Seq[Int] => StreamAction,
      iterations: Int = 100): Unit = {
    implicit val intEncoder = ExpressionEncoder[Int]()
    var dataPos = 0
    var running = true
    val actions = new ArrayBuffer[StreamAction]()

    def addCheck() = { actions += CheckAnswer(1 to dataPos: _*) }

    def addRandomData() = {
      val numItems = Random.nextInt(10)
      val data = dataPos until (dataPos + numItems)
      dataPos += numItems
      actions += addData(data)
    }

    (1 to iterations).foreach { i =>
      val rand = Random.nextDouble()
      if(!running) {
        rand match {
          case r if r < 0.7 => // AddData
            addRandomData()

          case _ => // StartStream
            actions += StartStream
            running = true
        }
      } else {
        rand match {
          case r if r < 0.1 =>
            addCheck()

          case r if r < 0.7 => // AddData
            addRandomData()

          case _ => // StopStream
            addCheck()
            actions += StopStream
            running = false
        }
      }
    }
    if(!running) { actions += StartStream }
    addCheck()
    testStream(ds)(actions: _*)
  }


  object AwaitTerminationTester {

    trait ExpectedBehavior

    /** Expect awaitTermination to not be blocked */
    case object ExpectNotBlocked extends ExpectedBehavior

    /** Expect awaitTermination to get blocked */
    case object ExpectBlocked extends ExpectedBehavior

    /** Expect awaitTermination to throw an exception */
    case class ExpectException[E <: Exception]()(implicit val t: ClassTag[E])
      extends ExpectedBehavior

    private val DEFAULT_TEST_TIMEOUT = 1 second

    def test(
        expectedBehavior: ExpectedBehavior,
        awaitTermFunc: () => Unit,
        testTimeout: Span = DEFAULT_TEST_TIMEOUT
      ): Unit = {

      expectedBehavior match {
        case ExpectNotBlocked =>
          withClue("Got blocked when expected non-blocking.") {
            failAfter(testTimeout) {
              awaitTermFunc()
            }
          }

        case ExpectBlocked =>
          withClue("Was not blocked when expected.") {
            intercept[TestFailedDueToTimeoutException] {
              failAfter(testTimeout) {
                awaitTermFunc()
              }
            }
          }

        case e: ExpectException[_] =>
          val thrownException =
            withClue(s"Did not throw ${e.t.runtimeClass.getSimpleName} when expected.") {
              intercept[ContinuousQueryException] {
                failAfter(testTimeout) {
                  awaitTermFunc()
                }
              }
            }
          assert(thrownException.cause.getClass === e.t.runtimeClass,
            "exception of incorrect type was throw")
      }
    }
  }
}
