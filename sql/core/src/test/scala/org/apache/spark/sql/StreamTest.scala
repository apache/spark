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

import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.encoders.{RowEncoder, encoderFor}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.streaming._

/**
 * A framework for implementing tests for streaming queries and sources.
 *
 * A test consists of a set of steps (i.e. a [[StreamAction]]) that are executed in order, blocking
 * as necessary to let the stream catch up.  For example, the following adds some data to a stream
 * and verifies that
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
 * [[StreamAction]] checks should not depend on the amount of time spent sleeping.  Instead they
 * should check the actual progress of the stream before verifying the required test condition.
 */
trait StreamTest extends QueryTest with Timeouts {

  implicit class RichSource(s: Source) {
    def toDF(): DataFrame = new DataFrame(sqlContext, StreamingRelation(s))
  }

  /** How long to wait for an active stream to catch up when checking a result. */
  val streamingTimout = 10.seconds

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
  case object StopStream extends StreamAction

  /** Starts the stream, resuming if data has already been processed.  It must not be running. */
  case object StartStream extends StreamAction

  /** Signals that a failure is expected and should not kill the test. */
  case object ExpectFailure extends StreamAction

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
    val awaiting = new mutable.HashMap[Source, Offset]()
    val sink = new MemorySink(stream.schema)

    @volatile
    var streamDeathCause: Throwable = null

    // If the test doesn't manually start the stream, we do it automatically at the beginning.
    val startedManually =
      actions.takeWhile(_.isInstanceOf[StreamMustBeRunning]).contains(StartStream)
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
      if (currentStream != null) currentStream.currentOffsets.toString else "not started"

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
         |$sink
         |
         |== Plan ==
         |${if (currentStream != null) currentStream.lastExecution else ""}
         """

    def checkState(check: Boolean, error: String) = if (!check) {
      fail(
        s"""
           |Invalid State: $error
           |$testState
         """.stripMargin)
    }

    val testThread = Thread.currentThread()

    try {
      startedTest.foreach { action =>
        action match {
          case StartStream =>
            checkState(currentStream == null, "stream already running")

            currentStream = new StreamExecution(sqlContext, stream.logicalPlan, sink)
            currentStream.microBatchThread.setUncaughtExceptionHandler(
              new UncaughtExceptionHandler {
                override def uncaughtException(t: Thread, e: Throwable): Unit = {
                  streamDeathCause = e
                  testThread.interrupt()
                }
              })

          case StopStream =>
            checkState(currentStream != null, "can not stop a stream that is not running")
            currentStream.stop()
            currentStream = null

          case DropBatches(num) =>
            checkState(currentStream == null, "dropping batches while running leads to corruption")
            sink.dropBatches(num)

          case ExpectFailure =>
            try failAfter(streamingTimout) {
              while (streamDeathCause == null) {
                Thread.sleep(100)
              }
            } catch {
              case _: InterruptedException =>
              case _: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
                fail(
                  s"""
                     |Timed out while waiting for failure.
                     |$testState
                   """.stripMargin)
            }

            currentStream = null
            streamDeathCause = null

          case a: AddData =>
            awaiting.put(a.source, a.addData())

          case CheckAnswerRows(expectedAnswer) =>
            checkState(currentStream != null, "stream not running")

            // Block until all data added has been processed
            awaiting.foreach { case (source, offset) =>
              failAfter(streamingTimout) {
                currentStream.awaitOffset(source, offset)
              }
            }
            QueryTest.sameRows(expectedAnswer, sink.allData).foreach {
              error => fail(
                s"""
                   |$error
                   |$testState
                 """.stripMargin)
            }
        }
        pos += 1
      }
    } catch {
      case _: InterruptedException if streamDeathCause != null =>
        fail(
          s"""
             |Stream Thread Died
             |$testState
                      """.stripMargin)
      case _: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
        fail(
          s"""
             |Timed out waiting for stream
             |$testState
                   """.stripMargin)
    } finally {
      if (currentStream != null && currentStream.microBatchThread.isAlive) {
        currentStream.stop()
      }
    }
  }
}