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

package org.apache.spark.sql.streaming

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

import org.apache.spark.sql.{Dataset, Encoder, QueryTest, Row}
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.{Clock, ManualClock, SystemClock, Utils}

/**
 * A framework for implementing tests for streaming queries and sources.
 *
 * A test consists of a set of steps (expressed as a `StreamAction`) that are executed in order,
 * blocking as necessary to let the stream catch up.  For example, the following adds some data to
 * a stream, blocking until it can verify that the correct values are eventually produced.
 *
 * {{{
 *  val inputData = MemoryStream[Int]
 *  val mapped = inputData.toDS().map(_ + 1)
 *
 *  testStream(mapped)(
 *    AddData(inputData, 1, 2, 3),
 *    CheckAnswer(2, 3, 4))
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
trait StreamTest extends QueryTest with SharedSQLContext with Timeouts {

  /** How long to wait for an active stream to catch up when checking a result. */
  val streamingTimeout = 10.seconds

  /** A trait for actions that can be performed while testing a streaming DataFrame. */
  trait StreamAction

  /** A trait to mark actions that require the stream to be actively running. */
  trait StreamMustBeRunning

  /**
   * Adds the given data to the stream. Subsequent check answers will block until this data has
   * been processed.
   */
  object AddData {
    def apply[A](source: MemoryStream[A], data: A*): AddDataMemory[A] =
      AddDataMemory(source, data)
  }

  /** A trait that can be extended when testing a source. */
  trait AddData extends StreamAction {
    /**
     * Called to adding the data to a source. It should find the source to add data to from
     * the active query, and then return the source object the data was added, as well as the
     * offset of added data.
     */
    def addData(query: Option[StreamExecution]): (Source, Offset)
  }

  /** A trait that can be extended when testing a source. */
  trait ExternalAction extends StreamAction {
    def runAction(): Unit
  }

  case class AddDataMemory[A](source: MemoryStream[A], data: Seq[A]) extends AddData {
    override def toString: String = s"AddData to $source: ${data.mkString(",")}"

    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      (source, source.addData(data))
    }
  }

  /**
   * Checks to make sure that the current data stored in the sink matches the `expectedAnswer`.
   * This operation automatically blocks until all added data has been processed.
   */
  object CheckAnswer {
    def apply[A : Encoder](data: A*): CheckAnswerRows = {
      val encoder = encoderFor[A]
      val toExternalRow = RowEncoder(encoder.schema).resolveAndBind()
      CheckAnswerRows(
        data.map(d => toExternalRow.fromRow(encoder.toRow(d))),
        lastOnly = false,
        isSorted = false)
    }

    def apply(rows: Row*): CheckAnswerRows = CheckAnswerRows(rows, false, false)
  }

  /**
   * Checks to make sure that the current data stored in the sink matches the `expectedAnswer`.
   * This operation automatically blocks until all added data has been processed.
   */
  object CheckLastBatch {
    def apply[A : Encoder](data: A*): CheckAnswerRows = {
      apply(isSorted = false, data: _*)
    }

    def apply[A: Encoder](isSorted: Boolean, data: A*): CheckAnswerRows = {
      val encoder = encoderFor[A]
      val toExternalRow = RowEncoder(encoder.schema).resolveAndBind()
      CheckAnswerRows(
        data.map(d => toExternalRow.fromRow(encoder.toRow(d))),
        lastOnly = true,
        isSorted = isSorted)
    }

    def apply(rows: Row*): CheckAnswerRows = CheckAnswerRows(rows, true, false)
  }

  case class CheckAnswerRows(expectedAnswer: Seq[Row], lastOnly: Boolean, isSorted: Boolean)
      extends StreamAction with StreamMustBeRunning {
    override def toString: String = s"$operatorName: ${expectedAnswer.mkString(",")}"
    private def operatorName = if (lastOnly) "CheckLastBatch" else "CheckAnswer"
  }

  /** Stops the stream. It must currently be running. */
  case object StopStream extends StreamAction with StreamMustBeRunning

  /** Starts the stream, resuming if data has already been processed. It must not be running. */
  case class StartStream(
      trigger: Trigger = ProcessingTime(0),
      triggerClock: Clock = new SystemClock)
    extends StreamAction

  /** Advance the trigger clock's time manually. */
  case class AdvanceManualClock(timeToAdd: Long) extends StreamAction

  /** Signals that a failure is expected and should not kill the test. */
  case class ExpectFailure[T <: Throwable : ClassTag]() extends StreamAction {
    val causeClass: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    override def toString(): String = s"ExpectFailure[${causeClass.getName}]"
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

  /**
   * Executes the specified actions on the given streaming DataFrame and provides helpful
   * error messages in the case of failures or incorrect answers.
   *
   * Note that if the stream is not explicitly started before an action that requires it to be
   * running then it will be automatically started before performing any other actions.
   */
  def testStream(
      _stream: Dataset[_],
      outputMode: OutputMode = OutputMode.Append)(actions: StreamAction*): Unit = {

    val stream = _stream.toDF()
    var pos = 0
    var currentPlan: LogicalPlan = stream.logicalPlan
    var currentStream: StreamExecution = null
    var lastStream: StreamExecution = null
    val awaiting = new mutable.HashMap[Int, Offset]() // source index -> offset to wait for
    val sink = new MemorySink(stream.schema, outputMode)

    @volatile
    var streamDeathCause: Throwable = null

    // If the test doesn't manually start the stream, we do it automatically at the beginning.
    val startedManually =
      actions.takeWhile(!_.isInstanceOf[StreamMustBeRunning]).exists(_.isInstanceOf[StartStream])
    val startedTest = if (startedManually) actions else StartStream() +: actions

    def testActions = actions.zipWithIndex.map {
      case (a, i) =>
        if ((pos == i && startedManually) || (pos == (i + 1) && !startedManually)) {
          "=> " + a.toString
        } else {
          "   " + a.toString
        }
    }.mkString("\n")

    def currentOffsets =
      if (currentStream != null) currentStream.committedOffsets.toString else "not started"

    def threadState =
      if (currentStream != null && currentStream.microBatchThread.isAlive) "alive" else "dead"

    def testState =
      s"""
         |== Progress ==
         |$testActions
         |
         |== Stream ==
         |Output Mode: $outputMode
         |Stream state: $currentOffsets
         |Thread state: $threadState
         |${if (streamDeathCause != null) stackTraceToString(streamDeathCause) else ""}
         |
         |== Sink ==
         |${sink.toDebugString}
         |
         |
         |== Plan ==
         |${if (currentStream != null) currentStream.lastExecution else ""}
         """.stripMargin

    def verify(condition: => Boolean, message: String): Unit = {
      if (!condition) {
        failTest(message)
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
    val metadataRoot = Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath

    try {
      startedTest.foreach { action =>
        action match {
          case StartStream(trigger, triggerClock) =>
            verify(currentStream == null, "stream already running")
            lastStream = currentStream
            currentStream =
              spark
                .streams
                .startQuery(
                  None,
                  Some(metadataRoot),
                  stream,
                  sink,
                  outputMode,
                  trigger = trigger,
                  triggerClock = triggerClock)
                .asInstanceOf[StreamExecution]
            currentStream.microBatchThread.setUncaughtExceptionHandler(
              new UncaughtExceptionHandler {
                override def uncaughtException(t: Thread, e: Throwable): Unit = {
                  streamDeathCause = e
                }
              })

          case AdvanceManualClock(timeToAdd) =>
            verify(currentStream != null,
                   "can not advance manual clock when a stream is not running")
            verify(currentStream.triggerClock.isInstanceOf[ManualClock],
                   s"can not advance clock of type ${currentStream.triggerClock.getClass}")
            val clock = currentStream.triggerClock.asInstanceOf[ManualClock]
            // Make sure we don't advance ManualClock too early. See SPARK-16002.
            eventually("ManualClock has not yet entered the waiting state") {
              assert(clock.isWaiting)
            }
            currentStream.triggerClock.asInstanceOf[ManualClock].advance(timeToAdd)

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

          case ef: ExpectFailure[_] =>
            verify(currentStream != null, "can not expect failure when stream is not running")
            try failAfter(streamingTimeout) {
              val thrownException = intercept[StreamingQueryException] {
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
              "cannot assert, when no stream has been started")
            val streamToAssert = Option(currentStream).getOrElse(lastStream)
            verify(a.condition(streamToAssert), s"Assert on query failed: ${a.message}")

          case a: Assert =>
            val streamToAssert = Option(currentStream).getOrElse(lastStream)
            verify({ a.run(); true }, s"Assert failed: ${a.message}")

          case a: AddData =>
            try {
              // Add data and get the source where it was added, and the expected offset of the
              // added data.
              val queryToUse = Option(currentStream).orElse(Option(lastStream))
              val (source, offset) = a.addData(queryToUse)

              def findSourceIndex(plan: LogicalPlan): Option[Int] = {
                plan
                  .collect { case StreamingExecutionRelation(s, _) => s }
                  .zipWithIndex
                  .find(_._1 == source)
                  .map(_._2)
              }

              // Try to find the index of the source to which data was added. Either get the index
              // from the current active query or the original input logical plan.
              val sourceIndex =
                queryToUse.flatMap { query =>
                  findSourceIndex(query.logicalPlan)
                }.orElse {
                  findSourceIndex(stream.logicalPlan)
                }.getOrElse {
                  throw new IllegalArgumentException(
                    "Could find index of the source to which data was added")
                }

              // Store the expected offset of added data to wait for it later
              awaiting.put(sourceIndex, offset)
            } catch {
              case NonFatal(e) =>
                failTest("Error adding data", e)
            }

          case e: ExternalAction =>
            e.runAction()

          case CheckAnswerRows(expectedAnswer, lastOnly, isSorted) =>
            verify(currentStream != null, "stream not running")
            // Get the map of source index to the current source objects
            val indexToSource = currentStream
              .logicalPlan
              .collect { case StreamingExecutionRelation(s, _) => s }
              .zipWithIndex
              .map(_.swap)
              .toMap

            // Block until all data added has been processed for all the source
            awaiting.foreach { case (sourceIndex, offset) =>
              failAfter(streamingTimeout) {
                currentStream.awaitOffset(indexToSource(sourceIndex), offset)
              }
            }

            val sparkAnswer = try if (lastOnly) sink.latestBatchData else sink.allData catch {
              case e: Exception =>
                failTest("Exception while getting data from sink", e)
            }

            QueryTest.sameRows(expectedAnswer, sparkAnswer, isSorted).foreach {
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
   * @param ds a dataframe that executes + 1 on a stream of integers, returning the result
   * @param addData an add data action that adds the given numbers to the stream, encoding them
   *                as needed
   * @param iterations the iteration number
   */
  def runStressTest(
    ds: Dataset[Int],
    addData: Seq[Int] => StreamAction,
    iterations: Int = 100): Unit = {
    runStressTest(ds, Seq.empty, (data, running) => addData(data), iterations)
  }

  /**
   * Creates a stress test that randomly starts/stops/adds data/checks the result.
   *
   * @param ds a dataframe that executes + 1 on a stream of integers, returning the result
   * @param prepareActions actions need to run before starting the stress test.
   * @param addData an add data action that adds the given numbers to the stream, encoding them
   *                as needed
   * @param iterations the iteration number
   */
  def runStressTest(
      ds: Dataset[Int],
      prepareActions: Seq[StreamAction],
      addData: (Seq[Int], Boolean) => StreamAction,
      iterations: Int): Unit = {
    implicit val intEncoder = ExpressionEncoder[Int]()
    var dataPos = 0
    var running = true
    val actions = new ArrayBuffer[StreamAction]()
    actions ++= prepareActions

    def addCheck() = { actions += CheckAnswer(1 to dataPos: _*) }

    def addRandomData() = {
      val numItems = Random.nextInt(10)
      val data = dataPos until (dataPos + numItems)
      dataPos += numItems
      actions += addData(data, running)
    }

    (1 to iterations).foreach { i =>
      val rand = Random.nextDouble()
      if(!running) {
        rand match {
          case r if r < 0.7 => // AddData
            addRandomData()

          case _ => // StartStream
            actions += StartStream()
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
    if(!running) { actions += StartStream() }
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

    private val DEFAULT_TEST_TIMEOUT = 1.second

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
              intercept[StreamingQueryException] {
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
