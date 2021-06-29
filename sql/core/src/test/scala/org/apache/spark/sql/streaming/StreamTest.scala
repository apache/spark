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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

import org.scalatest.{Assertions, BeforeAndAfterAll}
import org.scalatest.concurrent.{Eventually, Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Dataset, Encoder, QueryTest, Row}
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2, SparkDataStream}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous.{ContinuousExecution, EpochCoordinatorRef, IncrementAndGetEpoch}
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.{Clock, SystemClock, Utils}

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
trait StreamTest extends QueryTest with SharedSparkSession with TimeLimits with BeforeAndAfterAll {

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      StateStore.stop() // stop the state store maintenance thread and unload store providers
    }
  }

  protected val defaultTrigger = Trigger.ProcessingTime(0)

  /** How long to wait for an active stream to catch up when checking a result. */
  val streamingTimeout = 60.seconds

  /** A trait for actions that can be performed while testing a streaming DataFrame. */
  trait StreamAction

  /** A trait to mark actions that require the stream to be actively running. */
  trait StreamMustBeRunning

  /**
   * Adds the given data to the stream. Subsequent check answers will block until this data has
   * been processed.
   */
  object AddData {
    def apply[A](source: MemoryStreamBase[A], data: A*): AddDataMemory[A] =
      AddDataMemory(source, data)
  }

  /**
   * Adds data to multiple memory streams such that all the data will be made visible in the
   * same batch. This is applicable only to MicroBatchExecution, as this coordination cannot be
   * performed at the driver in ContinuousExecutions.
   */
  object MultiAddData {
    def apply[A]
      (source1: MemoryStream[A], data1: A*)(source2: MemoryStream[A], data2: A*): StreamAction = {
      apply((source1, data1), (source2, data2))
    }

    def apply[A](inputs: (MemoryStream[A], Seq[A])*): StreamAction = {
      val actions = inputs.map { case (source, data) => AddDataMemory(source, data) }
      StreamProgressLockedActions(actions, desc = actions.mkString("[ ", " | ", " ]"))
    }
  }

  /** A trait that can be extended when testing a source. */
  trait AddData extends StreamAction {
    /**
     * Called to adding the data to a source. It should find the source to add data to from
     * the active query, and then return the source object the data was added, as well as the
     * offset of added data.
     */
    def addData(query: Option[StreamExecution]): (SparkDataStream, OffsetV2)
  }

  /** A trait that can be extended when testing a source. */
  trait ExternalAction extends StreamAction {
    def runAction(): Unit
  }

  case class AddDataMemory[A](source: MemoryStreamBase[A], data: Seq[A]) extends AddData {
    override def toString: String = s"AddData to $source: ${data.mkString(",")}"

    override def addData(query: Option[StreamExecution]): (SparkDataStream, OffsetV2) = {
      (source, source.addData(data))
    }
  }

  private def createToExternalRowConverter[A : Encoder](): A => Row = {
    val encoder = encoderFor[A]
    val toInternalRow = encoder.createSerializer()
    val toExternalRow = RowEncoder(encoder.schema).resolveAndBind().createDeserializer()
    toExternalRow.compose(toInternalRow)
  }

  /**
   * Checks to make sure that the current data stored in the sink matches the `expectedAnswer`.
   * This operation automatically blocks until all added data has been processed.
   */
  object CheckAnswer {
    def apply[A : Encoder](data: A*): CheckAnswerRows = {
      val toExternalRow = createToExternalRowConverter[A]()
      CheckAnswerRows(
        data.map(toExternalRow),
        lastOnly = false,
        isSorted = false)
    }

    def apply(rows: Row*): CheckAnswerRows = CheckAnswerRows(rows, false, false)

    def apply(globalCheckFunction: Seq[Row] => Unit): CheckAnswerRowsByFunc =
      CheckAnswerRowsByFunc(globalCheckFunction, false)
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
      val toExternalRow = createToExternalRowConverter[A]()
      CheckAnswerRows(
        data.map(toExternalRow),
        lastOnly = true,
        isSorted = isSorted)
    }

    def apply(rows: Row*): CheckAnswerRows = CheckAnswerRows(rows, true, false)

    def apply(globalCheckFunction: Seq[Row] => Unit): CheckAnswerRowsByFunc =
      CheckAnswerRowsByFunc(globalCheckFunction, true)
  }

  case class CheckAnswerRows(expectedAnswer: Seq[Row], lastOnly: Boolean, isSorted: Boolean)
      extends StreamAction with StreamMustBeRunning {
    override def toString: String = s"$operatorName: ${expectedAnswer.mkString(",")}"
    private def operatorName = if (lastOnly) "CheckLastBatch" else "CheckAnswer"
  }

  case class CheckAnswerRowsContains(expectedAnswer: Seq[Row], lastOnly: Boolean = false)
    extends StreamAction with StreamMustBeRunning {
    override def toString: String = s"$operatorName: ${expectedAnswer.mkString(",")}"
    private def operatorName = if (lastOnly) "CheckLastBatchContains" else "CheckAnswerContains"
  }

  case class CheckAnswerRowsByFunc(
      globalCheckFunction: Seq[Row] => Unit,
      lastOnly: Boolean) extends StreamAction with StreamMustBeRunning {
    override def toString: String = if (lastOnly) "CheckLastBatchByFunc" else "CheckAnswerByFunc"
  }

  case class CheckNewAnswerRows(expectedAnswer: Seq[Row])
    extends StreamAction with StreamMustBeRunning {
    override def toString: String = s"CheckNewAnswer: ${expectedAnswer.mkString(",")}"
  }

  object CheckNewAnswer {
    def apply(): CheckNewAnswerRows = CheckNewAnswerRows(Seq.empty)

    def apply[A: Encoder](data: A, moreData: A*): CheckNewAnswerRows = {
      val toExternalRow = createToExternalRowConverter[A]()
      CheckNewAnswerRows((data +: moreData).map(toExternalRow))
    }

    def apply(rows: Row*): CheckNewAnswerRows = CheckNewAnswerRows(rows)
  }

  /** Stops the stream. It must currently be running. */
  case object StopStream extends StreamAction with StreamMustBeRunning

  /** Starts the stream, resuming if data has already been processed. It must not be running. */
  case class StartStream(
      trigger: Trigger = defaultTrigger,
      triggerClock: Clock = new SystemClock,
      additionalConfs: Map[String, String] = Map.empty,
      checkpointLocation: String = null)
    extends StreamAction

  /** Advance the trigger clock's time manually. */
  case class AdvanceManualClock(timeToAdd: Long) extends StreamAction

  /**
   * Signals that a failure is expected and should not kill the test.
   *
   * @param isFatalError if this is a fatal error. If so, the error should also be caught by
   *                     UncaughtExceptionHandler.
   * @param assertFailure a function to verify the error.
   */
  case class ExpectFailure[T <: Throwable : ClassTag](
      assertFailure: Throwable => Unit = _ => {},
      isFatalError: Boolean = false) extends StreamAction {
    val causeClass: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    override def toString(): String =
      s"ExpectFailure[${causeClass.getName}, isFatalError: $isFatalError]"
  }

  /**
   * Performs multiple actions while locking the stream from progressing.
   * This is applicable only to MicroBatchExecution, as progress of ContinuousExecution
   * cannot be controlled from the driver.
   */
  case class StreamProgressLockedActions(actions: Seq[StreamAction], desc: String = null)
    extends StreamAction {

    override def toString(): String = {
      if (desc != null) desc else super.toString
    }
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

  /** Execute arbitrary code */
  object Execute {
    def apply(name: String)(func: StreamExecution => Any): AssertOnQuery =
      AssertOnQuery(query => { func(query); true }, name)

    def apply(func: StreamExecution => Any): AssertOnQuery = apply("Execute")(func)
  }

  /** Call `StreamingQuery.processAllAvailable()` to wait. */
  object ProcessAllAvailable {
    def apply(): AssertOnQuery = AssertOnQuery { query =>
      query.processAllAvailable()
      true
    }
  }

  object AwaitEpoch {
    def apply(epoch: Long): AssertOnQuery =
      Execute {
        case s: ContinuousExecution => s.awaitEpoch(epoch)
        case _ => throw new IllegalStateException("microbatch cannot await epoch")
      }
  }

  object IncrementEpoch {
    def apply(): AssertOnQuery =
      Execute {
        case s: ContinuousExecution =>
          val newEpoch = EpochCoordinatorRef.get(s.currentEpochCoordinatorId, SparkEnv.get)
            .askSync[Long](IncrementAndGetEpoch)
          s.awaitEpoch(newEpoch - 1)
        case _ => throw new IllegalStateException("microbatch cannot increment epoch")
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
      outputMode: OutputMode = OutputMode.Append)(actions: StreamAction*): Unit = synchronized {
    import org.apache.spark.sql.streaming.util.StreamManualClock

    // `synchronized` is added to prevent the user from calling multiple `testStream`s concurrently
    // because this method assumes there is only one active query in its `StreamingQueryListener`
    // and it may not work correctly when multiple `testStream`s run concurrently.

    val stream = _stream.toDF()
    val sparkSession = stream.sparkSession  // use the session in DF, not the default session
    var pos = 0
    var currentStream: StreamExecution = null
    var lastStream: StreamExecution = null
    val awaiting = new mutable.HashMap[Int, OffsetV2]() // source index -> offset to wait for
    val sink = new MemorySink
    val resetConfValues = mutable.Map[String, Option[String]]()
    val defaultCheckpointLocation =
      Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath
    var manualClockExpectedTime = -1L

    @volatile
    var streamThreadDeathCause: Throwable = null
    // Set UncaughtExceptionHandler in `onQueryStarted` so that we can ensure catching fatal errors
    // during query initialization.
    val listener = new StreamingQueryListener {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {
        // Note: this assumes there is only one query active in the `testStream` method.
        Thread.currentThread.setUncaughtExceptionHandler(
          (_: Thread, e: Throwable) => streamThreadDeathCause = e)
      }

      override def onQueryProgress(event: QueryProgressEvent): Unit = {}
      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
    }
    sparkSession.streams.addListener(listener)

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
      if (currentStream != null && currentStream.queryExecutionThread.isAlive) "alive" else "dead"

    def threadStackTrace =
      if (currentStream != null && currentStream.queryExecutionThread.isAlive) {
        s"Thread stack trace: ${currentStream.queryExecutionThread.getStackTrace.mkString("\n")}"
      } else {
        ""
      }

    def testState = {
      val sinkDebugString = sink.toDebugString

      s"""
         |== Progress ==
         |$testActions
         |
         |== Stream ==
         |Output Mode: $outputMode
         |Stream state: $currentOffsets
         |Thread state: $threadState
         |$threadStackTrace
         |${if (streamThreadDeathCause != null) stackTraceToString(streamThreadDeathCause) else ""}
         |
         |== Sink ==
         |$sinkDebugString
         |
         |
         |== Plan ==
         |${if (currentStream != null) currentStream.lastExecution else ""}
         """.stripMargin
    }

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

    var lastFetchedMemorySinkLastBatchId: Long = -1

    def fetchStreamAnswer(
        currentStream: StreamExecution,
        lastOnly: Boolean = false,
        sinceLastFetchOnly: Boolean = false) = {
      verify(
        !(lastOnly && sinceLastFetchOnly), "both lastOnly and sinceLastFetchOnly cannot be true")
      verify(currentStream != null, "stream not running")

      // Block until all data added has been processed for all the source
      awaiting.foreach { case (sourceIndex, offset) =>
        failAfter(streamingTimeout) {
          currentStream.awaitOffset(sourceIndex, offset, streamingTimeout.toMillis)
          // Make sure all processing including no-data-batches have been executed
          if (!currentStream.triggerClock.isInstanceOf[StreamManualClock]) {
            currentStream.processAllAvailable()
          }
        }
      }

      val lastExecution = currentStream.lastExecution
      if (currentStream.isInstanceOf[MicroBatchExecution] && lastExecution != null) {
        // Verify if stateful operators have correct metadata and distribution
        // This can often catch hard to debug errors when developing stateful operators
        lastExecution.executedPlan.collect { case s: StatefulOperator => s }.foreach { s =>
          assert(s.stateInfo.map(_.numPartitions).contains(lastExecution.numStateStores))
          s.requiredChildDistribution.foreach { d =>
            withClue(s"$s specifies incorrect # partitions in requiredChildDistribution $d") {
              assert(d.requiredNumPartitions.isDefined)
              assert(d.requiredNumPartitions.get >= 1)
              if (d != AllTuples) {
                assert(d.requiredNumPartitions.get == s.stateInfo.get.numPartitions)
              }
            }
          }
        }
      }

      val rows = try {
        if (sinceLastFetchOnly) {
          if (sink.latestBatchId.getOrElse(-1L) < lastFetchedMemorySinkLastBatchId) {
            failTest("MemorySink was probably cleared since last fetch. Use CheckAnswer instead.")
          }
          sink.dataSinceBatch(lastFetchedMemorySinkLastBatchId)
        } else {
          if (lastOnly) sink.latestBatchData else sink.allData
        }
      } catch {
        case e: Exception =>
          failTest("Exception while getting data from sink", e)
      }
      lastFetchedMemorySinkLastBatchId = sink.latestBatchId.getOrElse(-1L)
      rows
    }

    def executeAction(action: StreamAction): Unit = {
      logInfo(s"Processing test stream action: $action")
      action match {
        case StartStream(trigger, triggerClock, additionalConfs, checkpointLocation) =>
          verify(currentStream == null || !currentStream.isActive, "stream already running")
          verify(triggerClock.isInstanceOf[SystemClock]
            || triggerClock.isInstanceOf[StreamManualClock],
            "Use either SystemClock or StreamManualClock to start the stream")
          if (triggerClock.isInstanceOf[StreamManualClock]) {
            manualClockExpectedTime = triggerClock.asInstanceOf[StreamManualClock].getTimeMillis()
          }
          val metadataRoot = Option(checkpointLocation).getOrElse(defaultCheckpointLocation)

          additionalConfs.foreach(pair => {
            val value =
              if (sparkSession.conf.contains(pair._1)) {
                Some(sparkSession.conf.get(pair._1))
              } else None
            resetConfValues(pair._1) = value
            sparkSession.conf.set(pair._1, pair._2)
          })

          lastStream = currentStream
          currentStream =
            sparkSession
              .streams
              .startQuery(
                None,
                Some(metadataRoot),
                stream,
                Map(),
                sink,
                outputMode,
                trigger = trigger,
                triggerClock = triggerClock)
              .asInstanceOf[StreamingQueryWrapper]
              .streamingQuery
          // Wait until the initialization finishes, because some tests need to use `logicalPlan`
          // after starting the query.
          try {
            currentStream.awaitInitialization(streamingTimeout.toMillis)
            currentStream match {
              case s: ContinuousExecution => eventually("IncrementalExecution was not created") {
                assert(s.lastExecution != null)
              }
              case _ =>
            }
          } catch {
            case _: StreamingQueryException =>
              // Ignore the exception. `StopStream` or `ExpectFailure` will catch it as well.
          }

        case AdvanceManualClock(timeToAdd) =>
          verify(currentStream != null,
                 "can not advance manual clock when a stream is not running")
          verify(currentStream.triggerClock.isInstanceOf[StreamManualClock],
                 s"can not advance clock of type ${currentStream.triggerClock.getClass}")
          val clock = currentStream.triggerClock.asInstanceOf[StreamManualClock]
          assert(manualClockExpectedTime >= 0)

          // Make sure we don't advance ManualClock too early. See SPARK-16002.
          eventually("StreamManualClock has not yet entered the waiting state") {
            assert(clock.isStreamWaitingAt(manualClockExpectedTime))
          }

          clock.advance(timeToAdd)
          manualClockExpectedTime += timeToAdd
          verify(clock.getTimeMillis() === manualClockExpectedTime,
            s"Unexpected clock time after updating: " +
              s"expecting $manualClockExpectedTime, current ${clock.getTimeMillis()}")

        case StopStream =>
          verify(currentStream != null, "can not stop a stream that is not running")
          try failAfter(streamingTimeout) {
            currentStream.stop()
            verify(!currentStream.queryExecutionThread.isAlive,
              s"microbatch thread not stopped")
            verify(!currentStream.isActive,
              "query.isActive() is false even after stopping")
            verify(currentStream.exception.isEmpty,
              s"query.exception() is not empty after clean stop: " +
                currentStream.exception.map(_.toString()).getOrElse(""))
          } catch {
            case _: InterruptedException =>
            case e: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
              failTest(
                "Timed out while stopping and waiting for microbatchthread to terminate.", e)
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
              assert(!currentStream.queryExecutionThread.isAlive)
            }
            verify(currentStream.exception === Some(thrownException),
              s"incorrect exception returned by query.exception()")

            val exception = currentStream.exception.get
            verify(exception.cause.getClass === ef.causeClass,
              "incorrect cause in exception returned by query.exception()\n" +
                s"\tExpected: ${ef.causeClass}\n\tReturned: ${exception.cause.getClass}")
            if (ef.isFatalError) {
              // This is a fatal error, `streamThreadDeathCause` should be set to this error in
              // UncaughtExceptionHandler.
              verify(streamThreadDeathCause != null &&
                streamThreadDeathCause.getClass === ef.causeClass,
                "UncaughtExceptionHandler didn't receive the correct error\n" +
                  s"\tExpected: ${ef.causeClass}\n\tReturned: $streamThreadDeathCause")
              streamThreadDeathCause = null
            }
            ef.assertFailure(exception.getCause)
          } catch {
            case _: InterruptedException =>
            case e: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
              failTest("Timed out while waiting for failure", e)
            case t: Throwable =>
              failTest("Error while checking stream failure", t)
          } finally {
            lastStream = currentStream
            currentStream = null
          }

        case a: AssertOnQuery =>
          verify(currentStream != null || lastStream != null,
            "cannot assert when no stream has been started")
          val streamToAssert = Option(currentStream).getOrElse(lastStream)
          try {
            verify(a.condition(streamToAssert), s"Assert on query failed: ${a.message}")
          } catch {
            case NonFatal(e) =>
              failTest(s"Assert on query failed: ${a.message}", e)
          }

        case a: Assert =>
          val streamToAssert = Option(currentStream).getOrElse(lastStream)
          verify({ a.run(); true }, s"Assert failed: ${a.message}")

        case a: AddData =>
          try {

            // If the query is running with manual clock, then wait for the stream execution
            // thread to start waiting for the clock to increment. This is needed so that we
            // are adding data when there is no trigger that is active. This would ensure that
            // the data gets deterministically added to the next batch triggered after the manual
            // clock is incremented in following AdvanceManualClock. This avoid race conditions
            // between the test thread and the stream execution thread in tests using manual
            // clock.
            if (currentStream != null &&
                currentStream.triggerClock.isInstanceOf[StreamManualClock]) {
              val clock = currentStream.triggerClock.asInstanceOf[StreamManualClock]
              eventually("Error while synchronizing with manual clock before adding data") {
                if (currentStream.isActive) {
                  assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
                }
              }
              if (!currentStream.isActive) {
                failTest("Query terminated while synchronizing with manual clock")
              }
            }
            // Add data
            val queryToUse = Option(currentStream).orElse(Option(lastStream))
            val (source, offset) = a.addData(queryToUse)

            def findSourceIndex(plan: LogicalPlan): Option[Int] = {
              plan
                .collect {
                  // v1 source
                  case r: StreamingExecutionRelation => r.source
                  // v2 source
                  case r: StreamingDataSourceV2Relation => r.stream
                  // We can add data to memory stream before starting it. Then the input plan has
                  // not been processed by the streaming engine and contains `StreamingRelationV2`.
                  case r: StreamingRelationV2 if r.sourceName == "memory" =>
                    r.table.asInstanceOf[MemoryStreamTable].stream
                }
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
              }.orElse {
                queryToUse.flatMap { q =>
                  findSourceIndex(q.lastExecution.logical)
                }
              }.getOrElse {
                throw new IllegalArgumentException(
                  "Could not find index of the source to which data was added")
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
          val sparkAnswer = fetchStreamAnswer(currentStream, lastOnly)
          QueryTest.sameRows(expectedAnswer, sparkAnswer, isSorted).foreach {
            error => failTest(error)
          }

        case CheckAnswerRowsContains(expectedAnswer, lastOnly) =>
          val sparkAnswer = currentStream match {
            case null => fetchStreamAnswer(lastStream, lastOnly)
            case s => fetchStreamAnswer(s, lastOnly)
          }
          QueryTest.includesRows(expectedAnswer, sparkAnswer).foreach {
            error => failTest(error)
          }

        case CheckAnswerRowsByFunc(globalCheckFunction, lastOnly) =>
          val sparkAnswer = currentStream match {
            case null => fetchStreamAnswer(lastStream, lastOnly)
            case s => fetchStreamAnswer(s, lastOnly)
          }
          try {
            globalCheckFunction(sparkAnswer)
          } catch {
            case e: Throwable => failTest(e.toString)
          }

        case CheckNewAnswerRows(expectedAnswer) =>
          val sparkAnswer = fetchStreamAnswer(currentStream, sinceLastFetchOnly = true)
          QueryTest.sameRows(expectedAnswer, sparkAnswer).foreach {
            error => failTest(error)
          }
      }
    }

    try {
      startedTest.foreach {
        case StreamProgressLockedActions(actns, _) =>
          // Perform actions while holding the stream from progressing
          assert(currentStream != null,
            s"Cannot perform stream-progress-locked actions $actns when query is not active")
          assert(currentStream.isInstanceOf[MicroBatchExecution],
            s"Cannot perform stream-progress-locked actions on non-microbatch queries")
          currentStream.asInstanceOf[MicroBatchExecution].withProgressLocked {
            actns.foreach(executeAction)
          }
          pos += 1

        case action: StreamAction =>
          executeAction(action)
          pos += 1
      }
      if (streamThreadDeathCause != null) {
        failTest("Stream Thread Died", streamThreadDeathCause)
      }
    } catch {
      case _: InterruptedException if streamThreadDeathCause != null =>
        failTest("Stream Thread Died", streamThreadDeathCause)
      case e: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
        failTest("Timed out waiting for stream", e)
    } finally {
      if (currentStream != null && currentStream.queryExecutionThread.isAlive) {
        currentStream.stop()
      }

      // Rollback prev configuration values
      resetConfValues.foreach {
        case (key, Some(value)) => sparkSession.conf.set(key, value)
        case (key, None) => sparkSession.conf.unset(key)
      }
      sparkSession.streams.removeListener(listener)
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
    testStream(ds)(actions.toSeq: _*)
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
