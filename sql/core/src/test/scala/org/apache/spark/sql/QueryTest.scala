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
import java.util.{Locale, TimeZone}

import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.encoders.{RowEncoder, encoderFor}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.Queryable

import scala.collection.mutable

abstract class QueryTest extends PlanTest with Timeouts {

  protected def sqlContext: SQLContext

  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  Locale.setDefault(Locale.US)

  /**
   * Runs the plan and makes sure the answer contains all of the keywords, or the
   * none of keywords are listed in the answer
   * @param df the [[DataFrame]] to be executed
   * @param exists true for make sure the keywords are listed in the output, otherwise
   *               to make sure none of the keyword are not listed in the output
   * @param keywords keyword in string array
   */
  def checkExistence(df: DataFrame, exists: Boolean, keywords: String*) {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      if (exists) {
        assert(outputs.contains(key), s"Failed for $df ($key doesn't exist in result)")
      } else {
        assert(!outputs.contains(key), s"Failed for $df ($key existed in the result)")
      }
    }
  }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given
   * expected answer.
   *  - Special handling is done based on whether the query plan should be expected to return
   *    the results in sorted order.
   *  - This function also checks to make sure that the schema for serializing the expected answer
   *    matches that produced by the dataset (i.e. does manual construction of object match
   *    the constructed encoder for cases like joins, etc).  Note that this means that it will fail
   *    for cases where reordering is done on fields.  For such tests, user `checkDecoding` instead
   *    which performs a subset of the checks done by this function.
   */
  protected def checkAnswer[T](
      ds: Dataset[T],
      expectedAnswer: T*): Unit = {
    checkAnswer(
      ds.toDF(),
      sqlContext.createDataset(expectedAnswer)(ds.unresolvedTEncoder).toDF().collect().toSeq)

    checkDecoding(ds, expectedAnswer: _*)
  }

  protected def checkDecoding[T](
      ds: => Dataset[T],
      expectedAnswer: T*): Unit = {
    val decoded = try ds.collect().toSet catch {
      case e: Exception =>
        fail(
          s"""
             |Exception collecting dataset as objects
             |${ds.resolvedTEncoder}
             |${ds.resolvedTEncoder.fromRowExpression.treeString}
             |${ds.queryExecution}
           """.stripMargin, e)
    }

    if (decoded != expectedAnswer.toSet) {
      val expected = expectedAnswer.toSet.toSeq.map((a: Any) => a.toString).sorted
      val actual = decoded.toSet.toSeq.map((a: Any) => a.toString).sorted

      val comparision = sideBySide("expected" +: expected, "spark" +: actual).mkString("\n")
      fail(
        s"""Decoded objects do not match expected objects:
            |$comparision
            |${ds.resolvedTEncoder.fromRowExpression.treeString}
         """.stripMargin)
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df catch {
      case ae: AnalysisException =>
        val currentValue = sqlContext.conf.dataFrameEagerAnalysis
        sqlContext.setConf(SQLConf.DATAFRAME_EAGER_ANALYSIS, false)
        val partiallyAnalzyedPlan = df.queryExecution.analyzed
        sqlContext.setConf(SQLConf.DATAFRAME_EAGER_ANALYSIS, currentValue)
        fail(
          s"""
             |Failed to analyze query: $ae
             |$partiallyAnalzyedPlan
             |
             |${stackTraceToString(ae)}
             |""".stripMargin)
    }

    QueryTest.checkAnswer(analyzedDF, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect())
  }

  /**
   * Runs the plan and makes sure the answer is within absTol of the expected result.
   * @param dataFrame the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param absTol the absolute tolerance between actual and expected answers.
   */
  protected def checkAggregatesWithTol(dataFrame: DataFrame,
      expectedAnswer: Seq[Row],
      absTol: Double): Unit = {
    // TODO: catch exceptions in data frame execution
    val actualAnswer = dataFrame.collect()
    require(actualAnswer.length == expectedAnswer.length,
      s"actual num rows ${actualAnswer.length} != expected num of rows ${expectedAnswer.length}")

    actualAnswer.zip(expectedAnswer).foreach {
      case (actualRow, expectedRow) =>
        QueryTest.checkAggregatesWithTol(actualRow, expectedRow, absTol)
    }
  }

  protected def checkAggregatesWithTol(dataFrame: DataFrame,
      expectedAnswer: Row,
      absTol: Double): Unit = {
    checkAggregatesWithTol(dataFrame, Seq(expectedAnswer), absTol)
  }

  /**
   * Asserts that a given [[Queryable]] will be executed using the given number of cached results.
   */
  def assertCached(query: Queryable, numCachedTables: Int = 1): Unit = {
    val planWithCaching = query.queryExecution.withCachedData
    val cachedData = planWithCaching collect {
      case cached: InMemoryRelation => cached
    }

    assert(
      cachedData.size == numCachedTables,
      s"Expected query to contain $numCachedTables, but it actually had ${cachedData.size}\n" +
        planWithCaching)
  }

  // ==========================
  // Streaming helper functions
  // ==========================

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

  case class AddDataMemory[A](source: MemoryStream[A], data: Seq[A]) extends StreamAction {
    override def toString: String = s"AddData to $source: ${data.mkString(",")}"
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

  /** Stops the stream.  It must currently be running. */
  case object StopStream extends StreamAction

  /** Starts the stream, resuming if data has already been processed.  It must not be running. */
  case object StartStream extends StreamAction

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
    var currentStream: StreamExecution = null
    val awaiting = new mutable.HashMap[Source, Watermark]()
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

    def currentWatermarks =
      if (currentStream != null) currentStream.currentWatermarks.toString else "not started"

    def threadState =
      if (currentStream != null && currentStream.microBatchThread.isAlive) "alive" else "dead"
    def testState =
      s"""
         |== Progress ==
         |$testActions
         |
         |== Stream ==
         |Stream state: $currentWatermarks
         |Thread state: $threadState
         |Event time trigger: ${if (currentStream != null) currentStream.maxEventTime else ""}
         |${if (streamDeathCause != null) stackTraceToString(streamDeathCause) else ""}
         |
         |== Sink ==
         |$sink
         |
         |== Plan ==
         |${if (currentStream != null) currentStream.logicalPlan else ""}
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
        if (streamDeathCause != null) {
          fail(
            s"""
               |Stream Thread Died
               |$testState
             """.stripMargin)
        }

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

          case AddDataMemory(source, data) =>
            awaiting.put(source, source.addData(data))

          case CheckAnswerRows(expectedAnswer) =>
            checkState(currentStream != null, "stream not running")

            // Block until all data added has been processed
            awaiting.foreach { case (source, watermark) =>
              try failAfter(streamingTimout) {
                currentStream.awaitWatermark(source, watermark)
              } catch {
                case _: InterruptedException =>
                  fail(
                    s"""
                       |Stream Thread Died
                       |$testState
                      """.stripMargin)
                case _: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
                  fail(
                    s"""
                       |Timed out while waiting for $source to catch up to $watermark
                       |Currently at: ${sink.currentWatermark(source)}
                       |$testState
                   """.stripMargin)
              }
            }
            QueryTest.sameRows(sink.allData, expectedAnswer).foreach {
              error => fail(
                s"""
                   |$error
                   |$testState
                 """.stripMargin)
            }
        }
        pos += 1
      }
    } finally {
      if (currentStream != null && currentStream.microBatchThread.isAlive) {
        currentStream.stop()
      }
    }
  }
}

object QueryTest {
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a [[None]] will
   * be returned.
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row]): Option[String] = {
    val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty


    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
            |Exception thrown while executing query:
            |${df.queryExecution}
            |== Exception ==
            |$e
            |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    sameRows(expectedAnswer, sparkAnswer, isSorted).map { results =>
        s"""
        |Results do not match for query:
        |${df.queryExecution}
        |== Results ==
        |$results
       """.stripMargin
    }
  }


  def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isSorted) converted.sortBy(_.toString()) else converted
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case d: java.math.BigDecimal => BigDecimal(d)
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  def sameRows(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): Option[String] = {
    if (prepareAnswer(expectedAnswer, isSorted) != prepareAnswer(sparkAnswer, isSorted)) {
      val errorMessage =
          s"""
           |== Results ==
           |${sideBySide(
          s"== Correct Answer - ${expectedAnswer.size} ==" +:
              prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
          s"== Spark Answer - ${sparkAnswer.size} ==" +:
              prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")}
          """.stripMargin
      return Some(errorMessage)
    }

    None
  }

  /**
   * Runs the plan and makes sure the answer is within absTol of the expected result.
   * @param actualAnswer the actual result in a [[Row]].
   * @param expectedAnswer the expected result in a[[Row]].
   * @param absTol the absolute tolerance between actual and expected answers.
   */
  protected def checkAggregatesWithTol(actualAnswer: Row, expectedAnswer: Row, absTol: Double) = {
    require(actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")

    // TODO: support other numeric types besides Double
    // TODO: support struct types?
    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Double, expected: Double) =>
        assert(math.abs(actual - expected) < absTol,
          s"actual answer $actual not within $absTol of correct answer $expected")
      case (actual, expected) =>
        assert(actual == expected, s"$actual did not equal $expected")
    }
  }

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]): String = {
    checkAnswer(df, expectedAnswer.asScala) match {
      case Some(errorMessage) => errorMessage
      case None => null
    }
  }
}
