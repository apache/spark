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

package org.apache.spark.sql.connect.test

import java.util.TimeZone

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.scalatest.Assertions

import org.apache.spark.{QueryContextType, SparkThrowable}
import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SparkStringUtils.sideBySide

abstract class QueryTest extends ConnectFunSuite with SQLHelper {

  def spark: SparkSession

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df
   *   the [[DataFrame]] to be executed
   * @param expectedAnswer
   *   the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(df, expectedAnswer)
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect().toImmutableArraySeq)
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Array[Row]): Unit = {
    checkAnswer(df, expectedAnswer.toImmutableArraySeq)
  }

  case class ExpectedContext(
      contextType: QueryContextType,
      objectType: String,
      objectName: String,
      startIndex: Int,
      stopIndex: Int,
      fragment: String,
      callSitePattern: String)

  object ExpectedContext {
    def apply(fragment: String, start: Int, stop: Int): ExpectedContext = {
      ExpectedContext("", "", start, stop, fragment)
    }

    def apply(
        objectType: String,
        objectName: String,
        startIndex: Int,
        stopIndex: Int,
        fragment: String): ExpectedContext = {
      new ExpectedContext(
        QueryContextType.SQL,
        objectType,
        objectName,
        startIndex,
        stopIndex,
        fragment,
        "")
    }

    def apply(fragment: String, callSitePattern: String): ExpectedContext = {
      new ExpectedContext(QueryContextType.DataFrame, "", "", -1, -1, fragment, callSitePattern)
    }
  }

  /**
   * Checks an exception with an error condition against expected results.
   * @param exception
   *   The exception to check
   * @param condition
   *   The expected error condition identifying the error
   * @param sqlState
   *   Optional the expected SQLSTATE, not verified if not supplied
   * @param parameters
   *   A map of parameter names and values. The names are as defined in the error-classes file.
   * @param matchPVals
   *   Optionally treat the parameters value as regular expression pattern. false if not supplied.
   */
  protected def checkError(
      exception: SparkThrowable,
      condition: String,
      sqlState: Option[String] = None,
      parameters: Map[String, String] = Map.empty,
      matchPVals: Boolean = false,
      queryContext: Array[ExpectedContext] = Array.empty): Unit = {
    val mismatches = new ListBuffer[String]

    if (exception.getCondition != condition) {
      mismatches += s"condition: expected '$condition' but got '${exception.getCondition}'"
    }
    sqlState.foreach { state =>
      if (exception.getSqlState != state) {
        mismatches += s"sqlState: expected '$state' but got '${exception.getSqlState}'"
      }
    }

    val actualParameters = exception.getMessageParameters.asScala
    if (matchPVals) {
      if (actualParameters.size != parameters.size) {
        mismatches += s"parameters size: expected ${parameters.size} but got" +
          s" ${actualParameters.size}"
      }
      actualParameters.foreach { case (key, actualVal) =>
        parameters.get(key) match {
          case None =>
            mismatches += s"parameters: unexpected key '$key' with value '$actualVal'"
          case Some(pattern) if !actualVal.matches(pattern) =>
            mismatches += s"parameters['$key']: value '$actualVal' does not match pattern" +
              s" '$pattern'"
          case _ =>
        }
      }
      parameters.keys.filterNot(actualParameters.contains).foreach { key =>
        mismatches += s"parameters: missing expected key '$key'"
      }
    } else if (actualParameters != parameters) {
      mismatches += s"parameters: expected $parameters but got $actualParameters"
    }

    val actualQueryContext = exception.getQueryContext()
    if (actualQueryContext.length != queryContext.length) {
      mismatches += s"queryContext.length: expected ${queryContext.length}" +
        s" but got ${actualQueryContext.length}"
    }
    actualQueryContext.zip(queryContext).zipWithIndex.foreach { case ((actual, expected), idx) =>
      if (actual.contextType() != expected.contextType) {
        mismatches += s"queryContext[$idx].contextType: expected ${expected.contextType}" +
          s" but got ${actual.contextType()}"
      }
      if (actual.contextType() == QueryContextType.SQL) {
        if (actual.objectType() != expected.objectType) {
          mismatches += s"queryContext[$idx].objectType: expected '${expected.objectType}'" +
            s" but got '${actual.objectType()}'"
        }
        if (actual.objectName() != expected.objectName) {
          mismatches += s"queryContext[$idx].objectName: expected '${expected.objectName}'" +
            s" but got '${actual.objectName()}'"
        }
        // If startIndex and stopIndex are -1, it means we simply want to check the
        // fragment of the query context. This should be the case when the fragment is
        // distinguished within the query text.
        if (expected.startIndex != -1 && actual.startIndex() != expected.startIndex) {
          mismatches += s"queryContext[$idx].startIndex: expected ${expected.startIndex}" +
            s" but got ${actual.startIndex()}"
        }
        if (expected.stopIndex != -1 && actual.stopIndex() != expected.stopIndex) {
          mismatches += s"queryContext[$idx].stopIndex: expected ${expected.stopIndex}" +
            s" but got ${actual.stopIndex()}"
        }
        if (actual.fragment() != expected.fragment) {
          mismatches += s"queryContext[$idx].fragment: expected '${expected.fragment}'" +
            s" but got '${actual.fragment()}'"
        }
      } else if (actual.contextType() == QueryContextType.DataFrame) {
        if (actual.fragment() != expected.fragment) {
          mismatches += s"queryContext[$idx].fragment: expected '${expected.fragment}'" +
            s" but got '${actual.fragment()}'"
        }
        if (expected.callSitePattern.nonEmpty &&
          !actual.callSite().matches(expected.callSitePattern)) {
          mismatches += s"queryContext[$idx].callSite: '${actual.callSite()}'" +
            s" does not match pattern '${expected.callSitePattern}'"
        }
      }
    }

    if (mismatches.nonEmpty) {
      val sb = new StringBuilder
      sb.append(s"checkError found ${mismatches.size} mismatch(es).\n\n")
      sb.append("=== Actual Exception State ===\n")
      sb.append(s"  condition: ${exception.getCondition}\n")
      sb.append(s"  sqlState:  ${exception.getSqlState}\n")
      sb.append(s"  parameters:\n")
      if (actualParameters.isEmpty) {
        sb.append("    (empty)\n")
      } else {
        actualParameters.foreach { case (k, v) => sb.append(s"    $k -> $v\n") }
      }
      actualQueryContext.zipWithIndex.foreach { case (ctx, idx) =>
        sb.append(s"  queryContext[$idx] (${ctx.contextType()}):\n")
        if (ctx.contextType() == QueryContextType.SQL) {
          sb.append(s"    objectType: ${ctx.objectType()}\n")
          sb.append(s"    objectName: ${ctx.objectName()}\n")
          sb.append(s"    startIndex: ${ctx.startIndex()}\n")
          sb.append(s"    stopIndex:  ${ctx.stopIndex()}\n")
          sb.append(s"    fragment:   ${ctx.fragment()}\n")
        } else if (ctx.contextType() == QueryContextType.DataFrame) {
          sb.append(s"    fragment:   ${ctx.fragment()}\n")
          sb.append(s"    callSite:   ${ctx.callSite()}\n")
        }
      }
      sb.append("\n=== Mismatches ===\n")
      mismatches.foreach(m => sb.append(s"  $m\n"))
      fail(sb.toString())
    }
  }

  protected def checkError(
      exception: SparkThrowable,
      condition: String,
      sqlState: String,
      parameters: Map[String, String]): Unit =
    checkError(exception, condition, Some(sqlState), parameters)

  protected def checkError(
      exception: SparkThrowable,
      condition: String,
      sqlState: String,
      parameters: Map[String, String],
      context: ExpectedContext): Unit =
    checkError(exception, condition, Some(sqlState), parameters, false, Array(context))

  protected def checkError(
      exception: SparkThrowable,
      condition: String,
      parameters: Map[String, String],
      context: ExpectedContext): Unit =
    checkError(exception, condition, None, parameters, false, Array(context))

  protected def checkError(
      exception: SparkThrowable,
      condition: String,
      sqlState: String,
      context: ExpectedContext): Unit =
    checkError(exception, condition, Some(sqlState), Map.empty, false, Array(context))

  protected def checkError(
      exception: SparkThrowable,
      condition: String,
      sqlState: Option[String],
      parameters: Map[String, String],
      context: ExpectedContext): Unit =
    checkError(exception, condition, sqlState, parameters, false, Array(context))

  protected def getCurrentClassCallSitePattern: String = {
    val cs = Thread.currentThread().getStackTrace()(2)
    // {classloader}//{class.name}({file_name.scala}:{line_number})
    s".*//${cs.getClassName}\\..*\\(${cs.getFileName}:\\d+\\)"
  }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given
   * expected answer.
   */
  protected def checkDataset[T](ds: => Dataset[T], expectedAnswer: T*): Unit = {
    val result = ds.collect()

    if (!QueryTest.compare(result.toSeq, expectedAnswer)) {
      fail(s"""
              |Decoded objects do not match expected objects:
              |expected: $expectedAnswer
              |actual:   ${result.toSeq}
       """.stripMargin)
    }
  }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given
   * expected answer, after sort.
   */
  protected def checkDatasetUnorderly[T: Ordering](
      ds: => Dataset[T],
      expectedAnswer: T*): Unit = {
    val result = ds.collect()

    if (!QueryTest.compare(result.toSeq.sorted, expectedAnswer.sorted)) {
      fail(s"""
              |Decoded objects do not match expected objects:
              |expected: $expectedAnswer
              |actual:   ${result.toSeq}
       """.stripMargin)
    }
  }
}

object QueryTest extends Assertions {

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df
   *   the DataFrame to be executed
   * @param expectedAnswer
   *   the expected result in a Seq of Rows.
   */
  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row], isSorted: Boolean = false): Unit = {
    getErrorMessageInCheckAnswer(df, expectedAnswer, isSorted) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result. If there was exception
   * during the execution or the contents of the DataFrame does not match the expected result, an
   * error message will be returned. Otherwise, a None will be returned.
   *
   * @param df
   *   the DataFrame to be executed
   * @param expectedAnswer
   *   the expected result in a Seq of Rows.
   */
  def getErrorMessageInCheckAnswer(
      df: DataFrame,
      expectedAnswer: Seq[Row],
      isSorted: Boolean = false): Option[String] = {
    val sparkAnswer =
      try df.collect().toSeq
      catch {
        case e: Exception =>
          val errorMessage =
            s"""
             |Exception thrown while executing query:
             |${df.analyze}
             |== Exception ==
             |$e
             |${org.apache.spark.util.SparkErrorUtils.stackTraceToString(e)}
        """.stripMargin
          return Some(errorMessage)
      }

    sameRows(expectedAnswer, sparkAnswer, isSorted).map { results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
         |${df.analyze}
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
      case bd: java.math.BigDecimal => BigDecimal(bd)
      // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
      case seq: Seq[_] =>
        seq.map {
          case b: java.lang.Byte => b.byteValue
          case s: java.lang.Short => s.shortValue
          case i: java.lang.Integer => i.intValue
          case l: java.lang.Long => l.longValue
          case f: java.lang.Float => f.floatValue
          case d: java.lang.Double => d.doubleValue
          case x => x
        }
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  private def genError(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): String = {
    val getRowType: Option[Row] => String = row =>
      row
        .map(row =>
          if (row.schema == null) {
            "struct<>"
          } else {
            s"${row.schema.catalogString}"
          })
        .getOrElse("struct<>")

    s"""
       |== Results ==
       |${sideBySide(
        s"== Correct Answer - ${expectedAnswer.size} ==" +:
          getRowType(expectedAnswer.headOption) +:
          prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
        s"== Spark Answer - ${sparkAnswer.size} ==" +:
          getRowType(sparkAnswer.headOption) +:
          prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")}
  """.stripMargin
  }

  def includesRows(expectedRows: Seq[Row], sparkAnswer: Seq[Row]): Option[String] = {
    if (!prepareAnswer(expectedRows, true).toSet.subsetOf(
        prepareAnswer(sparkAnswer, true).toSet)) {
      return Some(genError(expectedRows, sparkAnswer, true))
    }
    None
  }

  def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r) }
    case (a: Map[_, _], b: Map[_, _]) =>
      a.size == b.size && a.keys.forall { aKey =>
        b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey)))
      }
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r) }
    case (a: Product, b: Product) =>
      compare(a.productIterator.toSeq, b.productIterator.toSeq)
    case (a: Row, b: Row) =>
      compare(a.toSeq, b.toSeq)
    // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
    case (a: Double, b: Double) =>
      java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
    case (a: Float, b: Float) =>
      java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
    case (a, b) => a == b
  }

  def sameRows(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): Option[String] = {
    if (!compare(prepareAnswer(expectedAnswer, isSorted), prepareAnswer(sparkAnswer, isSorted))) {
      return Some(genError(expectedAnswer, sparkAnswer, isSorted))
    }
    None
  }
}
