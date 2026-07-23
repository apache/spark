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

package org.apache.spark

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.scalatest.Suite

trait CheckErrorHelper { self: Suite =>

  case class ExpectedContext(
      contextType: QueryContextType,
      objectType: String,
      objectName: String,
      startIndex: Int,
      stopIndex: Int,
      fragment: String,
      callSitePattern: String
   )

  object ExpectedContext {
    def apply(fragment: String, start: Int, stop: Int): ExpectedContext = {
      ExpectedContext("", "", start, stop, fragment)
    }

    // Check the fragment only. This is only used when the fragment is distinguished within
    // the query text
    def apply(fragment: String): ExpectedContext = {
      ExpectedContext("", "", -1, -1, fragment)
    }

    def apply(
        objectType: String,
        objectName: String,
        startIndex: Int,
        stopIndex: Int,
        fragment: String): ExpectedContext = {
      new ExpectedContext(QueryContextType.SQL, objectType, objectName, startIndex, stopIndex,
        fragment, "")
    }

    def apply(fragment: String, callSitePattern: String): ExpectedContext = {
      new ExpectedContext(QueryContextType.DataFrame, "", "", -1, -1, fragment, callSitePattern)
    }
  }

  /**
   * Parameter keys that are omitted from comparison when absent from the expected map.
   * For each error condition, the set lists keys that are removed from the actual
   * exception parameters before comparison with the expected map.
   * Test suites may override this to add or change ignorable parameters per condition.
   */
  protected def checkErrorIgnorableParameters: Map[String, Set[String]] = Map(
    "TABLE_OR_VIEW_NOT_FOUND" -> Set("searchPath")
  )

  /**
   * Checks an exception with an error condition against expected results.
   * @param exception     The exception to check
   * @param condition     The expected error condition identifying the error
   * @param sqlState      Optional the expected SQLSTATE, not verified if not supplied
   * @param parameters    A map of parameter names and values. The names are as defined
   *                      in the error-classes file.
   * @param matchPVals    Optionally treat the parameters value as regular expression pattern.
   *                      false if not supplied.
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
    val ignorable = checkErrorIgnorableParameters.getOrElse(condition, Set.empty[String])
    val actualParametersToCompare = actualParameters.filter { case (k, _) =>
      !ignorable.contains(k) || parameters.contains(k)
    }
    if (matchPVals) {
      if (actualParametersToCompare.size != parameters.size) {
        mismatches += s"parameters size: expected ${parameters.size} but got" +
          s" ${actualParametersToCompare.size}"
      }
      actualParametersToCompare.foreach { case (key, actualVal) =>
        parameters.get(key) match {
          case None =>
            mismatches += s"parameters: unexpected key '$key' with value '$actualVal'"
          case Some(pattern) if !actualVal.matches(pattern) =>
            mismatches += s"parameters['$key']: value '$actualVal' does not match pattern" +
              s" '$pattern'"
          case _ =>
        }
      }
      parameters.keys.filterNot(actualParametersToCompare.contains).foreach { key =>
        mismatches += s"parameters: missing expected key '$key'"
      }
    } else if (actualParametersToCompare != parameters) {
      mismatches += s"parameters: expected $parameters but got $actualParametersToCompare"
    }

    val actualQueryContext = exception.getQueryContext()
    if (actualQueryContext.length != queryContext.length) {
      mismatches += s"queryContext.length: expected ${queryContext.length}" +
        s" but got ${actualQueryContext.length}"
    }
    actualQueryContext.zip(queryContext).zipWithIndex.foreach {
      case ((actual, expected), idx) =>
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
}
