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

import scala.util.control.NonFatal

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.ErrorMessageFormat.MINIMAL
import org.apache.spark.SparkThrowableHelper.getMessage
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{CurrentDate, CurrentTimestampLike, CurrentUser, Literal}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.{DescribeColumnCommand, DescribeCommandBase}
import org.apache.spark.sql.types.{DateType, StructType, TimestampType}

trait SQLQueryTestHelper extends Logging {

  private val notIncludedMsg = "[not included in comparison]"
  private val clsName = this.getClass.getCanonicalName
  protected val emptySchema = StructType(Seq.empty).catalogString

  protected val validFileExtensions = ".sql"

  protected def replaceNotIncludedMsg(line: String): String = {
    line.replaceAll("#\\d+", "#x")
      .replaceAll("plan_id=\\d+", "plan_id=x")
      .replaceAll(
        s"Location.*$clsName/",
        s"Location $notIncludedMsg/{warehouse_dir}/")
      .replaceAll(s"file:[^\\s,]*$clsName", s"file:$notIncludedMsg/{warehouse_dir}")
      .replaceAll("Created By.*", s"Created By $notIncludedMsg")
      .replaceAll("Created Time.*", s"Created Time $notIncludedMsg")
      .replaceAll(s"transient_lastDdlTime=\\d+", s"transient_lastDdlTime=$notIncludedMsg")
      .replaceAll(s""""transient_lastDdlTime":"\\d+"""",
        s""""transient_lastDdlTime $notIncludedMsg":"None"""")
      .replaceAll("Last Access.*", s"Last Access $notIncludedMsg")
      .replaceAll("Owner\t.*", s"Owner\t$notIncludedMsg")
      .replaceAll("Partition Statistics\t\\d+", s"Partition Statistics\t$notIncludedMsg")
      .replaceAll("CTERelationDef \\d+,", s"CTERelationDef xxxx,")
      .replaceAll("CTERelationRef \\d+,", s"CTERelationRef xxxx,")
      .replaceAll("@\\w*,", s"@xxxxxxxx,")
      .replaceAll("\\*\\(\\d+\\) ", "*") // remove the WholeStageCodegen codegenStageIds
  }

  /**
   * Analyzes a query and returns the result as (schema of the output, normalized resolved plan
   * tree string representation).
   */
  protected def getNormalizedQueryAnalysisResult(
      session: SparkSession, sql: String): (String, Seq[String]) = {
    // Note that creating the following DataFrame includes eager execution for commands that create
    // objects such as views. Therefore any following queries that reference these objects should
    // find them in the catalog.
    val df = session.sql(sql)
    val schema = df.schema.catalogString
    val analyzed = df.queryExecution.analyzed
    // Determine if the analyzed plan contains any nondeterministic expressions.
    var deterministic = true
    analyzed.transformAllExpressionsWithSubqueries {
      case expr: CurrentDate =>
        deterministic = false
        expr
      case expr: CurrentTimestampLike =>
        deterministic = false
        expr
      case expr: CurrentUser =>
        deterministic = false
        expr
      case expr: Literal if expr.dataType == DateType || expr.dataType == TimestampType =>
        deterministic = false
        expr
      case expr if !expr.deterministic =>
        deterministic = false
        expr
    }
    if (deterministic) {
      // Perform query analysis, but also get rid of the #1234 expression IDs that show up in the
      // resolved plans.
      (schema, Seq(replaceNotIncludedMsg(analyzed.toString)))
    } else {
      // The analyzed plan is nondeterministic so elide it from the result to keep tests reliable.
      (schema, Seq("[Analyzer test output redacted due to nondeterminism]"))
    }
  }

  /** Executes a query and returns the result as (schema of the output, normalized output). */
  protected def getNormalizedQueryExecutionResult(
      session: SparkSession, sql: String): (String, Seq[String]) = {
    // Returns true if the plan is supposed to be sorted.
    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case _: DescribeCommandBase
          | _: DescribeColumnCommand
          | _: DescribeRelation
          | _: DescribeColumn => true
      case PhysicalOperation(_, _, Sort(_, true, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    val df = session.sql(sql)
    val schema = df.schema.catalogString
    // Get answer, but also get rid of the #1234 expression ids that show up in explain plans
    val answer = SQLExecution.withNewExecutionId(df.queryExecution, Some(sql)) {
      hiveResultString(df.queryExecution.executedPlan).map(replaceNotIncludedMsg)
    }

    // If the output is not pre-sorted, sort it.
    if (isSorted(df.queryExecution.analyzed)) (schema, answer) else (schema, answer.sorted)
  }

  /**
   * This method handles exceptions occurred during query execution as they may need special care
   * to become comparable to the expected output.
   *
   * @param result a function that returns a pair of schema and output
   */
  protected def handleExceptions(result: => (String, Seq[String])): (String, Seq[String]) = {
    val format = MINIMAL
    try {
      result
    } catch {
      case e: SparkThrowable with Throwable if e.getErrorClass != null =>
        (emptySchema, Seq(e.getClass.getName, getMessage(e, format)))
      case a: AnalysisException =>
        // Do not output the logical plan tree which contains expression IDs.
        // Also implement a crude way of masking expression IDs in the error message
        // with a generic pattern "###".
        (emptySchema, Seq(a.getClass.getName, a.getSimpleMessage.replaceAll("#\\d+", "#x")))
      case s: SparkException if s.getCause != null =>
        // For a runtime exception, it is hard to match because its message contains
        // information of stage, task ID, etc.
        // To make result matching simpler, here we match the cause of the exception if it exists.
        s.getCause match {
          case e: SparkThrowable with Throwable if e.getErrorClass != null =>
            (emptySchema, Seq(e.getClass.getName, getMessage(e, format)))
          case cause =>
            (emptySchema, Seq(cause.getClass.getName, cause.getMessage))
        }
      case NonFatal(e) =>
        // If there is an exception, put the exception class followed by the message.
        (emptySchema, Seq(e.getClass.getName, e.getMessage))
    }
  }
}
