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

package org.apache.spark.sql.scripting

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{CommandResult, CompoundBody}

/**
 * SQL scripting executor - executes script and returns result statements.
 * This supports returning multiple result statements from a single script.
 *
 * @param sqlScript CompoundBody which need to be executed.
 * @param session Spark session that SQL script is executed within.
 * @param args A map of parameter names to SQL literal expressions.
 */
class SqlScriptingExecution(
    sqlScript: CompoundBody,
    session: SparkSession,
    args: Map[String, Expression]) extends Iterator[DataFrame] {

  // Build the execution plan for the script.
  private val executionPlan: Iterator[CompoundStatementExec] =
    SqlScriptingInterpreter(session).buildExecutionPlan(sqlScript, args)

  private var current = getNextResult

  override def hasNext: Boolean = current.isDefined

  override def next(): DataFrame = {
    if (!hasNext) throw SparkException.internalError("No more elements to iterate through.")
    val nextDataFrame = current.get
    current = getNextResult
    nextDataFrame
  }

  /** Helper method to iterate through statements until next result statement is encountered. */
  private def getNextResult: Option[DataFrame] = {

    def getNextStatement: Option[CompoundStatementExec] =
      if (executionPlan.hasNext) Some(executionPlan.next()) else None

    var currentStatement = getNextStatement
    // While we don't have a result statement, execute the statements.
    while (currentStatement.isDefined) {
      currentStatement match {
        case Some(stmt: SingleStatementExec) if !stmt.isExecuted =>
          withErrorHandling {
            val df = stmt.buildDataFrame(session)
            df.logicalPlan match {
              case _: CommandResult => // pass
              case _ => return Some(df) // If the statement is a result, return it to the caller.
            }
          }
        case _ => // pass
      }
      currentStatement = getNextStatement
    }
    None
  }

  private def handleException(e: Throwable): Unit = {
    // Rethrow the exception.
    // TODO: SPARK-48353 Add error handling for SQL scripts
    throw e
  }

  def withErrorHandling(f: => Unit): Unit = {
    try {
      f
    } catch {
      case e: Throwable =>
        handleException(e)
    }
  }
}
