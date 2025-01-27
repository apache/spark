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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{CommandResult, CompoundBody}
import org.apache.spark.sql.classic.{DataFrame, SparkSession}

/**
 * SQL scripting executor - executes script and returns result statements.
 * This supports returning multiple result statements from a single script.
 * The caller of the SqlScriptingExecution API must adhere to the contract of executing
 * the returned statement before continuing iteration. Executing the statement needs to be done
 * inside withErrorHandling block.
 *
 * @param sqlScript CompoundBody which need to be executed.
 * @param session Spark session that SQL script is executed within.
 * @param args A map of parameter names to SQL literal expressions.
 */
class SqlScriptingExecution(
    sqlScript: CompoundBody,
    session: SparkSession,
    args: Map[String, Expression]) {

  private val interpreter = SqlScriptingInterpreter(session)

  // Frames to keep what is being executed.
  private val context: SqlScriptingExecutionContext = {
    val ctx = new SqlScriptingExecutionContext()
    val executionPlan = interpreter.buildExecutionPlan(sqlScript, args, ctx)
    // Add frame which represents SQL Script to the context.
    ctx.frames.append(new SqlScriptingExecutionFrame(executionPlan.getTreeIterator))
    // Enter the scope of the top level compound.
    // We don't need to exit this scope explicitly as it will be done automatically
    // when the frame is removed during iteration.
    executionPlan.enterScope()
    ctx
  }


  /** Helper method to iterate get next statements from the first available frame. */
  private def getNextStatement: Option[CompoundStatementExec] = {
    // Remove frames that are already executed.
    while (context.frames.nonEmpty && !context.frames.last.hasNext) {
      context.frames.remove(context.frames.size - 1)
    }
    // If there are still frames available, get the next statement.
    if (context.frames.nonEmpty) {
      return Some(context.frames.last.next())
    }
    None
  }

  /**
   * Advances through the script and executes statements until a result statement or
   * end of script is encountered.
   *
   * To know if there is result statement available, the method has to advance through script and
   * execute statements until the result statement or end of script is encountered. For that reason
   * the returned result must be executed before subsequent calls. Multiple calls without executing
   * the intermediate results will lead to incorrect behavior.
   *
   * @return Result DataFrame if it is available, otherwise None.
   */
  def getNextResult: Option[DataFrame] = {
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
