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

  private val interpreter = SqlScriptingInterpreter(session)

  // Frames to keep what is being executed.
  private val context: SqlScriptingExecutionContext = {
    val ctx = new SqlScriptingExecutionContext()
    interpreter.buildExecutionPlan(sqlScript, args, ctx)
    ctx
  }

  private var current: Option[DataFrame] = None
  private var resultConsumed: Boolean = true

  override def hasNext: Boolean = {
    // If the previous result was not consumed, return true if current element exists.
    if (!resultConsumed) {
      return current.isDefined
    }

    // If the previous result was consumed, get the next result and return true if it exists.
    current = getNextResult
    resultConsumed = false
    current.isDefined
  }

  override def next(): DataFrame = {
    if (!hasNext) throw SparkException.internalError("No more elements to iterate through.")
    resultConsumed = true
    current.get
  }

  /** Helper method to iterate get next statements from the first available frame. */
  private def getNextStatement: Option[CompoundStatementExec] = {
    while (context.frames.nonEmpty && !context.frames.last.hasNext) {
      context.frames.remove(context.frames.size - 1)
    }
    if (context.frames.nonEmpty && context.frames.last.hasNext) {
      return Some(context.frames.last.next())
    }
    None
  }

  /** Helper method to iterate through statements until next result statement is encountered. */
  private def getNextResult: Option[DataFrame] = {
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
