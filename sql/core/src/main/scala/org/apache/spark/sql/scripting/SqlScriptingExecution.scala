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

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{CommandResult, CompoundBody, HandlerType}
import org.apache.spark.sql.exceptions.SqlScriptingUserRaisedException

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
    val executionPlan = interpreter.buildExecutionPlan(sqlScript, args, ctx)
    // Add frame which represents SQL Script to the context.
    ctx.frames.addOne(new SqlScriptingExecutionFrame(executionPlan))
    // Enter the scope of the top level compound.
    // We don't need to exit this scope explicitly as it will be done automatically
    // when the frame is removed during iteration.
    executionPlan.enterScope()
    ctx
  }

  private var current: Option[DataFrame] = None

  override def hasNext: Boolean = {
    try {
      current = getNextResult
    } catch {
      case e: SparkThrowable =>
        handleException(e)
        hasNext
      case exception: Exception =>
        throw exception
    }
    current.isDefined
  }

  override def next(): DataFrame = {
    if (current.isEmpty) {
      throw SparkException.internalError("No more elements to iterate through.")
    }

    current.get
  }

  /** Helper method to iterate get next statements from the first available frame. */
  private def getNextStatement: Option[CompoundStatementExec] = {
    while (context.frames.nonEmpty && !context.frames.last.hasNext) {
      val lastFrame = context.frames.last
      context.frames.remove(context.frames.size - 1)

      if (lastFrame.isExitHandler && context.frames.nonEmpty) {
        var execPlan: CompoundBodyExec = context.frames.last.executionPlan
        while (execPlan.curr.get.isInstanceOf[CompoundBodyExec]) {
          execPlan = execPlan.curr.get.asInstanceOf[CompoundBodyExec]
        }
        execPlan.curr = Some(new LeaveStatementExec(lastFrame.scopeToExit.get))
      }

      if (lastFrame.isContinueHandler && context.frames.nonEmpty) {
        var execPlan: CompoundBodyExec = context.frames.last.executionPlan
        while (execPlan.curr.get.isInstanceOf[CompoundBodyExec]) {
          execPlan = execPlan.curr.get.asInstanceOf[CompoundBodyExec]
        }
        if (execPlan.curr.get.isInstanceOf[IfElseStatementExec]) {
          execPlan.curr = None
        }
      }

    }
    if (context.frames.nonEmpty) {
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
        case Some(signal: SignalStatementExec) =>
          throw new SqlScriptingUserRaisedException(
            condition = signal.errorCondition,
            sqlState = signal.sqlState,
            message = signal.messageText,
          )
        case Some(stmt: SingleStatementExec) if !stmt.isExecuted =>
          val df = stmt.buildDataFrame(session)
          df.logicalPlan match {
            case _: CommandResult => // pass
            case _ => return Some(df) // If the statement is a result, return it to the caller.
          }
        case _ => // pass
      }
      currentStatement = getNextStatement
    }
    None
  }

  private def handleException(e: SparkThrowable): Unit = {
    context.findHandler(e.getSqlState) match {
      case Some(handler) =>
        context.frames.addOne(
          new SqlScriptingExecutionFrame(
            handler.body,
            isExitHandler = handler.handlerType == HandlerType.EXIT,
            isContinueHandler = handler.handlerType == HandlerType.CONTINUE,
            handler.scopeToExit
          )
        )
      case None =>
        throw e.asInstanceOf[Throwable]
    }
  }

  def withErrorHandling(f: => Unit): Unit = {
    try {
      f
    } catch {
      case e: SparkThrowable =>
        handleException(e) // Try to find a handler for the exception.
      case exception: Exception =>
        // Throw the exception as is.
        throw exception
    }
  }
}
