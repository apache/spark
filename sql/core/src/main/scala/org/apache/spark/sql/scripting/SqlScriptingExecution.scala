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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.SqlScriptingLocalVariableManager
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{CommandResult, CompoundBody}
import org.apache.spark.sql.classic.{DataFrame, SparkSession}

/**
 * SQL scripting executor - executes script and returns result statements.
 * This supports returning multiple result statements from a single script.
 * The caller of the SqlScriptingExecution API must wrap the interpretation and execution of
 * statements with the [[withLocalVariableManager]] method, and adhere to the contract of executing
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
    ctx.frames.append(
      new SqlScriptingExecutionFrame(executionPlan, SqlScriptingFrameType.SQL_SCRIPT))
    // Enter the scope of the top level compound.
    // We exit this scope explicitly in the getNextStatement method when there are no more
    // statements to execute.
    executionPlan.enterScope()
    // Return the context.
    ctx
  }

  private val variableManager = new SqlScriptingLocalVariableManager(context)
  private val variableManagerHandle = SqlScriptingLocalVariableManager.create(variableManager)

  /**
   * Handles scripting context creation/access/deletion. Calls to execution API must be wrapped
   * with this method.
   */
  def withLocalVariableManager[R](f: => R): R = {
    variableManagerHandle.runWith(f)
  }

  /** Helper method to iterate get next statements from the first available frame. */
  private def getNextStatement: Option[CompoundStatementExec] = {
    // Remove frames that are already executed.
    while (context.frames.nonEmpty && !context.frames.last.hasNext) {
      val lastFrame = context.frames.last

      // First frame on stack is always script frame. If there are no more statements to execute,
      // exit the scope of the script frame.
      // This scope was entered when the script frame was created and added to the context.
      if (context.frames.size == 1 && context.frames.last.scopes.size == 1) {
        context.frames.last.executionPlan.exitScope()
      }

      context.frames.remove(context.frames.size - 1)

      // If the last frame is a handler, set leave statement to be the next one in the
      // innermost scope that should be exited.
      if (lastFrame.frameType == SqlScriptingFrameType.HANDLER && context.frames.nonEmpty) {
        // Remove the scope if handler is executed.
        if (context.firstHandlerScopeLabel.isDefined
          && lastFrame.scopeLabel.get == context.firstHandlerScopeLabel.get) {
          context.firstHandlerScopeLabel = None
        }

        var execPlan: CompoundBodyExec = context.frames.last.executionPlan
        while (execPlan.curr.exists(_.isInstanceOf[CompoundBodyExec])) {
          execPlan = execPlan.curr.get.asInstanceOf[CompoundBodyExec]
        }
        execPlan.curr = Some(new LeaveStatementExec(lastFrame.scopeLabel.get))
      }
    }
    // If there are still frames available, get the next statement.
    if (context.frames.nonEmpty) {
      return Some(context.frames.last.next())
    }
    None
  }

  /** Helper method to get the next result statement from the script. */
  private def getNextResultInternal: Option[DataFrame] = {
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
    try {
      getNextResultInternal
    } catch {
      case e: SparkThrowable =>
        handleException(e)
        getNextResult // After setup for exception handling, try to get the next result again.
      case throwable: Throwable =>
        throw throwable // Uncaught exception will be thrown.
    }
  }

  private def handleException(e: SparkThrowable): Unit = {
    context.findHandler(e.getCondition, e.getSqlState) match {
      case Some(handler) =>
        val handlerFrame = new SqlScriptingExecutionFrame(
          handler.body,
          SqlScriptingFrameType.HANDLER,
          handler.scopeLabel
        )
        context.frames.append(
          handlerFrame
        )
        handlerFrame.executionPlan.enterScope()
      case None =>
        throw e.asInstanceOf[Throwable]
    }
  }

  def withErrorHandling(f: => Unit): Unit = {
    try {
      f
    } catch {
      case sparkThrowable: SparkThrowable =>
        handleException(sparkThrowable)
      case throwable: Throwable =>
        throw throwable
    }
  }
}
