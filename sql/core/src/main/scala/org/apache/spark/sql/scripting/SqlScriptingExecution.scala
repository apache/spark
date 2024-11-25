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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{CommandResult, CompoundBody}

/**
 * SQL scripting executor - executes script and returns result statements.
 */
class SqlScriptingExecution(
    sqlScript: CompoundBody,
    session: SparkSession,
    args: Map[String, Expression]) extends Iterator[DataFrame] {

  // Build the execution plan for the script
  private val executionPlan: Iterator[CompoundStatementExec] =
    SqlScriptingInterpreter(session).buildExecutionPlan(sqlScript, args)

  private var current = getNextResult

  override def hasNext: Boolean = current.isDefined

  override def next(): DataFrame = {
    if (!hasNext) {
      throw new NoSuchElementException("No more statements to execute")
    }
    val nextDataFrame = current.get
    current = getNextResult
    nextDataFrame
  }

  /** Helper method to iterate through statements until next result statement is encountered */
  private def getNextResult: Option[DataFrame] = {
    var currentStatement = if (executionPlan.hasNext) Some(executionPlan.next()) else None
    // While we don't have a result statement, execute the statements
    while (currentStatement.isDefined) {
      currentStatement match {
        case Some(stmt: SingleStatementExec) if !stmt.isExecuted =>
          withErrorHandling() {
            val df = stmt.buildDataFrame(session)
            if (df.logicalPlan.isInstanceOf[CommandResult]) {
              // If the statement is not a result, we need to write it to a noop sink to execute it
              df.write.format("noop").mode("overwrite").save()
            } else {
              // If the statement is a result, we need to return it to the caller
              return Some(df)
            }
          }
        case _ => // pass
      }
      currentStatement = if (executionPlan.hasNext) Some(executionPlan.next()) else None
    }
    None
  }

  private def handleException(e: Exception): Unit = {
    // Rethrow the exception
    // TODO: SPARK-48353 Add error handling for SQL scripts
    throw e
  }

  def withErrorHandling()(f: => Unit): Unit = {
    try {
      f
    } catch {
      case e: Exception =>
        handleException(e)
    }
  }
}
