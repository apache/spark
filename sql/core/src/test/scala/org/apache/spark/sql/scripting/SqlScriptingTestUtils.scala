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

import org.apache.spark.sql.catalyst.SqlScriptingContextManager
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.CompoundBody
import org.apache.spark.sql.classic.SparkSession

/**
 * Utility trait for SQL scripting context manager initialization in test suites.
 *
 * This trait provides common functionality for setting up SQL scripting execution contexts
 * and context managers.
 */
trait SqlScriptingTestUtils {

  /**
   * Creates and initializes a SQL scripting context manager with the given compound body
   * and arguments, then executes the provided body function within that context.
   *
   * Context needs to be initialized so scopes can be entered correctly.
   *
   * @param spark SparkSession to use
   * @param compoundBody The compound body to execute
   * @param args Arguments to pass to the execution plan
   * @param body Function to execute within the scripting context
   * @tparam R Return type of the body function
   * @return Result of executing the body function
   */
  def withSqlScriptingContextManager[R](
      spark: SparkSession,
      compoundBody: CompoundBody,
      args: Map[String, Expression] = Map.empty)(body: CompoundBodyExec => R): R = {

    val interpreter = SqlScriptingInterpreter(spark)

    val context = new SqlScriptingExecutionContext()
    val executionPlan = interpreter.buildExecutionPlan(compoundBody, args, context)
    context.frames.append(
      new SqlScriptingExecutionFrame(executionPlan, SqlScriptingFrameType.SQL_SCRIPT)
    )
    executionPlan.enterScope()

    val handle = SqlScriptingContextManager.create(
      new SqlScriptingContextManagerImpl(context)
    )
    handle.runWith {
      body(executionPlan)
    }
  }
}
