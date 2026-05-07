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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Trait providing parameterized query execution capabilities.
 * This trait unifies the logic for parsing and executing SQL queries with parameter binding,
 * which is shared between:
 * - EXECUTE IMMEDIATE (ResolveExecuteImmediate)
 * - OPEN CURSOR with USING clause (OpenCursorExec)
 *
 * The trait provides a single source of truth for how parameterized queries are executed,
 * ensuring consistent behavior across different SQL features.
 */
trait ParameterizedQueryExecutor {

  /**
   * The SparkSession to use for query execution.
   */
  protected def session: SparkSession

  /**
   * Executes a parameterized SQL query by parsing and analyzing it with bound parameters.
   *
   * This method handles both parameterized and non-parameterized queries:
   * - Parameterized queries: Uses ParameterBindingUtils to bind parameters to markers (? or :name)
   * - Non-parameterized queries: Directly parses and analyzes the SQL text
   *
   * @param queryText The SQL query text (may contain parameter markers ? or :name)
   * @param args Parameter expressions from USING clause (empty for non-parameterized queries)
   * @param paramNames Parameter names extracted at parse time (empty for positional parameters)
   * @return The analyzed logical plan with parameters bound
   */
  protected def executeParameterizedQuery(
      queryText: String,
      args: Seq[Expression],
      paramNames: Seq[String] = Seq.empty): LogicalPlan = {
    if (args.nonEmpty) {
      // Parameterized query: parse with bound parameters
      val (paramValues, paramNamesArray) =
        ParameterBindingUtils.buildUnifiedParameters(args, paramNames)

      val df = session.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
        .sql(queryText, paramValues, paramNamesArray)

      df.queryExecution.analyzed
    } else {
      // Non-parameterized query: parse without parameters
      val df = session.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
        .sql(queryText)

      df.queryExecution.analyzed
    }
  }
}
