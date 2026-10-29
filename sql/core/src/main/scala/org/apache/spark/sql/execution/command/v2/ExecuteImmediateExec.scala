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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan for an EXECUTE IMMEDIATE command payload. Runs the already-analyzed inner command
 * once, here at the execution level, and returns its rows. [[ExecuteImmediateCommand]] keeps the
 * payload out of the logical plan's children, so the eager-command path does not run it first and
 * this node is its sole executor. The inner plan is exposed via [[innerChildren]] so EXPLAIN shows
 * the payload.
 */
case class ExecuteImmediateExec(
    output: Seq[Attribute],
    sourceStatement: LogicalPlan) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    // sourceStatement is already analyzed, so runCommand does not re-bind names (local variables
    // stay hidden as resolved by ResolveExecuteImmediate).
    val (_, result) = QueryExecution.runCommand(session, sourceStatement, "execute-immediate")
    result.toImmutableArraySeq
  }

  // Expose the inner plan so EXPLAIN shows what EXECUTE IMMEDIATE runs.
  override def innerChildren: Seq[QueryPlan[_]] = Seq(sourceStatement)
}
