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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.trees.TreePattern.{PIPE_OPERATOR_SELECT, RUNTIME_REPLACEABLE, TreePattern}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Represents a SELECT clause when used with the |> SQL pipe operator.
 * We use this to make sure that no aggregate functions exist in the SELECT expressions.
 */
case class PipeSelect(child: Expression)
  extends UnaryExpression with RuntimeReplaceable {
  final override val nodePatterns: Seq[TreePattern] = Seq(PIPE_OPERATOR_SELECT, RUNTIME_REPLACEABLE)
  override def withNewChildInternal(newChild: Expression): Expression = PipeSelect(newChild)
  override lazy val replacement: Expression = {
    def visit(e: Expression): Unit = e match {
      case a: AggregateFunction =>
        // If we used the pipe operator |> SELECT clause to specify an aggregate function, this is
        // invalid; return an error message instructing the user to use the pipe operator
        // |> AGGREGATE clause for this purpose instead.
        throw QueryCompilationErrors.pipeOperatorSelectContainsAggregateFunction(a)
      case _: WindowExpression =>
        // Window functions are allowed in pipe SELECT operators, so do not traverse into children.
      case _ =>
        e.children.foreach(visit)
    }
    visit(child)
    child
  }
}
