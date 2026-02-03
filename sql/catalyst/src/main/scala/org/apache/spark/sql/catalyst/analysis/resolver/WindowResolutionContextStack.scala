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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.ArrayDeque

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.WindowExpression

/**
 * A resolution context used to collect potential expressions that need to be handled
 * by a [[Window]]'s child [[Project]] or [[Aggregate]] operator.
 */
case class WindowResolutionContext() {
  val windowSourceExpressions = new SemanticComparator()
}

/**
 * The stack of window source expression sets required for resolving [[Window]]. Every [[Project]]
 * operator is responsible only for the expressions which are part of its project list. The same is
 * true for each [[Aggregate]] operator. Therefore, in case of operator nesting, each operator
 * requires having a dedicated context. This approach allows for preemptive collection of window
 * dependencies, to avoid multiple passes even if the operator's expression list doesn't have any
 * window expressions.
 *
 * Consider this example:
 *
 * {{{
 * SELECT
 *   emp_name,
 *   SUM(salary) OVER (PARTITION BY EXISTS (
 *     SELECT 1 FROM dept WHERE emp.dept_id = dept.dept_id
 *   ))
 * FROM
 *   emp;
 * }}}
 *
 * Such a query should result in multiple (in this case two [[Project]] nodes), one for the
 * subquery and the other one for the main `SELECT`. Although there is no [[WindowExpression]]
 * in the subquery, we must preemptively collect emp.dept_id to avoid multiple traversals of
 * the project list. In order to separate the attribute scope of the two [[Project]] nodes,
 * each operates within a unique [[WindowResolutionContext]].
 */
class WindowResolutionContextStack {
  private val stack = new ArrayDeque[WindowResolutionContext]

  /**
   * Current context object. Must exist when resolving [[Project]] operator.
   */
  def current: WindowResolutionContext = {
    if (stack.isEmpty) {
      throw SparkException.internalError("No current window resolution context")
    }
    stack.peek()
  }

  /**
   * Pushes a new [[WindowResolutionContext]] object.
   */
  def pushScope() {
    stack.push(WindowResolutionContext())
  }

  /**
   * Pops the current object.
   */
  def popScope(): Unit = {
    stack.pop()
  }
}
