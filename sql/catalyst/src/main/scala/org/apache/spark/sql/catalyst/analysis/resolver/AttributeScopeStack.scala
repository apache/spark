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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}

/**
 * The [[AttributeScopeStack]] is used to validate that the attribute which was encountered by the
 * [[ExpressionResolutionValidator]] is in the current operator's visibility scope. We use
 * [[AttributeSet]] as scope implementation here to check the equality of attributes based on their
 * expression IDs.
 *
 * E.g. for the following SQL query:
 * {{{
 * SELECT a, a, a + col2 FROM (SELECT col1 as a, col2 FROM VALUES (1, 2));
 * }}}
 *
 * Having the following logical plan:
 * {{{
 * Project [a#2, a#2, (a#2 + col2#1) AS (a + col2)#3]
 * +- SubqueryAlias __auto_generated_subquery_name
 *    +- Project [col1#0 AS a#2, col2#1]
 *       +- LocalRelation [col1#0, col2#1]
 * }}}
 *
 * The [[LocalRelation]] outputs attributes with IDs #0 and #1, which can be referenced by the lower
 * [[Project]]. This [[Project]] produces a new attribute ID #2 for an alias and retains the old
 * ID #1 for col2. The upper [[Project]] references `a` twice using the same ID #2 and produces a
 * new ID #3 for an alias of `a + col2`.
 */
class AttributeScopeStack {
  private val stack = new ArrayDeque[AttributeSet]
  push()

  /**
   * Get the relevant attribute scope in the context of the current operator.
   */
  def top: AttributeSet = {
    stack.peek()
  }

  /**
   * Overwrite current relevant scope with a sequence of attributes which is an output of some
   * operator. `attributes` can have duplicate IDs if the output of the operator contains multiple
   * occurrences of the same attribute.
   */
  def overwriteTop(attributes: Seq[Attribute]): Unit = {
    stack.pop()
    stack.push(AttributeSet(attributes))
  }

  /**
   * Execute `body` in the context of a fresh attribute scope. Used by [[Project]] and [[Aggregate]]
   * validation code since those operators introduce a new scope with fresh expression IDs.
   */
  def withNewScope[R](body: => R): Unit = {
    push()
    try {
      body
    } finally {
      pop()
    }
  }

  private def push(): Unit = {
    stack.push(AttributeSet(Seq.empty))
  }

  private def pop(): Unit = {
    stack.pop()
  }
}
