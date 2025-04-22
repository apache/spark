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
 * A scope with registered attributes encountered during the logical plan validation process. We
 * use [[AttributeSet]] here to check the equality of attributes based on their expression IDs.
 */
case class AttributeScope(attributes: AttributeSet, isSubqueryRoot: Boolean = false)

/**
 * The [[AttributeScopeStack]] is used to validate that the attribute which was encountered by the
 * [[ExpressionResolutionValidator]] is in the current operator's visibility scope.
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
  private val stack = new ArrayDeque[AttributeScope]
  stack.push(AttributeScope(attributes = AttributeSet(Seq.empty)))

  /**
   * Check if the `attribute` is present in this stack. We check the current scope by default. If
   * `isOuterReference` is true, we check the first scope above our subquery root.
   */
  def contains(attribute: Attribute, isOuterReference: Boolean = false): Boolean = {
    if (!isOuterReference) {
      current.attributes.contains(attribute)
    } else {
      outer match {
        case Some(outer) => outer.attributes.contains(attribute)
        case _ => false
      }
    }
  }

  /**
   * Overwrite current relevant scope with a sequence of attributes which is an output of some
   * operator. `attributes` can have duplicate IDs if the output of the operator contains multiple
   * occurencies of the same attribute.
   */
  def overwriteCurrent(attributes: Seq[Attribute]): Unit = {
    val current = stack.pop()

    stack.push(current.copy(attributes = AttributeSet(attributes)))
  }

  /**
   * Execute `body` in the context of a fresh attribute scope. Used by [[Project]] and [[Aggregate]]
   * validation code since those operators introduce a new scope with fresh expression IDs.
   */
  def withNewScope[R](isSubqueryRoot: Boolean = false)(body: => R): Unit = {
    stack.push(
      AttributeScope(
        attributes = AttributeSet(Seq.empty),
        isSubqueryRoot = isSubqueryRoot
      )
    )
    try {
      body
    } finally {
      stack.pop()
    }
  }

  override def toString: String = stack.toString

  private def current: AttributeScope = stack.peek

  private def outer: Option[AttributeScope] = {
    var outerScope: Option[AttributeScope] = None

    val iter = stack.iterator
    while (iter.hasNext && !outerScope.isDefined) {
      val scope = iter.next

      if (scope.isSubqueryRoot && iter.hasNext) {
        outerScope = Some(iter.next)
      }
    }

    outerScope
  }
}
