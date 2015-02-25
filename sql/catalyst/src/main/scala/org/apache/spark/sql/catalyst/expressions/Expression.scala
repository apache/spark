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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types._

abstract class Expression extends TreeNode[Expression] {
  self: Product =>

  /** The narrowest possible type that is produced when this expression is evaluated. */
  type EvaluatedType <: Any

  /**
   * Returns true when an expression is a candidate for static evaluation before the query is
   * executed.
   *
   * The following conditions are used to determine suitability for constant folding:
   *  - A [[Coalesce]] is foldable if all of its children are foldable
   *  - A [[BinaryExpression]] is foldable if its both left and right child are foldable
   *  - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
   *  - A [[Literal]] is foldable
   *  - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
   */
  def foldable: Boolean = false
  def nullable: Boolean
  def references: AttributeSet = AttributeSet(children.flatMap(_.references.iterator))

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: Row = null): EvaluatedType

  /**
   * Returns `true` if this expression and all its children have been resolved to a specific schema
   * and `false` if it still contains any unresolved placeholders. Implementations of expressions
   * should override this if the resolution of this type of expression involves more than just
   * the resolution of its children.
   */
  lazy val resolved: Boolean = childrenResolved

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  def dataType: DataType

  /**
   * Returns true if  all the children of this expression have been resolved to a specific schema
   * and false if any still contains any unresolved placeholders.
   */
  def childrenResolved = !children.exists(!_.resolved)

  /**
   * Returns a string representation of this expression that does not have developer centric
   * debugging information like the expression id.
   */
  def prettyString: String = {
    transform {
      case a: AttributeReference => PrettyAttribute(a.name)
      case u: UnresolvedAttribute => PrettyAttribute(u.name)
    }.toString
  }
}

abstract class BinaryExpression extends Expression with trees.BinaryNode[Expression] {
  self: Product =>

  def symbol: String

  override def foldable = left.foldable && right.foldable

  override def toString = s"($left $symbol $right)"
}

abstract class LeafExpression extends Expression with trees.LeafNode[Expression] {
  self: Product =>
}

abstract class UnaryExpression extends Expression with trees.UnaryNode[Expression] {
  self: Product =>
}

// TODO Semantically we probably not need GroupExpression
// All we need is holding the Seq[Expression], and ONLY used in doing the
// expressions transformation correctly. Probably will be removed since it's
// not like a real expressions.
case class GroupExpression(children: Seq[Expression]) extends Expression {
  self: Product =>
  type EvaluatedType = Seq[Any]
  override def eval(input: Row): EvaluatedType = ???
  override def nullable = false
  override def foldable = false
  override def dataType = ???
}
