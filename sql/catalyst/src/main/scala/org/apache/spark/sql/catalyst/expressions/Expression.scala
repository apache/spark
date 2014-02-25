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

package org.apache.spark.sql
package catalyst
package expressions

import trees._
import types._

abstract class Expression extends TreeNode[Expression] {
  self: Product =>

  def dataType: DataType
  /**
   * Returns true when an expression is a candidate for static evaluation before the query is
   * executed.
   *
   * The following conditions are used to determine suitability for constant folding:
   *  - A [[expressions.Coalesce Coalesce]] is foldable if all of its children are foldable
   *  - A [[expressions.BinaryExpression BinaryExpression]] is foldable if its both left and right
   *    child are foldable
   *  - A [[expressions.Not Not]], [[expressions.IsNull IsNull]], or
   *    [[expressions.IsNotNull IsNotNull]] is foldable if its child is foldable.
   *  - A [[expressions.Literal]] is foldable.
   *  - A [[expressions.Cast Cast]] or [[expressions.UnaryMinus UnaryMinus]] is foldable if its
   *    child is foldable.
   */
  // TODO: Supporting more foldable expressions. For example, deterministic Hive UDFs.
  def foldable: Boolean = false
  def nullable: Boolean
  def references: Set[Attribute]

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it is still contains any unresolved placeholders. Implementations of expressions
   * should override this.
   */
  lazy val resolved: Boolean = childrenResolved

  /**
   * Returns true if  all the children of this expression have been resolved to a specific schema
   * and false if it is still contains any unresolved placeholders.
   */
  def childrenResolved = !children.exists(!_.resolved)
}

abstract class BinaryExpression extends Expression with trees.BinaryNode[Expression] {
  self: Product =>

  def symbol: String

  override def foldable = left.foldable && right.foldable

  def references = left.references ++ right.references

  override def toString = s"($left $symbol $right)"
}

abstract class LeafExpression extends Expression with trees.LeafNode[Expression] {
  self: Product =>
}

abstract class UnaryExpression extends Expression with trees.UnaryNode[Expression] {
  self: Product =>

  def references = child.references
}
