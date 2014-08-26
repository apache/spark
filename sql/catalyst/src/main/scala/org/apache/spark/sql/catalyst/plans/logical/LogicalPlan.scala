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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.catalyst.trees

abstract class LogicalPlan extends QueryPlan[LogicalPlan] {
  self: Product =>

  /**
   * Estimates of various statistics.  The default estimation logic simply lazily multiplies the
   * corresponding statistic produced by the children.  To override this behavior, override
   * `statistics` and assign it an overriden version of `Statistics`.
   *
   * '''NOTE''': concrete and/or overriden versions of statistics fields should pay attention to the
   * performance of the implementations.  The reason is that estimations might get triggered in
   * performance-critical processes, such as query plan planning.
   *
   * @param sizeInBytes Physical size in bytes. For leaf operators this defaults to 1, otherwise it
   *                    defaults to the product of children's `sizeInBytes`.
   */
  case class Statistics(
    sizeInBytes: BigInt
  )
  lazy val statistics: Statistics = Statistics(
    sizeInBytes = children.map(_.statistics).map(_.sizeInBytes).product
  )

  /**
   * Returns the set of attributes that this node takes as
   * input from its children.
   */
  lazy val inputSet: AttributeSet = AttributeSet(children.flatMap(_.output))

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it is still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g.
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
   * should return `false`).
   */
  lazy val resolved: Boolean = !expressions.exists(!_.resolved) && childrenResolved

  /**
   * Returns true if all its children of this query plan have been resolved.
   */
  def childrenResolved: Boolean = !children.exists(!_.resolved)

  /**
   * Optionally resolves the given string to a [[NamedExpression]] using the input from all child
   * nodes of this LogicalPlan. The attribute is expressed as
   * as string in the following form: `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolveChildren(name: String): Option[NamedExpression] =
    resolve(name, children.flatMap(_.output))

  /**
   * Optionally resolves the given string to a [[NamedExpression]] based on the output of this
   * LogicalPlan. The attribute is expressed as string in the following form:
   * `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolve(name: String): Option[NamedExpression] =
    resolve(name, output)

  /** Performs attribute resolution given a name and a sequence of possible attributes. */
  protected def resolve(name: String, input: Seq[Attribute]): Option[NamedExpression] = {
    val parts = name.split("\\.")
    // Collect all attributes that are output by this nodes children where either the first part
    // matches the name or where the first part matches the scope and the second part matches the
    // name.  Return these matches along with any remaining parts, which represent dotted access to
    // struct fields.
    val options = input.flatMap { option =>
      // If the first part of the desired name matches a qualifier for this possible match, drop it.
      val remainingParts =
        if (option.qualifiers.contains(parts.head) && parts.size > 1) parts.drop(1) else parts
      if (option.name == remainingParts.head) (option, remainingParts.tail.toList) :: Nil else Nil
    }

    options.distinct match {
      case Seq((a, Nil)) => Some(a) // One match, no nested fields, use it.
      // One match, but we also need to extract the requested nested field.
      case Seq((a, nestedFields)) =>
        a.dataType match {
          case StructType(fields) =>
            Some(Alias(nestedFields.foldLeft(a: Expression)(GetField), nestedFields.last)())
          case _ => None // Don't know how to resolve these field references
        }
      case Seq() => None         // No matches.
      case ambiguousReferences =>
        throw new TreeNodeException(
          this, s"Ambiguous references to $name: ${ambiguousReferences.mkString(",")}")
    }
  }
}

/**
 * A logical plan node with no children.
 */
abstract class LeafNode extends LogicalPlan with trees.LeafNode[LogicalPlan] {
  self: Product =>

  override lazy val statistics: Statistics =
    throw new UnsupportedOperationException(s"LeafNode $nodeName must implement statistics.")
}

/**
 * A logical plan node with single child.
 */
abstract class UnaryNode extends LogicalPlan with trees.UnaryNode[LogicalPlan] {
  self: Product =>
}

/**
 * A logical plan node with a left and right child.
 */
abstract class BinaryNode extends LogicalPlan with trees.BinaryNode[LogicalPlan] {
  self: Product =>
}
