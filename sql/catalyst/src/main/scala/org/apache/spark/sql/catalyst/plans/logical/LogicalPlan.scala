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
  lazy val statistics: Statistics = {
    if (children.size == 0) {
      throw new UnsupportedOperationException(s"LeafNode $nodeName must implement statistics.")
    }

    Statistics(
      sizeInBytes = children.map(_.statistics).map(_.sizeInBytes).product)
  }

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
    def handleResult[A <: NamedExpression](result: Seq[A]) = {
      result match {
        case Seq(a) => Some(a)
        case Seq() => None
        case ambiguousReferences =>
          throw new TreeNodeException(
            this, s"Ambiguous references to $name: ${ambiguousReferences.mkString(",")}")
      }
    }

    name.split("\\.") match {
      case Array(s) => handleResult(input.filter(_.name == s))
      case Array(s1, s2) =>
        handleResult(input.collect {
          case a if a.qualifiers.contains(s1) && a.name == s2 => a
          case a if a.name == s1 && a.dataType.isInstanceOf[StructType] =>
            Alias(GetField(a, s2), s2)()
        })
      case _ => None
    }
  }
}

/**
 * A logical plan node with no children.
 */
abstract class LeafNode extends LogicalPlan with trees.LeafNode[LogicalPlan] {
  self: Product =>
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
