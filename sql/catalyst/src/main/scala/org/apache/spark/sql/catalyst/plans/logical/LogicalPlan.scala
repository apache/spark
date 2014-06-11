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
import org.apache.spark.sql.catalyst.types.{StringType, StructType}
import org.apache.spark.sql.catalyst.trees

abstract class LogicalPlan extends QueryPlan[LogicalPlan] {
  self: Product =>

  /**
   * Returns the set of attributes that are referenced by this node
   * during evaluation.
   */
  def references: Set[Attribute]

  /**
   * Returns the set of attributes that this node takes as
   * input from its children.
   */
  lazy val inputSet: Set[Attribute] = children.flatMap(_.output).toSet

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it is still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g. [[catalyst.analysis.UnresolvedRelation UnresolvedRelation]] should
   * return `false`).
   */
  lazy val resolved: Boolean = !expressions.exists(!_.resolved) && childrenResolved

  /**
   * Returns true if all its children of this query plan have been resolved.
   */
  def childrenResolved = !children.exists(!_.resolved)

  /**
   * Optionally resolves the given string to a
   * [[catalyst.expressions.NamedExpression NamedExpression]]. The attribute is expressed as
   * as string in the following form: `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolve(name: String): Option[NamedExpression] = {
    val parts = name.split("\\.")
    // Collect all attributes that are output by this nodes children where either the first part
    // matches the name or where the first part matches the scope and the second part matches the
    // name.  Return these matches along with any remaining parts, which represent dotted access to
    // struct fields.
    val options = children.flatMap(_.output).flatMap { option =>
      // If the first part of the desired name matches a qualifier for this possible match, drop it.
      val remainingParts =
        if (option.qualifiers.contains(parts.head) && parts.size > 1) parts.drop(1) else parts
      if (option.name == remainingParts.head) (option, remainingParts.tail.toList) :: Nil else Nil
    }

    options.distinct match {
      case (a, Nil) :: Nil => Some(a) // One match, no nested fields, use it.
      // One match, but we also need to extract the requested nested field.
      case (a, nestedFields) :: Nil =>
        a.dataType match {
          case StructType(fields) =>
            Some(Alias(nestedFields.foldLeft(a: Expression)(GetField), nestedFields.last)())
          case _ => None // Don't know how to resolve these field references
        }
      case Nil => None         // No matches.
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

  // Leaf nodes by definition cannot reference any input attributes.
  def references = Set.empty
}

/**
 * A logical node that represents a non-query command to be executed by the system.  For example,
 * commands can be used by parsers to represent DDL operations.
 */
abstract class Command extends LeafNode {
  self: Product =>
  def output: Seq[Attribute] = Seq.empty  // TODO: SPARK-2081 should fix this
}

/**
 * Returned for commands supported by a given parser, but not catalyst.  In general these are DDL
 * commands that are passed directly to another system.
 */
case class NativeCommand(cmd: String) extends Command

/**
 * Commands of the form "SET (key) (= value)".
 */
case class SetCommand(key: Option[String], value: Option[String]) extends Command {
  override def output = Seq(
    AttributeReference("key", StringType, nullable = false)(),
    AttributeReference("value", StringType, nullable = false)()
  )
}

/**
 * Returned by a parser when the users only wants to see what query plan would be executed, without
 * actually performing the execution.
 */
case class ExplainCommand(plan: LogicalPlan) extends Command {
  override def output = Seq(AttributeReference("plan", StringType, nullable = false)())
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
