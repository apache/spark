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
import org.apache.spark.sql.catalyst.types.{DataType, ArrayType, StructType}
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
   * as string in the following form: `[scope].AttributeName.[nested].[fields]...`. Fields
   * can contain ordinal expressions, such as `field[i][j][k]...`.
   */
  def resolve(name: String): Option[NamedExpression] = {
    def expandFunc(expType: (Expression, DataType), field: String): (Expression, DataType) = {
      val (exp, t) = expType
      val ordinalRegExp = """(\[(\d+)\])""".r
      val fieldName = if (field.matches("\\w*(\\[\\d\\])+")) {
        field.substring(0, field.indexOf("["))
      } else {
        field
      }
      t match {
        case ArrayType(elementType) =>
          val ordinals = ordinalRegExp.findAllIn(field).matchData.map(_.group(2))
          (ordinals.foldLeft(exp)((v1: Expression, v2: String) =>
            GetItem(v1, Literal(v2.toInt))), elementType)
        case StructType(fields) =>
          // Note: this only works if we are not on the top-level!
          val structField = fields.find(_.name == fieldName)
          if (!structField.isDefined) {
            throw new TreeNodeException(
              this, s"Trying to resolve Attribute but field ${fieldName} is not defined")
          }
          structField.get.dataType match {
            case ArrayType(elementType) =>
              val ordinals = ordinalRegExp.findAllIn(field).matchData.map(_.group(2))
              (ordinals.foldLeft(
                  GetField(exp, fieldName).asInstanceOf[Expression])((v1: Expression, v2: String) =>
                    GetItem(v1, Literal(v2.toInt))),
                elementType)
            case _ =>
              (GetField(exp, fieldName), structField.get.dataType)
          }
        case _ =>
          expType
      }
    }

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
      // TODO from rebase!
      /*val remainingParts = if (option.qualifiers contains parts.head) parts.drop(1) else parts
      val relevantRemaining =
        if (remainingParts.head.matches("\\w*\\[(\\d+)\\]")) { // array field name
          remainingParts.head.substring(0, remainingParts.head.indexOf("["))
        } else {
          remainingParts.head
        }
      if (option.name == relevantRemaining) (option, remainingParts.tail.toList) :: Nil else Nil*/
    }

    options.distinct match {
      case (a, Nil) :: Nil => {
        a.dataType match {
          case ArrayType(elementType) =>
            val expression = expandFunc((a: Expression, a.dataType), name)._1
            Some(Alias(expression, name)())
          case _ => Some(a)
        }
      } // One match, no nested fields, use it.
      // One match, but we also need to extract the requested nested field.
      case (a, nestedFields) :: Nil =>
        a.dataType match {
          case StructType(fields) =>
            // this is compatibility reasons with earlier code!
            // TODO: why only nestedFields and not parts?
            // check for absence of nested arrays so there are only fields
            if ((parts(0) :: nestedFields).forall(!_.matches("\\w*\\[\\d+\\]+"))) {
              Some(Alias(nestedFields.foldLeft(a: Expression)(GetField), nestedFields.last)())
            } else {
              val expression = parts.foldLeft((a: Expression, a.dataType))(expandFunc)._1
              Some(Alias(expression, nestedFields.last)())
            }
          case _ =>
            val expression = parts.foldLeft((a: Expression, a.dataType))(expandFunc)._1
            Some(Alias(expression, nestedFields.last)())
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
