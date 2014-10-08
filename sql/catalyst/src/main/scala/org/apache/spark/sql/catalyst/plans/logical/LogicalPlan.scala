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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.catalyst.trees

abstract class LogicalPlan extends QueryPlan[LogicalPlan] with Logging {
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
   * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
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
   * Returns true when the given logical plan will return the same results as this logical plan.
   *
   * Since its likely undecideable to generally determine if two given plans will produce the same
   * results, it is okay for this function to return false, even if the results are actually
   * the same.  Such behavior will not affect correctness, only the application of performance
   * enhancements like caching.  However, it is not acceptable to return true if the results could
   * possibly be different.
   *
   * By default this function performs a modified version of equality that is tolerant of cosmetic
   * differences like attribute naming and or expression id differences.  Logical operators that
   * can do better should override this function.
   */
  def sameResult(plan: LogicalPlan): Boolean = {
    plan.getClass == this.getClass &&
    plan.children.size == children.size && {
      logDebug(s"[${cleanArgs.mkString(", ")}] == [${plan.cleanArgs.mkString(", ")}]")
      cleanArgs == plan.cleanArgs
    } &&
    (plan.children, children).zipped.forall(_ sameResult _)
  }

  /** Args that have cleaned such that differences in expression id should not affect equality */
  protected lazy val cleanArgs: Seq[Any] = {
    val input = children.flatMap(_.output)
    productIterator.map {
      // Children are checked using sameResult above.
      case tn: TreeNode[_] if children contains tn => null
      case e: Expression => BindReferences.bindReference(e, input, allowFailures = true)
      case s: Option[_] => s.map {
        case e: Expression => BindReferences.bindReference(e, input, allowFailures = true)
        case other => other
      }
      case s: Seq[_] => s.map {
        case e: Expression => BindReferences.bindReference(e, input, allowFailures = true)
        case other => other
      }
      case other => other
    }.toSeq
  }

  /**
   * Optionally resolves the given string to a [[NamedExpression]] using the input from all child
   * nodes of this LogicalPlan. The attribute is expressed as
   * as string in the following form: `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolveChildren(name: String, resolver: Resolver): Option[NamedExpression] =
    resolve(name, children.flatMap(_.output), resolver)

  /**
   * Optionally resolves the given string to a [[NamedExpression]] based on the output of this
   * LogicalPlan. The attribute is expressed as string in the following form:
   * `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolve(name: String, resolver: Resolver): Option[NamedExpression] =
    resolve(name, output, resolver)

  /** Performs attribute resolution given a name and a sequence of possible attributes. */
  protected def resolve(
      name: String,
      input: Seq[Attribute],
      resolver: Resolver): Option[NamedExpression] = {

    val parts = name.split("\\.")

    // Collect all attributes that are output by this nodes children where either the first part
    // matches the name or where the first part matches the scope and the second part matches the
    // name.  Return these matches along with any remaining parts, which represent dotted access to
    // struct fields.
    val options = input.flatMap { option =>
      // If the first part of the desired name matches a qualifier for this possible match, drop it.
      val remainingParts =
        if (option.qualifiers.find(resolver(_, parts.head)).nonEmpty && parts.size > 1) {
          parts.drop(1)
        } else {
          parts
        }

      if (resolver(option.name, remainingParts.head)) {
        // Preserve the case of the user's attribute reference.
        (option.withName(remainingParts.head), remainingParts.tail.toList) :: Nil
      } else {
        Nil
      }
    }

    options.distinct match {
      // One match, no nested fields, use it.
      case Seq((a, Nil)) => Some(a)

      // One match, but we also need to extract the requested nested field.
      case Seq((a, nestedFields)) =>
        val aliased =
          Alias(
            resolveNesting(nestedFields, a, resolver),
            nestedFields.last)() // Preserve the case of the user's field access.
        Some(aliased)

      // No matches.
      case Seq() =>
        logTrace(s"Could not find $name in ${input.mkString(", ")}")
        None

      // More than one match.
      case ambiguousReferences =>
        throw new TreeNodeException(
          this, s"Ambiguous references to $name: ${ambiguousReferences.mkString(",")}")
    }
  }

  /**
   * Given a list of successive nested field accesses, and a based expression, attempt to resolve
   * the actual field lookups on this expression.
   */
  private def resolveNesting(
      nestedFields: List[String],
      expression: Expression,
      resolver: Resolver): Expression = {

    (nestedFields, expression.dataType) match {
      case (Nil, _) => expression
      case (requestedField :: rest, StructType(fields)) =>
        val actualField = fields.filter(f => resolver(f.name, requestedField))
        actualField match {
          case Seq() =>
            sys.error(
              s"No such struct field $requestedField in ${fields.map(_.name).mkString(", ")}")
          case Seq(singleMatch) =>
            resolveNesting(rest, GetField(expression, singleMatch.name), resolver)
          case multipleMatches =>
            sys.error(s"Ambiguous reference to fields ${multipleMatches.mkString(", ")}")
        }
      case (_, dt) => sys.error(s"Can't access nested field in type $dt")
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
