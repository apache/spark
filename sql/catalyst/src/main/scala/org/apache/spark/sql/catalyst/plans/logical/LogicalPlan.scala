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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{UnresolvedGetField, Resolver}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.trees


abstract class LogicalPlan extends QueryPlan[LogicalPlan] with Logging {
  self: Product =>

  /**
   * Computes [[Statistics]] for this plan. The default implementation assumes the output
   * cardinality is the product of of all child plan's cardinality, i.e. applies in the case
   * of cartesian joins.
   *
   * [[LeafNode]]s must override this.
   */
  def statistics: Statistics = {
    if (children.size == 0) {
      throw new UnsupportedOperationException(s"LeafNode $nodeName must implement statistics.")
    }
    Statistics(sizeInBytes = children.map(_.statistics.sizeInBytes).product)
  }

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g.
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
   * should return `false`).
   */
  lazy val resolved: Boolean = !expressions.exists(!_.resolved) && childrenResolved

  override protected def statePrefix = if (!resolved) "'" else super.statePrefix

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

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
   *
   * This assumes `name` has multiple parts, where the 1st part is a qualifier
   * (i.e. table name, alias, or subquery alias).
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   */
  private def resolveAsTableColumn(
      nameParts: Array[String],
      resolver: Resolver,
      attribute: Attribute): Option[(Attribute, List[String])] = {
    assert(nameParts.length > 1)
    if (attribute.qualifiers.exists(resolver(_, nameParts.head))) {
      // At least one qualifier matches. See if remaining parts match.
      val remainingParts = nameParts.tail
      resolveAsColumn(remainingParts, resolver, attribute)
    } else {
      None
    }
  }

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
   *
   * Different from resolveAsTableColumn, this assumes `name` does NOT start with a qualifier.
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   */
  private def resolveAsColumn(
      nameParts: Array[String],
      resolver: Resolver,
      attribute: Attribute): Option[(Attribute, List[String])] = {
    if (resolver(attribute.name, nameParts.head)) {
      Option((attribute.withName(nameParts.head), nameParts.tail.toList))
    } else {
      None
    }
  }

  /** Performs attribute resolution given a name and a sequence of possible attributes. */
  protected def resolve(
      name: String,
      input: Seq[Attribute],
      resolver: Resolver): Option[NamedExpression] = {

    val parts = name.split("\\.")

    // A sequence of possible candidate matches.
    // Each candidate is a tuple. The first element is a resolved attribute, followed by a list
    // of parts that are to be resolved.
    // For example, consider an example where "a" is the table name, "b" is the column name,
    // and "c" is the struct field name, i.e. "a.b.c". In this case, Attribute will be "a.b",
    // and the second element will be List("c").
    var candidates: Seq[(Attribute, List[String])] = {
      // If the name has 2 or more parts, try to resolve it as `table.column` first.
      if (parts.length > 1) {
        input.flatMap { option =>
          resolveAsTableColumn(parts, resolver, option)
        }
      } else {
        Seq.empty
      }
    }

    // If none of attributes match `table.column` pattern, we try to resolve it as a column.
    if (candidates.isEmpty) {
      candidates = input.flatMap { candidate =>
        resolveAsColumn(parts, resolver, candidate)
      }
    }

    candidates.distinct match {
      // One match, no nested fields, use it.
      case Seq((a, Nil)) => Some(a)

      // One match, but we also need to extract the requested nested field.
      case Seq((a, nestedFields)) =>
        // The foldLeft adds UnresolvedGetField for every remaining parts of the name,
        // and aliased it with the last part of the name.
        // For example, consider name "a.b.c", where "a" is resolved to an existing attribute.
        // Then this will add UnresolvedGetField("b") and UnresolvedGetField("c"), and alias
        // the final expression as "c".
        val fieldExprs = nestedFields.foldLeft(a: Expression)(UnresolvedGetField)
        val aliasName = nestedFields.last
        Some(Alias(fieldExprs, aliasName)())

      // No matches.
      case Seq() =>
        logTrace(s"Could not find $name in ${input.mkString(", ")}")
        None

      // More than one match.
      case ambiguousReferences =>
        throw new AnalysisException(
          s"Ambiguous references to $name: ${ambiguousReferences.mkString(",")}")
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
