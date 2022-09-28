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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{AliasAwareQueryOutputOrdering, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.LogicalPlanStats
import org.apache.spark.sql.catalyst.trees.{BinaryLike, LeafLike, TreeNodeTag, UnaryLike}
import org.apache.spark.sql.catalyst.util.MetadataColumnHelper
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}


abstract class LogicalPlan
  extends QueryPlan[LogicalPlan]
  with AnalysisHelper
  with LogicalPlanStats
  with LogicalPlanDistinctKeys
  with QueryPlanConstraints
  with Logging {

  /**
   * Metadata fields that can be projected from this node.
   * Should be overridden if the plan does not propagate its children's output.
   */
  def metadataOutput: Seq[Attribute] = children.flatMap(_.metadataOutput)

  /** Returns true if this subtree has data from a streaming data source. */
  def isStreaming: Boolean = _isStreaming
  private[this] lazy val _isStreaming = children.exists(_.isStreaming)

  override def verboseStringWithSuffix(maxFields: Int): String = {
    super.verboseString(maxFields) + statsCache.map(", " + _.toString).getOrElse("")
  }

  /**
   * Returns the maximum number of rows that this plan may compute.
   *
   * Any operator that a Limit can be pushed passed should override this function (e.g., Union).
   * Any operator that can push through a Limit should override this function (e.g., Project).
   */
  def maxRows: Option[Long] = None

  /**
   * Returns the maximum number of rows this plan may compute on each partition.
   */
  def maxRowsPerPartition: Option[Long] = maxRows

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g.
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
   * should return `false`).
   */
  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved

  override protected def statePrefix = if (!resolved) "'" else super.statePrefix

  /**
   * Returns true if all its children of this query plan have been resolved.
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * Resolves a given schema to concrete [[Attribute]] references in this query plan. This function
   * should only be called on analyzed plans since it will throw [[AnalysisException]] for
   * unresolved [[Attribute]]s.
   */
  def resolve(schema: StructType, resolver: Resolver): Seq[Attribute] = {
    schema.map { field =>
      resolve(field.name :: Nil, resolver).map {
        case a: AttributeReference => a
        case _ => throw QueryExecutionErrors.resolveCannotHandleNestedSchema(this)
      }.getOrElse {
        throw QueryCompilationErrors.cannotResolveAttributeError(
          field.name, output.map(_.name).mkString(", "))
      }
    }
  }

  private[this] lazy val childAttributes = AttributeSeq(children.flatMap(_.output))

  private[this] lazy val childMetadataAttributes = AttributeSeq(children.flatMap(_.metadataOutput))

  private[this] lazy val outputAttributes = AttributeSeq(output)

  private[this] lazy val outputMetadataAttributes = AttributeSeq(metadataOutput)

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] using the input from all child
   * nodes of this LogicalPlan. The attribute is expressed as
   * string in the following form: `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolveChildren(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    childAttributes.resolve(nameParts, resolver)
      .orElse(childMetadataAttributes.resolve(nameParts, resolver))

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] based on the output of this
   * LogicalPlan. The attribute is expressed as string in the following form:
   * `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolve(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    outputAttributes.resolve(nameParts, resolver)
      .orElse(outputMetadataAttributes.resolve(nameParts, resolver))

  /**
   * Given an attribute name, split it to name parts by dot, but
   * don't split the name parts quoted by backticks, for example,
   * `ab.cd`.`efg` should be split into two parts "ab.cd" and "efg".
   */
  def resolveQuoted(
      name: String,
      resolver: Resolver): Option[NamedExpression] = {
    resolve(UnresolvedAttribute.parseAttributeName(name), resolver)
  }

  /**
   * Refreshes (or invalidates) any metadata/data cached in the plan recursively.
   */
  def refresh(): Unit = children.foreach(_.refresh())

  /**
   * Returns true iff `other`'s output is semantically the same, i.e.:
   *  - it contains the same number of `Attribute`s;
   *  - references are the same;
   *  - the order is equal too.
   */
  def sameOutput(other: LogicalPlan): Boolean = {
    val thisOutput = this.output
    val otherOutput = other.output
    thisOutput.length == otherOutput.length && thisOutput.zip(otherOutput).forall {
      case (a1, a2) => a1.semanticEquals(a2)
    }
  }
}

object LogicalPlan {
  // A dedicated tag for Spark Connect.
  // If an expression (only support UnresolvedAttribute for now) was attached by this tag,
  // the analyzer will:
  //    1, extract the plan id;
  //    2, top-down traverse the query plan to find the node that was attached by the same tag.
  //    and fails the whole analysis if can not find it;
  //    3, resolve this expression with the matching node. If any error occurs, analyzer fallbacks
  //    to the old code path.
  private[spark] val PLAN_ID_TAG = TreeNodeTag[Long]("plan_id")
}

/**
 * A logical plan node with no children.
 */
trait LeafNode extends LogicalPlan with LeafLike[LogicalPlan] {
  override def producedAttributes: AttributeSet = outputSet

  /** Leaf nodes that can survive analysis must define their own statistics. */
  def computeStats(): Statistics = throw new UnsupportedOperationException
}

/**
 * A logical plan node with single child.
 */
trait UnaryNode extends LogicalPlan with UnaryLike[LogicalPlan] {
  val constraintProjectionLimit = conf.getConf(SQLConf.CONSTRAINT_PROJECTION_LIMIT)

  /**
   * Generates all valid constraints including a set of aliased constraints by replacing the
   * original constraint expressions with the corresponding alias.
   * This method only returns constraints whose referenced attributes are subset of `outputSet`.
   */
  protected def getAllValidConstraints(projectList: Seq[NamedExpression]): ExpressionSet = {
    val newLiteralConstraints = mutable.ArrayBuffer.empty[EqualNullSafe]
    val newConstraints = mutable.ArrayBuffer.empty[EqualNullSafe]
    val aliasMap = mutable.Map[Expression, mutable.ArrayBuffer[Attribute]]()
    projectList.foreach {
      // These new literal constraints doesn't need any projection, not they can project any other
      // constraint
      case a @ Alias(l: Literal, _) =>
        newLiteralConstraints += EqualNullSafe(a.toAttribute, l)

      // We need to add simple attributes to the alias as those attributes can be aliased as well.
      // Technically, we don't need to add attributes that are not otherwise aliased to the map, but
      // adding them does no harm.
      case a: Attribute =>
        aliasMap.getOrElseUpdate(a.canonicalized, mutable.ArrayBuffer.empty) += a

      // If we have an alias in the projection then we need to:
      // - add it to the alias map as it can project child's constraints
      // - and add it to the new constraints and let it be projected or pruned based on other
      //   aliases and attributes in the project list.
      //   E.g. `a + b <=> x` constraint can "survive" the projection if
      //   - `a + b` is aliased (like `a + b AS y` and so the projected constraint is `y <=> x`)
      //   - or both `a` and `b` are aliased or included in the output set
      case a @ Alias(child, _) if child.deterministic =>
        val attr = a.toAttribute
        aliasMap.getOrElseUpdate(child.canonicalized, mutable.ArrayBuffer.empty) += attr
        newConstraints += EqualNullSafe(child, attr)
      case _ =>
    }

    def projectConstraint(expr: Expression) = {
      // The current constraint projection doesn't do a full-blown projection which means that when
      // - a constraint contain an expression multiple times (E.g. `c + c > 1`)
      // - and we have a projection where an expression is aliased as multiple different attributes
      //   (E.g. `c AS c1`, `c AS c2`)
      // then we return only `c1 + c1 > 1` and `c2 + c2 > 1` but doesn't return `c1 + c2 > 1`.
      val currentAlias = mutable.Map.empty[Expression, Seq[Expression]]
      expr.multiTransformDown {
        // Mapping with aliases
        case e: Expression if aliasMap.contains(e.canonicalized) =>
          // When we encounter an expression for the first time in the tree, set up a cache to track
          // the current attribute alias and return that cached attribute when we encounter the same
          // expression at other places.
          currentAlias.getOrElse(e.canonicalized, {
            // If a parent expression can can be transformed return to original expression too to
            // let its children transformed too.
            val alternatives = if (e.containsChild.nonEmpty) {
              e +: aliasMap(e.canonicalized).toSeq
            } else {
              aliasMap(e.canonicalized).toSeq
            }

            // When iterate through the alternatives for the first encounter we also update the
            // cache.
            alternatives.toStream.map { a =>
              currentAlias += e.canonicalized -> Seq(a)
              a
            }.append {
              currentAlias -= e.canonicalized
              Seq.empty
            }
          })


        // Prune if we encounter an attribute that we can't map and it is not in output set.
        case a: Attribute if !outputSet.contains(a) => Seq.empty
      }.filter {
        case EqualNullSafe(a1: Attribute, a2: Attribute) => a1.canonicalized != a2.canonicalized
        case _ => true
      }
    }

    val projectedConstraints =
      // Transform child's constraints according to alias map
      child.constraints.toStream.flatMap(projectConstraint) ++
      // Transform child expressions of new constraints according to alias map
      newConstraints.toStream.flatMap(projectConstraint)

    ExpressionSet(
      constraintProjectionLimit.map(l => projectedConstraints.take(l))
        .getOrElse(projectedConstraints) ++
      newLiteralConstraints.toSeq)
  }

  override protected lazy val validConstraints: ExpressionSet = child.constraints
}

/**
 * A logical plan node with a left and right child.
 */
trait BinaryNode extends LogicalPlan with BinaryLike[LogicalPlan]

trait OrderPreservingUnaryNode extends UnaryNode
  with AliasAwareQueryOutputOrdering[LogicalPlan] {
  override protected def outputExpressions: Seq[NamedExpression] = child.output
  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering
}

object LogicalPlanIntegrity {

  def canGetOutputAttrs(p: LogicalPlan): Boolean = {
    p.resolved && !p.expressions.exists { e =>
      e.exists {
        // We cannot call `output` in plans with a `ScalarSubquery` expr having no column,
        // so, we filter out them in advance.
        case s: ScalarSubquery => s.plan.schema.fields.isEmpty
        case _ => false
      }
    }
  }

  /**
   * Since some logical plans (e.g., `Union`) can build `AttributeReference`s in their `output`,
   * this method checks if the same `ExprId` refers to attributes having the same data type
   * in plan output. Returns the error message if the check does not pass.
   */
  def hasUniqueExprIdsForOutput(plan: LogicalPlan): Option[String] = {
    val exprIds = plan.collect { case p if canGetOutputAttrs(p) =>
      // NOTE: we still need to filter resolved expressions here because the output of
      // some resolved logical plans can have unresolved references,
      // e.g., outer references in `ExistenceJoin`.
      p.output.filter(_.resolved).map { a => (a.exprId, a.dataType.asNullable) }
    }.flatten

    val ignoredExprIds = plan.collect {
      // NOTE: `Union` currently reuses input `ExprId`s for output references, but we cannot
      // simply modify the code for assigning new `ExprId`s in `Union#output` because
      // the modification will make breaking changes (See SPARK-32741(#29585)).
      // So, this check just ignores the `exprId`s of `Union` output.
      case u: Union if u.resolved => u.output.map(_.exprId)
    }.flatten.toSet

    val groupedDataTypesByExprId = exprIds.filterNot { case (exprId, _) =>
      ignoredExprIds.contains(exprId)
    }.groupBy(_._1).values.map(_.distinct)

    groupedDataTypesByExprId.collectFirst {
      case group if group.length > 1 =>
        val exprId = group.head._1
        val types = group.map(_._2.sql)
        s"Multiple attributes have the same expression ID ${exprId.id} but different data types: " +
          types.mkString(", ") + ". The plan tree:\n" + plan.treeString
    }
  }

  /**
   * This method checks if reference `ExprId`s are not reused when assigning a new `ExprId`.
   * For example, it returns false if plan transformers create an alias having the same `ExprId`
   * with one of reference attributes, e.g., `a#1 + 1 AS a#1`. Returns the error message if the
   * check does not pass.
   */
  def checkIfSameExprIdNotReused(plan: LogicalPlan): Option[String] = {
    plan.collectFirst { case p if p.resolved =>
      p.expressions.collectFirst {
        // Even if a plan is resolved, `a.references` can return unresolved references,
        // e.g., in `Grouping`/`GroupingID`, so we need to filter out them and
        // check if the same `exprId` in `Alias` does not exist among reference `exprId`s.
        case a: Alias if a.references.filter(_.resolved).map(_.exprId).exists(_ == a.exprId) =>
          "An alias reuses the same expression ID as previously present in an attribute, " +
            s"which is invalid: ${a.sql}. The plan tree:\n" + plan.treeString
      }
    }.flatten
  }

  /**
   * This method checks if the same `ExprId` refers to an unique attribute in a plan tree.
   * Some plan transformers (e.g., `RemoveNoopOperators`) rewrite logical
   * plans based on this assumption. Returns the error message if the check does not pass.
   */
  def validateExprIdUniqueness(plan: LogicalPlan): Option[String] = {
    LogicalPlanIntegrity.checkIfSameExprIdNotReused(plan).orElse(
      LogicalPlanIntegrity.hasUniqueExprIdsForOutput(plan))
  }

  /**
   * Validate the structural integrity of an optimized plan.
   * For example, we can check after the execution of each rule that each plan:
   * - is still resolved
   * - only host special expressions in supported operators
   * - has globally-unique attribute IDs
   * - has the same result schema as the previous plan
   * - has no dangling attribute references
   */
  def validateOptimizedPlan(
      previousPlan: LogicalPlan,
      currentPlan: LogicalPlan): Option[String] = {
    if (!currentPlan.resolved) {
      Some("The plan becomes unresolved: " + currentPlan.treeString + "\nThe previous plan: " +
        previousPlan.treeString)
    } else if (currentPlan.exists(PlanHelper.specialExpressionsInUnsupportedOperator(_).nonEmpty)) {
      Some("Special expressions are placed in the wrong plan: " + currentPlan.treeString)
    } else {
      LogicalPlanIntegrity.validateExprIdUniqueness(currentPlan).orElse {
        if (!DataType.equalsIgnoreNullability(previousPlan.schema, currentPlan.schema)) {
          Some(s"The plan output schema has changed from ${previousPlan.schema.sql} to " +
            currentPlan.schema.sql + s". The previous plan: ${previousPlan.treeString}\nThe new " +
            "plan:\n" + currentPlan.treeString)
        } else {
          None
        }
      }
    }
  }
}

/**
 * A logical plan node that can generate metadata columns
 */
trait ExposesMetadataColumns extends LogicalPlan {
  protected def metadataOutputWithOutConflicts(
      metadataOutput: Seq[AttributeReference]): Seq[AttributeReference] = {
    // If `metadataColFromOutput` is not empty that means `AddMetadataColumns` merged
    // metadata output into output. We should still return an available metadata output
    // so that the rule `ResolveReferences` can resolve metadata column correctly.
    val metadataColFromOutput = output.filter(_.isMetadataCol)
    if (metadataColFromOutput.isEmpty) {
      val resolve = conf.resolver
      val outputNames = outputSet.map(_.name)

      def isOutputColumn(col: AttributeReference): Boolean = {
        outputNames.exists(name => resolve(col.name, name))
      }
      // filter out the metadata struct column if it has the name conflicting with output columns.
      // if the file has a column "_metadata",
      // then the data column should be returned not the metadata struct column
      metadataOutput.filterNot(isOutputColumn)
    } else {
      metadataColFromOutput.asInstanceOf[Seq[AttributeReference]]
    }
  }

  def withMetadataColumns(): LogicalPlan
}
