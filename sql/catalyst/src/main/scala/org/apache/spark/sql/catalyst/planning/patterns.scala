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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.internal.SQLConf

/**
 * A pattern that matches any number of project or filter operations even if they are
 * non-deterministic, as long as they satisfy the requirement of CollapseProject and CombineFilters.
 * All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator. [[Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
object PhysicalOperation extends AliasHelper with PredicateHelper {
  import org.apache.spark.sql.catalyst.optimizer.CollapseProject.canCollapseExpressions

  type ReturnType =
    (Seq[NamedExpression], Seq[Expression], LogicalPlan)
  type IntermediateType =
    (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, AttributeMap[Alias])

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val alwaysInline = SQLConf.get.getConf(SQLConf.COLLAPSE_PROJECT_ALWAYS_INLINE)
    val (fields, filters, child, _) = collectProjectsAndFilters(plan, alwaysInline)
    Some((fields.getOrElse(child.output), filters, child))
  }

  /**
   * Collects all adjacent projects and filters, in-lining/substituting aliases if necessary.
   * Here are two examples for alias in-lining/substitution.
   * Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
   */
  private def collectProjectsAndFilters(
      plan: LogicalPlan,
      alwaysInline: Boolean): IntermediateType = {
    def empty: IntermediateType = (None, Nil, plan, AttributeMap.empty)

    plan match {
      case Project(fields, child) =>
        val (_, filters, other, aliases) = collectProjectsAndFilters(child, alwaysInline)
        if (canCollapseExpressions(fields, aliases, alwaysInline)) {
          val replaced = fields.map(replaceAliasButKeepName(_, aliases))
          (Some(replaced), filters, other, getAliasMap(replaced))
        } else {
          empty
        }

      case Filter(condition, child) =>
        val (fields, filters, other, aliases) = collectProjectsAndFilters(child, alwaysInline)
        // When collecting projects and filters, we effectively push down filters through
        // projects. We need to meet the following conditions to do so:
        //   1) no Project collected so far or the collected Projects are all deterministic
        //   2) the collected filters and this filter are all deterministic, or this is the
        //      first collected filter.
        //   3) this filter does not repeat any expensive expressions from the collected
        //      projects.
        val canIncludeThisFilter = fields.forall(_.forall(_.deterministic)) && {
          filters.isEmpty || (filters.forall(_.deterministic) && condition.deterministic)
        } && canCollapseExpressions(Seq(condition), aliases, alwaysInline)
        if (canIncludeThisFilter) {
          val replaced = replaceAlias(condition, aliases)
          (fields, filters ++ splitConjunctivePredicates(replaced), other, aliases)
        } else {
          empty
        }

      case h: ResolvedHint => collectProjectsAndFilters(h.child, alwaysInline)

      case _ => empty
    }
  }
}

object NodeWithOnlyDeterministicProjectAndFilter {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case Project(projectList, child) if projectList.forall(_.deterministic) => unapply(child)
    case Filter(cond, child) if cond.deterministic => unapply(child)
    case _ => Some(plan)
  }
}

/**
 * A pattern that finds joins with equality conditions that can be evaluated using equi-join.
 *
 * Null-safe equality will be transformed into equality as joining key (replace null with default
 * value).
 */
object ExtractEquiJoinKeys extends Logging with PredicateHelper {
  /** (joinType, leftKeys, rightKeys, otherCondition, conditionOnJoinKeys, leftChild,
   * rightChild, joinHint).
   */
  // Note that `otherCondition` is NOT the original Join condition and it contains only
  // the subset that is not handled by the 'leftKeys' to 'rightKeys' equijoin.
  // 'conditionOnJoinKeys' is the subset of the original Join condition that corresponds to the
  // 'leftKeys' to 'rightKeys' equijoin.
  type ReturnType =
    (JoinType, Seq[Expression], Seq[Expression],
      Option[Expression], Option[Expression], LogicalPlan, LogicalPlan, JoinHint)

  def unapply(join: Join): Option[ReturnType] = join match {
    case Join(left, right, joinType, condition, hint) =>
      logDebug(s"Considering join on: $condition")
      // Find equi-join predicates that can be evaluated before the join, and thus can be used
      // as join keys.
      val predicates = condition.map(splitConjunctivePredicates).getOrElse(Nil)
      val joinKeys = predicates.flatMap {
        case EqualTo(l, r) if l.references.isEmpty || r.references.isEmpty => None
        case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => Some((l, r))
        case EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => Some((r, l))
        // Replace null with default value for joining key, then those rows with null in it could
        // be joined together
        case EqualNullSafe(l, r) if canEvaluate(l, left) && canEvaluate(r, right) =>
          Seq((Coalesce(Seq(l, Literal.default(l.dataType))),
            Coalesce(Seq(r, Literal.default(r.dataType)))),
            (IsNull(l), IsNull(r))
          )  // (coalesce(l, default) = coalesce(r, default)) and (isnull(l) = isnull(r))
        case EqualNullSafe(l, r) if canEvaluate(l, right) && canEvaluate(r, left) =>
          Seq((Coalesce(Seq(r, Literal.default(r.dataType))),
            Coalesce(Seq(l, Literal.default(l.dataType)))),
            (IsNull(r), IsNull(l))
          )  // Same as above with left/right reversed.
        case _ => None
      }
      val (predicatesOfJoinKeys, otherPredicates) = predicates.partition {
        case EqualTo(l, r) if l.references.isEmpty || r.references.isEmpty => false
        case Equality(l, r) =>
          canEvaluate(l, left) && canEvaluate(r, right) ||
            canEvaluate(l, right) && canEvaluate(r, left)
        case _ => false
      }

      if (joinKeys.nonEmpty) {
        val (leftKeys, rightKeys) = joinKeys.unzip
        logDebug(s"leftKeys:$leftKeys | rightKeys:$rightKeys")
        Some((joinType, leftKeys, rightKeys, otherPredicates.reduceOption(And),
          predicatesOfJoinKeys.reduceOption(And), left, right, hint))
      } else {
        None
      }
  }
}

/**
 * A pattern that collects the filter and inner joins.
 *
 *          Filter
 *            |
 *        inner Join
 *          /    \            ---->      (Seq(plan0, plan1, plan2), conditions)
 *      Filter   plan2
 *        |
 *  inner join
 *      /    \
 *   plan0    plan1
 *
 * Note: This pattern currently only works for left-deep trees.
 */
object ExtractFiltersAndInnerJoins extends PredicateHelper {

  /**
   * Flatten all inner joins, which are next to each other.
   * Return a list of logical plans to be joined with a boolean for each plan indicating if it
   * was involved in an explicit cross join. Also returns the entire list of join conditions for
   * the left-deep tree.
   */
  def flattenJoin(plan: LogicalPlan, parentJoinType: InnerLike = Inner)
      : (Seq[(LogicalPlan, InnerLike)], Seq[Expression]) = plan match {
    case Join(left, right, joinType: InnerLike, cond, hint) if hint == JoinHint.NONE =>
      val (plans, conditions) = flattenJoin(left, joinType)
      (plans ++ Seq((right, joinType)), conditions ++
        cond.toSeq.flatMap(splitConjunctivePredicates))
    case Filter(filterCondition, j @ Join(_, _, _: InnerLike, _, hint)) if hint == JoinHint.NONE =>
      val (plans, conditions) = flattenJoin(j)
      (plans, conditions ++ splitConjunctivePredicates(filterCondition))

    case _ => (Seq((plan, parentJoinType)), Seq.empty)
  }

  def unapply(plan: LogicalPlan)
      : Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]
      = plan match {
    case f @ Filter(filterCondition, j @ Join(_, _, joinType: InnerLike, _, hint))
        if hint == JoinHint.NONE =>
      Some(flattenJoin(f))
    case j @ Join(_, _, joinType, _, hint) if hint == JoinHint.NONE =>
      Some(flattenJoin(j))
    case _ => None
  }
}

/**
 * An extractor used when planning the physical execution of an aggregation. Compared with a logical
 * aggregation, the following transformations are performed:
 *  - Unnamed grouping expressions are named so that they can be referred to across phases of
 *    aggregation
 *  - Aggregations that appear multiple times are deduplicated.
 *  - The computation of the aggregations themselves is separated from the final result. For
 *    example, the `count` in `count + 1` will be split into an [[AggregateExpression]] and a final
 *    computation that computes `count.resultAttribute + 1`.
 */
object PhysicalAggregation {
  // groupingExpressions, aggregateExpressions, resultExpressions, child
  type ReturnType =
    (Seq[NamedExpression], Seq[Expression], Seq[NamedExpression], LogicalPlan)

  def unapply(a: Any): Option[ReturnType] = a match {
    case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
      // A single aggregate expression might appear multiple times in resultExpressions.
      // In order to avoid evaluating an individual aggregate function multiple times, we'll
      // build a set of semantically distinct aggregate expressions and re-write expressions so
      // that they reference the single copy of the aggregate function which actually gets computed.
      // Non-deterministic aggregate expressions are not deduplicated.
      val equivalentAggregateExpressions = new EquivalentExpressions
      val aggregateExpressions = resultExpressions.flatMap { expr =>
        expr.collect {
          // addExpr() always returns false for non-deterministic expressions and do not add them.
          case a
            if AggregateExpression.isAggregate(a) && !equivalentAggregateExpressions.addExpr(a) =>
            a
        }
      }

      val namedGroupingExpressions = groupingExpressions.map {
        case ne: NamedExpression => ne -> ne
        // If the expression is not a NamedExpressions, we add an alias.
        // So, when we generate the result of the operator, the Aggregate Operator
        // can directly get the Seq of attributes representing the grouping expressions.
        case other =>
          val withAlias = Alias(other, other.toString)()
          other -> withAlias
      }
      val groupExpressionMap = namedGroupingExpressions.toMap

      // The original `resultExpressions` are a set of expressions which may reference
      // aggregate expressions, grouping column values, and constants. When aggregate operator
      // emits output rows, we will use `resultExpressions` to generate an output projection
      // which takes the grouping columns and final aggregate result buffer as input.
      // Thus, we must re-write the result expressions so that their attributes match up with
      // the attributes of the final result projection's input row:
      val rewrittenResultExpressions = resultExpressions.map { expr =>
        expr.transformDown {
          case ae: AggregateExpression =>
            // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
            // so replace each aggregate expression by its corresponding attribute in the set:
            equivalentAggregateExpressions.getExprState(ae).map(_.expr)
              .getOrElse(ae).asInstanceOf[AggregateExpression].resultAttribute
            // Similar to AggregateExpression
          case ue: PythonUDF if PythonUDF.isGroupedAggPandasUDF(ue) =>
            equivalentAggregateExpressions.getExprState(ue).map(_.expr)
              .getOrElse(ue).asInstanceOf[PythonUDF].resultAttribute
          case expression if !expression.foldable =>
            // Since we're using `namedGroupingAttributes` to extract the grouping key
            // columns, we need to replace grouping key expressions with their corresponding
            // attributes. We do not rely on the equality check at here since attributes may
            // differ cosmetically. Instead, we use semanticEquals.
            groupExpressionMap.collectFirst {
              case (expr, ne) if expr semanticEquals expression => ne.toAttribute
            }.getOrElse(expression)
        }.asInstanceOf[NamedExpression]
      }

      Some((
        namedGroupingExpressions.map(_._2),
        aggregateExpressions,
        rewrittenResultExpressions,
        child))

    case _ => None
  }
}

/**
 * An extractor used when planning physical execution of a window. This extractor outputs
 * the window function type of the logical window.
 *
 * The input logical window must contain same type of window functions, which is ensured by
 * the rule ExtractWindowExpressions in the analyzer.
 */
object PhysicalWindow {
  // windowFunctionType, windowExpression, partitionSpec, orderSpec, child
  private type ReturnType =
    (WindowFunctionType, Seq[NamedExpression], Seq[Expression], Seq[SortOrder], LogicalPlan)

  def unapply(a: Any): Option[ReturnType] = a match {
    case expr @ logical.Window(windowExpressions, partitionSpec, orderSpec, child) =>

      // The window expression should not be empty here, otherwise it's a bug.
      if (windowExpressions.isEmpty) {
        throw QueryCompilationErrors.emptyWindowExpressionError(expr)
      }

      val windowFunctionType = windowExpressions.map(WindowFunctionType.functionType)
        .reduceLeft { (t1: WindowFunctionType, t2: WindowFunctionType) =>
          if (t1 != t2) {
            // We shouldn't have different window function type here, otherwise it's a bug.
            throw QueryCompilationErrors.foundDifferentWindowFunctionTypeError(windowExpressions)
          } else {
            t1
          }
        }

      Some((windowFunctionType, windowExpressions, partitionSpec, orderSpec, child))

    case _ => None
  }
}

object ExtractSingleColumnNullAwareAntiJoin extends JoinSelectionHelper with PredicateHelper {

  // TODO support multi column NULL-aware anti join in future.
  // See. http://www.vldb.org/pvldb/vol2/vldb09-423.pdf Section 6
  // multi-column null aware anti join is much more complicated than single column ones.

  // streamedSideKeys, buildSideKeys
  private type ReturnType = (Seq[Expression], Seq[Expression])

  /**
   * See. [SPARK-32290]
   * LeftAnti(condition: Or(EqualTo(a=b), IsNull(EqualTo(a=b)))
   * will almost certainly be planned as a Broadcast Nested Loop join,
   * which is very time consuming because it's an O(M*N) calculation.
   * But if it's a single column case O(M*N) calculation could be optimized into O(M)
   * using hash lookup instead of loop lookup.
   */
  def unapply(join: Join): Option[ReturnType] = join match {
    case Join(left, right, LeftAnti,
      Some(Or(e @ EqualTo(leftAttr: Expression, rightAttr: Expression),
        IsNull(e2 @ EqualTo(_, _)))), _)
        if SQLConf.get.optimizeNullAwareAntiJoin &&
          e.semanticEquals(e2) =>
      if (canEvaluate(leftAttr, left) && canEvaluate(rightAttr, right)) {
        Some(Seq(leftAttr), Seq(rightAttr))
      } else if (canEvaluate(leftAttr, right) && canEvaluate(rightAttr, left)) {
        Some(Seq(rightAttr), Seq(leftAttr))
      } else {
        None
      }
    case _ => None
  }
}

/**
 * An extractor for row-level commands such as DELETE, UPDATE, MERGE that were rewritten using plans
 * that operate on groups of rows.
 *
 * This class extracts the following entities:
 *  - the group-based rewrite plan;
 *  - the condition that defines matching groups;
 *  - the read relation that can be either [[DataSourceV2Relation]] or [[DataSourceV2ScanRelation]]
 *  depending on whether the planning has already happened;
 */
object GroupBasedRowLevelOperation {
  type ReturnType = (ReplaceData, Expression, LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case rd @ ReplaceData(DataSourceV2Relation(table, _, _, _, _), cond, query, _, _) =>
      val readRelation = findReadRelation(table, query)
      readRelation.map((rd, cond, _))

    case _ =>
      None
  }

  private def findReadRelation(
      table: Table,
      plan: LogicalPlan): Option[LogicalPlan] = {

    val readRelations = plan.collect {
      case r: DataSourceV2Relation if r.table eq table => r
      case r: DataSourceV2ScanRelation if r.relation.table eq table => r
    }

    // in some cases, the optimizer replaces the v2 read relation with a local relation
    // for example, there is no reason to query the table if the condition is always false
    // that's why it is valid not to find the corresponding v2 read relation

    readRelations match {
      case relations if relations.isEmpty =>
        None

      case Seq(relation) =>
        Some(relation)

      case relations =>
        throw new AnalysisException(s"Expected only one row-level read relation: $relations")
    }
  }
}
