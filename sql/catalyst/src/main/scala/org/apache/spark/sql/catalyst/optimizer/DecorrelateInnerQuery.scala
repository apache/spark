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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.OUTER_REFERENCE
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.collection.Utils

/**
 * Decorrelate the inner query by eliminating outer references and create domain joins.
 * The implementation is based on the paper: Unnesting Arbitrary Queries by Thomas Neumann
 * and Alfons Kemper. https://dl.gi.de/handle/20.500.12116/2418.
 *
 * A correlated subquery can be viewed as a "dependent" nested loop join between the outer and
 * the inner query. For each row produced by the outer query, we bind the [[OuterReference]]s in
 * in the inner query with the corresponding values in the row, and then evaluate the inner query.
 *
 * Dependent Join
 * :- Outer Query
 * +- Inner Query
 *
 * If the [[OuterReference]]s are bound to the same value, the inner query will return the same
 * result. Based on this, we can reduce the times to evaluate the inner query by first getting
 * all distinct values of the [[OuterReference]]s.
 *
 * Normal Join
 * :- Outer Query
 * +- Dependent Join
 *    :- Inner Query
 *    +- Distinct Aggregate (outer_ref1, outer_ref2, ...)
 *       +- Outer Query
 *
 * The distinct aggregate of the outer references is called a "domain", and the dependent join
 * between the inner query and the domain is called a "domain join". We need to push down the
 * domain join through the inner query until there is no outer reference in the sub-tree and
 * the domain join will turn into a normal join.
 *
 * The decorrelation function returns a new query plan with optional placeholder [[DomainJoins]]s
 * added and a list of join conditions with the outer query. [[DomainJoin]]s need to be rewritten
 * into actual inner join between the inner query sub-tree and the outer query.
 *
 * E.g. decorrelate an inner query with equality predicates:
 *
 * SELECT (SELECT MIN(b) FROM t1 WHERE t2.c = t1.a) FROM t2
 *
 * Aggregate [] [min(b)]            Aggregate [a] [min(b), a]
 * +- Filter (outer(c) = a)   =>   +- Relation [t1]
 *    +- Relation [t1]
 *
 * Join conditions: [c = a]
 *
 * E.g. decorrelate an inner query with non-equality predicates:
 *
 * SELECT (SELECT MIN(b) FROM t1 WHERE t2.c > t1.a) FROM t2
 *
 * Aggregate [] [min(b)]            Aggregate [c'] [min(b), c']
 * +- Filter (outer(c) > a)   =>   +- Filter (c' > a)
 *    +- Relation [t1]                  +- DomainJoin [c']
 *                                         +- Relation [t1]
 *
 * Join conditions: [c <=> c']
 */
object DecorrelateInnerQuery extends PredicateHelper {

  /**
   * Check if an expression contains any attribute. Note OuterReference is a
   * leaf node and will not be found here.
   */
  private def containsAttribute(expression: Expression): Boolean = {
    expression.exists(_.isInstanceOf[Attribute])
  }

  /**
   * Check if an expression can be pulled up over an [[Aggregate]] without changing the
   * semantics of the plan. The expression must be an equality predicate that guarantees
   * one-to-one mapping between inner and outer attributes.
   * For example:
   *   (a = outer(c)) -> true
   *   (a > outer(c)) -> false
   *   (a + b = outer(c)) -> false
   *   (a = outer(c) - b) -> false
   */
  def canPullUpOverAgg(expression: Expression): Boolean = {
    def isSupported(e: Expression): Boolean = e match {
      case _: Attribute => true
      // Allow Cast expressions that guarantee 1:1 mapping.
      case Cast(a: Attribute, dataType, _, _) => Cast.canUpCast(a.dataType, dataType)
      case _ => false
    }

    // Only allow equality condition with one side being an attribute or an expression that
    // guarantees 1:1 mapping and another side being an expression without attributes from
    // the inner query.
    expression match {
      case Equality(a, b) if isSupported(a) => !containsAttribute(b)
      case Equality(a, b) if isSupported(b) => !containsAttribute(a)
      case o => !containsAttribute(o)
    }
  }

  /**
   * Collect outer references in an expressions that are in the output attributes of the outer plan.
   */
  private def collectOuterReferences(expression: Expression): AttributeSet = {
    AttributeSet(expression.collect { case o: OuterReference => o.toAttribute })
  }

  /**
   * Collect outer references in a sequence of expressions that are in the output attributes
   * of the outer plan.
   */
  private def collectOuterReferences(expressions: Seq[Expression]): AttributeSet = {
    AttributeSet.fromAttributeSets(expressions.map(collectOuterReferences))
  }

  /**
   * Collect outer references in all expressions in a plan tree.
   */
  private def collectOuterReferencesInPlanTree(plan: LogicalPlan): AttributeSet = {
    AttributeSet(plan.flatMap(
      _.expressions.flatMap(
        _.collect { case o: OuterReference => o.toAttribute })))
  }

  /**
   * Build a mapping between outer references with equivalent inner query attributes.
   * E.g. [outer(a) = x, y = outer(b), outer(c) = z + 1] => {a -> x, b -> y}
   */
  private def collectEquivalentOuterReferences(
      expressions: Seq[Expression]): AttributeMap[Attribute] = {
    AttributeMap(expressions.collect {
      case Equality(o: OuterReference, a: Attribute) => (o.toAttribute, a.toAttribute)
      case Equality(a: Attribute, o: OuterReference) => (o.toAttribute, a.toAttribute)
    })
  }

  /**
   * Replace all outer references using the expressions in the given outer reference map.
   */
  private def replaceOuterReference[E <: Expression](
      expression: E,
      outerReferenceMap: AttributeMap[Attribute]): E = {
    expression.transformWithPruning(_.containsPattern(OUTER_REFERENCE)) {
      case o: OuterReference => outerReferenceMap.getOrElse(o.toAttribute, o)
    }.asInstanceOf[E]
  }

  /**
   * Replace all outer references in the given expressions using the expressions in the
   * outer reference map.
   */
  private def replaceOuterReferences[E <: Expression](
      expressions: Seq[E],
      outerReferenceMap: AttributeMap[Attribute]): Seq[E] = {
    expressions.map(replaceOuterReference(_, outerReferenceMap))
  }

  /**
   * Replace all outer references in the given named expressions and keep the output
   * attributes unchanged.
   */
  private def replaceOuterInNamedExpressions(
      expressions: Seq[NamedExpression],
      outerReferenceMap: AttributeMap[Attribute]): Seq[NamedExpression] = {
    expressions.map { expr =>
      val newExpr = replaceOuterReference(expr, outerReferenceMap)
      if (!newExpr.toAttribute.semanticEquals(expr.toAttribute)) {
        Alias(newExpr, expr.name)(expr.exprId)
      } else {
        newExpr
      }
    }
  }

  /**
   * Return all references that are presented in the join conditions but not in the output
   * of the given named expressions.
   */
  private def missingReferences(
      namedExpressions: Seq[NamedExpression],
      joinCond: Seq[Expression]): AttributeSet = {
    val output = namedExpressions.map(_.toAttribute)
    AttributeSet(joinCond.flatMap(_.references)) -- AttributeSet(output)
  }

  /**
   * Deduplicate the inner and the outer query attributes and return an aliased
   * subquery plan and join conditions if duplicates are found. Duplicated attributes
   * can break the structural integrity when joining the inner and outer plan together.
   */
  def deduplicate(
      innerPlan: LogicalPlan,
      conditions: Seq[Expression],
      outerOutputSet: AttributeSet): (LogicalPlan, Seq[Expression]) = {
    val duplicates = innerPlan.outputSet.intersect(outerOutputSet)
    if (duplicates.nonEmpty) {
      val aliasMap = AttributeMap(duplicates.map { dup =>
        dup -> Alias(dup, dup.toString)()
      })
      val aliasedExpressions = innerPlan.output.map { ref =>
        aliasMap.getOrElse(ref, ref)
      }
      val aliasedProjection = Project(aliasedExpressions, innerPlan)
      val aliasedConditions = conditions.map(_.transform {
        case ref: Attribute => aliasMap.getOrElse(ref, ref).toAttribute
      })
      (aliasedProjection, aliasedConditions)
    } else {
      (innerPlan, conditions)
    }
  }

  /**
   * Build a mapping between domain attributes and corresponding outer query expressions
   * using the join conditions.
   */
  private def buildDomainAttrMap(
      conditions: Seq[Expression],
      domainAttrs: Seq[Attribute]): Map[Attribute, Expression] = {
    val domainAttrSet = AttributeSet(domainAttrs)
    conditions.collect {
      // When we build the join conditions between the domain attributes and outer references,
      // the left hand side is always the domain attribute used in the inner query and the right
      // hand side is the attribute from the outer query. Note here the right hand side of a
      // condition is not necessarily an attribute, for example it can be a literal (if foldable)
      // or a cast expression after the optimization.
      case EqualNullSafe(left: Attribute, right: Expression) if domainAttrSet.contains(left) =>
        left -> right
      // IsNull(x) results from a constant folding, so we restore the original predicate as
      // x <=> Null.
      case IsNull(child: Attribute) if domainAttrSet.contains(child) =>
        child -> Literal.create(null, child.dataType)
    }.toMap
  }

  /**
   * Rewrites a domain join cond so that it can be pushed to the right side of a
   * union/intersect/except operator.
   *
   * Example: Take a query like:
   * select * from t0 join lateral (
   *   select a from t1 where b < t0.x
   *   union all
   *   select b from t2 where c < t0.y)
   *
   * over tables t0(x, y), t1(a), t2(b).
   *
   * Step 1: After DecorrelateInnerQuery runs to introduce DomainJoins,
   * we have outer table t0 with attributes [x#1, y#2] and the subquery is a Union where
   * - the left side has DomainJoin [t0.x#4, t0.y#5] and output [t1.a#3, t0.x#4, t0.y#5]
   * - the right side has DomainJoin [t0.x#7, t0.y#8] and output [t2.b#6, t0.x#7, t0.y#8]
   * Here all the x and y attributes are from t0, but they are different instances from
   * different joins of t0.
   *
   * The domain join conditions are x#4 <=> x#1 and y#5 <=> y#2, i.e. it joins the attributes from
   * the original outer table with the attributes coming out of the DomainJoin of the left side,
   * because the output of a set op uses the attribute names from the left side.
   *
   * Step 2: rewriteDomainJoins runs, in which we arrive at this function.
   * In this function, we construct the domain join conditions for the children of the Union.
   * For the left side, those remain unchanged, while for the right side they are remapped to
   * use the attribute names of the right-side DomainJoin: x#7 <=> x#1 and y#8 <=> y#2.
   */
  def pushDomainConditionsThroughSetOperation(
      conditions: Seq[Expression],
      setOp: LogicalPlan, // Union or SetOperation
      child: LogicalPlan): Seq[Expression] = {
    // The output attributes are always equal to the left child's output
    assert(setOp.output.size == child.output.size)
    val map = AttributeMap(setOp.output.zip(child.output))
    conditions.collect {
      // The left hand side is the domain attribute used in the inner query and the right hand side
      // is the attribute from the outer query. (See comment above in buildDomainAttrMap.)
      // We need to remap the attribute names used in the inner query (left hand side) to account
      // for the different names in each union child. We should not remap the attribute names used
      // in the outer query.
      //
      // Note: the reason we can't just use the original joinCond from when the DomainJoin was
      // constructed is that constructing the DomainJoins happens much earlier than rewriting the
      // DomainJoins into actual joins, with many optimization steps in
      // between, which could change the attributes involved (e.g. CollapseProject).
      case EqualNullSafe(left: Attribute, right: Expression) =>
        EqualNullSafe(map.getOrElse(left, left), right)
    }
  }

  /**
   * This is to handle INTERSECT/EXCEPT DISTINCT which are rewritten to left semi/anti join in
   * ReplaceIntersectWithSemiJoin and ReplaceExceptWithAntiJoin.
   *
   * To rewrite the domain join on the right side, we need to remap the attributes in the domain
   * join cond, using the mapping between left and right sides in the semi/anti join cond.
   *
   * After DecorrelateInnerQuery, the domain join conds reference the output names of the
   * INTERSECT/EXCEPT, which come from the left side. When rewriting the DomainJoin in the
   * right child, we need to remap the domain attribute names to account for the different
   * names in the left vs right child, similar to pushDomainConditionsThroughSetOperation.
   * But after the rewrite to semi/anti join is performed, we instead need to do the remapping
   * based on the semi/anti join cond which contains equi-joins between the left and right
   * outputs.
   *
   * Example: Take a query like:
   * select * from t0 join lateral (
   *   select a from t1 where b < t0.x
   *   intersect distinct
   *   select b from t2 where c < t0.y)
   *
   * over tables t0(x, y), t1(a), t2(b).
   *
   * Step 1 (this is the same as the Union case described above):
   * After DecorrelateInnerQuery runs to introduce DomainJoins,
   * we have outer table t0 with attributes [x#1, y#2] and the subquery is a Intersect where
   * - the left side has DomainJoin [t0.x#4, t0.y#5] and output [t1.a#3, t0.x#4, t0.y#5]
   * - the right side has DomainJoin [t0.x#7, t0.y#8] and output [t2.b#6, t0.x#7, t0.y#8]
   * Here all the x and y attributes are from t0, but they are different instances from
   * different joins of t0.
   *
   * The domain join conditions are x#4 <=> x#1 and y#5 <=> y#2, i.e. it joins the attributes from
   * the original outer table with the attributes coming out of the DomainJoin of the left side,
   * because the output of a set op uses the attribute names from the left side.
   *
   * Step 2:
   * ReplaceIntersectWithSemiJoin runs and transforms the Intersect to
   * Join LeftSemi, (((a#3 <=> b#6) AND (x#4 <=> x#7)) AND (y#5 <=> y#8))
   * with equi-joins between the left and right outputs.
   * For EXCEPT DISTINCT the same thing happens but with anti join in ReplaceExceptWithAntiJoin.
   *
   * Step 3:
   * rewriteDomainJoins runs, in which we arrive at this function, which uses the
   * semijoin condition to construct the domain join cond remapping for the right side:
   * x#7 <=> x#1 and y#8 <=> y#2. These new conds together with the original domain join cond are
   * used to rewrite the DomainJoins.
   *
   * Note: This logic only applies to INTERSECT/EXCEPT DISTINCT. For INTERSECT/EXCEPT ALL,
   * step 1 is the same but instead of step 2, RewriteIntersectAll or RewriteExceptAll
   * replace the logical Intersect/Except operator with a combination of
   * Union, Aggregate, and Generate. Then the DomainJoin conds will go through
   * pushDomainConditionsThroughSetOperation, not this function.
   */
  def pushDomainConditionsThroughSemiAntiJoin(
      domainJoinConditions: Seq[Expression],
      join: Join
  ) : Seq[Expression] = {
    if (join.condition.isDefined
      && SQLConf.get.getConf(SQLConf.DECORRELATE_SET_OPS_ENABLED)) {
      // The domain conds will be like leftInner <=> outer, and the semi/anti join cond will be like
      // leftInner <=> rightInner. We add additional domain conds rightInner <=> outer which are
      // used to rewrite the right-side DomainJoin.
      val transitiveConds = splitConjunctivePredicates(join.condition.get).collect {
        case EqualNullSafe(joinLeft: Attribute, joinRight: Attribute) =>
          domainJoinConditions.collect {
            case EqualNullSafe(domainInner: Attribute, domainOuter: Expression)
              if domainInner.semanticEquals(joinLeft) =>
              EqualNullSafe(joinRight, domainOuter)
          }
      }.flatten
      domainJoinConditions ++ transitiveConds
    } else {
      domainJoinConditions
    }
  }

  /**
   * Rewrite all [[DomainJoin]]s in the inner query to actual joins with the outer query.
   */
  def rewriteDomainJoins(
      outerPlan: LogicalPlan,
      innerPlan: LogicalPlan,
      conditions: Seq[Expression]): LogicalPlan = innerPlan match {
    case d @ DomainJoin(domainAttrs, child, joinType, outerJoinCondition) =>
      val domainAttrMap = buildDomainAttrMap(conditions, domainAttrs)

      val newChild = joinType match {
        // Left outer domain joins are used to handle the COUNT bug.
        case LeftOuter =>
          // Replace the attributes in the domain join condition with the actual outer expressions
          // and use the new join conditions to rewrite domain joins in its child. For example:
          // DomainJoin [c'] LeftOuter (a = c') with domainAttrMap: { c' -> _1 }.
          // Then the new conditions to use will be [(a = _1)].
          assert(outerJoinCondition.isDefined,
            s"LeftOuter domain join should always have the join condition defined:\n$d")
          val newCond = outerJoinCondition.get.transform {
            case a: Attribute => domainAttrMap.getOrElse(a, a)
          }
          // Recursively rewrite domain joins using the new conditions.
          rewriteDomainJoins(outerPlan, child, splitConjunctivePredicates(newCond))
        case Inner =>
          // The decorrelation framework adds domain inner joins by traversing down the plan tree
          // recursively until it reaches a node that is not correlated with the outer query.
          // So the child node of a domain inner join shouldn't contain another domain join.
          assert(!child.exists(_.isInstanceOf[DomainJoin]),
            s"Child of a domain inner join shouldn't contain another domain join.\n$child")
          child
        case o =>
          throw SparkException.internalError(s"Unexpected domain join type $o")
      }

      // We should only rewrite a domain join when all corresponding outer plan attributes
      // can be found from the join condition.
      if (domainAttrMap.size == domainAttrs.size) {
        val groupingExprs = domainAttrs.map(domainAttrMap)
        val aggregateExprs = groupingExprs.zip(domainAttrs).map {
          // Rebuild the aliases.
          case (inputAttr, outputAttr) => Alias(inputAttr, outputAttr.name)(outputAttr.exprId)
        }
        // Construct a domain with the outer query plan.
        // DomainJoin [a', b']  =>  Aggregate [a, b] [a AS a', b AS b']
        //                          +- Relation [a, b]
        val domain = Aggregate(groupingExprs, aggregateExprs, outerPlan)
        newChild match {
          // A special optimization for OneRowRelation.
          // TODO: add a more general rule to optimize join with OneRowRelation.
          case _: OneRowRelation => domain
          // Construct a domain join.
          // Join joinType condition
          // :- Domain
          // +- Inner Query
          case _ => Join(domain, newChild, joinType, outerJoinCondition, JoinHint.NONE)
        }
      } else {
        throw SparkException.internalError(
          s"Unable to rewrite domain join with conditions: $conditions\n$d.")
      }
    case s @ (_ : Union | _: SetOperation) =>
      // Remap the domain attributes for the children of the set op - see comments on the function.
      s.mapChildren { child =>
        rewriteDomainJoins(outerPlan, child,
          pushDomainConditionsThroughSetOperation(conditions, s, child))
      }
    case j: Join if j.joinType == LeftSemi || j.joinType == LeftAnti =>
      // For the INTERSECT/EXCEPT DISTINCT case, the set op is rewritten to a semi/anti join and we
      // need to remap the domain attributes for the right child - see comments on the function.
      j.mapChildren { child =>
        rewriteDomainJoins(outerPlan, child,
          pushDomainConditionsThroughSemiAntiJoin(conditions, j))
      }
    case p: LogicalPlan =>
      p.mapChildren(rewriteDomainJoins(outerPlan, _, conditions))
  }

  def apply(
      innerPlan: LogicalPlan,
      outerPlan: LogicalPlan,
      handleCountBug: Boolean = false): (LogicalPlan, Seq[Expression]) = {
    val outputPlanInputAttrs = outerPlan.inputSet

    // The return type of the recursion.
    // The first parameter is a new logical plan with correlation eliminated.
    // The second parameter is a list of join conditions with the outer query.
    // The third parameter is a mapping between the outer references and equivalent
    // expressions from the inner query that is used to replace outer references.
    type ReturnType = (LogicalPlan, Seq[Expression], AttributeMap[Attribute])

    // Decorrelate the input plan.
    // parentOuterReferences: a set of parent outer references. As we recurse down we collect the
    // set of outer references that are part of the Domain, and use it to construct the DomainJoins
    // and join conditions.
    // aggregated: a boolean flag indicating whether the result of the plan will be aggregated
    // (or used as an input for a window function)
    // underSetOp: a boolean flag indicating whether a set operator (e.g. UNION) is a parent of the
    // inner plan.
    //
    // Steps:
    // 1. Recursively collects outer references from the inner query until it reaches a node
    //    that does not contain correlated value.
    // 2. Inserts an optional [[DomainJoin]] node to indicate whether a domain (inner) join is
    //    needed between the outer query and the specific sub-tree of the inner query.
    // 3. Returns a list of join conditions with the outer query and a mapping between outer
    //    references with references inside the inner query. The parent nodes need to preserve
    //    the references inside the join conditions and substitute all outer references using
    //    the mapping.
    def decorrelate(
        plan: LogicalPlan,
        parentOuterReferences: AttributeSet,
        aggregated: Boolean = false,
        underSetOp: Boolean = false
    ): ReturnType = {
      val isCorrelated = hasOuterReferences(plan)
      if (!isCorrelated) {
        // We have reached a plan without correlation to the outer plan.
        if (parentOuterReferences.isEmpty) {
          // If there is no outer references from the parent nodes, it means all outer
          // attributes can be substituted by attributes from the inner plan. So no
          // domain join is needed.
          (plan, Nil, AttributeMap.empty[Attribute])
        } else {
          // Build the domain join with the parent outer references.
          val attributes = parentOuterReferences.toSeq
          val domains = attributes.map(_.newInstance())
          // A placeholder to be rewritten into domain join.
          val domainJoin = DomainJoin(domains, plan)
          val outerReferenceMap = Utils.toMap(attributes, domains)
          // Build join conditions between domain attributes and outer references.
          // EqualNullSafe is used to make sure null key can be joined together. Note
          // outer referenced attributes can be changed during the outer query optimization.
          // The equality conditions will also serve as an attribute mapping between new
          // outer references and domain attributes when rewriting the domain joins.
          // E.g. if the attribute a is changed to a1, the join condition a' <=> outer(a)
          // will become a' <=> a1, and we can construct the aliases based on the condition:
          // DomainJoin [a']        Join Inner
          // +- InnerQuery     =>   :- InnerQuery
          //                        +- Aggregate [a1] [a1 AS a']
          //                           +- OuterQuery
          val conditions = outerReferenceMap.map {
            case (o, a) =>
              val cond = EqualNullSafe(a, OuterReference(o))
              // SPARK-40615: Certain data types (e.g. MapType) do not support ordering, so
              // the EqualNullSafe join condition can become unresolved.
              if (!cond.resolved) {
                if (!RowOrdering.isOrderable(a.dataType)) {
                  throw QueryCompilationErrors.unsupportedCorrelatedReferenceDataTypeError(
                    o, a.dataType, plan.origin)
                } else {
                  throw SparkException.internalError(s"Unable to decorrelate subquery: " +
                    s"join condition '${cond.sql}' cannot be resolved.")
                }
              }
              cond
          }
          (domainJoin, conditions.toSeq, AttributeMap(outerReferenceMap))
        }
      } else {
        plan match {
          case Filter(condition, child) =>
            val conditions = splitConjunctivePredicates(condition)
            val (correlated, uncorrelated) = conditions.partition(containsOuter)
            // Find outer references that can be substituted by attributes from the inner
            // query using the equality predicates.
            // If we are under a set op, we never use the predicates directly to substitute outer
            // refs for now. Future improvement: use the predicates directly if they exist in all
            // children of the set op.
            val equivalences =
              if (underSetOp) AttributeMap.empty[Attribute]
              else collectEquivalentOuterReferences(correlated)
            // Correlated predicates can be removed from the Filter's condition and used as
            // join conditions with the outer query. However, if the results of the sub-tree
            // is aggregated, only certain correlated equality predicates can be used, because
            // the references in the join conditions need to be preserved in both the grouping
            // and aggregate expressions of an Aggregate, which may change the semantics of the
            // plan and lead to incorrect results. Here is an example:
            // Relations:
            //   t1(a, b): [(1, 1)]
            //   t2(c, d): [(1, 1), (2, 0)]
            //
            // Query:
            //   SELECT * FROM t1 WHERE a = (SELECT MAX(c) FROM t2 WHERE b >= d)
            //
            // Subquery plan transformation if correlated predicates are used as join conditions:
            //   Aggregate [max(c)]               Aggregate [d] [max(c), d]
            //   +- Filter (outer(b) >= d)   =>   +- Relation [c, d]
            //      +- Relation [c, d]
            //
            // Plan after rewrite:
            //   Project [a, b]                                   -- [(1, 1)]
            //   +- Join LeftOuter (b >= d AND a = max(c))
            //      :- Relation [a, b]
            //      +- Aggregate [d] [max(c), d]                  -- [(1, 1), (2, 0)]
            //         +- Relation [c, d]
            //
            // The result of the original query should be an empty set but the transformed
            // query will output an incorrect result of (1, 1). The correct transformation
            // with domain join is illustrated below:
            //   Aggregate [max(c)]               Aggregate [b'] [max(c), b']
            //   +- Filter (outer(b) >= d)   =>   +- Filter (b' >= d)
            //      +- Relation [c, d]               +- DomainJoin [b']
            //                                          +- Relation [c, d]
            // Plan after rewrite:
            //   Project [a, b]
            //   +- Join LeftOuter (b <=> b' AND a = max(c))  -- []
            //      :- Relation [a, b]
            //      +- Aggregate [b'] [max(c), b']            -- [(2, 1)]
            //         +- Join Inner (b' >= d)                -- [(1, 1, 1), (2, 0, 1)] (DomainJoin)
            //            :- Relation [c, d]
            //            +- Aggregate [b] [b AS b']          -- [(1)] (Domain)
            //               +- Relation [a, b]
            if (aggregated || underSetOp) {
              // Split the correlated predicates into predicates that can and cannot be directly
              // used as join conditions with the outer query depending on whether they can
              // be pulled up over an Aggregate without changing the semantics of the plan.
              // If we are under a set op, we never use the predicates directly for now. Future
              // improvement: use the predicates directly if they exist in all children of the set
              // op.
              val (equalityCond, predicates) =
                if (underSetOp) (Seq.empty[Expression], correlated)
                else correlated.partition(canPullUpOverAgg)
              val outerReferences = collectOuterReferences(predicates)
              val newOuterReferences =
                parentOuterReferences ++ outerReferences -- equivalences.keySet
              val (newChild, joinCond, outerReferenceMap) =
                decorrelate(child, newOuterReferences, aggregated, underSetOp)
              // Add the outer references mapping collected from the equality conditions.
              val newOuterReferenceMap = outerReferenceMap ++ equivalences
              // Replace all outer references in the non-equality predicates.
              val newCorrelated = replaceOuterReferences(predicates, newOuterReferenceMap)
              // The new filter condition is the original filter condition with correlated
              // equality predicates removed.
              val newFilterCond = newCorrelated ++ uncorrelated
              val newFilter = newFilterCond match {
                case Nil => newChild
                case conditions => Filter(conditions.reduce(And), newChild)
              }
              // Equality predicates are used as join conditions with the outer query.
              val newJoinCond = joinCond ++ equalityCond
              (newFilter, newJoinCond, newOuterReferenceMap)
            } else {
              // Results of this sub-tree is not aggregated, so all correlated predicates
              // can be directly used as outer query join conditions.
              val newOuterReferences = parentOuterReferences -- equivalences.keySet
              val (newChild, joinCond, outerReferenceMap) =
                decorrelate(child, newOuterReferences, aggregated, underSetOp)
              // Add the outer references mapping collected from the equality conditions.
              val newOuterReferenceMap = outerReferenceMap ++ equivalences
              val newFilter = uncorrelated match {
                case Nil => newChild
                case conditions => Filter(conditions.reduce(And), newChild)
              }
              val newJoinCond = joinCond ++ correlated
              (newFilter, newJoinCond, newOuterReferenceMap)
            }

          case Project(projectList, child) =>
            val outerReferences = collectOuterReferences(projectList)
            val newOuterReferences = parentOuterReferences ++ outerReferences
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(child, newOuterReferences, aggregated, underSetOp)
            // Replace all outer references in the original project list and keep the output
            // attributes unchanged.
            val newProjectList = replaceOuterInNamedExpressions(projectList, outerReferenceMap)
            // Preserve required domain attributes in the join condition by adding the missing
            // references to the new project list.
            val referencesToAdd = missingReferences(newProjectList, joinCond)
            val newProject = Project(newProjectList ++ referencesToAdd, newChild)
            (newProject, joinCond, outerReferenceMap)

          case Offset(offset, input) =>
            // OFFSET K is decorrelated by skipping top k rows per every domain value
            // via a row_number() window function, which is similar to limit decorrelation.
            // Limit and Offset situation are handled by limit branch as offset is the child
            // of limit in that case. This branch is for the case where there's no limit operator
            // above offset.
            val (child, ordering) = input match {
              case Sort(order, _, child) => (child, order)
              case _ => (input, Seq())
            }
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(input, parentOuterReferences, aggregated = true, underSetOp)
            val collectedChildOuterReferences = collectOuterReferencesInPlanTree(child)
            // Add outer references to the PARTITION BY clause
            val partitionFields = collectedChildOuterReferences
              .filter(outerReferenceMap.contains(_))
              .map(outerReferenceMap(_)).toSeq
            if (partitionFields.isEmpty) {
              // Underlying subquery has no predicates connecting inner and outer query.
              // In this case, offset can be computed over the inner query directly.
              (Offset(offset, newChild), joinCond, outerReferenceMap)
            } else {
              val orderByFields = replaceOuterReferences(ordering, outerReferenceMap)

              val rowNumber = WindowExpression(RowNumber(),
                WindowSpecDefinition(partitionFields, orderByFields,
                  SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)))
              val rowNumberAlias = Alias(rowNumber, "rn")()
              // Window function computes row_number() when partitioning by correlated references,
              // and projects all the other fields from the input.
              val window = Window(Seq(rowNumberAlias),
                partitionFields, orderByFields, newChild)
              val filter = Filter(GreaterThan(rowNumberAlias.toAttribute, offset), window)
              val project = Project(newChild.output, filter)
              (project, joinCond, outerReferenceMap)
            }

          case Limit(limit, input) =>
            // LIMIT K (with potential ORDER BY or OFFSET) is decorrelated by computing
            // K rows per every domain value via a row_number() window function.
            // For example, for a subquery
            // (SELECT T2.a FROM T2 WHERE T2.b = OuterReference(x) ORDER BY T2.c LIMIT 3 OFFSET 2)
            // -- we need to get top 3 values of T2.a (ordering by T2.c) for every value of x with
            // an offset 2.
            // Following our general decorrelation procedure, 'x' is then replaced by T2.b, so the
            // subquery is decorrelated as:
            // SELECT * FROM (
            //   SELECT T2.a, row_number() OVER (PARTITION BY T2.b ORDER BY T2.c) AS rn FROM T2)
            // WHERE rn > 2 AND rn <= 2+3
            val (child, ordering, offsetExpr) = input match {
              case Sort(order, _, child) => (child, order, Literal(0))
              case Offset(offsetExpr, offsetChild@(Sort(order, _, child))) =>
                (child, order, offsetExpr)
              case Offset(offsetExpr, child) =>
                (child, Seq(), offsetExpr)
              case _ => (input, Seq(), Literal(0))
            }
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(child, parentOuterReferences, aggregated = true, underSetOp)
            val collectedChildOuterReferences = collectOuterReferencesInPlanTree(child)
            // Add outer references to the PARTITION BY clause
            val partitionFields = collectedChildOuterReferences
              .filter(outerReferenceMap.contains(_))
              .map(outerReferenceMap(_)).toSeq
            if (partitionFields.isEmpty) {
              // Underlying subquery has no predicates connecting inner and outer query.
              // In this case, limit can be computed over the inner query directly.
              offsetExpr match {
                case IntegerLiteral(0) => (Limit(limit, newChild), joinCond, outerReferenceMap)
                case _ => (Limit(limit, Offset(offsetExpr, newChild)), joinCond, outerReferenceMap)
              }
            } else {
              val orderByFields = replaceOuterReferences(ordering, outerReferenceMap)

              val rowNumber = WindowExpression(RowNumber(),
                WindowSpecDefinition(partitionFields, orderByFields,
                  SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)))
              val rowNumberAlias = Alias(rowNumber, "rn")()
              // Window function computes row_number() when partitioning by correlated references,
              // and projects all the other fields from the input.
              val window = Window(Seq(rowNumberAlias),
                partitionFields, orderByFields, newChild)
              val filter = offsetExpr match {
                case IntegerLiteral(0) =>
                  // If there is no offset, we can directly use the row number to filter the rows.
                  Filter(LessThanOrEqual(rowNumberAlias.toAttribute, limit), window)
                case _ =>
                  Filter(
                    And(
                      GreaterThan(rowNumberAlias.toAttribute, offsetExpr),
                      LessThanOrEqual(rowNumberAlias.toAttribute, Add(offsetExpr, limit))
                    ),
                    window
                  )
              }
              val project = Project(newChild.output, filter)
              (project, joinCond, outerReferenceMap)
            }

          case w @ Window(projectList, partitionSpec, orderSpec, child) =>
            val outerReferences = collectOuterReferences(w.expressions)
            assert(outerReferences.isEmpty, s"Correlated column is not allowed in window " +
              s"function: $w")
            val newOuterReferences = parentOuterReferences ++ outerReferences
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(child, newOuterReferences, aggregated = true, underSetOp)
            // For now these are no-op, as we don't allow correlated references in the window
            // function itself.
            val newProjectList = replaceOuterReferences(projectList, outerReferenceMap)
            val newPartitionSpec = replaceOuterReferences(partitionSpec, outerReferenceMap)
            val newOrderSpec = replaceOuterReferences(orderSpec, outerReferenceMap)
            val referencesToAdd = missingReferences(newProjectList, joinCond)

            val newWindow = Window(newProjectList ++ referencesToAdd,
              partitionSpec = newPartitionSpec ++ referencesToAdd,
              orderSpec = newOrderSpec, newChild)
            (newWindow, joinCond, outerReferenceMap)

          case a @ Aggregate(groupingExpressions, aggregateExpressions, child, _) =>
            val outerReferences = collectOuterReferences(a.expressions)
            val newOuterReferences = parentOuterReferences ++ outerReferences
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(child, newOuterReferences, aggregated = true, underSetOp)
            // Replace all outer references in grouping and aggregate expressions, and keep
            // the output attributes unchanged.
            val newGroupingExpr = replaceOuterReferences(groupingExpressions, outerReferenceMap)
            val newAggExpr = replaceOuterInNamedExpressions(aggregateExpressions, outerReferenceMap)
            // Add all required domain attributes to both grouping and aggregate expressions.
            val referencesToAdd = missingReferences(newAggExpr, joinCond)
            val newAggregate = a.copy(
              groupingExpressions = newGroupingExpr ++ referencesToAdd,
              aggregateExpressions = newAggExpr ++ referencesToAdd,
              child = newChild)

            // Preserving domain attributes over an Aggregate with an empty grouping expression
            // is subject to the "COUNT bug" that can lead to wrong answer:
            //
            // Suppose the original query is:
            //   SELECT a, (SELECT COUNT(*) cnt FROM t2 WHERE t1.a = t2.c) FROM t1
            //
            // Decorrelated plan:
            //   Project [a, scalar-subquery [a = c]]
            //   :  +- Aggregate [c] [count(*) AS cnt, c]
            //   :     +- Relation [c, d]
            //   +- Relation [a, b]
            //
            // After rewrite:
            //   Project [a, cnt]
            //   +- Join LeftOuter (a = c)
            //      :- Relation [a, b]
            //      +- Aggregate [c] [count(*) AS cnt, c]
            //         +- Relation [c, d]
            //
            //     T1            T2          T2' (GROUP BY c)
            // +---+---+     +---+---+     +---+-----+
            // | a | b |     | c | d |     | c | cnt |
            // +---+---+     +---+---+     +---+-----+
            // | 0 | 1 |     | 0 | 2 |     | 0 | 2   |
            // | 1 | 2 |     | 0 | 3 |     +---+-----+
            // +---+---+     +---+---+
            //
            // T1 nested loop join T2     T1 left outer join T2'
            // on (a = c):                on (a = c):
            // +---+-----+                +---+-----++
            // | a | cnt |                | a | cnt  |
            // +---+-----+                +---+------+
            // | 0 | 2   |                | 0 | 2    |
            // | 1 | 0   | <--- correct   | 1 | null | <--- wrong result
            // +---+-----+                +---+------+
            //
            // If an aggregate is subject to the COUNT bug:
            // 1) add a column `true AS alwaysTrue` to the result of the aggregate
            // 2) insert a left outer domain join between the outer query and this aggregate
            // 3) rewrite the original aggregate's output column using the default value of the
            //    aggregate function and the alwaysTrue column.
            //
            // For example, T1 left outer join T2' with `alwaysTrue` marker:
            // +---+------+------------+--------------------------------+
            // | c | cnt  | alwaysTrue | if(isnull(alwaysTrue), 0, cnt) |
            // +---+------+------------+--------------------------------+
            // | 0 | 2    | true       | 2                              |
            // | 0 | null | null       | 0                              |  <--- correct result
            // +---+------+------------+--------------------------------+
            if (groupingExpressions.isEmpty && handleCountBug) {
              // Evaluate the aggregate expressions with zero tuples.
              val resultMap = RewriteCorrelatedScalarSubquery.evalAggregateOnZeroTups(newAggregate)
              val alwaysTrue = Alias(Literal.TrueLiteral, "alwaysTrue")()
              val alwaysTrueRef = alwaysTrue.toAttribute.withNullability(true)
              val expressions = ArrayBuffer.empty[NamedExpression]
              // Create new aliases for aggregate expressions that have non-null default
              // values and reconstruct the output with the `alwaysTrue` marker.
              val projectList = newAggregate.aggregateExpressions.map { a =>
                resultMap.get(a.exprId) match {
                  // Aggregate expression is not subject to the count bug.
                  case Some(Literal(null, _)) | None =>
                    expressions += a
                    // The attribute is nullable since it is from the right-hand side of a
                    // left outer join.
                    a.toAttribute.withNullability(true)
                  case Some(default) =>
                    assert(a.isInstanceOf[Alias], s"Cannot have non-aliased expression $a in " +
                      s"aggregate that evaluates to non-null value with zero tuples.")
                    val newAttr = a.newInstance()
                    val ref = newAttr.toAttribute.withNullability(true)
                    expressions += newAttr
                    Alias(If(IsNull(alwaysTrueRef), default, ref), a.name)(a.exprId)
                }
              }
              // Insert a placeholder left outer domain join between the outer query and
              // and aggregate node and use the current collected join conditions as the
              // left outer join condition.
              //
              // Original subquery:
              //   Aggregate [count(1) AS cnt]
              //   +- Filter (a = outer(c))
              //      +- Relation [a, b]
              //
              // After decorrelation and before COUNT bug handling:
              //   Aggregate [a] [count(1) AS cnt, a]
              //   +- Relation [a, b]
              //
              // joinCond with the outer query: (a = outer(c))
              //
              // Handle the COUNT bug:
              //   Project [if(isnull(alwaysTrue), 0, cnt') AS cnt, c']
              //   +- DomainJoin [c'] LeftOuter (a = c')
              //      +- Aggregate [a] [count(1) AS cnt', a, true AS alwaysTrue]
              //         +- Relation [a, b]
              //
              // New joinCond with the outer query: (c' <=> outer(c)), and the DomainJoin
              // will be written as:
              //   Project [if(isnull(alwaysTrue), 0, cnt') AS cnt, c']
              //   +- Join LeftOuter (a = c')
              //      :- Aggregate [c] [c AS c']
              //      :  +- OuterQuery [c, d]
              //      +- Aggregate [a] [count(1) AS cnt', a, true AS alwaysTrue]
              //         +- Relation [a, b]
              //
              val agg = newAggregate.copy(aggregateExpressions = expressions.toSeq :+ alwaysTrue)
              // Find all outer references that are used in the join conditions.
              val outerAttrs = collectOuterReferences(joinCond).toSeq
              // Create new instance of the outer attributes as if they are generated inside
              // the subquery by a left outer join with the outer query. Use new instance here
              // to avoid conflicting join attributes with the inner query.
              val domainAttrs = outerAttrs.map(_.newInstance())
              val mapping = AttributeMap(outerAttrs.zip(domainAttrs))
              // Use the current join conditions returned from the recursive call as the join
              // conditions for the left outer join. All outer references in the join
              // conditions are replaced by the newly created domain attributes.
              val condition = replaceOuterReferences(joinCond, mapping).reduceOption(And)
              val domainJoin = DomainJoin(domainAttrs, agg, LeftOuter, condition)
              // Original domain attributes preserved through Aggregate are no longer needed.
              val newProjectList = projectList.filter(!referencesToAdd.contains(_))
              val project = Project(newProjectList ++ domainAttrs, domainJoin)
              val newJoinCond = outerAttrs.zip(domainAttrs).map { case (outer, inner) =>
                EqualNullSafe(inner, OuterReference(outer))
              }
              (project, newJoinCond, mapping)
            } else {
              (newAggregate, joinCond, outerReferenceMap)
            }

          case d: Distinct =>
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(d.child, parentOuterReferences, aggregated = true, underSetOp)
            (d.copy(child = newChild), joinCond, outerReferenceMap)

          case j @ Join(left, right, joinType, condition, _) =>
            // Given 'condition', computes the tuple of
            // (correlated, uncorrelated, equalityCond, predicates, equivalences).
            // 'correlated' and 'uncorrelated' are the conjuncts with (resp. without)
            // outer (correlated) references. Furthermore, correlated conjuncts are split
            // into 'equalityCond' (those that are equalities) and all rest ('predicates').
            // 'equivalences' track equivalent attributes given 'equalityCond'.
            // The split is only performed if 'shouldDecorrelatePredicates' is true.
            // The input parameter 'isInnerJoin' is set to true for INNER joins and helps
            // determine whether some predicates can be lifted up from the join (this is only
            // valid for inner joins).
            // Example: For a 'condition' A = outer(X) AND B > outer(Y) AND C = D, the output
            // would be:
            // correlated = (A = outer(X), B > outer(Y))
            // uncorrelated = (C = D)
            // equalityCond = (A = outer(X))
            // predicates = (B > outer(Y))
            // equivalences: (A -> outer(X))
            def splitCorrelatedPredicate(
                condition: Option[Expression],
                isInnerJoin: Boolean,
                shouldDecorrelatePredicates: Boolean):
            (Seq[Expression], Seq[Expression], Seq[Expression],
              Seq[Expression], AttributeMap[Attribute]) = {
              // Similar to Filters above, we split the join condition (if present) into correlated
              // and uncorrelated predicates, and separately handle joins under set and aggregation
              // operations.
              if (shouldDecorrelatePredicates) {
                val conditions =
                  if (condition.isDefined) splitConjunctivePredicates(condition.get)
                  else Seq.empty[Expression]
                val (correlated, uncorrelated) = conditions.partition(containsOuter)
                var equivalences =
                  if (underSetOp) AttributeMap.empty[Attribute]
                  else collectEquivalentOuterReferences(correlated)
                var (equalityCond, predicates) =
                  if (underSetOp) (Seq.empty[Expression], correlated)
                  else correlated.partition(canPullUpOverAgg)
                // Fully preserve the join predicate for non-inner joins.
                if (!isInnerJoin) {
                  predicates = correlated
                  equalityCond = Seq.empty[Expression]
                  equivalences = AttributeMap.empty[Attribute]
                }
                (correlated, uncorrelated, equalityCond, predicates, equivalences)
              } else {
                (Seq.empty[Expression],
                  if (condition.isEmpty) Seq.empty[Expression] else Seq(condition.get),
                  Seq.empty[Expression],
                  Seq.empty[Expression],
                  AttributeMap.empty[Attribute])
              }
            }

            val shouldDecorrelatePredicates =
              SQLConf.get.getConf(SQLConf.DECORRELATE_JOIN_PREDICATE_ENABLED)
            if (!shouldDecorrelatePredicates) {
              val outerReferences = collectOuterReferences(j.expressions)
              // Join condition containing outer references is not supported.
              assert(outerReferences.isEmpty, s"Correlated column is not allowed in join: $j")
            }
            val (correlated, uncorrelated, equalityCond, predicates, equivalences) =
              splitCorrelatedPredicate(condition, joinType == Inner, shouldDecorrelatePredicates)
            val outerReferences = collectOuterReferences(j.expressions) ++
              collectOuterReferences(predicates)
            val newOuterReferences =
              parentOuterReferences ++ outerReferences -- equivalences.keySet
            var shouldPushToLeft = joinType match {
              case LeftOuter | LeftSemiOrAnti(_) | FullOuter => true
              case _ => hasOuterReferences(left)
            }
            val shouldPushToRight = joinType match {
              case RightOuter | FullOuter => true
              case _ => hasOuterReferences(right)
            }
            if (shouldDecorrelatePredicates && !shouldPushToLeft && !shouldPushToRight
              && !predicates.isEmpty) {
              // Neither left nor right children of the join have correlations, but the join
              // predicate does, and the correlations can not be replaced via equivalences.
              // Introduce a domain join on the left side of the join
              // (chosen arbitrarily) to provide values for the correlated attribute reference.
              shouldPushToLeft = true;
            }
            val (newLeft, leftJoinCond, leftOuterReferenceMap) = if (shouldPushToLeft) {
              decorrelate(left, newOuterReferences, aggregated, underSetOp)
            } else {
              (left, Nil, AttributeMap.empty[Attribute])
            }
            val (newRight, rightJoinCond, rightOuterReferenceMap) = if (shouldPushToRight) {
              decorrelate(right, newOuterReferences, aggregated, underSetOp)
            } else {
              (right, Nil, AttributeMap.empty[Attribute])
            }
            val newOuterReferenceMap = leftOuterReferenceMap ++ rightOuterReferenceMap ++
              equivalences
            val newCorrelated =
              if (shouldDecorrelatePredicates) {
                replaceOuterReferences(correlated, newOuterReferenceMap)
              } else Seq.empty[Expression]
            val newJoinCond = leftJoinCond ++ rightJoinCond ++ equalityCond
            // If we push the dependent join to both sides, we can augment the join condition
            // such that both sides are matched on the domain attributes. For example,
            // - Left Map: {outer(c1) = c1}
            // - Right Map: {outer(c1) = 10 - c1}
            // Then the join condition can be augmented with (c1 <=> 10 - c1).
            val augmentedConditions = leftOuterReferenceMap.flatMap {
              case (outer, inner) => rightOuterReferenceMap.get(outer).map(EqualNullSafe(inner, _))
            }
            val newCondition = (newCorrelated ++ uncorrelated
              ++ augmentedConditions).reduceOption(And)
            val newJoin = j.copy(left = newLeft, right = newRight, condition = newCondition)
            (newJoin, newJoinCond, newOuterReferenceMap)

          case s @ (_ : Union | _: SetOperation) =>
            // Set ops are decorrelated by pushing the domain join into each child. For details see
            // https://docs.google.com/document/d/11b9ClCF2jYGU7vU2suOT7LRswYkg6tZ8_6xJbvxfh2I/edit

            // First collect outer references from all children - these must all be added to the
            // Domain (otherwise we’d be unioning together inner values corresponding to different
            // outer values).
            //
            // As an example, this inner subquery:
            //   select c from t1 where t1.a = t_outer.a
            //   UNION ALL
            //   select c from t2 where t2.b = t_outer.b
            // has columns a, b in the Domain and is rewritten to:
            //   select c, t_outer.a, t_outer.b from t1 join t_outer where t1.a = t_outer.a
            //   UNION ALL
            //   select c, t_outer.a, t_outer.b from t2 join t_outer where t2.b = t_outer.b
            val collectedChildOuterReferences = collectOuterReferencesInPlanTree(s)
            val newOuterReferences = AttributeSet(
              parentOuterReferences ++ collectedChildOuterReferences)

            val childDecorrelateResults =
              s.children.map { child =>
                val (decorrelatedChild, newJoinCond, newOuterReferenceMap) =
                  decorrelate(child, newOuterReferences, aggregated, underSetOp = true)
                // Create a Project to ensure that the domain attributes are added to the same
                // positions in each child of the set op. If we don't explicitly construct this
                // Project, they could get added at the beginning or the end of the output columns
                // depending on the child plan.
                // The inner expressions for the domain are the values of newOuterReferenceMap.
                val domainProjections = newOuterReferences.map(newOuterReferenceMap(_))
                val newChild = Project(child.output ++ domainProjections, decorrelatedChild)
                (newChild, newJoinCond, newOuterReferenceMap)
              }

            val newChildren = childDecorrelateResults.map(_._1)
            // Need to use the join cond and outer ref map from the first child, because attribute
            // names are from the first child
            val newJoinCond = childDecorrelateResults.head._2
            val newOuterReferenceMap = AttributeMap(childDecorrelateResults.head._3)
            (s.withNewChildren(newChildren), newJoinCond, newOuterReferenceMap)

          case g: Generate if g.requiredChildOutput.isEmpty =>
            // Generate with non-empty required child output cannot host
            // outer reference. It is blocked by CheckAnalysis.
            val outerReferences = collectOuterReferences(g.expressions)
            val newOuterReferences = parentOuterReferences ++ outerReferences
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(g.child, newOuterReferences, aggregated)
            // Replace all outer references in the original generator expression.
            val newGenerator = replaceOuterReference(g.generator, outerReferenceMap)
            val newGenerate = g.copy(generator = newGenerator, child = newChild)
            (newGenerate, joinCond, outerReferenceMap)

          case u: UnaryNode =>
            val outerReferences = collectOuterReferences(u.expressions)
            assert(outerReferences.isEmpty, s"Correlated column is not allowed in $u")
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(u.child, parentOuterReferences, aggregated, underSetOp)
            (u.withNewChildren(newChild :: Nil), joinCond, outerReferenceMap)

          case o =>
            throw QueryExecutionErrors.decorrelateInnerQueryThroughPlanUnsupportedError(o)
        }
      }
    }
    val (newChild, joinCond, _) = decorrelate(BooleanSimplification(innerPlan), AttributeSet.empty)
    val (plan, conditions) = deduplicate(newChild, joinCond, outputPlanInputAttrs)
    (plan, stripOuterReferences(conditions))
  }
}
