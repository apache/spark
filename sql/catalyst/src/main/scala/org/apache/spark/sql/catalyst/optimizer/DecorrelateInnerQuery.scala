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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.OUTER_REFERENCE
import org.apache.spark.sql.errors.QueryExecutionErrors

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
    expression.find(_.isInstanceOf[Attribute]).isDefined
  }

  /**
   * Check if an expression can be pulled up over an [[Aggregate]] without changing the
   * semantics of the plan. The expression must be an equality predicate that guarantees
   * one-to-one mapping between inner and outer attributes. More specifically, one side
   * of the predicate must be an attribute and another side of the predicate must not
   * contain other attributes from the inner query.
   * For example:
   *   (a = outer(c)) -> true
   *   (a > outer(c)) -> false
   *   (a + b = outer(c)) -> false
   *   (a = outer(c) - b) -> false
   */
  private def canPullUpOverAgg(expression: Expression): Boolean = expression match {
    case Equality(_: Attribute, b) => !containsAttribute(b)
    case Equality(a, _: Attribute) => !containsAttribute(a)
    case o => !containsAttribute(o)
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
   * Build a mapping between outer references with equivalent inner query attributes.
   * E.g. [outer(a) = x, y = outer(b), outer(c) = z + 1] => {a -> x, b -> y}
   */
  private def collectEquivalentOuterReferences(
      expressions: Seq[Expression]): Map[Attribute, Attribute] = {
    expressions.collect {
      case Equality(o: OuterReference, a: Attribute) => (o.toAttribute, a.toAttribute)
      case Equality(a: Attribute, o: OuterReference) => (o.toAttribute, a.toAttribute)
    }.toMap
  }

  /**
   * Replace all outer references using the expressions in the given outer reference map.
   */
  private def replaceOuterReference[E <: Expression](
      expression: E,
      outerReferenceMap: Map[Attribute, Attribute]): E = {
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
      outerReferenceMap: Map[Attribute, Attribute]): Seq[E] = {
    expressions.map(replaceOuterReference(_, outerReferenceMap))
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
      }.toSeq)
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
    }.toMap
  }

  /**
   * Rewrite all [[DomainJoin]]s in the inner query to actual inner joins with the outer query.
   */
  def rewriteDomainJoins(
      outerPlan: LogicalPlan,
      innerPlan: LogicalPlan,
      conditions: Seq[Expression]): LogicalPlan = innerPlan match {
    case d @ DomainJoin(domainAttrs, child) =>
      val domainAttrMap = buildDomainAttrMap(conditions, domainAttrs)
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
        child match {
          // A special optimization for OneRowRelation.
          // TODO: add a more general rule to optimize join with OneRowRelation.
          case _: OneRowRelation => domain
          // Construct a domain join.
          // Join Inner
          // :- Inner Query
          // +- Domain
          case _ =>
            // The decorrelation framework adds domain joins by traversing down the plan tree
            // recursively until it reaches a node that is not correlated with the outer query.
            // So the child node of a domain join shouldn't contain another domain join.
            assert(child.find(_.isInstanceOf[DomainJoin]).isEmpty,
              s"Child of a domain join shouldn't contain another domain join.\n$child")
            Join(child, domain, Inner, None, JoinHint.NONE)
        }
      } else {
        throw QueryExecutionErrors.cannotRewriteDomainJoinWithConditionsError(conditions, d)
      }
    case p: LogicalPlan =>
      p.mapChildren(rewriteDomainJoins(outerPlan, _, conditions))
  }

  def apply(
      innerPlan: LogicalPlan,
      outerPlan: LogicalPlan): (LogicalPlan, Seq[Expression]) = {
    apply(innerPlan, Seq(outerPlan))
  }

  def apply(
      innerPlan: LogicalPlan,
      outerPlans: Seq[LogicalPlan]): (LogicalPlan, Seq[Expression]) = {
    val outputSet = AttributeSet(outerPlans.flatMap(_.outputSet))

    // The return type of the recursion.
    // The first parameter is a new logical plan with correlation eliminated.
    // The second parameter is a list of join conditions with the outer query.
    // The third parameter is a mapping between the outer references and equivalent
    // expressions from the inner query that is used to replace outer references.
    type ReturnType = (LogicalPlan, Seq[Expression], Map[Attribute, Attribute])

    // Decorrelate the input plan with a set of parent outer references and a boolean flag
    // indicating whether the result of the plan will be aggregated. Steps:
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
        aggregated: Boolean = false): ReturnType = {
      val isCorrelated = hasOuterReferences(plan)
      if (!isCorrelated) {
        // We have reached a plan without correlation to the outer plan.
        if (parentOuterReferences.isEmpty) {
          // If there is no outer references from the parent nodes, it means all outer
          // attributes can be substituted by attributes from the inner plan. So no
          // domain join is needed.
          (plan, Nil, Map.empty[Attribute, Attribute])
        } else {
          // Build the domain join with the parent outer references.
          val attributes = parentOuterReferences.toSeq
          val domains = attributes.map(_.newInstance())
          // A placeholder to be rewritten into domain join.
          val domainJoin = DomainJoin(domains, plan)
          val outerReferenceMap = attributes.zip(domains).toMap
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
            case (o, a) => EqualNullSafe(a, OuterReference(o))
          }
          (domainJoin, conditions.toSeq, outerReferenceMap)
        }
      } else {
        plan match {
          case Filter(condition, child) =>
            val conditions = splitConjunctivePredicates(condition)
            val (correlated, uncorrelated) = conditions.partition(containsOuter)
            // Find outer references that can be substituted by attributes from the inner
            // query using the equality predicates.
            val equivalences = collectEquivalentOuterReferences(correlated)
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
            if (aggregated) {
              // Split the correlated predicates into predicates that can and cannot be directly
              // used as join conditions with the outer query depending on whether they can
              // be pulled up over an Aggregate without changing the semantics of the plan.
              val (equalityCond, predicates) = correlated.partition(canPullUpOverAgg)
              val outerReferences = collectOuterReferences(predicates)
              val newOuterReferences =
                parentOuterReferences ++ outerReferences -- equivalences.keySet
              val (newChild, joinCond, outerReferenceMap) =
                decorrelate(child, newOuterReferences, aggregated)
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
                decorrelate(child, newOuterReferences, aggregated)
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
              decorrelate(child, newOuterReferences, aggregated)
            // Replace all outer references in the original project list.
            val newProjectList = replaceOuterReferences(projectList, outerReferenceMap)
            // Preserve required domain attributes in the join condition by adding the missing
            // references to the new project list.
            val referencesToAdd = missingReferences(newProjectList, joinCond)
            val newProject = Project(newProjectList ++ referencesToAdd, newChild)
            (newProject, joinCond, outerReferenceMap)

          case a @ Aggregate(groupingExpressions, aggregateExpressions, child) =>
            val outerReferences = collectOuterReferences(a.expressions)
            val newOuterReferences = parentOuterReferences ++ outerReferences
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(child, newOuterReferences, aggregated = true)
            // Replace all outer references in grouping and aggregate expressions.
            val newGroupingExpr = replaceOuterReferences(groupingExpressions, outerReferenceMap)
            val newAggExpr = replaceOuterReferences(aggregateExpressions, outerReferenceMap)
            // Add all required domain attributes to both grouping and aggregate expressions.
            val referencesToAdd = missingReferences(newAggExpr, joinCond)
            val newAggregate = a.copy(
              groupingExpressions = newGroupingExpr ++ referencesToAdd,
              aggregateExpressions = newAggExpr ++ referencesToAdd,
              child = newChild)
            (newAggregate, joinCond, outerReferenceMap)

          case j @ Join(left, right, joinType, condition, _) =>
            val outerReferences = collectOuterReferences(j.expressions)
            // Join condition containing outer references is not supported.
            assert(outerReferences.isEmpty, s"Correlated column is not allowed in join: $j")
            val newOuterReferences = parentOuterReferences ++ outerReferences
            val shouldPushToLeft = joinType match {
              case LeftOuter | LeftSemiOrAnti(_) | FullOuter => true
              case _ => hasOuterReferences(left)
            }
            val shouldPushToRight = joinType match {
              case RightOuter | FullOuter => true
              case _ => hasOuterReferences(right)
            }
            val (newLeft, leftJoinCond, leftOuterReferenceMap) = if (shouldPushToLeft) {
              decorrelate(left, newOuterReferences, aggregated)
            } else {
              (left, Nil, Map.empty[Attribute, Attribute])
            }
            val (newRight, rightJoinCond, rightOuterReferenceMap) = if (shouldPushToRight) {
              decorrelate(right, newOuterReferences, aggregated)
            } else {
              (right, Nil, Map.empty[Attribute, Attribute])
            }
            val newOuterReferenceMap = leftOuterReferenceMap ++ rightOuterReferenceMap
            val newJoinCond = leftJoinCond ++ rightJoinCond
            // If we push the dependent join to both sides, we can augment the join condition
            // such that both sides are matched on the domain attributes. For example,
            // - Left Map: {outer(c1) = c1}
            // - Right Map: {outer(c1) = 10 - c1}
            // Then the join condition can be augmented with (c1 <=> 10 - c1).
            val augmentedConditions = leftOuterReferenceMap.flatMap {
              case (outer, inner) => rightOuterReferenceMap.get(outer).map(EqualNullSafe(inner, _))
            }
            val newCondition = (condition ++ augmentedConditions).reduceOption(And)
            val newJoin = j.copy(left = newLeft, right = newRight, condition = newCondition)
            (newJoin, newJoinCond, newOuterReferenceMap)

          case u: UnaryNode =>
            val outerReferences = collectOuterReferences(u.expressions)
            assert(outerReferences.isEmpty, s"Correlated column is not allowed in $u")
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(u.child, parentOuterReferences, aggregated)
            (u.withNewChildren(newChild :: Nil), joinCond, outerReferenceMap)

          case o =>
            throw QueryExecutionErrors.decorrelateInnerQueryThroughPlanUnsupportedError(o)
        }
      }
    }
    val (newChild, joinCond, _) = decorrelate(BooleanSimplification(innerPlan), AttributeSet.empty)
    val (plan, conditions) = deduplicate(newChild, joinCond, outputSet)
    (plan, stripOuterReferences(conditions))
  }
}
