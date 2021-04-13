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

/**
 * Decorrelate the inner query by eliminating outer references and create domain joins.
 * The implementation is based on the paper: Unnesting Arbitrary Queries by Thomas Neumann
 * and Alfons Kemper. https://dl.gi.de/handle/20.500.12116/2418.
 * (1) Recursively collects outer references from the inner query until it reaches a node
 *     that does not contain correlated value.
 * (2) Inserts an optional [[DomainJoin]] node to indicate whether a domain (inner) join is
 *     needed between the outer query and the specific subtree of the inner query.
 * (3) Returns a list of join conditions with the outer query and a mapping between outer
 *     references with references inside the inner query. The parent nodes need to preserve
 *     the references inside the join conditions and substitute all outer references using
 *     the mapping.
 *
 * E.g. decorrelate an inner query with equality predicates:
 *
 * Aggregate [] [min(c2)]            Aggregate [c1] [min(c2), c1]
 * +- Filter [outer(c3) = c1]   =>   +- Relation [t]
 *    +- Relation [t]
 *
 * Join conditions: [c3 = c1]
 *
 * E.g. decorrelate an inner query with non-equality predicates:
 *
 * Aggregate [] [min(c2)]            Aggregate [c3'] [min(c2), c3']
 * +- Filter [outer(c3) > c1]   =>   +- Filter [c3' > c1]
 *    +- Relation [t]                   +- DomainJoin [c3']
 *                                         +- Relation [t]
 *
 * Join conditions: [c3 <=> c3']
 */
object DecorrelateInnerQuery extends PredicateHelper {

  /**
   * Check if the given expression is an equality condition.
   */
  private def isEquality(expression: Expression): Boolean = expression match {
    case Equality(_, _) => true
    case _ => false
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
    expression.transform {
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

    // Recursively decorrelate the input plan with a set of parent outer references and
    // a boolean flag indicating whether the result of the plan will be aggregated.
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
            // Split the correlated predicates
            val (equality, nonEquality) = correlated.partition(isEquality)
            // Find outer references that can be substituted by attributes from the inner
            // query using the equality predicates.
            val equivalences = collectEquivalentOuterReferences(equality)
            // Correlated predicates can be removed from the Filter's condition and used as
            // join conditions with the outer query. However, if the results of the sub-tree
            // is aggregated, only the correlated equality predicates can be used, because
            // the inner query attributes from a non-equality predicate need to be preserved
            // in both grouping and aggregate expressions, which can change the semantics
            // of the plan and lead to incorrect results. Here is an example:
            // Relations:
            //   t1(c1, c2): [(1, 1)]
            //   t2(c1, c2): [(1, 1), (2, 0)]
            //
            // Query:
            //   SELECT * FROM t1 WHERE c1 = (SELECT MAX(c1) FROM t2 WHERE t1.c2 >= c2)
            //
            // Subquery plan transformation if non-equality predicates are used as join conditions:
            //   Aggregate [max(c1)]                Aggregate [c2] [max(c1), c2]
            //   +- Filter [outer(c2) >= c2]   =>   +- Relation [c1, c2]
            //      +- Relation [c1, c2]
            //
            // Which will be rewritten to this query:
            //   SELECT c1, c2 FROM t1 LEFT OUTER JOIN
            //   (SELECT MAX(c1) m, c2 FROM t2 GROUP BY c2) s ON t1.c2 >= s.c2 WHERE c1 = m
            //
            // The result of the original query should be an empty set but the transformed
            // query will output an incorrect result of (1, 1). The correct transformation
            // is illustrated below:
            //   Aggregate [max(c1)]                Aggregate [c2'] [max(c1), c2']
            //   +- Filter [outer(c2) >= c2]   =>   +- Filter [c2' >= c2]
            //      +- Relation [c1, c2]               +- DomainJoin [c2']
            //                                            +- Relation [c1, c2]
            // Which will be rewritten to this query (using CTE here to make the query clearer):
            //   WITH domain AS (                                                   -- [(1, 1)]
            //     SELECT DISTINCT c2 FROM t1
            //   ), domainJoin AS (                                   -- [(1, 1, 1), (2, 0, 1)]
            //     SELECT t2.c1, t2.c2, domain.c2 AS dc2 FROM t2 JOIN domain
            //   ), subquery AS (                                                   -- [(2, 1)]
            //     SELECT MAX(c1) m, dc2 FROM domainJoin WHERE dc2 >= c2 GROUP BY dc2
            //   )
            //   SELECT c1, c2 FROM t1 LEFT OUTER JOIN subquery ON c2 <=> dc2 WHERE c1 = m
            if (aggregated) {
              val outerReferences = collectOuterReferences(nonEquality)
              val newOuterReferences =
                parentOuterReferences ++ outerReferences -- equivalences.keySet
              val (newChild, joinCond, outerReferenceMap) =
                decorrelate(child, newOuterReferences, aggregated)
              // Add the outer references mapping collected from the equality conditions.
              val newOuterReferenceMap = outerReferenceMap ++ equivalences
              // Replace all outer references in the non-equality predicates.
              val nonEqualityCond = replaceOuterReferences(nonEquality, newOuterReferenceMap)
              // The new filter condition is the original filter condition with correlated
              // equality predicates removed.
              val newFilterCond = nonEqualityCond ++ uncorrelated
              val newFilter = newFilterCond match {
                case Nil => newChild
                case conditions => Filter(conditions.reduce(And), newChild)
              }
              // Equality predicates are used as join conditions with the outer query.
              val newJoinCond = joinCond ++ equality
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
            decorrelate(u.child, parentOuterReferences, aggregated)

          case o =>
            throw new UnsupportedOperationException(
              s"Decorrelate inner query through ${o.nodeName} is not supported.")
        }
      }
    }
    val (newChild, joinCond, _) = decorrelate(BooleanSimplification(innerPlan), AttributeSet.empty)
    val (plan, conditions) = deduplicate(newChild, joinCond, outputSet)
    (plan, stripOuterReferences(conditions))
  }
}
