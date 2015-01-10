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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

/**
 * A trivial [[Analyzer]] with an [[EmptyCatalog]] and [[EmptyFunctionRegistry]]. Used for testing
 * when all relations are already filled in and the analyser needs only to resolve attribute
 * references.
 */
object SimpleAnalyzer extends Analyzer(EmptyCatalog, EmptyFunctionRegistry, true)

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a schema [[Catalog]] and
 * a [[FunctionRegistry]].
 */
class Analyzer(catalog: Catalog, registry: FunctionRegistry, caseSensitive: Boolean)
  extends RuleExecutor[LogicalPlan] with HiveTypeCoercion {

  val resolver = if (caseSensitive) caseSensitiveResolution else caseInsensitiveResolution

  // TODO: pass this in as a parameter.
  val fixedPoint = FixedPoint(100)

  /**
   * Override to provide additional rules for the "Resolution" batch.
   */
  val extendedRules: Seq[Rule[LogicalPlan]] = Nil

  lazy val batches: Seq[Batch] = Seq(
    Batch("MultiInstanceRelations", Once,
      NewRelationInstances),
    Batch("Resolution", fixedPoint,
      ResolveReferences ::
      ResolveRelations ::
      ResolveSortReferences ::
      NewRelationInstances ::
      ImplicitGenerate ::
      StarExpansion ::
      ResolveFunctions ::
      GlobalAggregates ::
      UnresolvedHavingClauseAttributes ::
      TrimGroupingAliases ::
      typeCoercionRules ++
      extendedRules : _*),
    Batch("Check Analysis", Once,
      CheckResolution,
      CheckAggregation),
    Batch("AnalysisOperators", fixedPoint,
      EliminateAnalysisOperators)
  )

  /**
   * Makes sure all attributes and logical plans have been resolved.
   */
  object CheckResolution extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case p if p.expressions.exists(!_.resolved) =>
          throw new TreeNodeException(p,
            s"Unresolved attributes: ${p.expressions.filterNot(_.resolved).mkString(",")}")
        case p if !p.resolved && p.childrenResolved =>
          throw new TreeNodeException(p, "Unresolved plan found")
      } match {
        // As a backstop, use the root node to check that the entire plan tree is resolved.
        case p if !p.resolved =>
          throw new TreeNodeException(p, "Unresolved plan in tree")
        case p => p
      }
    }
  }

  /**
   * Removes no-op Alias expressions from the plan.
   */
  object TrimGroupingAliases extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Aggregate(groups, aggs, child) =>
        Aggregate(groups.map(_.transform { case Alias(c, _) => c }), aggs, child)
    }
  }

  /**
   * Checks for non-aggregated attributes with aggregation
   */
  object CheckAggregation extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case aggregatePlan @ Aggregate(groupingExprs, aggregateExprs, child) =>
          def isValidAggregateExpression(expr: Expression): Boolean = expr match {
            case _: AggregateExpression => true
            case e: Attribute => groupingExprs.contains(e)
            case e if groupingExprs.contains(e) => true
            case e if e.references.isEmpty => true
            case e => e.children.forall(isValidAggregateExpression)
          }

          aggregateExprs.find { e =>
            !isValidAggregateExpression(e.transform {
              // Should trim aliases around `GetField`s. These aliases are introduced while
              // resolving struct field accesses, because `GetField` is not a `NamedExpression`.
              // (Should we just turn `GetField` into a `NamedExpression`?)
              case Alias(g: GetField, _) => g
            })
          }.foreach { e =>
            throw new TreeNodeException(plan, s"Expression not in GROUP BY: $e")
          }

          aggregatePlan
      }
    }
  }

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelations extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case i @ InsertIntoTable(UnresolvedRelation(tableIdentifier, alias), _, _, _) =>
        i.copy(
          table = EliminateAnalysisOperators(catalog.lookupRelation(tableIdentifier, alias)))
      case UnresolvedRelation(tableIdentifier, alias) =>
        catalog.lookupRelation(tableIdentifier, alias)
    }
  }

  /**
   * Replaces [[UnresolvedAttribute]]s with concrete
   * [[catalyst.expressions.AttributeReference AttributeReferences]] from a logical plan node's
   * children.
   */
  object ResolveReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case q: LogicalPlan if q.childrenResolved =>
        logTrace(s"Attempting to resolve ${q.simpleString}")
        q transformExpressions {
          case u @ UnresolvedAttribute(name) =>
            // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
            val result = q.resolveChildren(name, resolver).getOrElse(u)
            logDebug(s"Resolving $u to $result")
            result
        }
    }
  }

  /**
   * In many dialects of SQL is it valid to sort by attributes that are not present in the SELECT
   * clause.  This rule detects such queries and adds the required attributes to the original
   * projection, so that they will be available during sorting. Another projection is added to
   * remove these attributes after sorting.
   */
  object ResolveSortReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case s @ Sort(ordering, p @ Project(projectList, child)) if !s.resolved && p.resolved =>
        val unresolved = ordering.flatMap(_.collect { case UnresolvedAttribute(name) => name })
        val resolved = unresolved.flatMap(child.resolve(_, resolver))
        val requiredAttributes = AttributeSet(resolved.collect { case a: Attribute => a })

        val missingInProject = requiredAttributes -- p.output
        if (missingInProject.nonEmpty) {
          // Add missing attributes and then project them away after the sort.
          Project(projectList.map(_.toAttribute),
            Sort(ordering,
              Project(projectList ++ missingInProject, child)))
        } else {
          logDebug(s"Failed to find $missingInProject in ${p.output.mkString(", ")}")
          s // Nothing we can do here. Return original plan.
        }
      case s @ Sort(ordering, a @ Aggregate(grouping, aggs, child)) if !s.resolved && a.resolved =>
        val unresolved = ordering.flatMap(_.collect { case UnresolvedAttribute(name) => name })
        // A small hack to create an object that will allow us to resolve any references that
        // refer to named expressions that are present in the grouping expressions.
        val groupingRelation = LocalRelation(
          grouping.collect { case ne: NamedExpression => ne.toAttribute }
        )

        logDebug(s"Grouping expressions: $groupingRelation")
        val resolved = unresolved.flatMap(groupingRelation.resolve(_, resolver))
        val missingInAggs = resolved.filterNot(a.outputSet.contains)
        logDebug(s"Resolved: $resolved Missing in aggs: $missingInAggs")
        if (missingInAggs.nonEmpty) {
          // Add missing grouping exprs and then project them away after the sort.
          Project(a.output,
            Sort(ordering,
              Aggregate(grouping, aggs ++ missingInAggs, child)))
        } else {
          s // Nothing we can do here. Return original plan.
        }
    }
  }

  /**
   * Replaces [[UnresolvedFunction]]s with concrete [[catalyst.expressions.Expression Expressions]].
   */
  object ResolveFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case q: LogicalPlan =>
        q transformExpressions {
          case u @ UnresolvedFunction(name, children) if u.childrenResolved =>
            registry.lookupFunction(name, children)
        }
    }
  }

  /**
   * Turns projections that contain aggregate expressions into aggregations.
   */
  object GlobalAggregates extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Project(projectList, child) if containsAggregates(projectList) =>
        Aggregate(Nil, projectList, child)
    }

    def containsAggregates(exprs: Seq[Expression]): Boolean = {
      exprs.foreach(_.foreach {
        case agg: AggregateExpression => return true
        case _ =>
      })
      false
    }
  }

  /**
   * This rule finds expressions in HAVING clause filters that depend on
   * unresolved attributes.  It pushes these expressions down to the underlying
   * aggregates and then projects them away above the filter.
   */
  object UnresolvedHavingClauseAttributes extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case filter @ Filter(havingCondition, aggregate @ Aggregate(_, originalAggExprs, _))
          if aggregate.resolved && containsAggregate(havingCondition) => {
        val evaluatedCondition = Alias(havingCondition,  "havingCondition")()
        val aggExprsWithHaving = evaluatedCondition +: originalAggExprs

        Project(aggregate.output,
          Filter(evaluatedCondition.toAttribute,
            aggregate.copy(aggregateExpressions = aggExprsWithHaving)))
      }
    }

    protected def containsAggregate(condition: Expression): Boolean =
      condition
        .collect { case ae: AggregateExpression => ae }
        .nonEmpty
  }

  /**
   * When a SELECT clause has only a single expression and that expression is a
   * [[catalyst.expressions.Generator Generator]] we convert the
   * [[catalyst.plans.logical.Project Project]] to a [[catalyst.plans.logical.Generate Generate]].
   */
  object ImplicitGenerate extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Project(Seq(Alias(g: Generator, _)), child) =>
        Generate(g, join = false, outer = false, None, child)
    }
  }

  /**
   * Expands any references to [[Star]] (*) in project operators.
   */
  object StarExpansion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved
      case p: LogicalPlan if !p.childrenResolved => p
      // If the projection list contains Stars, expand it.
      case p @ Project(projectList, child) if containsStar(projectList) =>
        Project(
          projectList.flatMap {
            case s: Star => s.expand(child.output, resolver)
            case o => o :: Nil
          },
          child)
      case t: ScriptTransformation if containsStar(t.input) =>
        t.copy(
          input = t.input.flatMap {
            case s: Star => s.expand(t.child.output, resolver)
            case o => o :: Nil
          }
        )
      // If the aggregate function argument contains Stars, expand it.
      case a: Aggregate if containsStar(a.aggregateExpressions) =>
        a.copy(
          aggregateExpressions = a.aggregateExpressions.flatMap {
            case s: Star => s.expand(a.child.output, resolver)
            case o => o :: Nil
          }
        )
    }

    /**
     * Returns true if `exprs` contains a [[Star]].
     */
    protected def containsStar(exprs: Seq[Expression]): Boolean =
      exprs.collect { case _: Star => true }.nonEmpty
  }
}

/**
 * Removes [[catalyst.plans.logical.Subquery Subquery]] operators from the plan.  Subqueries are
 * only required to provide scoping information for attributes and can be removed once analysis is
 * complete.
 */
object EliminateAnalysisOperators extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}
