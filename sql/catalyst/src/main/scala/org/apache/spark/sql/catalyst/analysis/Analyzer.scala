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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.{ScalaReflection, SimpleCatalystConf, CatalystConf}
import org.apache.spark.sql.types._

/**
 * A trivial [[Analyzer]] with an [[EmptyCatalog]] and [[EmptyFunctionRegistry]]. Used for testing
 * when all relations are already filled in and the analyzer needs only to resolve attribute
 * references.
 */
object SimpleAnalyzer
  extends Analyzer(EmptyCatalog, EmptyFunctionRegistry, new SimpleCatalystConf(true))

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a schema [[Catalog]] and
 * a [[FunctionRegistry]].
 */
class Analyzer(
    catalog: Catalog,
    registry: FunctionRegistry,
    conf: CatalystConf,
    maxIterations: Int = 100)
  extends RuleExecutor[LogicalPlan] with CheckAnalysis {

  def resolver: Resolver = {
    if (conf.caseSensitiveAnalysis) {
      caseSensitiveResolution
    } else {
      caseInsensitiveResolution
    }
  }

  val fixedPoint = FixedPoint(maxIterations)

  /**
   * Override to provide additional rules for the "Resolution" batch.
   */
  val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  lazy val batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      CTESubstitution,
      WindowsSubstitution),
    Batch("Resolution", fixedPoint,
      ResolveRelations ::
      ResolveReferences ::
      ResolveGroupingAnalytics ::
      ResolvePivot ::
      ResolveUpCast ::
      ResolveSortReferences ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      DistinctAggregationRewriter(conf) ::
      HiveTypeCoercion.typeCoercionRules ++
      extendedResolutionRules : _*),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic,
      ComputeCurrentTime),
    Batch("UDF", Once,
      HandleNullInputsForUDF),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )

  /**
   * Substitute child plan with cte definitions
   */
  object CTESubstitution extends Rule[LogicalPlan] {
    // TODO allow subquery to define CTE
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators  {
      case With(child, relations) => substituteCTE(child, relations)
      case other => other
    }

    def substituteCTE(plan: LogicalPlan, cteRelations: Map[String, LogicalPlan]): LogicalPlan = {
      plan transform {
        // In hive, if there is same table name in database and CTE definition,
        // hive will use the table in database, not the CTE one.
        // Taking into account the reasonableness and the implementation complexity,
        // here use the CTE definition first, check table name only and ignore database name
        // see https://github.com/apache/spark/pull/4929#discussion_r27186638 for more info
        case u : UnresolvedRelation =>
          val substituted = cteRelations.get(u.tableIdentifier.table).map { relation =>
            val withAlias = u.alias.map(Subquery(_, relation))
            withAlias.getOrElse(relation)
          }
          substituted.getOrElse(u)
      }
    }
  }

  /**
   * Substitute child plan with WindowSpecDefinitions.
   */
  object WindowsSubstitution extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      // Lookup WindowSpecDefinitions. This rule works with unresolved children.
      case WithWindowDefinition(windowDefinitions, child) =>
        child.transform {
          case plan => plan.transformExpressions {
            case UnresolvedWindowExpression(c, WindowSpecReference(windowName)) =>
              val errorMessage =
                s"Window specification $windowName is not defined in the WINDOW clause."
              val windowSpecDefinition =
                windowDefinitions
                  .get(windowName)
                  .getOrElse(failAnalysis(errorMessage))
              WindowExpression(c, windowSpecDefinition)
          }
        }
    }
  }

  /**
   * Replaces [[UnresolvedAlias]]s with concrete aliases.
   */
  object ResolveAliases extends Rule[LogicalPlan] {
    private def assignAliases(exprs: Seq[NamedExpression]) = {
      exprs.zipWithIndex.map {
        case (expr, i) =>
          expr transformUp {
            case u @ UnresolvedAlias(child) => child match {
              case ne: NamedExpression => ne
              case e if !e.resolved => u
              case g: Generator => MultiAlias(g, Nil)
              case c @ Cast(ne: NamedExpression, _) => Alias(c, ne.name)()
              case other => Alias(other, s"_c$i")()
            }
          }
      }.asInstanceOf[Seq[NamedExpression]]
    }

    private def hasUnresolvedAlias(exprs: Seq[NamedExpression]) =
      exprs.exists(_.find(_.isInstanceOf[UnresolvedAlias]).isDefined)

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case Aggregate(groups, aggs, child) if child.resolved && hasUnresolvedAlias(aggs) =>
        Aggregate(groups, assignAliases(aggs), child)

      case g: GroupingAnalytics if g.child.resolved && hasUnresolvedAlias(g.aggregations) =>
        g.withNewAggs(assignAliases(g.aggregations))

      case Pivot(groupByExprs, pivotColumn, pivotValues, aggregates, child)
        if child.resolved && hasUnresolvedAlias(groupByExprs) =>
        Pivot(assignAliases(groupByExprs), pivotColumn, pivotValues, aggregates, child)

      case Project(projectList, child) if child.resolved && hasUnresolvedAlias(projectList) =>
        Project(assignAliases(projectList), child)
    }
  }

  object ResolveGroupingAnalytics extends Rule[LogicalPlan] {
    /*
     *  GROUP BY a, b, c WITH ROLLUP
     *  is equivalent to
     *  GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (a), ( ) ).
     *  Group Count: N + 1 (N is the number of group expressions)
     *
     *  We need to get all of its subsets for the rule described above, the subset is
     *  represented as the bit masks.
     */
    def bitmasks(r: Rollup): Seq[Int] = {
      Seq.tabulate(r.groupByExprs.length + 1)(idx => {(1 << idx) - 1})
    }

    /*
     *  GROUP BY a, b, c WITH CUBE
     *  is equivalent to
     *  GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ( ) ).
     *  Group Count: 2 ^ N (N is the number of group expressions)
     *
     *  We need to get all of its subsets for a given GROUPBY expression, the subsets are
     *  represented as the bit masks.
     */
    def bitmasks(c: Cube): Seq[Int] = {
      Seq.tabulate(1 << c.groupByExprs.length)(i => i)
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case a if !a.childrenResolved => a // be sure all of the children are resolved.
      case a: Cube =>
        GroupingSets(bitmasks(a), a.groupByExprs, a.child, a.aggregations)
      case a: Rollup =>
        GroupingSets(bitmasks(a), a.groupByExprs, a.child, a.aggregations)
      case x: GroupingSets =>
        val gid = AttributeReference(VirtualColumn.groupingIdName, IntegerType, false)()

        // Expand works by setting grouping expressions to null as determined by the bitmasks. To
        // prevent these null values from being used in an aggregate instead of the original value
        // we need to create new aliases for all group by expressions that will only be used for
        // the intended purpose.
        val groupByAliases: Seq[Alias] = x.groupByExprs.map {
          case e: NamedExpression => Alias(e, e.name)()
          case other => Alias(other, other.toString)()
        }

        val nonNullBitmask = x.bitmasks.reduce(_ & _)

        val attributeMap = groupByAliases.zipWithIndex.map { case (a, idx) =>
          if ((nonNullBitmask & 1 << idx) == 0) {
            (a -> a.toAttribute.withNullability(true))
          } else {
            (a -> a.toAttribute)
          }
        }.toMap

        val aggregations: Seq[NamedExpression] = x.aggregations.map {
          // If an expression is an aggregate (contains a AggregateExpression) then we dont change
          // it so that the aggregation is computed on the unmodified value of its argument
          // expressions.
          case expr if expr.find(_.isInstanceOf[AggregateExpression]).nonEmpty => expr
          // If not then its a grouping expression and we need to use the modified (with nulls from
          // Expand) value of the expression.
          case expr => expr.transformDown {
            case e =>
              groupByAliases.find(_.child.semanticEquals(e)).map(attributeMap(_)).getOrElse(e)
          }.asInstanceOf[NamedExpression]
        }

        val child = Project(x.child.output ++ groupByAliases, x.child)
        val groupByAttributes = groupByAliases.map(attributeMap(_))

        Aggregate(
          groupByAttributes :+ VirtualColumn.groupingIdAttribute,
          aggregations,
          Expand(x.bitmasks, groupByAttributes, gid, child))
    }
  }

  object ResolvePivot extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case p: Pivot if !p.childrenResolved | !p.aggregates.forall(_.resolved) => p
      case Pivot(groupByExprs, pivotColumn, pivotValues, aggregates, child) =>
        val singleAgg = aggregates.size == 1
        val pivotAggregates: Seq[NamedExpression] = pivotValues.flatMap { value =>
          def ifExpr(expr: Expression) = {
            If(EqualTo(pivotColumn, value), expr, Literal(null))
          }
          aggregates.map { aggregate =>
            val filteredAggregate = aggregate.transformDown {
              // Assumption is the aggregate function ignores nulls. This is true for all current
              // AggregateFunction's with the exception of First and Last in their default mode
              // (which we handle) and possibly some Hive UDAF's.
              case First(expr, _) =>
                First(ifExpr(expr), Literal(true))
              case Last(expr, _) =>
                Last(ifExpr(expr), Literal(true))
              case a: AggregateFunction =>
                a.withNewChildren(a.children.map(ifExpr))
            }
            if (filteredAggregate.fastEquals(aggregate)) {
              throw new AnalysisException(
                s"Aggregate expression required for pivot, found '$aggregate'")
            }
            val name = if (singleAgg) value.toString else value + "_" + aggregate.prettyString
            Alias(filteredAggregate, name)()
          }
        }
        val newGroupByExprs = groupByExprs.map {
          case UnresolvedAlias(e) => e
          case e => e
        }
        Aggregate(newGroupByExprs, groupByExprs ++ pivotAggregates, child)
    }
  }

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelations extends Rule[LogicalPlan] {
    def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"Table not found: ${u.tableName}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case i @ InsertIntoTable(u: UnresolvedRelation, _, _, _, _) =>
        i.copy(table = EliminateSubQueries(getTable(u)))
      case u: UnresolvedRelation =>
        try {
          getTable(u)
        } catch {
          case _: AnalysisException if u.tableIdentifier.database.isDefined =>
            // delay the exception into CheckAnalysis, then it could be resolved as data source.
            u
        }
    }
  }

  /**
   * Replaces [[UnresolvedAttribute]]s with concrete [[AttributeReference]]s from
   * a logical plan node's children.
   */
  object ResolveReferences extends Rule[LogicalPlan] {
    /**
     * Foreach expression, expands the matching attribute.*'s in `child`'s input for the subtree
     * rooted at each expression.
     */
    def expandStarExpressions(exprs: Seq[Expression], child: LogicalPlan): Seq[Expression] = {
      exprs.flatMap {
        case s: Star => s.expand(child, resolver)
        case e =>
          e.transformDown {
            case f1: UnresolvedFunction if containsStar(f1.children) =>
              f1.copy(children = f1.children.flatMap {
                case s: Star => s.expand(child, resolver)
                case o => o :: Nil
              })
          } :: Nil
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p: LogicalPlan if !p.childrenResolved => p

      // If the projection list contains Stars, expand it.
      case p @ Project(projectList, child) if containsStar(projectList) =>
        Project(
          projectList.flatMap {
            case s: Star => s.expand(child, resolver)
            case UnresolvedAlias(f @ UnresolvedFunction(_, args, _)) if containsStar(args) =>
              val newChildren = expandStarExpressions(args, child)
              UnresolvedAlias(child = f.copy(children = newChildren)) :: Nil
            case Alias(f @ UnresolvedFunction(_, args, _), name) if containsStar(args) =>
              val newChildren = expandStarExpressions(args, child)
              Alias(child = f.copy(children = newChildren), name)() :: Nil
            case UnresolvedAlias(c @ CreateArray(args)) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child, resolver)
                case o => o :: Nil
              }
              UnresolvedAlias(c.copy(children = expandedArgs)) :: Nil
            case UnresolvedAlias(c @ CreateStruct(args)) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child, resolver)
                case o => o :: Nil
              }
              UnresolvedAlias(c.copy(children = expandedArgs)) :: Nil
            case o => o :: Nil
          },
          child)

      case t: ScriptTransformation if containsStar(t.input) =>
        t.copy(
          input = t.input.flatMap {
            case s: Star => s.expand(t.child, resolver)
            case o => o :: Nil
          }
        )

      // If the aggregate function argument contains Stars, expand it.
      case a: Aggregate if containsStar(a.aggregateExpressions) =>
        val expanded = expandStarExpressions(a.aggregateExpressions, a.child)
            .map(_.asInstanceOf[NamedExpression])
        a.copy(aggregateExpressions = expanded)

      // Special handling for cases when self-join introduce duplicate expression ids.
      case j @ Join(left, right, _, _) if !j.selfJoinResolved =>
        val conflictingAttributes = left.outputSet.intersect(right.outputSet)
        logDebug(s"Conflicting attributes ${conflictingAttributes.mkString(",")} in $j")

        right.collect {
          // Handle base relations that might appear more than once.
          case oldVersion: MultiInstanceRelation
              if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
            val newVersion = oldVersion.newInstance()
            (oldVersion, newVersion)

          // Handle projects that create conflicting aliases.
          case oldVersion @ Project(projectList, _)
              if findAliases(projectList).intersect(conflictingAttributes).nonEmpty =>
            (oldVersion, oldVersion.copy(projectList = newAliases(projectList)))

          case oldVersion @ Aggregate(_, aggregateExpressions, _)
              if findAliases(aggregateExpressions).intersect(conflictingAttributes).nonEmpty =>
            (oldVersion, oldVersion.copy(aggregateExpressions = newAliases(aggregateExpressions)))

          case oldVersion: Generate
              if oldVersion.generatedSet.intersect(conflictingAttributes).nonEmpty =>
            val newOutput = oldVersion.generatorOutput.map(_.newInstance())
            (oldVersion, oldVersion.copy(generatorOutput = newOutput))

          case oldVersion @ Window(_, windowExpressions, _, _, child)
              if AttributeSet(windowExpressions.map(_.toAttribute)).intersect(conflictingAttributes)
                .nonEmpty =>
            (oldVersion, oldVersion.copy(windowExpressions = newAliases(windowExpressions)))
        }
        // Only handle first case, others will be fixed on the next pass.
        .headOption match {
          case None =>
            /*
             * No result implies that there is a logical plan node that produces new references
             * that this rule cannot handle. When that is the case, there must be another rule
             * that resolves these conflicts. Otherwise, the analysis will fail.
             */
            j
          case Some((oldRelation, newRelation)) =>
            val attributeRewrites = AttributeMap(oldRelation.output.zip(newRelation.output))
            val newRight = right transformUp {
              case r if r == oldRelation => newRelation
            } transformUp {
              case other => other transformExpressions {
                case a: Attribute => attributeRewrites.get(a).getOrElse(a)
              }
            }
            j.copy(right = newRight)
        }

      // When resolve `SortOrder`s in Sort based on child, don't report errors as
      // we still have chance to resolve it based on grandchild
      case s @ Sort(ordering, global, child) if child.resolved && !s.resolved =>
        val newOrdering = resolveSortOrders(ordering, child, throws = false)
        Sort(newOrdering, global, child)

      // A special case for Generate, because the output of Generate should not be resolved by
      // ResolveReferences. Attributes in the output will be resolved by ResolveGenerate.
      case g @ Generate(generator, join, outer, qualifier, output, child)
        if child.resolved && !generator.resolved =>
        val newG = generator transformUp {
          case u @ UnresolvedAttribute(nameParts) =>
            withPosition(u) { child.resolve(nameParts, resolver).getOrElse(u) }
          case UnresolvedExtractValue(child, fieldExpr) =>
            ExtractValue(child, fieldExpr, resolver)
        }
        if (newG.fastEquals(generator)) {
          g
        } else {
          Generate(newG.asInstanceOf[Generator], join, outer, qualifier, output, child)
        }

      case q: LogicalPlan =>
        logTrace(s"Attempting to resolve ${q.simpleString}")
        q transformExpressionsUp  {
          case u @ UnresolvedAttribute(nameParts) =>
            // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
            val result =
              withPosition(u) { q.resolveChildren(nameParts, resolver).getOrElse(u) }
            logDebug(s"Resolving $u to $result")
            result
          case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
            ExtractValue(child, fieldExpr, resolver)
        }
    }

    def newAliases(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
      expressions.map {
        case a: Alias => Alias(a.child, a.name)()
        case other => other
      }
    }

    def findAliases(projectList: Seq[NamedExpression]): AttributeSet = {
      AttributeSet(projectList.collect { case a: Alias => a.toAttribute })
    }

    /**
     * Returns true if `exprs` contains a [[Star]].
     */
    def containsStar(exprs: Seq[Expression]): Boolean =
      exprs.exists(_.collect { case _: Star => true }.nonEmpty)
  }

  private def resolveSortOrders(ordering: Seq[SortOrder], plan: LogicalPlan, throws: Boolean) = {
    ordering.map { order =>
      // Resolve SortOrder in one round.
      // If throws == false or the desired attribute doesn't exist
      // (like try to resolve `a.b` but `a` doesn't exist), fail and return the origin one.
      // Else, throw exception.
      try {
        val newOrder = order transformUp {
          case u @ UnresolvedAttribute(nameParts) =>
            plan.resolve(nameParts, resolver).getOrElse(u)
          case UnresolvedExtractValue(child, fieldName) if child.resolved =>
            ExtractValue(child, fieldName, resolver)
        }
        newOrder.asInstanceOf[SortOrder]
      } catch {
        case a: AnalysisException if !throws => order
      }
    }
  }

  /**
   * In many dialects of SQL it is valid to sort by attributes that are not present in the SELECT
   * clause.  This rule detects such queries and adds the required attributes to the original
   * projection, so that they will be available during sorting. Another projection is added to
   * remove these attributes after sorting.
   */
  object ResolveSortReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case s @ Sort(ordering, global, p @ Project(projectList, child))
          if !s.resolved && p.resolved =>
        val (newOrdering, missing) = resolveAndFindMissing(ordering, p, child)

        // If this rule was not a no-op, return the transformed plan, otherwise return the original.
        if (missing.nonEmpty) {
          // Add missing attributes and then project them away after the sort.
          Project(p.output,
            Sort(newOrdering, global,
              Project(projectList ++ missing, child)))
        } else {
          logDebug(s"Failed to find $missing in ${p.output.mkString(", ")}")
          s // Nothing we can do here. Return original plan.
        }
    }

    /**
     * Given a child and a grandchild that are present beneath a sort operator, try to resolve
     * the sort ordering and returns it with a list of attributes that are missing from the
     * child but are present in the grandchild.
     */
    def resolveAndFindMissing(
        ordering: Seq[SortOrder],
        child: LogicalPlan,
        grandchild: LogicalPlan): (Seq[SortOrder], Seq[Attribute]) = {
      val newOrdering = resolveSortOrders(ordering, grandchild, throws = true)
      // Construct a set that contains all of the attributes that we need to evaluate the
      // ordering.
      val requiredAttributes = AttributeSet(newOrdering).filter(_.resolved)
      // Figure out which ones are missing from the projection, so that we can add them and
      // remove them after the sort.
      val missingInProject = requiredAttributes -- child.output
      // It is important to return the new SortOrders here, instead of waiting for the standard
      // resolving process as adding attributes to the project below can actually introduce
      // ambiguity that was not present before.
      (newOrdering, missingInProject.toSeq)
    }
  }

  /**
   * Replaces [[UnresolvedFunction]]s with concrete [[Expression]]s.
   */
  object ResolveFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case q: LogicalPlan =>
        q transformExpressions {
          case u if !u.childrenResolved => u // Skip until children are resolved.
          case u @ UnresolvedFunction(name, children, isDistinct) =>
            withPosition(u) {
              registry.lookupFunction(name, children) match {
                // DISTINCT is not meaningful for a Max or a Min.
                case max: Max if isDistinct =>
                  AggregateExpression(max, Complete, isDistinct = false)
                case min: Min if isDistinct =>
                  AggregateExpression(min, Complete, isDistinct = false)
                // We get an aggregate function, we need to wrap it in an AggregateExpression.
                case agg: AggregateFunction => AggregateExpression(agg, Complete, isDistinct)
                // This function is not an aggregate function, just return the resolved one.
                case other => other
              }
            }
        }
    }
  }

  /**
   * Turns projections that contain aggregate expressions into aggregations.
   */
  object GlobalAggregates extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
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
   * This rule finds aggregate expressions that are not in an aggregate operator.  For example,
   * those in a HAVING clause or ORDER BY clause.  These expressions are pushed down to the
   * underlying aggregate operator and then projected away after the original operator.
   */
  object ResolveAggregateFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case filter @ Filter(havingCondition,
             aggregate @ Aggregate(grouping, originalAggExprs, child))
          if aggregate.resolved =>

        // Try resolving the condition of the filter as though it is in the aggregate clause
        val aggregatedCondition =
          Aggregate(grouping, Alias(havingCondition, "havingCondition")() :: Nil, child)
        val resolvedOperator = execute(aggregatedCondition)
        def resolvedAggregateFilter =
          resolvedOperator
            .asInstanceOf[Aggregate]
            .aggregateExpressions.head

        // If resolution was successful and we see the filter has an aggregate in it, add it to
        // the original aggregate operator.
        if (resolvedOperator.resolved && containsAggregate(resolvedAggregateFilter)) {
          val aggExprsWithHaving = resolvedAggregateFilter +: originalAggExprs

          Project(aggregate.output,
            Filter(resolvedAggregateFilter.toAttribute,
              aggregate.copy(aggregateExpressions = aggExprsWithHaving)))
        } else {
          filter
        }

      case sort @ Sort(sortOrder, global, aggregate: Aggregate)
        if aggregate.resolved =>

        // Try resolving the ordering as though it is in the aggregate clause.
        try {
          val unresolvedSortOrders = sortOrder.filter(s => !s.resolved || containsAggregate(s))
          val aliasedOrdering = unresolvedSortOrders.map(o => Alias(o.child, "aggOrder")())
          val aggregatedOrdering = aggregate.copy(aggregateExpressions = aliasedOrdering)
          val resolvedAggregate: Aggregate = execute(aggregatedOrdering).asInstanceOf[Aggregate]
          val resolvedAliasedOrdering: Seq[Alias] =
            resolvedAggregate.aggregateExpressions.asInstanceOf[Seq[Alias]]

          // If we pass the analysis check, then the ordering expressions should only reference to
          // aggregate expressions or grouping expressions, and it's safe to push them down to
          // Aggregate.
          checkAnalysis(resolvedAggregate)

          val originalAggExprs = aggregate.aggregateExpressions.map(
            CleanupAliases.trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])

          // If the ordering expression is same with original aggregate expression, we don't need
          // to push down this ordering expression and can reference the original aggregate
          // expression instead.
          val needsPushDown = ArrayBuffer.empty[NamedExpression]
          val evaluatedOrderings = resolvedAliasedOrdering.zip(sortOrder).map {
            case (evaluated, order) =>
              val index = originalAggExprs.indexWhere {
                case Alias(child, _) => child semanticEquals evaluated.child
                case other => other semanticEquals evaluated.child
              }

              if (index == -1) {
                needsPushDown += evaluated
                order.copy(child = evaluated.toAttribute)
              } else {
                order.copy(child = originalAggExprs(index).toAttribute)
              }
          }

          val sortOrdersMap = unresolvedSortOrders
            .map(new TreeNodeRef(_))
            .zip(evaluatedOrderings)
            .toMap
          val finalSortOrders = sortOrder.map(s => sortOrdersMap.getOrElse(new TreeNodeRef(s), s))

          // Since we don't rely on sort.resolved as the stop condition for this rule,
          // we need to check this and prevent applying this rule multiple times
          if (sortOrder == finalSortOrders) {
            sort
          } else {
            Project(aggregate.output,
              Sort(finalSortOrders, global,
                aggregate.copy(aggregateExpressions = originalAggExprs ++ needsPushDown)))
          }
        } catch {
          // Attempting to resolve in the aggregate can result in ambiguity.  When this happens,
          // just return the original plan.
          case ae: AnalysisException => sort
        }
    }

    protected def containsAggregate(condition: Expression): Boolean = {
      condition.find(_.isInstanceOf[AggregateExpression]).isDefined
    }
  }

  /**
   * Rewrites table generating expressions that either need one or more of the following in order
   * to be resolved:
   *  - concrete attribute references for their output.
   *  - to be relocated from a SELECT clause (i.e. from  a [[Project]]) into a [[Generate]]).
   *
   * Names for the output [[Attribute]]s are extracted from [[Alias]] or [[MultiAlias]] expressions
   * that wrap the [[Generator]]. If more than one [[Generator]] is found in a Project, an
   * [[AnalysisException]] is throw.
   */
  object ResolveGenerate extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case g: Generate if ResolveReferences.containsStar(g.generator.children) =>
        failAnalysis("Cannot explode *, explode can only be applied on a specific column.")
      case p: Generate if !p.child.resolved || !p.generator.resolved => p
      case g: Generate if !g.resolved =>
        g.copy(generatorOutput = makeGeneratorOutput(g.generator, g.generatorOutput.map(_.name)))

      case p @ Project(projectList, child) =>
        // Holds the resolved generator, if one exists in the project list.
        var resolvedGenerator: Generate = null

        val newProjectList = projectList.flatMap {
          case AliasedGenerator(generator, names) if generator.childrenResolved =>
            if (resolvedGenerator != null) {
              failAnalysis(
                s"Only one generator allowed per select but ${resolvedGenerator.nodeName} and " +
                s"and ${generator.nodeName} found.")
            }

            resolvedGenerator =
              Generate(
                generator,
                join = projectList.size > 1, // Only join if there are other expressions in SELECT.
                outer = false,
                qualifier = None,
                generatorOutput = makeGeneratorOutput(generator, names),
                child)

            resolvedGenerator.generatorOutput
          case other => other :: Nil
        }

        if (resolvedGenerator != null) {
          Project(newProjectList, resolvedGenerator)
        } else {
          p
        }
    }

    /** Extracts a [[Generator]] expression and any names assigned by aliases to their output. */
    private object AliasedGenerator {
      def unapply(e: Expression): Option[(Generator, Seq[String])] = e match {
        case Alias(g: Generator, name) if g.resolved && g.elementTypes.size > 1 =>
          // If not given the default names, and the TGF with multiple output columns
          failAnalysis(
            s"""Expect multiple names given for ${g.getClass.getName},
               |but only single name '${name}' specified""".stripMargin)
        case Alias(g: Generator, name) if g.resolved => Some((g, name :: Nil))
        case MultiAlias(g: Generator, names) if g.resolved => Some(g, names)
        case _ => None
      }
    }

    /**
     * Construct the output attributes for a [[Generator]], given a list of names.  If the list of
     * names is empty names are assigned from field names in generator.
     */
    private def makeGeneratorOutput(
        generator: Generator,
        names: Seq[String]): Seq[Attribute] = {
      val elementTypes = generator.elementTypes

      if (names.length == elementTypes.length) {
        names.zip(elementTypes).map {
          case (name, (t, nullable, _)) =>
            AttributeReference(name, t, nullable)()
        }
      } else if (names.isEmpty) {
        elementTypes.map {
          case (t, nullable, name) => AttributeReference(name, t, nullable)()
        }
      } else {
        failAnalysis(
          "The number of aliases supplied in the AS clause does not match the number of columns " +
          s"output by the UDTF expected ${elementTypes.size} aliases but got " +
          s"${names.mkString(",")} ")
      }
    }
  }

  /**
   * Extracts [[WindowExpression]]s from the projectList of a [[Project]] operator and
   * aggregateExpressions of an [[Aggregate]] operator and creates individual [[Window]]
   * operators for every distinct [[WindowSpecDefinition]].
   *
   * This rule handles three cases:
   *  - A [[Project]] having [[WindowExpression]]s in its projectList;
   *  - An [[Aggregate]] having [[WindowExpression]]s in its aggregateExpressions.
   *  - An [[Filter]]->[[Aggregate]] pattern representing GROUP BY with a HAVING
   *    clause and the [[Aggregate]] has [[WindowExpression]]s in its aggregateExpressions.
   * Note: If there is a GROUP BY clause in the query, aggregations and corresponding
   * filters (expressions in the HAVING clause) should be evaluated before any
   * [[WindowExpression]]. If a query has SELECT DISTINCT, the DISTINCT part should be
   * evaluated after all [[WindowExpression]]s.
   *
   * For every case, the transformation works as follows:
   * 1. For a list of [[Expression]]s (a projectList or an aggregateExpressions), partitions
   *    it two lists of [[Expression]]s, one for all [[WindowExpression]]s and another for
   *    all regular expressions.
   * 2. For all [[WindowExpression]]s, groups them based on their [[WindowSpecDefinition]]s.
   * 3. For every distinct [[WindowSpecDefinition]], creates a [[Window]] operator and inserts
   *    it into the plan tree.
   */
  object ExtractWindowExpressions extends Rule[LogicalPlan] {
    private def hasWindowFunction(projectList: Seq[NamedExpression]): Boolean =
      projectList.exists(hasWindowFunction)

    private def hasWindowFunction(expr: NamedExpression): Boolean = {
      expr.find {
        case window: WindowExpression => true
        case _ => false
      }.isDefined
    }

    /**
     * From a Seq of [[NamedExpression]]s, extract expressions containing window expressions and
     * other regular expressions that do not contain any window expression. For example, for
     * `col1, Sum(col2 + col3) OVER (PARTITION BY col4 ORDER BY col5)`, we will extract
     * `col1`, `col2 + col3`, `col4`, and `col5` out and replace their appearances in
     * the window expression as attribute references. So, the first returned value will be
     * `[Sum(_w0) OVER (PARTITION BY _w1 ORDER BY _w2)]` and the second returned value will be
     * [col1, col2 + col3 as _w0, col4 as _w1, col5 as _w2].
     *
     * @return (seq of expressions containing at lease one window expressions,
     *          seq of non-window expressions)
     */
    private def extract(
        expressions: Seq[NamedExpression]): (Seq[NamedExpression], Seq[NamedExpression]) = {
      // First, we partition the input expressions to two part. For the first part,
      // every expression in it contain at least one WindowExpression.
      // Expressions in the second part do not have any WindowExpression.
      val (expressionsWithWindowFunctions, regularExpressions) =
        expressions.partition(hasWindowFunction)

      // Then, we need to extract those regular expressions used in the WindowExpression.
      // For example, when we have col1 - Sum(col2 + col3) OVER (PARTITION BY col4 ORDER BY col5),
      // we need to make sure that col1 to col5 are all projected from the child of the Window
      // operator.
      val extractedExprBuffer = new ArrayBuffer[NamedExpression]()
      def extractExpr(expr: Expression): Expression = expr match {
        case ne: NamedExpression =>
          // If a named expression is not in regularExpressions, add it to
          // extractedExprBuffer and replace it with an AttributeReference.
          val missingExpr =
            AttributeSet(Seq(expr)) -- (regularExpressions ++ extractedExprBuffer)
          if (missingExpr.nonEmpty) {
            extractedExprBuffer += ne
          }
          ne.toAttribute
        case e: Expression if e.foldable =>
          e // No need to create an attribute reference if it will be evaluated as a Literal.
        case e: Expression =>
          // For other expressions, we extract it and replace it with an AttributeReference (with
          // an interal column name, e.g. "_w0").
          val withName = Alias(e, s"_w${extractedExprBuffer.length}")()
          extractedExprBuffer += withName
          withName.toAttribute
      }

      // Now, we extract regular expressions from expressionsWithWindowFunctions
      // by using extractExpr.
      val newExpressionsWithWindowFunctions = expressionsWithWindowFunctions.map {
        _.transform {
          // Extracts children expressions of a WindowFunction (input parameters of
          // a WindowFunction).
          case wf : WindowFunction =>
            val newChildren = wf.children.map(extractExpr(_))
            wf.withNewChildren(newChildren)

          // Extracts expressions from the partition spec and order spec.
          case wsc @ WindowSpecDefinition(partitionSpec, orderSpec, _) =>
            val newPartitionSpec = partitionSpec.map(extractExpr(_))
            val newOrderSpec = orderSpec.map { so =>
              val newChild = extractExpr(so.child)
              so.copy(child = newChild)
            }
            wsc.copy(partitionSpec = newPartitionSpec, orderSpec = newOrderSpec)

          // Extracts AggregateExpression. For example, for SUM(x) - Sum(y) OVER (...),
          // we need to extract SUM(x).
          case agg: AggregateExpression =>
            val withName = Alias(agg, s"_w${extractedExprBuffer.length}")()
            extractedExprBuffer += withName
            withName.toAttribute

          // Extracts other attributes
          case attr: Attribute => extractExpr(attr)

        }.asInstanceOf[NamedExpression]
      }

      (newExpressionsWithWindowFunctions, regularExpressions ++ extractedExprBuffer)
    } // end of extract

    /**
     * Adds operators for Window Expressions. Every Window operator handles a single Window Spec.
     */
    private def addWindow(
        expressionsWithWindowFunctions: Seq[NamedExpression],
        child: LogicalPlan): LogicalPlan = {
      // First, we need to extract all WindowExpressions from expressionsWithWindowFunctions
      // and put those extracted WindowExpressions to extractedWindowExprBuffer.
      // This step is needed because it is possible that an expression contains multiple
      // WindowExpressions with different Window Specs.
      // After extracting WindowExpressions, we need to construct a project list to generate
      // expressionsWithWindowFunctions based on extractedWindowExprBuffer.
      // For example, for "sum(a) over (...) / sum(b) over (...)", we will first extract
      // "sum(a) over (...)" and "sum(b) over (...)" out, and assign "_we0" as the alias to
      // "sum(a) over (...)" and "_we1" as the alias to "sum(b) over (...)".
      // Then, the projectList will be [_we0/_we1].
      val extractedWindowExprBuffer = new ArrayBuffer[NamedExpression]()
      val newExpressionsWithWindowFunctions = expressionsWithWindowFunctions.map {
        // We need to use transformDown because we want to trigger
        // "case alias @ Alias(window: WindowExpression, _)" first.
        _.transformDown {
          case alias @ Alias(window: WindowExpression, _) =>
            // If a WindowExpression has an assigned alias, just use it.
            extractedWindowExprBuffer += alias
            alias.toAttribute
          case window: WindowExpression =>
            // If there is no alias assigned to the WindowExpressions. We create an
            // internal column.
            val withName = Alias(window, s"_we${extractedWindowExprBuffer.length}")()
            extractedWindowExprBuffer += withName
            withName.toAttribute
        }.asInstanceOf[NamedExpression]
      }

      // Second, we group extractedWindowExprBuffer based on their Partition and Order Specs.
      val groupedWindowExpressions = extractedWindowExprBuffer.groupBy { expr =>
        val distinctWindowSpec = expr.collect {
          case window: WindowExpression => window.windowSpec
        }.distinct

        // We do a final check and see if we only have a single Window Spec defined in an
        // expressions.
        if (distinctWindowSpec.length == 0 ) {
          failAnalysis(s"$expr does not have any WindowExpression.")
        } else if (distinctWindowSpec.length > 1) {
          // newExpressionsWithWindowFunctions only have expressions with a single
          // WindowExpression. If we reach here, we have a bug.
          failAnalysis(s"$expr has multiple Window Specifications ($distinctWindowSpec)." +
            s"Please file a bug report with this error message, stack trace, and the query.")
        } else {
          val spec = distinctWindowSpec.head
          (spec.partitionSpec, spec.orderSpec)
        }
      }.toSeq

      // Third, for every Window Spec, we add a Window operator and set currentChild as the
      // child of it.
      var currentChild = child
      var i = 0
      while (i < groupedWindowExpressions.size) {
        val ((partitionSpec, orderSpec), windowExpressions) = groupedWindowExpressions(i)
        // Set currentChild to the newly created Window operator.
        currentChild =
          Window(
            currentChild.output,
            windowExpressions,
            partitionSpec,
            orderSpec,
            currentChild)

        // Move to next Window Spec.
        i += 1
      }

      // Finally, we create a Project to output currentChild's output
      // newExpressionsWithWindowFunctions.
      Project(currentChild.output ++ newExpressionsWithWindowFunctions, currentChild)
    } // end of addWindow

    // We have to use transformDown at here to make sure the rule of
    // "Aggregate with Having clause" will be triggered.
    def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {

      // Aggregate with Having clause. This rule works with an unresolved Aggregate because
      // a resolved Aggregate will not have Window Functions.
      case f @ Filter(condition, a @ Aggregate(groupingExprs, aggregateExprs, child))
        if child.resolved &&
           hasWindowFunction(aggregateExprs) &&
           a.expressions.forall(_.resolved) =>
        val (windowExpressions, aggregateExpressions) = extract(aggregateExprs)
        // Create an Aggregate operator to evaluate aggregation functions.
        val withAggregate = Aggregate(groupingExprs, aggregateExpressions, child)
        // Add a Filter operator for conditions in the Having clause.
        val withFilter = Filter(condition, withAggregate)
        val withWindow = addWindow(windowExpressions, withFilter)

        // Finally, generate output columns according to the original projectList.
        val finalProjectList = aggregateExprs.map (_.toAttribute)
        Project(finalProjectList, withWindow)

      case p: LogicalPlan if !p.childrenResolved => p

      // Aggregate without Having clause.
      case a @ Aggregate(groupingExprs, aggregateExprs, child)
        if hasWindowFunction(aggregateExprs) &&
           a.expressions.forall(_.resolved) =>
        val (windowExpressions, aggregateExpressions) = extract(aggregateExprs)
        // Create an Aggregate operator to evaluate aggregation functions.
        val withAggregate = Aggregate(groupingExprs, aggregateExpressions, child)
        // Add Window operators.
        val withWindow = addWindow(windowExpressions, withAggregate)

        // Finally, generate output columns according to the original projectList.
        val finalProjectList = aggregateExprs.map (_.toAttribute)
        Project(finalProjectList, withWindow)

      // We only extract Window Expressions after all expressions of the Project
      // have been resolved.
      case p @ Project(projectList, child)
        if hasWindowFunction(projectList) && !p.expressions.exists(!_.resolved) =>
        val (windowExpressions, regularExpressions) = extract(projectList)
        // We add a project to get all needed expressions for window expressions from the child
        // of the original Project operator.
        val withProject = Project(regularExpressions, child)
        // Add Window operators.
        val withWindow = addWindow(windowExpressions, withProject)

        // Finally, generate output columns according to the original projectList.
        val finalProjectList = projectList.map (_.toAttribute)
        Project(finalProjectList, withWindow)
    }
  }

  /**
   * Pulls out nondeterministic expressions from LogicalPlan which is not Project or Filter,
   * put them into an inner Project and finally project them away at the outer Project.
   */
  object PullOutNondeterministic extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p if !p.resolved => p // Skip unresolved nodes.
      case p: Project => p
      case f: Filter => f

      // todo: It's hard to write a general rule to pull out nondeterministic expressions
      // from LogicalPlan, currently we only do it for UnaryNode which has same output
      // schema with its child.
      case p: UnaryNode if p.output == p.child.output && p.expressions.exists(!_.deterministic) =>
        val nondeterministicExprs = p.expressions.filterNot(_.deterministic).flatMap { expr =>
          val leafNondeterministic = expr.collect {
            case n: Nondeterministic => n
          }
          leafNondeterministic.map { e =>
            val ne = e match {
              case n: NamedExpression => n
              case _ => Alias(e, "_nondeterministic")()
            }
            new TreeNodeRef(e) -> ne
          }
        }.toMap
        val newPlan = p.transformExpressions { case e =>
          nondeterministicExprs.get(new TreeNodeRef(e)).map(_.toAttribute).getOrElse(e)
        }
        val newChild = Project(p.child.output ++ nondeterministicExprs.values, p.child)
        Project(p.output, newPlan.withNewChildren(newChild :: Nil))
    }
  }

  /**
   * Correctly handle null primitive inputs for UDF by adding extra [[If]] expression to do the
   * null check.  When user defines a UDF with primitive parameters, there is no way to tell if the
   * primitive parameter is null or not, so here we assume the primitive input is null-propagatable
   * and we should return null if the input is null.
   */
  object HandleNullInputsForUDF extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p if !p.resolved => p // Skip unresolved nodes.

      case p => p transformExpressionsUp {

        case udf @ ScalaUDF(func, _, inputs, _) =>
          val parameterTypes = ScalaReflection.getParameterTypes(func)
          assert(parameterTypes.length == inputs.length)

          val inputsNullCheck = parameterTypes.zip(inputs)
            // TODO: skip null handling for not-nullable primitive inputs after we can completely
            // trust the `nullable` information.
            // .filter { case (cls, expr) => cls.isPrimitive && expr.nullable }
            .filter { case (cls, _) => cls.isPrimitive }
            .map { case (_, expr) => IsNull(expr) }
            .reduceLeftOption[Expression]((e1, e2) => Or(e1, e2))
          inputsNullCheck.map(If(_, Literal.create(null, udf.dataType), udf)).getOrElse(udf)
      }
    }
  }
}

/**
 * Removes [[Subquery]] operators from the plan. Subqueries are only required to provide
 * scoping information for attributes and can be removed once analysis is complete.
 */
object EliminateSubQueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}

/**
 * Cleans up unnecessary Aliases inside the plan. Basically we only need Alias as a top level
 * expression in Project(project list) or Aggregate(aggregate expressions) or
 * Window(window expressions).
 */
object CleanupAliases extends Rule[LogicalPlan] {
  private def trimAliases(e: Expression): Expression = {
    var stop = false
    e.transformDown {
      // CreateStruct is a special case, we need to retain its top level Aliases as they decide the
      // name of StructField. We also need to stop transform down this expression, or the Aliases
      // under CreateStruct will be mistakenly trimmed.
      case c: CreateStruct if !stop =>
        stop = true
        c.copy(children = c.children.map(trimNonTopLevelAliases))
      case c: CreateStructUnsafe if !stop =>
        stop = true
        c.copy(children = c.children.map(trimNonTopLevelAliases))
      case Alias(child, _) if !stop => child
    }
  }

  def trimNonTopLevelAliases(e: Expression): Expression = e match {
    case a: Alias =>
      Alias(trimAliases(a.child), a.name)(a.exprId, a.qualifiers, a.explicitMetadata)
    case other => trimAliases(other)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case Project(projectList, child) =>
      val cleanedProjectList =
        projectList.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Project(cleanedProjectList, child)

    case Aggregate(grouping, aggs, child) =>
      val cleanedAggs = aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Aggregate(grouping.map(trimAliases), cleanedAggs, child)

    case w @ Window(projectList, windowExprs, partitionSpec, orderSpec, child) =>
      val cleanedWindowExprs =
        windowExprs.map(e => trimNonTopLevelAliases(e).asInstanceOf[NamedExpression])
      Window(projectList, cleanedWindowExprs, partitionSpec.map(trimAliases),
        orderSpec.map(trimAliases(_).asInstanceOf[SortOrder]), child)

    case other =>
      var stop = false
      other transformExpressionsDown {
        case c: CreateStruct if !stop =>
          stop = true
          c.copy(children = c.children.map(trimNonTopLevelAliases))
        case c: CreateStructUnsafe if !stop =>
          stop = true
          c.copy(children = c.children.map(trimNonTopLevelAliases))
        case Alias(child, _) if !stop => child
      }
  }
}

/**
 * Computes the current date and time to make sure we return the same result in a single query.
 */
object ComputeCurrentTime extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val dateExpr = CurrentDate()
    val timeExpr = CurrentTimestamp()
    val currentDate = Literal.create(dateExpr.eval(EmptyRow), dateExpr.dataType)
    val currentTime = Literal.create(timeExpr.eval(EmptyRow), timeExpr.dataType)

    plan transformAllExpressions {
      case CurrentDate() => currentDate
      case CurrentTimestamp() => currentTime
    }
  }
}

/**
 * Replace the `UpCast` expression by `Cast`, and throw exceptions if the cast may truncate.
 */
object ResolveUpCast extends Rule[LogicalPlan] {
  private def fail(from: Expression, to: DataType, walkedTypePath: Seq[String]) = {
    throw new AnalysisException(s"Cannot up cast `${from.prettyString}` from " +
      s"${from.dataType.simpleString} to ${to.simpleString} as it may truncate\n" +
      "The type path of the target object is:\n" + walkedTypePath.mkString("", "\n", "\n") +
      "You can either add an explicit cast to the input data or choose a higher precision " +
      "type of the field in the target object")
  }

  private def illegalNumericPrecedence(from: DataType, to: DataType): Boolean = {
    val fromPrecedence = HiveTypeCoercion.numericPrecedence.indexOf(from)
    val toPrecedence = HiveTypeCoercion.numericPrecedence.indexOf(to)
    toPrecedence > 0 && fromPrecedence > toPrecedence
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case u @ UpCast(child, _, _) if !child.resolved => u

      case UpCast(child, dataType, walkedTypePath) => (child.dataType, dataType) match {
        case (from: NumericType, to: DecimalType) if !to.isWiderThan(from) =>
          fail(child, to, walkedTypePath)
        case (from: DecimalType, to: NumericType) if !from.isTighterThan(to) =>
          fail(child, to, walkedTypePath)
        case (from, to) if illegalNumericPrecedence(from, to) =>
          fail(child, to, walkedTypePath)
        case (TimestampType, DateType) =>
          fail(child, DateType, walkedTypePath)
        case (StringType, to: NumericType) =>
          fail(child, to, walkedTypePath)
        case _ => Cast(child, dataType)
      }
    }
  }
}
