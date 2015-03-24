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

import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._

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
class Analyzer(catalog: Catalog,
               registry: FunctionRegistry,
               caseSensitive: Boolean,
               maxIterations: Int = 100)
  extends RuleExecutor[LogicalPlan] with HiveTypeCoercion {

  val resolver = if (caseSensitive) caseSensitiveResolution else caseInsensitiveResolution

  val fixedPoint = FixedPoint(maxIterations)

  /**
   * Override to provide additional rules for the "Resolution" batch.
   */
  val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  lazy val batches: Seq[Batch] = Seq(
    Batch("Resolution", fixedPoint,
      ResolveRelations ::
      ResolveReferences ::
      ResolveGroupingAnalytics ::
      ResolveSortReferences ::
      ImplicitGenerate ::
      ResolveFunctions ::
      GlobalAggregates ::
      UnresolvedHavingClauseAttributes ::
      TrimGroupingAliases ::
      typeCoercionRules ++
      extendedResolutionRules : _*),
    Batch("Remove SubQueries", fixedPoint,
      EliminateSubQueries)
  )

  /**
   * Removes no-op Alias expressions from the plan.
   */
  object TrimGroupingAliases extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Aggregate(groups, aggs, child) =>
        Aggregate(groups.map(_.transform { case Alias(c, _) => c }), aggs, child)
    }
  }

  object ResolveGroupingAnalytics extends Rule[LogicalPlan] {
    /**
     * Extract attribute set according to the grouping id
     * @param bitmask bitmask to represent the selected of the attribute sequence
     * @param exprs the attributes in sequence
     * @return the attributes of non selected specified via bitmask (with the bit set to 1)
     */
    private def buildNonSelectExprSet(bitmask: Int, exprs: Seq[Expression])
    : OpenHashSet[Expression] = {
      val set = new OpenHashSet[Expression](2)

      var bit = exprs.length - 1
      while (bit >= 0) {
        if (((bitmask >> bit) & 1) == 0) set.add(exprs(bit))
        bit -= 1
      }

      set
    }

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

    /**
     * Create an array of Projections for the child projection, and replace the projections'
     * expressions which equal GroupBy expressions with Literal(null), if those expressions
     * are not set for this grouping set (according to the bit mask).
     */
    private[this] def expand(g: GroupingSets): Seq[GroupExpression] = {
      val result = new scala.collection.mutable.ArrayBuffer[GroupExpression]

      g.bitmasks.foreach { bitmask =>
        // get the non selected grouping attributes according to the bit mask
        val nonSelectedGroupExprSet = buildNonSelectExprSet(bitmask, g.groupByExprs)

        val substitution = (g.child.output :+ g.gid).map(expr => expr transformDown {
          case x: Expression if nonSelectedGroupExprSet.contains(x) =>
            // if the input attribute in the Invalid Grouping Expression set of for this group
            // replace it with constant null
            Literal(null, expr.dataType)
          case x if x == g.gid =>
            // replace the groupingId with concrete value (the bit mask)
            Literal(bitmask, IntegerType)
        })

        result += GroupExpression(substitution)
      }

      result.toSeq
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case a: Cube if a.resolved =>
        GroupingSets(bitmasks(a), a.groupByExprs, a.child, a.aggregations, a.gid)
      case a: Rollup if a.resolved =>
        GroupingSets(bitmasks(a), a.groupByExprs, a.child, a.aggregations, a.gid)
      case x: GroupingSets if x.resolved =>
        Aggregate(
          x.groupByExprs :+ x.gid,
          x.aggregations,
          Expand(expand(x), x.child.output :+ x.gid, x.child))
    }
  }

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelations extends Rule[LogicalPlan] {
    def getTable(u: UnresolvedRelation) = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"no such table ${u.tableName}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case i @ InsertIntoTable(u: UnresolvedRelation, _, _, _) =>
        i.copy(
          table = EliminateSubQueries(getTable(u)))
      case u: UnresolvedRelation =>
        getTable(u)
    }
  }

  /**
   * Replaces [[UnresolvedAttribute]]s with concrete
   * [[catalyst.expressions.AttributeReference AttributeReferences]] from a logical plan node's
   * children.
   */
  object ResolveReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case p: LogicalPlan if !p.childrenResolved => p

      // If the projection list contains Stars, expand it.
      case p @ Project(projectList, child) if containsStar(projectList) =>
        Project(
          projectList.flatMap {
            case s: Star => s.expand(child.output, resolver)
            case Alias(f @ UnresolvedFunction(_, args), name) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child.output, resolver)
                case o => o :: Nil
              }
              Alias(child = f.copy(children = expandedArgs), name)() :: Nil
            case Alias(c @ CreateArray(args), name) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child.output, resolver)
                case o => o :: Nil
              }
              Alias(c.copy(children = expandedArgs), name)() :: Nil
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

      // Special handling for cases when self-join introduce duplicate expression ids.
      case j @ Join(left, right, _, _) if left.outputSet.intersect(right.outputSet).nonEmpty =>
        val conflictingAttributes = left.outputSet.intersect(right.outputSet)
        logDebug(s"Conflicting attributes ${conflictingAttributes.mkString(",")} in $j")

        val (oldRelation, newRelation) = right.collect {
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
        }.head // Only handle first case found, others will be fixed on the next pass.

        val attributeRewrites = AttributeMap(oldRelation.output.zip(newRelation.output))
        val newRight = right transformUp {
          case r if r == oldRelation => newRelation
        } transformUp {
          case other => other transformExpressions {
            case a: Attribute => attributeRewrites.get(a).getOrElse(a)
          }
        }
        j.copy(right = newRight)

      case q: LogicalPlan =>
        logTrace(s"Attempting to resolve ${q.simpleString}")
        q transformExpressionsUp  {
          case u @ UnresolvedAttribute(name) if resolver(name, VirtualColumn.groupingIdName) &&
            q.isInstanceOf[GroupingAnalytics] =>
            // Resolve the virtual column GROUPING__ID for the operator GroupingAnalytics
            q.asInstanceOf[GroupingAnalytics].gid
          case u @ UnresolvedAttribute(name) =>
            // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
            val result =
              withPosition(u) { q.resolveChildren(name, resolver).getOrElse(u) }
            logDebug(s"Resolving $u to $result")
            result
          case UnresolvedGetField(child, fieldName) if child.resolved =>
            resolveGetField(child, fieldName)
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
    protected def containsStar(exprs: Seq[Expression]): Boolean =
      exprs.exists(_.collect { case _: Star => true }.nonEmpty)

    /**
     * Returns the resolved `GetField`, and report error if no desired field or over one
     * desired fields are found.
     */
    protected def resolveGetField(expr: Expression, fieldName: String): Expression = {
      def findField(fields: Array[StructField]): Int = {
        val checkField = (f: StructField) => resolver(f.name, fieldName)
        val ordinal = fields.indexWhere(checkField)
        if (ordinal == -1) {
          throw new AnalysisException(
            s"No such struct field $fieldName in ${fields.map(_.name).mkString(", ")}")
        } else if (fields.indexWhere(checkField, ordinal + 1) != -1) {
          throw new AnalysisException(
            s"Ambiguous reference to fields ${fields.filter(checkField).mkString(", ")}")
        } else {
          ordinal
        }
      }
      expr.dataType match {
        case StructType(fields) =>
          val ordinal = findField(fields)
          StructGetField(expr, fields(ordinal), ordinal)
        case ArrayType(StructType(fields), containsNull) =>
          val ordinal = findField(fields)
          ArrayGetField(expr, fields(ordinal), ordinal, containsNull)
        case otherType =>
          throw new AnalysisException(s"GetField is not valid on fields of type $otherType")
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
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case s @ Sort(ordering, global, p @ Project(projectList, child))
          if !s.resolved && p.resolved =>
        val unresolved = ordering.flatMap(_.collect { case UnresolvedAttribute(name) => name })
        val resolved = unresolved.flatMap(child.resolve(_, resolver))
        val requiredAttributes =
          AttributeSet(resolved.flatMap(_.collect { case a: Attribute => a }))

        val missingInProject = requiredAttributes -- p.output
        if (missingInProject.nonEmpty) {
          // Add missing attributes and then project them away after the sort.
          Project(projectList.map(_.toAttribute),
            Sort(ordering, global,
              Project(projectList ++ missingInProject, child)))
        } else {
          logDebug(s"Failed to find $missingInProject in ${p.output.mkString(", ")}")
          s // Nothing we can do here. Return original plan.
        }
      case s @ Sort(ordering, global, a @ Aggregate(grouping, aggs, child))
          if !s.resolved && a.resolved =>
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
            Sort(ordering, global, Aggregate(grouping, aggs ++ missingInAggs, child)))
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
}

/**
 * Removes [[catalyst.plans.logical.Subquery Subquery]] operators from the plan.  Subqueries are
 * only required to provide scoping information for attributes and can be removed once analysis is
 * complete.
 */
object EliminateSubQueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}
