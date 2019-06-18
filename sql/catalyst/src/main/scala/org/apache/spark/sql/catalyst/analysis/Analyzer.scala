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

import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalog.v2.{CatalogNotFoundException, CatalogPlugin, LookupCatalog}
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.OuterScopes
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * A trivial [[Analyzer]] with a dummy [[SessionCatalog]] and [[EmptyFunctionRegistry]].
 * Used for testing when all relations are already filled in and the analyzer needs only
 * to resolve attribute references.
 */
object SimpleAnalyzer extends Analyzer(
  new SessionCatalog(
    new InMemoryCatalog,
    EmptyFunctionRegistry,
    new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true)) {
    override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean) {}
  },
  new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true))

/**
 * Provides a way to keep state during the analysis, this enables us to decouple the concerns
 * of analysis environment from the catalog.
 * The state that is kept here is per-query.
 *
 * Note this is thread local.
 *
 * @param defaultDatabase The default database used in the view resolution, this overrules the
 *                        current catalog database.
 * @param nestedViewDepth The nested depth in the view resolution, this enables us to limit the
 *                        depth of nested views.
 */
case class AnalysisContext(
    defaultDatabase: Option[String] = None,
    nestedViewDepth: Int = 0)

object AnalysisContext {
  private val value = new ThreadLocal[AnalysisContext]() {
    override def initialValue: AnalysisContext = AnalysisContext()
  }

  def get: AnalysisContext = value.get()
  def reset(): Unit = value.remove()

  private def set(context: AnalysisContext): Unit = value.set(context)

  def withAnalysisContext[A](database: Option[String])(f: => A): A = {
    val originContext = value.get()
    val context = AnalysisContext(defaultDatabase = database,
      nestedViewDepth = originContext.nestedViewDepth + 1)
    set(context)
    try f finally { set(originContext) }
  }
}

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a [[SessionCatalog]].
 */
class Analyzer(
    catalog: SessionCatalog,
    conf: SQLConf,
    maxIterations: Int)
  extends RuleExecutor[LogicalPlan] with CheckAnalysis with LookupCatalog {

  def this(catalog: SessionCatalog, conf: SQLConf) = {
    this(catalog, conf, conf.optimizerMaxIterations)
  }

  override protected def lookupCatalog(name: String): CatalogPlugin =
    throw new CatalogNotFoundException("No catalog lookup function")

  def executeAndCheck(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
    AnalysisHelper.markInAnalyzer {
      val analyzed = executeAndTrack(plan, tracker)
      try {
        checkAnalysis(analyzed)
        analyzed
      } catch {
        case e: AnalysisException =>
          val ae = new AnalysisException(e.message, e.line, e.startPosition, Option(analyzed))
          ae.setStackTrace(e.getStackTrace)
          throw ae
      }
    }
  }

  override def execute(plan: LogicalPlan): LogicalPlan = {
    AnalysisContext.reset()
    try {
      executeSameContext(plan)
    } finally {
      AnalysisContext.reset()
    }
  }

  private def executeSameContext(plan: LogicalPlan): LogicalPlan = super.execute(plan)

  def resolver: Resolver = conf.resolver

  protected val fixedPoint = FixedPoint(maxIterations)

  /**
   * Override to provide additional rules for the "Resolution" batch.
   */
  val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  /**
   * Override to provide rules to do post-hoc resolution. Note that these rules will be executed
   * in an individual batch. This batch is to run right after the normal resolution batch and
   * execute its rules in one pass.
   */
  val postHocResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  lazy val batches: Seq[Batch] = Seq(
    Batch("Hints", fixedPoint,
      new ResolveHints.ResolveJoinStrategyHints(conf),
      ResolveHints.ResolveCoalesceHints,
      new ResolveHints.RemoveAllHints(conf)),
    Batch("Simple Sanity Check", Once,
      LookupFunctions),
    Batch("Substitution", fixedPoint,
      CTESubstitution,
      WindowsSubstitution,
      EliminateUnions,
      new SubstituteUnresolvedOrdinals(conf)),
    Batch("Resolution", fixedPoint,
      ResolveTableValuedFunctions ::
      ResolveTables ::
      ResolveRelations ::
      ResolveReferences ::
      ResolveCreateNamedStruct ::
      ResolveDeserializer ::
      ResolveNewInstance ::
      ResolveUpCast ::
      ResolveGroupingAnalytics ::
      ResolvePivot ::
      ResolveOrdinalInOrderByAndGroupBy ::
      ResolveAggAliasInGroupBy ::
      ResolveMissingReferences ::
      ExtractGenerator ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      ResolveSubquery ::
      ResolveSubqueryColumnAliases ::
      ResolveWindowOrder ::
      ResolveWindowFrame ::
      ResolveNaturalAndUsingJoin ::
      ResolveOutputRelation ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      TimeWindowing ::
      ResolveInlineTables(conf) ::
      ResolveHigherOrderFunctions(catalog) ::
      ResolveLambdaVariables(conf) ::
      ResolveTimeZone(conf) ::
      ResolveRandomSeed ::
      TypeCoercion.typeCoercionRules(conf) ++
      extendedResolutionRules : _*),
    Batch("Post-Hoc Resolution", Once, postHocResolutionRules: _*),
    Batch("View", Once,
      AliasViewChild(conf)),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("UDF", Once,
      HandleNullInputsForUDF),
    Batch("UpdateNullability", Once,
      UpdateAttributeNullability),
    Batch("Subquery", Once,
      UpdateOuterReferences),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )

  /**
   * Analyze cte definitions and substitute child plan with analyzed cte definitions.
   */
  object CTESubstitution extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case With(child, relations) =>
        // substitute CTE expressions right-to-left to resolve references to previous CTEs:
        // with a as (select * from t), b as (select * from a) select * from b
        relations.foldRight(child) {
          case ((cteName, ctePlan), currentPlan) =>
            substituteCTE(currentPlan, cteName, ctePlan)
        }
      case other => other
    }

    def substituteCTE(plan: LogicalPlan, cteName: String, ctePlan: LogicalPlan): LogicalPlan = {
      plan resolveOperatorsUp {
        case UnresolvedRelation(Seq(table)) if resolver(cteName, table) =>
          ctePlan
        case u: UnresolvedRelation =>
          u
        case other =>
          // This cannot be done in ResolveSubquery because ResolveSubquery does not know the CTE.
          other transformExpressions {
            case e: SubqueryExpression =>
              e.withNewPlan(substituteCTE(e.plan, cteName, ctePlan))
          }
      }
    }
  }

  /**
   * Substitute child plan with WindowSpecDefinitions.
   */
  object WindowsSubstitution extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      // Lookup WindowSpecDefinitions. This rule works with unresolved children.
      case WithWindowDefinition(windowDefinitions, child) => child.resolveExpressions {
        case UnresolvedWindowExpression(c, WindowSpecReference(windowName)) =>
          val errorMessage =
            s"Window specification $windowName is not defined in the WINDOW clause."
          val windowSpecDefinition =
            windowDefinitions.getOrElse(windowName, failAnalysis(errorMessage))
          WindowExpression(c, windowSpecDefinition)
      }
    }
  }

  /**
   * Replaces [[UnresolvedAlias]]s with concrete aliases.
   */
  object ResolveAliases extends Rule[LogicalPlan] {
    private def assignAliases(exprs: Seq[NamedExpression]) = {
      exprs.map(_.transformUp { case u @ UnresolvedAlias(child, optGenAliasFunc) =>
          child match {
            case ne: NamedExpression => ne
            case go @ GeneratorOuter(g: Generator) if g.resolved => MultiAlias(go, Nil)
            case e if !e.resolved => u
            case g: Generator => MultiAlias(g, Nil)
            case c @ Cast(ne: NamedExpression, _, _) => Alias(c, ne.name)()
            case e: ExtractValue => Alias(e, toPrettySQL(e))()
            case e if optGenAliasFunc.isDefined =>
              Alias(child, optGenAliasFunc.get.apply(e))()
            case e => Alias(e, toPrettySQL(e))()
          }
        }
      ).asInstanceOf[Seq[NamedExpression]]
    }

    private def hasUnresolvedAlias(exprs: Seq[NamedExpression]) =
      exprs.exists(_.find(_.isInstanceOf[UnresolvedAlias]).isDefined)

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case Aggregate(groups, aggs, child) if child.resolved && hasUnresolvedAlias(aggs) =>
        Aggregate(groups, assignAliases(aggs), child)

      case g: GroupingSets if g.child.resolved && hasUnresolvedAlias(g.aggregations) =>
        g.copy(aggregations = assignAliases(g.aggregations))

      case Pivot(groupByOpt, pivotColumn, pivotValues, aggregates, child)
        if child.resolved && groupByOpt.isDefined && hasUnresolvedAlias(groupByOpt.get) =>
        Pivot(Some(assignAliases(groupByOpt.get)), pivotColumn, pivotValues, aggregates, child)

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
     *  represented as sequence of expressions.
     */
    def rollupExprs(exprs: Seq[Expression]): Seq[Seq[Expression]] = exprs.inits.toIndexedSeq

    /*
     *  GROUP BY a, b, c WITH CUBE
     *  is equivalent to
     *  GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ( ) ).
     *  Group Count: 2 ^ N (N is the number of group expressions)
     *
     *  We need to get all of its subsets for a given GROUPBY expression, the subsets are
     *  represented as sequence of expressions.
     */
    def cubeExprs(exprs: Seq[Expression]): Seq[Seq[Expression]] = {
      // `cubeExprs0` is recursive and returns a lazy Stream. Here we call `toIndexedSeq` to
      // materialize it and avoid serialization problems later on.
      cubeExprs0(exprs).toIndexedSeq
    }

    def cubeExprs0(exprs: Seq[Expression]): Seq[Seq[Expression]] = exprs.toList match {
      case x :: xs =>
        val initial = cubeExprs0(xs)
        initial.map(x +: _) ++ initial
      case Nil =>
        Seq(Seq.empty)
    }

    private[analysis] def hasGroupingFunction(e: Expression): Boolean = {
      e.collectFirst {
        case g: Grouping => g
        case g: GroupingID => g
      }.isDefined
    }

    private def replaceGroupingFunc(
        expr: Expression,
        groupByExprs: Seq[Expression],
        gid: Expression): Expression = {
      expr transform {
        case e: GroupingID =>
          if (e.groupByExprs.isEmpty || e.groupByExprs == groupByExprs) {
            Alias(gid, toPrettySQL(e))()
          } else {
            throw new AnalysisException(
              s"Columns of grouping_id (${e.groupByExprs.mkString(",")}) does not match " +
                s"grouping columns (${groupByExprs.mkString(",")})")
          }
        case e @ Grouping(col: Expression) =>
          val idx = groupByExprs.indexWhere(_.semanticEquals(col))
          if (idx >= 0) {
            Alias(Cast(BitwiseAnd(ShiftRight(gid, Literal(groupByExprs.length - 1 - idx)),
              Literal(1)), ByteType), toPrettySQL(e))()
          } else {
            throw new AnalysisException(s"Column of grouping ($col) can't be found " +
              s"in grouping columns ${groupByExprs.mkString(",")}")
          }
      }
    }

    /*
     * Create new alias for all group by expressions for `Expand` operator.
     */
    private def constructGroupByAlias(groupByExprs: Seq[Expression]): Seq[Alias] = {
      groupByExprs.map {
        case e: NamedExpression => Alias(e, e.name)()
        case other => Alias(other, other.toString)()
      }
    }

    /*
     * Construct [[Expand]] operator with grouping sets.
     */
    private def constructExpand(
        selectedGroupByExprs: Seq[Seq[Expression]],
        child: LogicalPlan,
        groupByAliases: Seq[Alias],
        gid: Attribute): LogicalPlan = {
      // Change the nullability of group by aliases if necessary. For example, if we have
      // GROUPING SETS ((a,b), a), we do not need to change the nullability of a, but we
      // should change the nullabilty of b to be TRUE.
      // TODO: For Cube/Rollup just set nullability to be `true`.
      val expandedAttributes = groupByAliases.map { alias =>
        if (selectedGroupByExprs.exists(!_.contains(alias.child))) {
          alias.toAttribute.withNullability(true)
        } else {
          alias.toAttribute
        }
      }

      val groupingSetsAttributes = selectedGroupByExprs.map { groupingSetExprs =>
        groupingSetExprs.map { expr =>
          val alias = groupByAliases.find(_.child.semanticEquals(expr)).getOrElse(
            failAnalysis(s"$expr doesn't show up in the GROUP BY list $groupByAliases"))
          // Map alias to expanded attribute.
          expandedAttributes.find(_.semanticEquals(alias.toAttribute)).getOrElse(
            alias.toAttribute)
        }
      }

      Expand(groupingSetsAttributes, groupByAliases, expandedAttributes, gid, child)
    }

    /*
     * Construct new aggregate expressions by replacing grouping functions.
     */
    private def constructAggregateExprs(
        groupByExprs: Seq[Expression],
        aggregations: Seq[NamedExpression],
        groupByAliases: Seq[Alias],
        groupingAttrs: Seq[Expression],
        gid: Attribute): Seq[NamedExpression] = aggregations.map {
      // collect all the found AggregateExpression, so we can check an expression is part of
      // any AggregateExpression or not.
      val aggsBuffer = ArrayBuffer[Expression]()
      // Returns whether the expression belongs to any expressions in `aggsBuffer` or not.
      def isPartOfAggregation(e: Expression): Boolean = {
        aggsBuffer.exists(a => a.find(_ eq e).isDefined)
      }
      replaceGroupingFunc(_, groupByExprs, gid).transformDown {
        // AggregateExpression should be computed on the unmodified value of its argument
        // expressions, so we should not replace any references to grouping expression
        // inside it.
        case e: AggregateExpression =>
          aggsBuffer += e
          e
        case e if isPartOfAggregation(e) => e
        case e =>
          // Replace expression by expand output attribute.
          val index = groupByAliases.indexWhere(_.child.semanticEquals(e))
          if (index == -1) {
            e
          } else {
            groupingAttrs(index)
          }
      }.asInstanceOf[NamedExpression]
    }

    /*
     * Construct [[Aggregate]] operator from Cube/Rollup/GroupingSets.
     */
    private def constructAggregate(
        selectedGroupByExprs: Seq[Seq[Expression]],
        groupByExprs: Seq[Expression],
        aggregationExprs: Seq[NamedExpression],
        child: LogicalPlan): LogicalPlan = {
      val gid = AttributeReference(VirtualColumn.groupingIdName, IntegerType, false)()

      // In case of ANSI-SQL compliant syntax for GROUPING SETS, groupByExprs is optional and
      // can be null. In such case, we derive the groupByExprs from the user supplied values for
      // grouping sets.
      val finalGroupByExpressions = if (groupByExprs == Nil) {
        selectedGroupByExprs.flatten.foldLeft(Seq.empty[Expression]) { (result, currentExpr) =>
          // Only unique expressions are included in the group by expressions and is determined
          // based on their semantic equality. Example. grouping sets ((a * b), (b * a)) results
          // in grouping expression (a * b)
          if (result.find(_.semanticEquals(currentExpr)).isDefined) {
            result
          } else {
            result :+ currentExpr
          }
        }
      } else {
        groupByExprs
      }

      // Expand works by setting grouping expressions to null as determined by the
      // `selectedGroupByExprs`. To prevent these null values from being used in an aggregate
      // instead of the original value we need to create new aliases for all group by expressions
      // that will only be used for the intended purpose.
      val groupByAliases = constructGroupByAlias(finalGroupByExpressions)

      val expand = constructExpand(selectedGroupByExprs, child, groupByAliases, gid)
      val groupingAttrs = expand.output.drop(child.output.length)

      val aggregations = constructAggregateExprs(
        finalGroupByExpressions, aggregationExprs, groupByAliases, groupingAttrs, gid)

      Aggregate(groupingAttrs, aggregations, expand)
    }

    private def findGroupingExprs(plan: LogicalPlan): Seq[Expression] = {
      plan.collectFirst {
        case a: Aggregate =>
          // this Aggregate should have grouping id as the last grouping key.
          val gid = a.groupingExpressions.last
          if (!gid.isInstanceOf[AttributeReference]
            || gid.asInstanceOf[AttributeReference].name != VirtualColumn.groupingIdName) {
            failAnalysis(s"grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
          }
          a.groupingExpressions.take(a.groupingExpressions.length - 1)
      }.getOrElse {
        failAnalysis(s"grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
      }
    }

    // This require transformUp to replace grouping()/grouping_id() in resolved Filter/Sort
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp {
      case a if !a.childrenResolved => a // be sure all of the children are resolved.

      // Ensure group by expressions and aggregate expressions have been resolved.
      case Aggregate(Seq(c @ Cube(groupByExprs)), aggregateExpressions, child)
        if (groupByExprs ++ aggregateExpressions).forall(_.resolved) =>
        constructAggregate(cubeExprs(groupByExprs), groupByExprs, aggregateExpressions, child)
      case Aggregate(Seq(r @ Rollup(groupByExprs)), aggregateExpressions, child)
        if (groupByExprs ++ aggregateExpressions).forall(_.resolved) =>
        constructAggregate(rollupExprs(groupByExprs), groupByExprs, aggregateExpressions, child)
      // Ensure all the expressions have been resolved.
      case x: GroupingSets if x.expressions.forall(_.resolved) =>
        constructAggregate(x.selectedGroupByExprs, x.groupByExprs, x.aggregations, x.child)

      // We should make sure all expressions in condition have been resolved.
      case f @ Filter(cond, child) if hasGroupingFunction(cond) && cond.resolved =>
        val groupingExprs = findGroupingExprs(child)
        // The unresolved grouping id will be resolved by ResolveMissingReferences
        val newCond = replaceGroupingFunc(cond, groupingExprs, VirtualColumn.groupingIdAttribute)
        f.copy(condition = newCond)

      // We should make sure all [[SortOrder]]s have been resolved.
      case s @ Sort(order, _, child)
        if order.exists(hasGroupingFunction) && order.forall(_.resolved) =>
        val groupingExprs = findGroupingExprs(child)
        val gid = VirtualColumn.groupingIdAttribute
        // The unresolved grouping id will be resolved by ResolveMissingReferences
        val newOrder = order.map(replaceGroupingFunc(_, groupingExprs, gid).asInstanceOf[SortOrder])
        s.copy(order = newOrder)
    }
  }

  object ResolvePivot extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p: Pivot if !p.childrenResolved || !p.aggregates.forall(_.resolved)
        || (p.groupByExprsOpt.isDefined && !p.groupByExprsOpt.get.forall(_.resolved))
        || !p.pivotColumn.resolved || !p.pivotValues.forall(_.resolved) => p
      case Pivot(groupByExprsOpt, pivotColumn, pivotValues, aggregates, child) =>
        if (!RowOrdering.isOrderable(pivotColumn.dataType)) {
          throw new AnalysisException(
            s"Invalid pivot column '${pivotColumn}'. Pivot columns must be comparable.")
        }
        // Check all aggregate expressions.
        aggregates.foreach(checkValidAggregateExpression)
        // Check all pivot values are literal and match pivot column data type.
        val evalPivotValues = pivotValues.map { value =>
          val foldable = value match {
            case Alias(v, _) => v.foldable
            case _ => value.foldable
          }
          if (!foldable) {
            throw new AnalysisException(
              s"Literal expressions required for pivot values, found '$value'")
          }
          if (!Cast.canCast(value.dataType, pivotColumn.dataType)) {
            throw new AnalysisException(s"Invalid pivot value '$value': " +
              s"value data type ${value.dataType.simpleString} does not match " +
              s"pivot column data type ${pivotColumn.dataType.catalogString}")
          }
          Cast(value, pivotColumn.dataType, Some(conf.sessionLocalTimeZone)).eval(EmptyRow)
        }
        // Group-by expressions coming from SQL are implicit and need to be deduced.
        val groupByExprs = groupByExprsOpt.getOrElse {
          val pivotColAndAggRefs = pivotColumn.references ++ AttributeSet(aggregates)
          child.output.filterNot(pivotColAndAggRefs.contains)
        }
        val singleAgg = aggregates.size == 1
        def outputName(value: Expression, aggregate: Expression): String = {
          val stringValue = value match {
            case n: NamedExpression => n.name
            case _ =>
              val utf8Value =
                Cast(value, StringType, Some(conf.sessionLocalTimeZone)).eval(EmptyRow)
              Option(utf8Value).map(_.toString).getOrElse("null")
          }
          if (singleAgg) {
            stringValue
          } else {
            val suffix = aggregate match {
              case n: NamedExpression => n.name
              case _ => toPrettySQL(aggregate)
            }
            stringValue + "_" + suffix
          }
        }
        if (aggregates.forall(a => PivotFirst.supportsDataType(a.dataType))) {
          // Since evaluating |pivotValues| if statements for each input row can get slow this is an
          // alternate plan that instead uses two steps of aggregation.
          val namedAggExps: Seq[NamedExpression] = aggregates.map(a => Alias(a, a.sql)())
          val namedPivotCol = pivotColumn match {
            case n: NamedExpression => n
            case _ => Alias(pivotColumn, "__pivot_col")()
          }
          val bigGroup = groupByExprs :+ namedPivotCol
          val firstAgg = Aggregate(bigGroup, bigGroup ++ namedAggExps, child)
          val pivotAggs = namedAggExps.map { a =>
            Alias(PivotFirst(namedPivotCol.toAttribute, a.toAttribute, evalPivotValues)
              .toAggregateExpression()
            , "__pivot_" + a.sql)()
          }
          val groupByExprsAttr = groupByExprs.map(_.toAttribute)
          val secondAgg = Aggregate(groupByExprsAttr, groupByExprsAttr ++ pivotAggs, firstAgg)
          val pivotAggAttribute = pivotAggs.map(_.toAttribute)
          val pivotOutputs = pivotValues.zipWithIndex.flatMap { case (value, i) =>
            aggregates.zip(pivotAggAttribute).map { case (aggregate, pivotAtt) =>
              Alias(ExtractValue(pivotAtt, Literal(i), resolver), outputName(value, aggregate))()
            }
          }
          Project(groupByExprsAttr ++ pivotOutputs, secondAgg)
        } else {
          val pivotAggregates: Seq[NamedExpression] = pivotValues.flatMap { value =>
            def ifExpr(e: Expression) = {
              If(
                EqualNullSafe(
                  pivotColumn,
                  Cast(value, pivotColumn.dataType, Some(conf.sessionLocalTimeZone))),
                e, Literal(null))
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
              }.transform {
                // We are duplicating aggregates that are now computing a different value for each
                // pivot value.
                // TODO: Don't construct the physical container until after analysis.
                case ae: AggregateExpression => ae.copy(resultId = NamedExpression.newExprId)
              }
              Alias(filteredAggregate, outputName(value, aggregate))()
            }
          }
          Aggregate(groupByExprs, groupByExprs ++ pivotAggregates, child)
        }
    }

    // Support any aggregate expression that can appear in an Aggregate plan except Pandas UDF.
    // TODO: Support Pandas UDF.
    private def checkValidAggregateExpression(expr: Expression): Unit = expr match {
      case _: AggregateExpression => // OK and leave the argument check to CheckAnalysis.
      case expr: PythonUDF if PythonUDF.isGroupedAggPandasUDF(expr) =>
        failAnalysis("Pandas UDF aggregate expressions are currently not supported in pivot.")
      case e: Attribute =>
        failAnalysis(
          s"Aggregate expression required for pivot, but '${e.sql}' " +
          s"did not appear in any aggregate function.")
      case e => e.children.foreach(checkValidAggregateExpression)
    }
  }

  /**
   * Resolve table relations with concrete relations from v2 catalog.
   *
   * [[ResolveRelations]] still resolves v1 tables.
   */
  object ResolveTables extends Rule[LogicalPlan] {
    import org.apache.spark.sql.catalog.v2.utils.CatalogV2Util._

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case u @ UnresolvedRelation(CatalogObjectIdentifier(Some(catalogPlugin), ident)) =>
        loadTable(catalogPlugin, ident).map(DataSourceV2Relation.create).getOrElse(u)
    }
  }

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelations extends Rule[LogicalPlan] {

    // If the unresolved relation is running directly on files, we just return the original
    // UnresolvedRelation, the plan will get resolved later. Else we look up the table from catalog
    // and change the default database name(in AnalysisContext) if it is a view.
    // We usually look up a table from the default database if the table identifier has an empty
    // database part, for a view the default database should be the currentDb when the view was
    // created. When the case comes to resolving a nested view, the view may have different default
    // database with that the referenced view has, so we need to use
    // `AnalysisContext.defaultDatabase` to track the current default database.
    // When the relation we resolve is a view, we fetch the view.desc(which is a CatalogTable), and
    // then set the value of `CatalogTable.viewDefaultDatabase` to
    // `AnalysisContext.defaultDatabase`, we look up the relations that the view references using
    // the default database.
    // For example:
    // |- view1 (defaultDatabase = db1)
    //   |- operator
    //     |- table2 (defaultDatabase = db1)
    //     |- view2 (defaultDatabase = db2)
    //        |- view3 (defaultDatabase = db3)
    //   |- view4 (defaultDatabase = db4)
    // In this case, the view `view1` is a nested view, it directly references `table2`, `view2`
    // and `view4`, the view `view2` references `view3`. On resolving the table, we look up the
    // relations `table2`, `view2`, `view4` using the default database `db1`, and look up the
    // relation `view3` using the default database `db2`.
    //
    // Note this is compatible with the views defined by older versions of Spark(before 2.2), which
    // have empty defaultDatabase and all the relations in viewText have database part defined.
    def resolveRelation(plan: LogicalPlan): LogicalPlan = plan match {
      case u @ UnresolvedRelation(AsTableIdentifier(ident)) if !isRunningDirectlyOnFiles(ident) =>
        val defaultDatabase = AnalysisContext.get.defaultDatabase
        val foundRelation = lookupTableFromCatalog(ident, u, defaultDatabase)
        if (foundRelation != u) {
          resolveRelation(foundRelation)
        } else {
          u
        }

      // The view's child should be a logical plan parsed from the `desc.viewText`, the variable
      // `viewText` should be defined, or else we throw an error on the generation of the View
      // operator.
      case view @ View(desc, _, child) if !child.resolved =>
        // Resolve all the UnresolvedRelations and Views in the child.
        val newChild = AnalysisContext.withAnalysisContext(desc.viewDefaultDatabase) {
          if (AnalysisContext.get.nestedViewDepth > conf.maxNestedViewDepth) {
            view.failAnalysis(s"The depth of view ${view.desc.identifier} exceeds the maximum " +
              s"view resolution depth (${conf.maxNestedViewDepth}). Analysis is aborted to " +
              s"avoid errors. Increase the value of ${SQLConf.MAX_NESTED_VIEW_DEPTH.key} to work " +
              "around this.")
          }
          executeSameContext(child)
        }
        view.copy(child = newChild)
      case p @ SubqueryAlias(_, view: View) =>
        val newChild = resolveRelation(view)
        p.copy(child = newChild)
      case _ => plan
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case i @ InsertIntoTable(u @ UnresolvedRelation(AsTableIdentifier(ident)), _, child, _, _)
          if child.resolved =>
        EliminateSubqueryAliases(lookupTableFromCatalog(ident, u)) match {
          case v: View =>
            u.failAnalysis(s"Inserting into a view is not allowed. View: ${v.desc.identifier}.")
          case other => i.copy(table = other)
        }
      case u: UnresolvedRelation => resolveRelation(u)
    }

    // Look up the table with the given name from catalog. The database we used is decided by the
    // precedence:
    // 1. Use the database part of the table identifier, if it is defined;
    // 2. Use defaultDatabase, if it is defined(In this case, no temporary objects can be used,
    //    and the default database is only used to look up a view);
    // 3. Use the currentDb of the SessionCatalog.
    private def lookupTableFromCatalog(
        tableIdentifier: TableIdentifier,
        u: UnresolvedRelation,
        defaultDatabase: Option[String] = None): LogicalPlan = {
      val tableIdentWithDb = tableIdentifier.copy(
        database = tableIdentifier.database.orElse(defaultDatabase))
      try {
        catalog.lookupRelation(tableIdentWithDb)
      } catch {
        case _: NoSuchTableException | _: NoSuchDatabaseException =>
          u
      }
    }

    // If the database part is specified, and we support running SQL directly on files, and
    // it's not a temporary view, and the table does not exist, then let's just return the
    // original UnresolvedRelation. It is possible we are matching a query like "select *
    // from parquet.`/path/to/query`". The plan will get resolved in the rule `ResolveDataSource`.
    // Note that we are testing (!db_exists || !table_exists) because the catalog throws
    // an exception from tableExists if the database does not exist.
    private def isRunningDirectlyOnFiles(table: TableIdentifier): Boolean = {
      table.database.isDefined && conf.runSQLonFile && !catalog.isTemporaryTable(table) &&
        (!catalog.databaseExists(table.database.get) || !catalog.tableExists(table))
    }
  }

  /**
   * Replaces [[UnresolvedAttribute]]s with concrete [[AttributeReference]]s from
   * a logical plan node's children.
   */
  object ResolveReferences extends Rule[LogicalPlan] {
    /**
     * Generate a new logical plan for the right child with different expression IDs
     * for all conflicting attributes.
     */
    private def dedupRight (left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
      val conflictingAttributes = left.outputSet.intersect(right.outputSet)
      logDebug(s"Conflicting attributes ${conflictingAttributes.mkString(",")} " +
        s"between $left and $right")

      right.collect {
        // Handle base relations that might appear more than once.
        case oldVersion: MultiInstanceRelation
            if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
          val newVersion = oldVersion.newInstance()
          (oldVersion, newVersion)

        case oldVersion: SerializeFromObject
            if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
          (oldVersion, oldVersion.copy(serializer = oldVersion.serializer.map(_.newInstance())))

        // Handle projects that create conflicting aliases.
        case oldVersion @ Project(projectList, _)
            if findAliases(projectList).intersect(conflictingAttributes).nonEmpty =>
          (oldVersion, oldVersion.copy(projectList = newAliases(projectList)))

        case oldVersion @ Aggregate(_, aggregateExpressions, _)
            if findAliases(aggregateExpressions).intersect(conflictingAttributes).nonEmpty =>
          (oldVersion, oldVersion.copy(aggregateExpressions = newAliases(aggregateExpressions)))

        case oldVersion @ FlatMapGroupsInPandas(_, _, output, _)
            if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
          (oldVersion, oldVersion.copy(output = output.map(_.newInstance())))

        case oldVersion: Generate
            if oldVersion.producedAttributes.intersect(conflictingAttributes).nonEmpty =>
          val newOutput = oldVersion.generatorOutput.map(_.newInstance())
          (oldVersion, oldVersion.copy(generatorOutput = newOutput))

        case oldVersion @ Window(windowExpressions, _, _, child)
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
          right
        case Some((oldRelation, newRelation)) =>
          val attributeRewrites = AttributeMap(oldRelation.output.zip(newRelation.output))
          right transformUp {
            case r if r == oldRelation => newRelation
          } transformUp {
            case other => other transformExpressions {
              case a: Attribute =>
                dedupAttr(a, attributeRewrites)
              case s: SubqueryExpression =>
                s.withNewPlan(dedupOuterReferencesInSubquery(s.plan, attributeRewrites))
            }
          }
      }
    }

    private def dedupAttr(attr: Attribute, attrMap: AttributeMap[Attribute]): Attribute = {
      val exprId = attrMap.getOrElse(attr, attr).exprId
      attr.withExprId(exprId)
    }

    /**
     * The outer plan may have been de-duplicated and the function below updates the
     * outer references to refer to the de-duplicated attributes.
     *
     * For example (SQL):
     * {{{
     *   SELECT * FROM t1
     *   INTERSECT
     *   SELECT * FROM t1
     *   WHERE EXISTS (SELECT 1
     *                 FROM t2
     *                 WHERE t1.c1 = t2.c1)
     * }}}
     * Plan before resolveReference rule.
     *    'Intersect
     *    :- Project [c1#245, c2#246]
     *    :  +- SubqueryAlias t1
     *    :     +- Relation[c1#245,c2#246] parquet
     *    +- 'Project [*]
     *       +- Filter exists#257 [c1#245]
     *       :  +- Project [1 AS 1#258]
     *       :     +- Filter (outer(c1#245) = c1#251)
     *       :        +- SubqueryAlias t2
     *       :           +- Relation[c1#251,c2#252] parquet
     *       +- SubqueryAlias t1
     *          +- Relation[c1#245,c2#246] parquet
     * Plan after the resolveReference rule.
     *    Intersect
     *    :- Project [c1#245, c2#246]
     *    :  +- SubqueryAlias t1
     *    :     +- Relation[c1#245,c2#246] parquet
     *    +- Project [c1#259, c2#260]
     *       +- Filter exists#257 [c1#259]
     *       :  +- Project [1 AS 1#258]
     *       :     +- Filter (outer(c1#259) = c1#251) => Updated
     *       :        +- SubqueryAlias t2
     *       :           +- Relation[c1#251,c2#252] parquet
     *       +- SubqueryAlias t1
     *          +- Relation[c1#259,c2#260] parquet  => Outer plan's attributes are de-duplicated.
     */
    private def dedupOuterReferencesInSubquery(
        plan: LogicalPlan,
        attrMap: AttributeMap[Attribute]): LogicalPlan = {
      plan transformDown { case currentFragment =>
        currentFragment transformExpressions {
          case OuterReference(a: Attribute) =>
            OuterReference(dedupAttr(a, attrMap))
          case s: SubqueryExpression =>
            s.withNewPlan(dedupOuterReferencesInSubquery(s.plan, attrMap))
        }
      }
    }

    /**
     * Resolves the attribute and extract value expressions(s) by traversing the
     * input expression in top down manner. The traversal is done in top-down manner as
     * we need to skip over unbound lamda function expression. The lamda expressions are
     * resolved in a different rule [[ResolveLambdaVariables]]
     *
     * Example :
     * SELECT transform(array(1, 2, 3), (x, i) -> x + i)"
     *
     * In the case above, x and i are resolved as lamda variables in [[ResolveLambdaVariables]]
     *
     * Note : In this routine, the unresolved attributes are resolved from the input plan's
     * children attributes.
     */
    private def resolveExpressionTopDown(e: Expression, q: LogicalPlan): Expression = {
      if (e.resolved) return e
      e match {
        case f: LambdaFunction if !f.bound => f
        case u @ UnresolvedAttribute(nameParts) =>
          // Leave unchanged if resolution fails. Hopefully will be resolved next round.
          val result =
            withPosition(u) {
              q.resolveChildren(nameParts, resolver)
                .orElse(resolveLiteralFunction(nameParts, u, q))
                .getOrElse(u)
            }
          logDebug(s"Resolving $u to $result")
          result
        case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
          ExtractValue(child, fieldExpr, resolver)
        case _ => e.mapChildren(resolveExpressionTopDown(_, q))
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p: LogicalPlan if !p.childrenResolved => p

      // If the projection list contains Stars, expand it.
      case p: Project if containsStar(p.projectList) =>
        p.copy(projectList = buildExpandedProjectList(p.projectList, p.child))
      // If the aggregate function argument contains Stars, expand it.
      case a: Aggregate if containsStar(a.aggregateExpressions) =>
        if (a.groupingExpressions.exists(_.isInstanceOf[UnresolvedOrdinal])) {
          failAnalysis(
            "Star (*) is not allowed in select list when GROUP BY ordinal position is used")
        } else {
          a.copy(aggregateExpressions = buildExpandedProjectList(a.aggregateExpressions, a.child))
        }
      // If the script transformation input contains Stars, expand it.
      case t: ScriptTransformation if containsStar(t.input) =>
        t.copy(
          input = t.input.flatMap {
            case s: Star => s.expand(t.child, resolver)
            case o => o :: Nil
          }
        )
      case g: Generate if containsStar(g.generator.children) =>
        failAnalysis("Invalid usage of '*' in explode/json_tuple/UDTF")

      // To resolve duplicate expression IDs for Join and Intersect
      case j @ Join(left, right, _, _, _) if !j.duplicateResolved =>
        j.copy(right = dedupRight(left, right))
      case i @ Intersect(left, right, _) if !i.duplicateResolved =>
        i.copy(right = dedupRight(left, right))
      case e @ Except(left, right, _) if !e.duplicateResolved =>
        e.copy(right = dedupRight(left, right))
      case u @ Union(children) if !u.duplicateResolved =>
        // Use projection-based de-duplication for Union to avoid breaking the checkpoint sharing
        // feature in streaming.
        val newChildren = children.foldRight(Seq.empty[LogicalPlan]) { (head, tail) =>
          head +: tail.map {
            case child if head.outputSet.intersect(child.outputSet).isEmpty =>
              child
            case child =>
              val projectList = child.output.map { attr =>
                Alias(attr, attr.name)()
              }
              Project(projectList, child)
          }
        }
        u.copy(children = newChildren)

      // When resolve `SortOrder`s in Sort based on child, don't report errors as
      // we still have chance to resolve it based on its descendants
      case s @ Sort(ordering, global, child) if child.resolved && !s.resolved =>
        val newOrdering =
          ordering.map(order => resolveExpressionBottomUp(order, child).asInstanceOf[SortOrder])
        Sort(newOrdering, global, child)

      // A special case for Generate, because the output of Generate should not be resolved by
      // ResolveReferences. Attributes in the output will be resolved by ResolveGenerate.
      case g @ Generate(generator, _, _, _, _, _) if generator.resolved => g

      case g @ Generate(generator, join, outer, qualifier, output, child) =>
        val newG = resolveExpressionBottomUp(generator, child, throws = true)
        if (newG.fastEquals(generator)) {
          g
        } else {
          Generate(newG.asInstanceOf[Generator], join, outer, qualifier, output, child)
        }

      // Skips plan which contains deserializer expressions, as they should be resolved by another
      // rule: ResolveDeserializer.
      case plan if containsDeserializer(plan.expressions) => plan

      // SPARK-25942: Resolves aggregate expressions with `AppendColumns`'s children, instead of
      // `AppendColumns`, because `AppendColumns`'s serializer might produce conflict attribute
      // names leading to ambiguous references exception.
      case a @ Aggregate(groupingExprs, aggExprs, appendColumns: AppendColumns) =>
        a.mapExpressions(resolveExpressionTopDown(_, appendColumns))

      case o: OverwriteByExpression if !o.outputResolved =>
        // do not resolve expression attributes until the query attributes are resolved against the
        // table by ResolveOutputRelation. that rule will alias the attributes to the table's names.
        o

      case q: LogicalPlan =>
        logTrace(s"Attempting to resolve ${q.simpleString(SQLConf.get.maxToStringFields)}")
        q.mapExpressions(resolveExpressionTopDown(_, q))
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
     * Build a project list for Project/Aggregate and expand the star if possible
     */
    private def buildExpandedProjectList(
      exprs: Seq[NamedExpression],
      child: LogicalPlan): Seq[NamedExpression] = {
      exprs.flatMap {
        // Using Dataframe/Dataset API: testData2.groupBy($"a", $"b").agg($"*")
        case s: Star => s.expand(child, resolver)
        // Using SQL API without running ResolveAlias: SELECT * FROM testData2 group by a, b
        case UnresolvedAlias(s: Star, _) => s.expand(child, resolver)
        case o if containsStar(o :: Nil) => expandStarExpression(o, child) :: Nil
        case o => o :: Nil
      }.map(_.asInstanceOf[NamedExpression])
    }

    /**
     * Returns true if `exprs` contains a [[Star]].
     */
    def containsStar(exprs: Seq[Expression]): Boolean =
      exprs.exists(_.collect { case _: Star => true }.nonEmpty)

    /**
     * Expands the matching attribute.*'s in `child`'s output.
     */
    def expandStarExpression(expr: Expression, child: LogicalPlan): Expression = {
      expr.transformUp {
        case f1: UnresolvedFunction if containsStar(f1.children) =>
          f1.copy(children = f1.children.flatMap {
            case s: Star => s.expand(child, resolver)
            case o => o :: Nil
          })
        case c: CreateNamedStruct if containsStar(c.valExprs) =>
          val newChildren = c.children.grouped(2).flatMap {
            case Seq(k, s : Star) => CreateStruct(s.expand(child, resolver)).children
            case kv => kv
          }
          c.copy(children = newChildren.toList )
        case c: CreateArray if containsStar(c.children) =>
          c.copy(children = c.children.flatMap {
            case s: Star => s.expand(child, resolver)
            case o => o :: Nil
          })
        case p: Murmur3Hash if containsStar(p.children) =>
          p.copy(children = p.children.flatMap {
            case s: Star => s.expand(child, resolver)
            case o => o :: Nil
          })
        case p: XxHash64 if containsStar(p.children) =>
          p.copy(children = p.children.flatMap {
            case s: Star => s.expand(child, resolver)
            case o => o :: Nil
          })
        // count(*) has been replaced by count(1)
        case o if containsStar(o.children) =>
          failAnalysis(s"Invalid usage of '*' in expression '${o.prettyName}'")
      }
    }
  }

  private def containsDeserializer(exprs: Seq[Expression]): Boolean = {
    exprs.exists(_.find(_.isInstanceOf[UnresolvedDeserializer]).isDefined)
  }

  /**
   * Literal functions do not require the user to specify braces when calling them
   * When an attributes is not resolvable, we try to resolve it as a literal function.
   */
  private def resolveLiteralFunction(
      nameParts: Seq[String],
      attribute: UnresolvedAttribute,
      plan: LogicalPlan): Option[Expression] = {
    if (nameParts.length != 1) return None
    val isNamedExpression = plan match {
      case Aggregate(_, aggregateExpressions, _) => aggregateExpressions.contains(attribute)
      case Project(projectList, _) => projectList.contains(attribute)
      case Window(windowExpressions, _, _, _) => windowExpressions.contains(attribute)
      case _ => false
    }
    val wrapper: Expression => Expression =
      if (isNamedExpression) f => Alias(f, toPrettySQL(f))() else identity
    // support CURRENT_DATE and CURRENT_TIMESTAMP
    val literalFunctions = Seq(CurrentDate(), CurrentTimestamp())
    val name = nameParts.head
    val func = literalFunctions.find(e => caseInsensitiveResolution(e.prettyName, name))
    func.map(wrapper)
  }

  /**
   * Resolves the attribute, column value and extract value expressions(s) by traversing the
   * input expression in bottom-up manner. In order to resolve the nested complex type fields
   * correctly, this function makes use of `throws` parameter to control when to raise an
   * AnalysisException.
   *
   * Example :
   * SELECT a.b FROM t ORDER BY b[0].d
   *
   * In the above example, in b needs to be resolved before d can be resolved. Given we are
   * doing a bottom up traversal, it will first attempt to resolve d and fail as b has not
   * been resolved yet. If `throws` is false, this function will handle the exception by
   * returning the original attribute. In this case `d` will be resolved in subsequent passes
   * after `b` is resolved.
   */
  protected[sql] def resolveExpressionBottomUp(
      expr: Expression,
      plan: LogicalPlan,
      throws: Boolean = false): Expression = {
    if (expr.resolved) return expr
    // Resolve expression in one round.
    // If throws == false or the desired attribute doesn't exist
    // (like try to resolve `a.b` but `a` doesn't exist), fail and return the origin one.
    // Else, throw exception.
    try {
      expr transformUp {
        case GetColumnByOrdinal(ordinal, _) => plan.output(ordinal)
        case u @ UnresolvedAttribute(nameParts) =>
          val result =
            withPosition(u) {
              plan.resolve(nameParts, resolver)
                .orElse(resolveLiteralFunction(nameParts, u, plan))
                .getOrElse(u)
            }
          logDebug(s"Resolving $u to $result")
          result
        case UnresolvedExtractValue(child, fieldName) if child.resolved =>
          ExtractValue(child, fieldName, resolver)
      }
    } catch {
      case a: AnalysisException if !throws => expr
    }
  }

  /**
   * In many dialects of SQL it is valid to use ordinal positions in order/sort by and group by
   * clauses. This rule is to convert ordinal positions to the corresponding expressions in the
   * select list. This support is introduced in Spark 2.0.
   *
   * - When the sort references or group by expressions are not integer but foldable expressions,
   * just ignore them.
   * - When spark.sql.orderByOrdinal/spark.sql.groupByOrdinal is set to false, ignore the position
   * numbers too.
   *
   * Before the release of Spark 2.0, the literals in order/sort by and group by clauses
   * have no effect on the results.
   */
  object ResolveOrdinalInOrderByAndGroupBy extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.childrenResolved => p
      // Replace the index with the related attribute for ORDER BY,
      // which is a 1-base position of the projection list.
      case Sort(orders, global, child)
        if orders.exists(_.child.isInstanceOf[UnresolvedOrdinal]) =>
        val newOrders = orders map {
          case s @ SortOrder(UnresolvedOrdinal(index), direction, nullOrdering, _) =>
            if (index > 0 && index <= child.output.size) {
              SortOrder(child.output(index - 1), direction, nullOrdering, Set.empty)
            } else {
              s.failAnalysis(
                s"ORDER BY position $index is not in select list " +
                  s"(valid range is [1, ${child.output.size}])")
            }
          case o => o
        }
        Sort(newOrders, global, child)

      // Replace the index with the corresponding expression in aggregateExpressions. The index is
      // a 1-base position of aggregateExpressions, which is output columns (select expression)
      case Aggregate(groups, aggs, child) if aggs.forall(_.resolved) &&
        groups.exists(_.isInstanceOf[UnresolvedOrdinal]) =>
        val newGroups = groups.map {
          case u @ UnresolvedOrdinal(index) if index > 0 && index <= aggs.size =>
            aggs(index - 1)
          case ordinal @ UnresolvedOrdinal(index) =>
            ordinal.failAnalysis(
              s"GROUP BY position $index is not in select list " +
                s"(valid range is [1, ${aggs.size}])")
          case o => o
        }
        Aggregate(newGroups, aggs, child)
    }
  }

  /**
   * Replace unresolved expressions in grouping keys with resolved ones in SELECT clauses.
   * This rule is expected to run after [[ResolveReferences]] applied.
   */
  object ResolveAggAliasInGroupBy extends Rule[LogicalPlan] {

    // This is a strict check though, we put this to apply the rule only if the expression is not
    // resolvable by child.
    private def notResolvableByChild(attrName: String, child: LogicalPlan): Boolean = {
      !child.output.exists(a => resolver(a.name, attrName))
    }

    private def mayResolveAttrByAggregateExprs(
        exprs: Seq[Expression], aggs: Seq[NamedExpression], child: LogicalPlan): Seq[Expression] = {
      exprs.map { _.transform {
        case u: UnresolvedAttribute if notResolvableByChild(u.name, child) =>
          aggs.find(ne => resolver(ne.name, u.name)).getOrElse(u)
      }}
    }

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case agg @ Aggregate(groups, aggs, child)
          if conf.groupByAliases && child.resolved && aggs.forall(_.resolved) &&
            groups.exists(!_.resolved) =>
        agg.copy(groupingExpressions = mayResolveAttrByAggregateExprs(groups, aggs, child))

      case gs @ GroupingSets(selectedGroups, groups, child, aggs)
          if conf.groupByAliases && child.resolved && aggs.forall(_.resolved) &&
            groups.exists(_.isInstanceOf[UnresolvedAttribute]) =>
        gs.copy(
          selectedGroupByExprs = selectedGroups.map(mayResolveAttrByAggregateExprs(_, aggs, child)),
          groupByExprs = mayResolveAttrByAggregateExprs(groups, aggs, child))
    }
  }

  /**
   * In many dialects of SQL it is valid to sort by attributes that are not present in the SELECT
   * clause.  This rule detects such queries and adds the required attributes to the original
   * projection, so that they will be available during sorting. Another projection is added to
   * remove these attributes after sorting.
   *
   * The HAVING clause could also used a grouping columns that is not presented in the SELECT.
   */
  object ResolveMissingReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      // Skip sort with aggregate. This will be handled in ResolveAggregateFunctions
      case sa @ Sort(_, _, child: Aggregate) => sa

      case s @ Sort(order, _, child)
          if (!s.resolved || s.missingInput.nonEmpty) && child.resolved =>
        val (newOrder, newChild) = resolveExprsAndAddMissingAttrs(order, child)
        val ordering = newOrder.map(_.asInstanceOf[SortOrder])
        if (child.output == newChild.output) {
          s.copy(order = ordering)
        } else {
          // Add missing attributes and then project them away.
          val newSort = s.copy(order = ordering, child = newChild)
          Project(child.output, newSort)
        }

      case f @ Filter(cond, child) if (!f.resolved || f.missingInput.nonEmpty) && child.resolved =>
        val (newCond, newChild) = resolveExprsAndAddMissingAttrs(Seq(cond), child)
        if (child.output == newChild.output) {
          f.copy(condition = newCond.head)
        } else {
          // Add missing attributes and then project them away.
          val newFilter = Filter(newCond.head, newChild)
          Project(child.output, newFilter)
        }
    }

    /**
     * This method tries to resolve expressions and find missing attributes recursively. Specially,
     * when the expressions used in `Sort` or `Filter` contain unresolved attributes or resolved
     * attributes which are missed from child output. This method tries to find the missing
     * attributes out and add into the projection.
     */
    private def resolveExprsAndAddMissingAttrs(
        exprs: Seq[Expression], plan: LogicalPlan): (Seq[Expression], LogicalPlan) = {
      // Missing attributes can be unresolved attributes or resolved attributes which are not in
      // the output attributes of the plan.
      if (exprs.forall(e => e.resolved && e.references.subsetOf(plan.outputSet))) {
        (exprs, plan)
      } else {
        plan match {
          case p: Project =>
            // Resolving expressions against current plan.
            val maybeResolvedExprs = exprs.map(resolveExpressionBottomUp(_, p))
            // Recursively resolving expressions on the child of current plan.
            val (newExprs, newChild) = resolveExprsAndAddMissingAttrs(maybeResolvedExprs, p.child)
            // If some attributes used by expressions are resolvable only on the rewritten child
            // plan, we need to add them into original projection.
            val missingAttrs = (AttributeSet(newExprs) -- p.outputSet).intersect(newChild.outputSet)
            (newExprs, Project(p.projectList ++ missingAttrs, newChild))

          case a @ Aggregate(groupExprs, aggExprs, child) =>
            val maybeResolvedExprs = exprs.map(resolveExpressionBottomUp(_, a))
            val (newExprs, newChild) = resolveExprsAndAddMissingAttrs(maybeResolvedExprs, child)
            val missingAttrs = (AttributeSet(newExprs) -- a.outputSet).intersect(newChild.outputSet)
            if (missingAttrs.forall(attr => groupExprs.exists(_.semanticEquals(attr)))) {
              // All the missing attributes are grouping expressions, valid case.
              (newExprs, a.copy(aggregateExpressions = aggExprs ++ missingAttrs, child = newChild))
            } else {
              // Need to add non-grouping attributes, invalid case.
              (exprs, a)
            }

          case g: Generate =>
            val maybeResolvedExprs = exprs.map(resolveExpressionBottomUp(_, g))
            val (newExprs, newChild) = resolveExprsAndAddMissingAttrs(maybeResolvedExprs, g.child)
            (newExprs, g.copy(unrequiredChildIndex = Nil, child = newChild))

          // For `Distinct` and `SubqueryAlias`, we can't recursively resolve and add attributes
          // via its children.
          case u: UnaryNode if !u.isInstanceOf[Distinct] && !u.isInstanceOf[SubqueryAlias] =>
            val maybeResolvedExprs = exprs.map(resolveExpressionBottomUp(_, u))
            val (newExprs, newChild) = resolveExprsAndAddMissingAttrs(maybeResolvedExprs, u.child)
            (newExprs, u.withNewChildren(Seq(newChild)))

          // For other operators, we can't recursively resolve and add attributes via its children.
          case other =>
            (exprs.map(resolveExpressionBottomUp(_, other)), other)
        }
      }
    }
  }

  /**
   * Checks whether a function identifier referenced by an [[UnresolvedFunction]] is defined in the
   * function registry. Note that this rule doesn't try to resolve the [[UnresolvedFunction]]. It
   * only performs simple existence check according to the function identifier to quickly identify
   * undefined functions without triggering relation resolution, which may incur potentially
   * expensive partition/schema discovery process in some cases.
   * In order to avoid duplicate external functions lookup, the external function identifier will
   * store in the local hash set externalFunctionNameSet.
   * @see [[ResolveFunctions]]
   * @see https://issues.apache.org/jira/browse/SPARK-19737
   */
  object LookupFunctions extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      val externalFunctionNameSet = new mutable.HashSet[FunctionIdentifier]()
      plan.resolveExpressions {
        case f: UnresolvedFunction
          if externalFunctionNameSet.contains(normalizeFuncName(f.name)) => f
        case f: UnresolvedFunction if catalog.isRegisteredFunction(f.name) => f
        case f: UnresolvedFunction if catalog.isPersistentFunction(f.name) =>
          externalFunctionNameSet.add(normalizeFuncName(f.name))
          f
        case f: UnresolvedFunction =>
          withPosition(f) {
            throw new NoSuchFunctionException(f.name.database.getOrElse(catalog.getCurrentDatabase),
              f.name.funcName)
          }
      }
    }

    def normalizeFuncName(name: FunctionIdentifier): FunctionIdentifier = {
      val funcName = if (conf.caseSensitiveAnalysis) {
        name.funcName
      } else {
        name.funcName.toLowerCase(Locale.ROOT)
      }

      val databaseName = name.database match {
        case Some(a) => formatDatabaseName(a)
        case None => catalog.getCurrentDatabase
      }

      FunctionIdentifier(funcName, Some(databaseName))
    }

    protected def formatDatabaseName(name: String): String = {
      if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
    }
  }

  /**
   * Replaces [[UnresolvedFunction]]s with concrete [[Expression]]s.
   */
  object ResolveFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case q: LogicalPlan =>
        q transformExpressions {
          case u if !u.childrenResolved => u // Skip until children are resolved.
          case u: UnresolvedAttribute if resolver(u.name, VirtualColumn.hiveGroupingIdName) =>
            withPosition(u) {
              Alias(GroupingID(Nil), VirtualColumn.hiveGroupingIdName)()
            }
          case u @ UnresolvedGenerator(name, children) =>
            withPosition(u) {
              catalog.lookupFunction(name, children) match {
                case generator: Generator => generator
                case other =>
                  failAnalysis(s"$name is expected to be a generator. However, " +
                    s"its class is ${other.getClass.getCanonicalName}, which is not a generator.")
              }
            }
          case u @ UnresolvedFunction(funcId, children, isDistinct) =>
            withPosition(u) {
              catalog.lookupFunction(funcId, children) match {
                // AggregateWindowFunctions are AggregateFunctions that can only be evaluated within
                // the context of a Window clause. They do not need to be wrapped in an
                // AggregateExpression.
                case wf: AggregateWindowFunction =>
                  if (isDistinct) {
                    failAnalysis(s"${wf.prettyName} does not support the modifier DISTINCT")
                  } else {
                    wf
                  }
                // We get an aggregate function, we need to wrap it in an AggregateExpression.
                case agg: AggregateFunction => AggregateExpression(agg, Complete, isDistinct)
                // This function is not an aggregate function, just return the resolved one.
                case other =>
                  if (isDistinct) {
                    failAnalysis(s"${other.prettyName} does not support the modifier DISTINCT")
                  } else {
                    other
                  }
              }
            }
        }
    }
  }

  /**
   * This rule resolves and rewrites subqueries inside expressions.
   *
   * Note: CTEs are handled in CTESubstitution.
   */
  object ResolveSubquery extends Rule[LogicalPlan] with PredicateHelper {
    /**
     * Resolve the correlated expressions in a subquery by using the an outer plans' references. All
     * resolved outer references are wrapped in an [[OuterReference]]
     */
    private def resolveOuterReferences(plan: LogicalPlan, outer: LogicalPlan): LogicalPlan = {
      plan resolveOperatorsDown {
        case q: LogicalPlan if q.childrenResolved && !q.resolved =>
          q transformExpressions {
            case u @ UnresolvedAttribute(nameParts) =>
              withPosition(u) {
                try {
                  outer.resolve(nameParts, resolver) match {
                    case Some(outerAttr) => OuterReference(outerAttr)
                    case None => u
                  }
                } catch {
                  case _: AnalysisException => u
                }
              }
          }
      }
    }

    /**
     * Resolves the subquery plan that is referenced in a subquery expression. The normal
     * attribute references are resolved using regular analyzer and the outer references are
     * resolved from the outer plans using the resolveOuterReferences method.
     *
     * Outer references from the correlated predicates are updated as children of
     * Subquery expression.
     */
    private def resolveSubQuery(
        e: SubqueryExpression,
        plans: Seq[LogicalPlan])(
        f: (LogicalPlan, Seq[Expression]) => SubqueryExpression): SubqueryExpression = {
      // Step 1: Resolve the outer expressions.
      var previous: LogicalPlan = null
      var current = e.plan
      do {
        // Try to resolve the subquery plan using the regular analyzer.
        previous = current
        current = executeSameContext(current)

        // Use the outer references to resolve the subquery plan if it isn't resolved yet.
        val i = plans.iterator
        val afterResolve = current
        while (!current.resolved && current.fastEquals(afterResolve) && i.hasNext) {
          current = resolveOuterReferences(current, i.next())
        }
      } while (!current.resolved && !current.fastEquals(previous))

      // Step 2: If the subquery plan is fully resolved, pull the outer references and record
      // them as children of SubqueryExpression.
      if (current.resolved) {
        // Record the outer references as children of subquery expression.
        f(current, SubExprUtils.getOuterReferences(current))
      } else {
        e.withNewPlan(current)
      }
    }

    /**
     * Resolves the subquery. Apart of resolving the subquery and outer references (if any)
     * in the subquery plan, the children of subquery expression are updated to record the
     * outer references. This is needed to make sure
     * (1) The column(s) referred from the outer query are not pruned from the plan during
     *     optimization.
     * (2) Any aggregate expression(s) that reference outer attributes are pushed down to
     *     outer plan to get evaluated.
     */
    private def resolveSubQueries(plan: LogicalPlan, plans: Seq[LogicalPlan]): LogicalPlan = {
      plan transformExpressions {
        case s @ ScalarSubquery(sub, _, exprId) if !sub.resolved =>
          resolveSubQuery(s, plans)(ScalarSubquery(_, _, exprId))
        case e @ Exists(sub, _, exprId) if !sub.resolved =>
          resolveSubQuery(e, plans)(Exists(_, _, exprId))
        case InSubquery(values, l @ ListQuery(_, _, exprId, _))
            if values.forall(_.resolved) && !l.resolved =>
          val expr = resolveSubQuery(l, plans)((plan, exprs) => {
            ListQuery(plan, exprs, exprId, plan.output)
          })
          InSubquery(values, expr.asInstanceOf[ListQuery])
      }
    }

    /**
     * Resolve and rewrite all subqueries in an operator tree..
     */
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      // In case of HAVING (a filter after an aggregate) we use both the aggregate and
      // its child for resolution.
      case f @ Filter(_, a: Aggregate) if f.childrenResolved =>
        resolveSubQueries(f, Seq(a, a.child))
      // Only a few unary nodes (Project/Filter/Aggregate) can contain subqueries.
      case q: UnaryNode if q.childrenResolved =>
        resolveSubQueries(q, q.children)
    }
  }

  /**
   * Replaces unresolved column aliases for a subquery with projections.
   */
  object ResolveSubqueryColumnAliases extends Rule[LogicalPlan] {

     def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case u @ UnresolvedSubqueryColumnAliases(columnNames, child) if child.resolved =>
        // Resolves output attributes if a query has alias names in its subquery:
        // e.g., SELECT * FROM (SELECT 1 AS a, 1 AS b) t(col1, col2)
        val outputAttrs = child.output
        // Checks if the number of the aliases equals to the number of output columns
        // in the subquery.
        if (columnNames.size != outputAttrs.size) {
          u.failAnalysis("Number of column aliases does not match number of columns. " +
            s"Number of column aliases: ${columnNames.size}; " +
            s"number of columns: ${outputAttrs.size}.")
        }
        val aliases = outputAttrs.zip(columnNames).map { case (attr, aliasName) =>
          Alias(attr, aliasName)()
        }
        Project(aliases, child)
    }
  }

  /**
   * Turns projections that contain aggregate expressions into aggregations.
   */
  object GlobalAggregates extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
      case Project(projectList, child) if containsAggregates(projectList) =>
        Aggregate(Nil, projectList, child)
    }

    def containsAggregates(exprs: Seq[Expression]): Boolean = {
      // Collect all Windowed Aggregate Expressions.
      val windowedAggExprs = exprs.flatMap { expr =>
        expr.collect {
          case WindowExpression(ae: AggregateExpression, _) => ae
        }
      }.toSet

      // Find the first Aggregate Expression that is not Windowed.
      exprs.exists(_.collectFirst {
        case ae: AggregateExpression if !windowedAggExprs.contains(ae) => ae
      }.isDefined)
    }
  }

  /**
   * This rule finds aggregate expressions that are not in an aggregate operator.  For example,
   * those in a HAVING clause or ORDER BY clause.  These expressions are pushed down to the
   * underlying aggregate operator and then projected away after the original operator.
   */
  object ResolveAggregateFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case f @ Filter(cond, agg @ Aggregate(grouping, originalAggExprs, child)) if agg.resolved =>

        // Try resolving the condition of the filter as though it is in the aggregate clause
        try {
          val aggregatedCondition =
            Aggregate(
              grouping,
              Alias(cond, "havingCondition")() :: Nil,
              child)
          val resolvedOperator = executeSameContext(aggregatedCondition)
          def resolvedAggregateFilter =
            resolvedOperator
              .asInstanceOf[Aggregate]
              .aggregateExpressions.head

          // If resolution was successful and we see the filter has an aggregate in it, add it to
          // the original aggregate operator.
          if (resolvedOperator.resolved) {
            // Try to replace all aggregate expressions in the filter by an alias.
            val aggregateExpressions = ArrayBuffer.empty[NamedExpression]
            val transformedAggregateFilter = resolvedAggregateFilter.transform {
              case ae: AggregateExpression =>
                val alias = Alias(ae, ae.toString)()
                aggregateExpressions += alias
                alias.toAttribute
              // Grouping functions are handled in the rule [[ResolveGroupingAnalytics]].
              case e: Expression if grouping.exists(_.semanticEquals(e)) &&
                  !ResolveGroupingAnalytics.hasGroupingFunction(e) &&
                  !agg.output.exists(_.semanticEquals(e)) =>
                e match {
                  case ne: NamedExpression =>
                    aggregateExpressions += ne
                    ne.toAttribute
                  case _ =>
                    val alias = Alias(e, e.toString)()
                    aggregateExpressions += alias
                    alias.toAttribute
                }
            }

            // Push the aggregate expressions into the aggregate (if any).
            if (aggregateExpressions.nonEmpty) {
              Project(agg.output,
                Filter(transformedAggregateFilter,
                  agg.copy(aggregateExpressions = originalAggExprs ++ aggregateExpressions)))
            } else {
              f
            }
          } else {
            f
          }
        } catch {
          // Attempting to resolve in the aggregate can result in ambiguity.  When this happens,
          // just return the original plan.
          case ae: AnalysisException => f
        }

      case sort @ Sort(sortOrder, global, aggregate: Aggregate) if aggregate.resolved =>

        // Try resolving the ordering as though it is in the aggregate clause.
        try {
          // If a sort order is unresolved, containing references not in aggregate, or containing
          // `AggregateExpression`, we need to push down it to the underlying aggregate operator.
          val unresolvedSortOrders = sortOrder.filter { s =>
            !s.resolved || !s.references.subsetOf(aggregate.outputSet) || containsAggregate(s)
          }
          val aliasedOrdering =
            unresolvedSortOrders.map(o => Alias(o.child, "aggOrder")())
          val aggregatedOrdering = aggregate.copy(aggregateExpressions = aliasedOrdering)
          val resolvedAggregate: Aggregate =
            executeSameContext(aggregatedOrdering).asInstanceOf[Aggregate]
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
          val evaluatedOrderings = resolvedAliasedOrdering.zip(unresolvedSortOrders).map {
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

    def containsAggregate(condition: Expression): Boolean = {
      condition.find(_.isInstanceOf[AggregateExpression]).isDefined
    }
  }

  /**
   * Extracts [[Generator]] from the projectList of a [[Project]] operator and creates [[Generate]]
   * operator under [[Project]].
   *
   * This rule will throw [[AnalysisException]] for following cases:
   * 1. [[Generator]] is nested in expressions, e.g. `SELECT explode(list) + 1 FROM tbl`
   * 2. more than one [[Generator]] is found in projectList,
   *    e.g. `SELECT explode(list), explode(list) FROM tbl`
   * 3. [[Generator]] is found in other operators that are not [[Project]] or [[Generate]],
   *    e.g. `SELECT * FROM tbl SORT BY explode(list)`
   */
  object ExtractGenerator extends Rule[LogicalPlan] {
    private def hasGenerator(expr: Expression): Boolean = {
      expr.find(_.isInstanceOf[Generator]).isDefined
    }

    private def hasNestedGenerator(expr: NamedExpression): Boolean = {
      CleanupAliases.trimNonTopLevelAliases(expr) match {
        case UnresolvedAlias(_: Generator, _) => false
        case Alias(_: Generator, _) => false
        case MultiAlias(_: Generator, _) => false
        case other => hasGenerator(other)
      }
    }

    private def trimAlias(expr: NamedExpression): Expression = expr match {
      case UnresolvedAlias(child, _) => child
      case Alias(child, _) => child
      case MultiAlias(child, _) => child
      case _ => expr
    }

    private object AliasedGenerator {
      /**
       * Extracts a [[Generator]] expression, any names assigned by aliases to the outputs
       * and the outer flag. The outer flag is used when joining the generator output.
       * @param e the [[Expression]]
       * @return (the [[Generator]], seq of output names, outer flag)
       */
      def unapply(e: Expression): Option[(Generator, Seq[String], Boolean)] = e match {
        case Alias(GeneratorOuter(g: Generator), name) if g.resolved => Some((g, name :: Nil, true))
        case MultiAlias(GeneratorOuter(g: Generator), names) if g.resolved => Some((g, names, true))
        case Alias(g: Generator, name) if g.resolved => Some((g, name :: Nil, false))
        case MultiAlias(g: Generator, names) if g.resolved => Some((g, names, false))
        case _ => None
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case Project(projectList, _) if projectList.exists(hasNestedGenerator) =>
        val nestedGenerator = projectList.find(hasNestedGenerator).get
        throw new AnalysisException("Generators are not supported when it's nested in " +
          "expressions, but got: " + toPrettySQL(trimAlias(nestedGenerator)))

      case Project(projectList, _) if projectList.count(hasGenerator) > 1 =>
        val generators = projectList.filter(hasGenerator).map(trimAlias)
        throw new AnalysisException("Only one generator allowed per select clause but found " +
          generators.size + ": " + generators.map(toPrettySQL).mkString(", "))

      case p @ Project(projectList, child) =>
        // Holds the resolved generator, if one exists in the project list.
        var resolvedGenerator: Generate = null

        val newProjectList = projectList
          .map(CleanupAliases.trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
          .flatMap {
            case AliasedGenerator(generator, names, outer) if generator.childrenResolved =>
              // It's a sanity check, this should not happen as the previous case will throw
              // exception earlier.
              assert(resolvedGenerator == null, "More than one generator found in SELECT.")

              resolvedGenerator =
                Generate(
                  generator,
                  unrequiredChildIndex = Nil,
                  outer = outer,
                  qualifier = None,
                  generatorOutput = ResolveGenerate.makeGeneratorOutput(generator, names),
                  child)

              resolvedGenerator.generatorOutput
            case other => other :: Nil
          }

        if (resolvedGenerator != null) {
          Project(newProjectList, resolvedGenerator)
        } else {
          p
        }

      case g: Generate => g

      case p if p.expressions.exists(hasGenerator) =>
        throw new AnalysisException("Generators are not supported outside the SELECT clause, but " +
          "got: " + p.simpleString(SQLConf.get.maxToStringFields))
    }
  }

  /**
   * Rewrites table generating expressions that either need one or more of the following in order
   * to be resolved:
   *  - concrete attribute references for their output.
   *  - to be relocated from a SELECT clause (i.e. from  a [[Project]]) into a [[Generate]]).
   *
   * Names for the output [[Attribute]]s are extracted from [[Alias]] or [[MultiAlias]] expressions
   * that wrap the [[Generator]].
   */
  object ResolveGenerate extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case g: Generate if !g.child.resolved || !g.generator.resolved => g
      case g: Generate if !g.resolved =>
        g.copy(generatorOutput = makeGeneratorOutput(g.generator, g.generatorOutput.map(_.name)))
    }

    /**
     * Construct the output attributes for a [[Generator]], given a list of names.  If the list of
     * names is empty names are assigned from field names in generator.
     */
    private[analysis] def makeGeneratorOutput(
        generator: Generator,
        names: Seq[String]): Seq[Attribute] = {
      val elementAttrs = generator.elementSchema.toAttributes

      if (names.length == elementAttrs.length) {
        names.zip(elementAttrs).map {
          case (name, attr) => attr.withName(name)
        }
      } else if (names.isEmpty) {
        elementAttrs
      } else {
        failAnalysis(
          "The number of aliases supplied in the AS clause does not match the number of columns " +
          s"output by the UDTF expected ${elementAttrs.size} aliases but got " +
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
   *  - A [[Filter]]->[[Aggregate]] pattern representing GROUP BY with a HAVING
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
   * 2. For all [[WindowExpression]]s, groups them based on their [[WindowSpecDefinition]]s
   *    and [[WindowFunctionType]]s.
   * 3. For every distinct [[WindowSpecDefinition]] and [[WindowFunctionType]], creates a
   *    [[Window]] operator and inserts it into the plan tree.
   */
  object ExtractWindowExpressions extends Rule[LogicalPlan] {
    private def hasWindowFunction(exprs: Seq[Expression]): Boolean =
      exprs.exists(hasWindowFunction)

    private def hasWindowFunction(expr: Expression): Boolean = {
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
     * @return (seq of expressions containing at least one window expression,
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
          // alias will be cleaned in the rule CleanupAliases
          ne
        case e: Expression if e.foldable =>
          e // No need to create an attribute reference if it will be evaluated as a Literal.
        case e: Expression =>
          // For other expressions, we extract it and replace it with an AttributeReference (with
          // an internal column name, e.g. "_w0").
          val withName = Alias(e, s"_w${extractedExprBuffer.length}")()
          extractedExprBuffer += withName
          withName.toAttribute
      }

      // Now, we extract regular expressions from expressionsWithWindowFunctions
      // by using extractExpr.
      val seenWindowAggregates = new ArrayBuffer[AggregateExpression]
      val newExpressionsWithWindowFunctions = expressionsWithWindowFunctions.map {
        _.transform {
          // Extracts children expressions of a WindowFunction (input parameters of
          // a WindowFunction).
          case wf: WindowFunction =>
            val newChildren = wf.children.map(extractExpr)
            wf.withNewChildren(newChildren)

          // Extracts expressions from the partition spec and order spec.
          case wsc @ WindowSpecDefinition(partitionSpec, orderSpec, _) =>
            val newPartitionSpec = partitionSpec.map(extractExpr)
            val newOrderSpec = orderSpec.map { so =>
              val newChild = extractExpr(so.child)
              so.copy(child = newChild)
            }
            wsc.copy(partitionSpec = newPartitionSpec, orderSpec = newOrderSpec)

          // Extract Windowed AggregateExpression
          case we @ WindowExpression(
              ae @ AggregateExpression(function, _, _, _),
              spec: WindowSpecDefinition) =>
            val newChildren = function.children.map(extractExpr)
            val newFunction = function.withNewChildren(newChildren).asInstanceOf[AggregateFunction]
            val newAgg = ae.copy(aggregateFunction = newFunction)
            seenWindowAggregates += newAgg
            WindowExpression(newAgg, spec)

          case AggregateExpression(aggFunc, _, _, _) if hasWindowFunction(aggFunc.children) =>
            failAnalysis("It is not allowed to use a window function inside an aggregate " +
              "function. Please use the inner window function in a sub-query.")

          // Extracts AggregateExpression. For example, for SUM(x) - Sum(y) OVER (...),
          // we need to extract SUM(x).
          case agg: AggregateExpression if !seenWindowAggregates.contains(agg) =>
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
        if (distinctWindowSpec.isEmpty) {
          failAnalysis(s"$expr does not have any WindowExpression.")
        } else if (distinctWindowSpec.length > 1) {
          // newExpressionsWithWindowFunctions only have expressions with a single
          // WindowExpression. If we reach here, we have a bug.
          failAnalysis(s"$expr has multiple Window Specifications ($distinctWindowSpec)." +
            s"Please file a bug report with this error message, stack trace, and the query.")
        } else {
          val spec = distinctWindowSpec.head
          (spec.partitionSpec, spec.orderSpec, WindowFunctionType.functionType(expr))
        }
      }.toSeq

      // Third, we aggregate them by adding each Window operator for each Window Spec and then
      // setting this to the child of the next Window operator.
      val windowOps =
        groupedWindowExpressions.foldLeft(child) {
          case (last, ((partitionSpec, orderSpec, _), windowExpressions)) =>
            Window(windowExpressions, partitionSpec, orderSpec, last)
        }

      // Finally, we create a Project to output windowOps's output
      // newExpressionsWithWindowFunctions.
      Project(windowOps.output ++ newExpressionsWithWindowFunctions, windowOps)
    } // end of addWindow

    // We have to use transformDown at here to make sure the rule of
    // "Aggregate with Having clause" will be triggered.
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {

      case Filter(condition, _) if hasWindowFunction(condition) =>
        failAnalysis("It is not allowed to use window functions inside WHERE and HAVING clauses")

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
        val finalProjectList = aggregateExprs.map(_.toAttribute)
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
        val finalProjectList = aggregateExprs.map(_.toAttribute)
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
        val finalProjectList = projectList.map(_.toAttribute)
        Project(finalProjectList, withWindow)
    }
  }

  /**
   * Pulls out nondeterministic expressions from LogicalPlan which is not Project or Filter,
   * put them into an inner Project and finally project them away at the outer Project.
   */
  object PullOutNondeterministic extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.resolved => p // Skip unresolved nodes.
      case p: Project => p
      case f: Filter => f

      case a: Aggregate if a.groupingExpressions.exists(!_.deterministic) =>
        val nondeterToAttr = getNondeterToAttr(a.groupingExpressions)
        val newChild = Project(a.child.output ++ nondeterToAttr.values, a.child)
        a.transformExpressions { case e =>
          nondeterToAttr.get(e).map(_.toAttribute).getOrElse(e)
        }.copy(child = newChild)

      // todo: It's hard to write a general rule to pull out nondeterministic expressions
      // from LogicalPlan, currently we only do it for UnaryNode which has same output
      // schema with its child.
      case p: UnaryNode if p.output == p.child.output && p.expressions.exists(!_.deterministic) =>
        val nondeterToAttr = getNondeterToAttr(p.expressions)
        val newPlan = p.transformExpressions { case e =>
          nondeterToAttr.get(e).map(_.toAttribute).getOrElse(e)
        }
        val newChild = Project(p.child.output ++ nondeterToAttr.values, p.child)
        Project(p.output, newPlan.withNewChildren(newChild :: Nil))
    }

    private def getNondeterToAttr(exprs: Seq[Expression]): Map[Expression, NamedExpression] = {
      exprs.filterNot(_.deterministic).flatMap { expr =>
        val leafNondeterministic = expr.collect { case n: Nondeterministic => n }
        leafNondeterministic.distinct.map { e =>
          val ne = e match {
            case n: NamedExpression => n
            case _ => Alias(e, "_nondeterministic")()
          }
          e -> ne
        }
      }.toMap
    }
  }

  /**
   * Set the seed for random number generation.
   */
  object ResolveRandomSeed extends Rule[LogicalPlan] {
    private lazy val random = new Random()

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if p.resolved => p
      case p => p transformExpressionsUp {
        case Uuid(None) => Uuid(Some(random.nextLong()))
        case Shuffle(child, None) => Shuffle(child, Some(random.nextLong()))
      }
    }
  }

  /**
   * Correctly handle null primitive inputs for UDF by adding extra [[If]] expression to do the
   * null check.  When user defines a UDF with primitive parameters, there is no way to tell if the
   * primitive parameter is null or not, so here we assume the primitive input is null-propagatable
   * and we should return null if the input is null.
   */
  object HandleNullInputsForUDF extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.resolved => p // Skip unresolved nodes.

      case p => p transformExpressionsUp {

        case udf @ ScalaUDF(_, _, inputs, inputPrimitives, _, _, _, _)
            if inputPrimitives.contains(true) =>
          // Otherwise, add special handling of null for fields that can't accept null.
          // The result of operations like this, when passed null, is generally to return null.
          assert(inputPrimitives.length == inputs.length)

          val inputPrimitivesPair = inputPrimitives.zip(inputs)
          val inputNullCheck = inputPrimitivesPair.collect {
            case (isPrimitive, input) if isPrimitive && input.nullable =>
              IsNull(input)
          }.reduceLeftOption[Expression](Or)

          if (inputNullCheck.isDefined) {
            // Once we add an `If` check above the udf, it is safe to mark those checked inputs
            // as null-safe (i.e., wrap with `KnownNotNull`), because the null-returning
            // branch of `If` will be called if any of these checked inputs is null. Thus we can
            // prevent this rule from being applied repeatedly.
            val newInputs = inputPrimitivesPair.map {
              case (isPrimitive, input) =>
                if (isPrimitive && input.nullable) {
                  KnownNotNull(input)
                } else {
                  input
                }
            }
            val newUDF = udf.copy(children = newInputs)
            If(inputNullCheck.get, Literal.create(null, udf.dataType), newUDF)
          } else {
            udf
          }
      }
    }
  }

  /**
   * Check and add proper window frames for all window functions.
   */
  object ResolveWindowFrame extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      case WindowExpression(wf: WindowFunction, WindowSpecDefinition(_, _, f: SpecifiedWindowFrame))
          if wf.frame != UnspecifiedFrame && wf.frame != f =>
        failAnalysis(s"Window Frame $f must match the required frame ${wf.frame}")
      case WindowExpression(wf: WindowFunction, s @ WindowSpecDefinition(_, _, UnspecifiedFrame))
          if wf.frame != UnspecifiedFrame =>
        WindowExpression(wf, s.copy(frameSpecification = wf.frame))
      case we @ WindowExpression(e, s @ WindowSpecDefinition(_, o, UnspecifiedFrame))
          if e.resolved =>
        val frame = if (o.nonEmpty) {
          SpecifiedWindowFrame(RangeFrame, UnboundedPreceding, CurrentRow)
        } else {
          SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
        }
        we.copy(windowSpec = s.copy(frameSpecification = frame))
    }
  }

  /**
   * Check and add order to [[AggregateWindowFunction]]s.
   */
  object ResolveWindowOrder extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      case WindowExpression(wf: WindowFunction, spec) if spec.orderSpec.isEmpty =>
        failAnalysis(s"Window function $wf requires window to be ordered, please add ORDER BY " +
          s"clause. For example SELECT $wf(value_expr) OVER (PARTITION BY window_partition " +
          s"ORDER BY window_ordering) from table")
      case WindowExpression(rank: RankLike, spec) if spec.resolved =>
        val order = spec.orderSpec.map(_.child)
        WindowExpression(rank.withOrder(order), spec)
    }
  }

  /**
   * Removes natural or using joins by calculating output columns based on output from two sides,
   * Then apply a Project on a normal Join to eliminate natural or using join.
   */
  object ResolveNaturalAndUsingJoin extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case j @ Join(left, right, UsingJoin(joinType, usingCols), _, hint)
          if left.resolved && right.resolved && j.duplicateResolved =>
        commonNaturalJoinProcessing(left, right, joinType, usingCols, None, hint)
      case j @ Join(left, right, NaturalJoin(joinType), condition, hint)
          if j.resolvedExceptNatural =>
        // find common column names from both sides
        val joinNames = left.output.map(_.name).intersect(right.output.map(_.name))
        commonNaturalJoinProcessing(left, right, joinType, joinNames, condition, hint)
    }
  }

  /**
   * Resolves columns of an output table from the data in a logical plan. This rule will:
   *
   * - Reorder columns when the write is by name
   * - Insert safe casts when data types do not match
   * - Insert aliases when column names do not match
   * - Detect plans that are not compatible with the output table and throw AnalysisException
   */
  object ResolveOutputRelation extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
      case append @ AppendData(table, query, isByName)
          if table.resolved && query.resolved && !append.outputResolved =>
        val projection = resolveOutputColumns(table.name, table.output, query, isByName)

        if (projection != query) {
          append.copy(query = projection)
        } else {
          append
        }

      case overwrite @ OverwriteByExpression(table, _, query, isByName)
          if table.resolved && query.resolved && !overwrite.outputResolved =>
        val projection = resolveOutputColumns(table.name, table.output, query, isByName)

        if (projection != query) {
          overwrite.copy(query = projection)
        } else {
          overwrite
        }

      case overwrite @ OverwritePartitionsDynamic(table, query, isByName)
          if table.resolved && query.resolved && !overwrite.outputResolved =>
        val projection = resolveOutputColumns(table.name, table.output, query, isByName)

        if (projection != query) {
          overwrite.copy(query = projection)
        } else {
          overwrite
        }
    }

    def resolveOutputColumns(
        tableName: String,
        expected: Seq[Attribute],
        query: LogicalPlan,
        byName: Boolean): LogicalPlan = {

      if (expected.size < query.output.size) {
        throw new AnalysisException(
          s"""Cannot write to '$tableName', too many data columns:
             |Table columns: ${expected.map(c => s"'${c.name}'").mkString(", ")}
             |Data columns: ${query.output.map(c => s"'${c.name}'").mkString(", ")}""".stripMargin)
      }

      val errors = new mutable.ArrayBuffer[String]()
      val resolved: Seq[NamedExpression] = if (byName) {
        expected.flatMap { tableAttr =>
          query.resolveQuoted(tableAttr.name, resolver) match {
            case Some(queryExpr) =>
              checkField(tableAttr, queryExpr, byName, err => errors += err)
            case None =>
              errors += s"Cannot find data for output column '${tableAttr.name}'"
              None
          }
        }

      } else {
        if (expected.size > query.output.size) {
          throw new AnalysisException(
            s"""Cannot write to '$tableName', not enough data columns:
               |Table columns: ${expected.map(c => s"'${c.name}'").mkString(", ")}
               |Data columns: ${query.output.map(c => s"'${c.name}'").mkString(", ")}"""
                .stripMargin)
        }

        query.output.zip(expected).flatMap {
          case (queryExpr, tableAttr) =>
            checkField(tableAttr, queryExpr, byName, err => errors += err)
        }
      }

      if (errors.nonEmpty) {
        throw new AnalysisException(
          s"Cannot write incompatible data to table '$tableName':\n- ${errors.mkString("\n- ")}")
      }

      Project(resolved, query)
    }

    private def checkField(
        tableAttr: Attribute,
        queryExpr: NamedExpression,
        byName: Boolean,
        addError: String => Unit): Option[NamedExpression] = {

      // run the type check first to ensure type errors are present
      val canWrite = DataType.canWrite(
        queryExpr.dataType, tableAttr.dataType, byName, resolver, tableAttr.name, addError)

      if (queryExpr.nullable && !tableAttr.nullable) {
        addError(s"Cannot write nullable values to non-null column '${tableAttr.name}'")
        None

      } else if (!canWrite) {
        None

      } else {
        // always add an UpCast. it will be removed in the optimizer if it is unnecessary.
        Some(Alias(
          UpCast(queryExpr, tableAttr.dataType), tableAttr.name
        )(
          explicitMetadata = Option(tableAttr.metadata)
        ))
      }
    }
  }

  private def commonNaturalJoinProcessing(
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      joinNames: Seq[String],
      condition: Option[Expression],
      hint: JoinHint) = {
    val leftKeys = joinNames.map { keyName =>
      left.output.find(attr => resolver(attr.name, keyName)).getOrElse {
        throw new AnalysisException(s"USING column `$keyName` cannot be resolved on the left " +
          s"side of the join. The left-side columns: [${left.output.map(_.name).mkString(", ")}]")
      }
    }
    val rightKeys = joinNames.map { keyName =>
      right.output.find(attr => resolver(attr.name, keyName)).getOrElse {
        throw new AnalysisException(s"USING column `$keyName` cannot be resolved on the right " +
          s"side of the join. The right-side columns: [${right.output.map(_.name).mkString(", ")}]")
      }
    }
    val joinPairs = leftKeys.zip(rightKeys)

    val newCondition = (condition ++ joinPairs.map(EqualTo.tupled)).reduceOption(And)

    // columns not in joinPairs
    val lUniqueOutput = left.output.filterNot(att => leftKeys.contains(att))
    val rUniqueOutput = right.output.filterNot(att => rightKeys.contains(att))

    // the output list looks like: join keys, columns from left, columns from right
    val projectList = joinType match {
      case LeftOuter =>
        leftKeys ++ lUniqueOutput ++ rUniqueOutput.map(_.withNullability(true))
      case LeftExistence(_) =>
        leftKeys ++ lUniqueOutput
      case RightOuter =>
        rightKeys ++ lUniqueOutput.map(_.withNullability(true)) ++ rUniqueOutput
      case FullOuter =>
        // in full outer join, joinCols should be non-null if there is.
        val joinedCols = joinPairs.map { case (l, r) => Alias(Coalesce(Seq(l, r)), l.name)() }
        joinedCols ++
          lUniqueOutput.map(_.withNullability(true)) ++
          rUniqueOutput.map(_.withNullability(true))
      case _ : InnerLike =>
        leftKeys ++ lUniqueOutput ++ rUniqueOutput
      case _ =>
        sys.error("Unsupported natural join type " + joinType)
    }
    // use Project to trim unnecessary fields
    Project(projectList, Join(left, right, joinType, newCondition, hint))
  }

  /**
   * Replaces [[UnresolvedDeserializer]] with the deserialization expression that has been resolved
   * to the given input attributes.
   */
  object ResolveDeserializer extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.childrenResolved => p
      case p if p.resolved => p

      case p => p transformExpressions {
        case UnresolvedDeserializer(deserializer, inputAttributes) =>
          val inputs = if (inputAttributes.isEmpty) {
            p.children.flatMap(_.output)
          } else {
            inputAttributes
          }

          validateTopLevelTupleFields(deserializer, inputs)
          val resolved = resolveExpressionBottomUp(
            deserializer, LocalRelation(inputs), throws = true)
          val result = resolved transformDown {
            case UnresolvedMapObjects(func, inputData, cls) if inputData.resolved =>
              inputData.dataType match {
                case ArrayType(et, cn) =>
                  MapObjects(func, inputData, et, cn, cls) transformUp {
                    case UnresolvedExtractValue(child, fieldName) if child.resolved =>
                      ExtractValue(child, fieldName, resolver)
                  }
                case other =>
                  throw new AnalysisException("need an array field but got " + other.catalogString)
              }
            case u: UnresolvedCatalystToExternalMap if u.child.resolved =>
              u.child.dataType match {
                case _: MapType =>
                  CatalystToExternalMap(u) transformUp {
                    case UnresolvedExtractValue(child, fieldName) if child.resolved =>
                      ExtractValue(child, fieldName, resolver)
                  }
                case other =>
                  throw new AnalysisException("need a map field but got " + other.catalogString)
              }
          }
          validateNestedTupleFields(result)
          result
      }
    }

    private def fail(schema: StructType, maxOrdinal: Int): Unit = {
      throw new AnalysisException(s"Try to map ${schema.catalogString} to Tuple${maxOrdinal + 1}" +
        ", but failed as the number of fields does not line up.")
    }

    /**
     * For each top-level Tuple field, we use [[GetColumnByOrdinal]] to get its corresponding column
     * by position.  However, the actual number of columns may be different from the number of Tuple
     * fields.  This method is used to check the number of columns and fields, and throw an
     * exception if they do not match.
     */
    private def validateTopLevelTupleFields(
        deserializer: Expression, inputs: Seq[Attribute]): Unit = {
      val ordinals = deserializer.collect {
        case GetColumnByOrdinal(ordinal, _) => ordinal
      }.distinct.sorted

      if (ordinals.nonEmpty && ordinals != inputs.indices) {
        fail(inputs.toStructType, ordinals.last)
      }
    }

    /**
     * For each nested Tuple field, we use [[GetStructField]] to get its corresponding struct field
     * by position.  However, the actual number of struct fields may be different from the number
     * of nested Tuple fields.  This method is used to check the number of struct fields and nested
     * Tuple fields, and throw an exception if they do not match.
     */
    private def validateNestedTupleFields(deserializer: Expression): Unit = {
      val structChildToOrdinals = deserializer
        // There are 2 kinds of `GetStructField`:
        //   1. resolved from `UnresolvedExtractValue`, and it will have a `name` property.
        //   2. created when we build deserializer expression for nested tuple, no `name` property.
        // Here we want to validate the ordinals of nested tuple, so we should only catch
        // `GetStructField` without the name property.
        .collect { case g: GetStructField if g.name.isEmpty => g }
        .groupBy(_.child)
        .mapValues(_.map(_.ordinal).distinct.sorted)

      structChildToOrdinals.foreach { case (expr, ordinals) =>
        val schema = expr.dataType.asInstanceOf[StructType]
        if (ordinals != schema.indices) {
          fail(schema, ordinals.last)
        }
      }
    }
  }

  /**
   * Resolves [[NewInstance]] by finding and adding the outer scope to it if the object being
   * constructed is an inner class.
   */
  object ResolveNewInstance extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.childrenResolved => p
      case p if p.resolved => p

      case p => p transformExpressions {
        case n: NewInstance if n.childrenResolved && !n.resolved =>
          val outer = OuterScopes.getOuterScope(n.cls)
          if (outer == null) {
            throw new AnalysisException(
              s"Unable to generate an encoder for inner class `${n.cls.getName}` without " +
                "access to the scope that this class was defined in.\n" +
                "Try moving this class out of its parent class.")
          }
          n.copy(outerPointer = Some(outer))
      }
    }
  }

  /**
   * Replace the [[UpCast]] expression by [[Cast]], and throw exceptions if the cast may truncate.
   */
  object ResolveUpCast extends Rule[LogicalPlan] {
    private def fail(from: Expression, to: DataType, walkedTypePath: Seq[String]) = {
      val fromStr = from match {
        case l: LambdaVariable => "array element"
        case e => e.sql
      }
      throw new AnalysisException(s"Cannot up cast $fromStr from " +
        s"${from.dataType.catalogString} to ${to.catalogString}.\n" +
        "The type path of the target object is:\n" + walkedTypePath.mkString("", "\n", "\n") +
        "You can either add an explicit cast to the input data or choose a higher precision " +
        "type of the field in the target object")
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case p if !p.childrenResolved => p
      case p if p.resolved => p

      case p => p transformExpressions {
        case u @ UpCast(child, _, _) if !child.resolved => u

        case UpCast(child, dt: AtomicType, _)
            if SQLConf.get.getConf(SQLConf.LEGACY_LOOSE_UPCAST) &&
              child.dataType == StringType =>
          Cast(child, dt.asNullable)

        case UpCast(child, dataType, walkedTypePath) if !Cast.canUpCast(child.dataType, dataType) =>
          fail(child, dataType, walkedTypePath)

        case UpCast(child, dataType, _) => Cast(child, dataType.asNullable)
      }
    }
  }
}

/**
 * Removes [[SubqueryAlias]] operators from the plan. Subqueries are only required to provide
 * scoping information for attributes and can be removed once analysis is complete.
 */
object EliminateSubqueryAliases extends Rule[LogicalPlan] {
  // This is also called in the beginning of the optimization phase, and as a result
  // is using transformUp rather than resolveOperators.
  def apply(plan: LogicalPlan): LogicalPlan = AnalysisHelper.allowInvokingTransformsInAnalyzer {
    plan transformUp {
      case SubqueryAlias(_, child) => child
    }
  }
}

/**
 * Removes [[Union]] operators from the plan if it just has one child.
 */
object EliminateUnions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case Union(children) if children.size == 1 => children.head
  }
}

/**
 * Cleans up unnecessary Aliases inside the plan. Basically we only need Alias as a top level
 * expression in Project(project list) or Aggregate(aggregate expressions) or
 * Window(window expressions). Notice that if an expression has other expression parameters which
 * are not in its `children`, e.g. `RuntimeReplaceable`, the transformation for Aliases in this
 * rule can't work for those parameters.
 */
object CleanupAliases extends Rule[LogicalPlan] {
  private def trimAliases(e: Expression): Expression = {
    e.transformDown {
      case Alias(child, _) => child
      case MultiAlias(child, _) => child
    }
  }

  def trimNonTopLevelAliases(e: Expression): Expression = e match {
    case a: Alias =>
      a.copy(child = trimAliases(a.child))(
        exprId = a.exprId,
        qualifier = a.qualifier,
        explicitMetadata = Some(a.metadata))
    case a: MultiAlias =>
      a.copy(child = trimAliases(a.child))
    case other => trimAliases(other)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case Project(projectList, child) =>
      val cleanedProjectList =
        projectList.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Project(cleanedProjectList, child)

    case Aggregate(grouping, aggs, child) =>
      val cleanedAggs = aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Aggregate(grouping.map(trimAliases), cleanedAggs, child)

    case Window(windowExprs, partitionSpec, orderSpec, child) =>
      val cleanedWindowExprs =
        windowExprs.map(e => trimNonTopLevelAliases(e).asInstanceOf[NamedExpression])
      Window(cleanedWindowExprs, partitionSpec.map(trimAliases),
        orderSpec.map(trimAliases(_).asInstanceOf[SortOrder]), child)

    // Operators that operate on objects should only have expressions from encoders, which should
    // never have extra aliases.
    case o: ObjectConsumer => o
    case o: ObjectProducer => o
    case a: AppendColumns => a

    case other =>
      other transformExpressionsDown {
        case Alias(child, _) => child
      }
  }
}

/**
 * Ignore event time watermark in batch query, which is only supported in Structured Streaming.
 * TODO: add this rule into analyzer rule list.
 */
object EliminateEventTimeWatermark extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case EventTimeWatermark(_, _, child) if !child.isStreaming => child
  }
}

/**
 * Maps a time column to multiple time windows using the Expand operator. Since it's non-trivial to
 * figure out how many windows a time column can map to, we over-estimate the number of windows and
 * filter out the rows where the time column is not inside the time window.
 */
object TimeWindowing extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.dsl.expressions._

  private final val WINDOW_COL_NAME = "window"
  private final val WINDOW_START = "start"
  private final val WINDOW_END = "end"

  /**
   * Generates the logical plan for generating window ranges on a timestamp column. Without
   * knowing what the timestamp value is, it's non-trivial to figure out deterministically how many
   * window ranges a timestamp will map to given all possible combinations of a window duration,
   * slide duration and start time (offset). Therefore, we express and over-estimate the number of
   * windows there may be, and filter the valid windows. We use last Project operator to group
   * the window columns into a struct so they can be accessed as `window.start` and `window.end`.
   *
   * The windows are calculated as below:
   * maxNumOverlapping <- ceil(windowDuration / slideDuration)
   * for (i <- 0 until maxNumOverlapping)
   *   windowId <- ceil((timestamp - startTime) / slideDuration)
   *   windowStart <- windowId * slideDuration + (i - maxNumOverlapping) * slideDuration + startTime
   *   windowEnd <- windowStart + windowDuration
   *   return windowStart, windowEnd
   *
   * This behaves as follows for the given parameters for the time: 12:05. The valid windows are
   * marked with a +, and invalid ones are marked with a x. The invalid ones are filtered using the
   * Filter operator.
   * window: 12m, slide: 5m, start: 0m :: window: 12m, slide: 5m, start: 2m
   *     11:55 - 12:07 +                      11:52 - 12:04 x
   *     12:00 - 12:12 +                      11:57 - 12:09 +
   *     12:05 - 12:17 +                      12:02 - 12:14 +
   *
   * @param plan The logical plan
   * @return the logical plan that will generate the time windows using the Expand operator, with
   *         the Filter operator for correctness and Project for usability.
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case p: LogicalPlan if p.children.size == 1 =>
      val child = p.children.head
      val windowExpressions =
        p.expressions.flatMap(_.collect { case t: TimeWindow => t }).toSet

      val numWindowExpr = windowExpressions.size
      // Only support a single window expression for now
      if (numWindowExpr == 1 &&
          windowExpressions.head.timeColumn.resolved &&
          windowExpressions.head.checkInputDataTypes().isSuccess) {

        val window = windowExpressions.head

        val metadata = window.timeColumn match {
          case a: Attribute => a.metadata
          case _ => Metadata.empty
        }

        def getWindow(i: Int, overlappingWindows: Int): Expression = {
          val division = (PreciseTimestampConversion(
            window.timeColumn, TimestampType, LongType) - window.startTime) / window.slideDuration
          val ceil = Ceil(division)
          // if the division is equal to the ceiling, our record is the start of a window
          val windowId = CaseWhen(Seq((ceil === division, ceil + 1)), Some(ceil))
          val windowStart = (windowId + i - overlappingWindows) *
            window.slideDuration + window.startTime
          val windowEnd = windowStart + window.windowDuration

          CreateNamedStruct(
            Literal(WINDOW_START) ::
              PreciseTimestampConversion(windowStart, LongType, TimestampType) ::
              Literal(WINDOW_END) ::
              PreciseTimestampConversion(windowEnd, LongType, TimestampType) ::
              Nil)
        }

        val windowAttr = AttributeReference(
          WINDOW_COL_NAME, window.dataType, metadata = metadata)()

        if (window.windowDuration == window.slideDuration) {
          val windowStruct = Alias(getWindow(0, 1), WINDOW_COL_NAME)(
            exprId = windowAttr.exprId, explicitMetadata = Some(metadata))

          val replacedPlan = p transformExpressions {
            case t: TimeWindow => windowAttr
          }

          // For backwards compatibility we add a filter to filter out nulls
          val filterExpr = IsNotNull(window.timeColumn)

          replacedPlan.withNewChildren(
            Filter(filterExpr,
              Project(windowStruct +: child.output, child)) :: Nil)
        } else {
          val overlappingWindows =
            math.ceil(window.windowDuration * 1.0 / window.slideDuration).toInt
          val windows =
            Seq.tabulate(overlappingWindows)(i => getWindow(i, overlappingWindows))

          val projections = windows.map(_ +: child.output)

          val filterExpr =
            window.timeColumn >= windowAttr.getField(WINDOW_START) &&
              window.timeColumn < windowAttr.getField(WINDOW_END)

          val substitutedPlan = Filter(filterExpr,
            Expand(projections, windowAttr +: child.output, child))

          val renamedPlan = p transformExpressions {
            case t: TimeWindow => windowAttr
          }

          renamedPlan.withNewChildren(substitutedPlan :: Nil)
        }
      } else if (numWindowExpr > 1) {
        p.failAnalysis("Multiple time window expressions would result in a cartesian product " +
          "of rows, therefore they are currently not supported.")
      } else {
        p // Return unchanged. Analyzer will throw exception later
      }
  }
}

/**
 * Resolve a [[CreateNamedStruct]] if it contains [[NamePlaceholder]]s.
 */
object ResolveCreateNamedStruct extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressions {
    case e: CreateNamedStruct if !e.resolved =>
      val children = e.children.grouped(2).flatMap {
        case Seq(NamePlaceholder, e: NamedExpression) if e.resolved =>
          Seq(Literal(e.name), e)
        case kv =>
          kv
      }
      CreateNamedStruct(children.toList)
  }
}

/**
 * The aggregate expressions from subquery referencing outer query block are pushed
 * down to the outer query block for evaluation. This rule below updates such outer references
 * as AttributeReference referring attributes from the parent/outer query block.
 *
 * For example (SQL):
 * {{{
 *   SELECT l.a FROM l GROUP BY 1 HAVING EXISTS (SELECT 1 FROM r WHERE r.d < min(l.b))
 * }}}
 * Plan before the rule.
 *    Project [a#226]
 *    +- Filter exists#245 [min(b#227)#249]
 *       :  +- Project [1 AS 1#247]
 *       :     +- Filter (d#238 < min(outer(b#227)))       <-----
 *       :        +- SubqueryAlias r
 *       :           +- Project [_1#234 AS c#237, _2#235 AS d#238]
 *       :              +- LocalRelation [_1#234, _2#235]
 *       +- Aggregate [a#226], [a#226, min(b#227) AS min(b#227)#249]
 *          +- SubqueryAlias l
 *             +- Project [_1#223 AS a#226, _2#224 AS b#227]
 *                +- LocalRelation [_1#223, _2#224]
 * Plan after the rule.
 *    Project [a#226]
 *    +- Filter exists#245 [min(b#227)#249]
 *       :  +- Project [1 AS 1#247]
 *       :     +- Filter (d#238 < outer(min(b#227)#249))   <-----
 *       :        +- SubqueryAlias r
 *       :           +- Project [_1#234 AS c#237, _2#235 AS d#238]
 *       :              +- LocalRelation [_1#234, _2#235]
 *       +- Aggregate [a#226], [a#226, min(b#227) AS min(b#227)#249]
 *          +- SubqueryAlias l
 *             +- Project [_1#223 AS a#226, _2#224 AS b#227]
 *                +- LocalRelation [_1#223, _2#224]
 */
object UpdateOuterReferences extends Rule[LogicalPlan] {
  private def stripAlias(expr: Expression): Expression = expr match { case a: Alias => a.child }

  private def updateOuterReferenceInSubquery(
      plan: LogicalPlan,
      refExprs: Seq[Expression]): LogicalPlan = {
    plan resolveExpressions { case e =>
      val outerAlias =
        refExprs.find(stripAlias(_).semanticEquals(stripOuterReference(e)))
      outerAlias match {
        case Some(a: Alias) => OuterReference(a.toAttribute)
        case _ => e
      }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case f @ Filter(_, a: Aggregate) if f.resolved =>
        f transformExpressions {
          case s: SubqueryExpression if s.children.nonEmpty =>
            // Collect the aliases from output of aggregate.
            val outerAliases = a.aggregateExpressions collect { case a: Alias => a }
            // Update the subquery plan to record the OuterReference to point to outer query plan.
            s.withNewPlan(updateOuterReferenceInSubquery(s.plan, outerAliases))
      }
    }
  }
}
