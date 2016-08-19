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

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{CatalystConf, ScalaReflection, SimpleCatalystConf}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.encoders.OuterScopes
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance
import org.apache.spark.sql.catalyst.optimizer.BooleanSimplification
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.{TreeNodeRef}
import org.apache.spark.sql.catalyst.util.toPrettySQL
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
      new SimpleCatalystConf(caseSensitiveAnalysis = true)) {
      override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean) {}
    },
    new SimpleCatalystConf(caseSensitiveAnalysis = true))

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a
 * [[SessionCatalog]] and a [[FunctionRegistry]].
 */
class Analyzer(
    catalog: SessionCatalog,
    conf: CatalystConf,
    maxIterations: Int)
  extends RuleExecutor[LogicalPlan] with CheckAnalysis {

  def this(catalog: SessionCatalog, conf: CatalystConf) = {
    this(catalog, conf, conf.optimizerMaxIterations)
  }

  def resolver: Resolver = conf.resolver

  protected val fixedPoint = FixedPoint(maxIterations)

  /**
   * Override to provide additional rules for the "Resolution" batch.
   */
  val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  lazy val batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      CTESubstitution,
      WindowsSubstitution,
      EliminateUnions,
      new SubstituteUnresolvedOrdinals(conf)),
    Batch("Resolution", fixedPoint,
      ResolveTableValuedFunctions ::
      ResolveRelations ::
      ResolveReferences ::
      ResolveDeserializer ::
      ResolveNewInstance ::
      ResolveUpCast ::
      ResolveGroupingAnalytics ::
      ResolvePivot ::
      ResolveOrdinalInOrderByAndGroupBy ::
      ResolveMissingReferences ::
      ExtractGenerator ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      ResolveSubquery ::
      ResolveWindowOrder ::
      ResolveWindowFrame ::
      ResolveNaturalAndUsingJoin ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      TimeWindowing ::
      ResolveInlineTables ::
      TypeCoercion.typeCoercionRules ++
      extendedResolutionRules : _*),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("UDF", Once,
      HandleNullInputsForUDF),
    Batch("FixNullability", Once,
      FixNullability),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )

  /**
   * Substitute child plan with cte definitions
   */
  object CTESubstitution extends Rule[LogicalPlan] {
    // TODO allow subquery to define CTE
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators  {
      case With(child, relations) =>
        substituteCTE(child, relations.foldLeft(Seq.empty[(String, LogicalPlan)]) {
          case (resolved, (name, relation)) =>
            resolved :+ name -> ResolveRelations(substituteCTE(relation, resolved))
        })
      case other => other
    }

    def substituteCTE(plan: LogicalPlan, cteRelations: Seq[(String, LogicalPlan)]): LogicalPlan = {
      plan transformDown {
        case u : UnresolvedRelation =>
          val substituted = cteRelations.find(x => resolver(x._1, u.tableIdentifier.table))
            .map(_._2).map { relation =>
              val withAlias = u.alias.map(SubqueryAlias(_, relation, None))
              withAlias.getOrElse(relation)
            }
          substituted.getOrElse(u)
        case other =>
          // This cannot be done in ResolveSubquery because ResolveSubquery does not know the CTE.
          other transformExpressions {
            case e: SubqueryExpression =>
              e.withNewPlan(substituteCTE(e.plan, cteRelations))
          }
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
          case p => p.transformExpressions {
            case UnresolvedWindowExpression(c, WindowSpecReference(windowName)) =>
              val errorMessage =
                s"Window specification $windowName is not defined in the WINDOW clause."
              val windowSpecDefinition =
                windowDefinitions.getOrElse(windowName, failAnalysis(errorMessage))
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
          expr.transformUp { case u @ UnresolvedAlias(child, optGenAliasFunc) =>
            child match {
              case ne: NamedExpression => ne
              case e if !e.resolved => u
              case g: Generator => MultiAlias(g, Nil)
              case c @ Cast(ne: NamedExpression, _) => Alias(c, ne.name)()
              case e: ExtractValue => Alias(e, toPrettySQL(e))()
              case e if optGenAliasFunc.isDefined =>
                Alias(child, optGenAliasFunc.get.apply(e))()
              case e => Alias(e, toPrettySQL(e))()
            }
          }
      }.asInstanceOf[Seq[NamedExpression]]
    }

    private def hasUnresolvedAlias(exprs: Seq[NamedExpression]) =
      exprs.exists(_.find(_.isInstanceOf[UnresolvedAlias]).isDefined)

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case Aggregate(groups, aggs, child) if child.resolved && hasUnresolvedAlias(aggs) =>
        Aggregate(groups, assignAliases(aggs), child)

      case g: GroupingSets if g.child.resolved && hasUnresolvedAlias(g.aggregations) =>
        g.copy(aggregations = assignAliases(g.aggregations))

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
      Seq.tabulate(r.groupByExprs.length + 1)(idx => (1 << idx) - 1)
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

    private def hasGroupingAttribute(expr: Expression): Boolean = {
      expr.collectFirst {
        case u: UnresolvedAttribute if resolver(u.name, VirtualColumn.hiveGroupingIdName) => u
      }.isDefined
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
            gid
          } else {
            throw new AnalysisException(
              s"Columns of grouping_id (${e.groupByExprs.mkString(",")}) does not match " +
                s"grouping columns (${groupByExprs.mkString(",")})")
          }
        case Grouping(col: Expression) =>
          val idx = groupByExprs.indexOf(col)
          if (idx >= 0) {
            Cast(BitwiseAnd(ShiftRight(gid, Literal(groupByExprs.length - 1 - idx)),
              Literal(1)), ByteType)
          } else {
            throw new AnalysisException(s"Column of grouping ($col) can't be found " +
              s"in grouping columns ${groupByExprs.mkString(",")}")
          }
      }
    }

    // This require transformUp to replace grouping()/grouping_id() in resolved Filter/Sort
    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case a if !a.childrenResolved => a // be sure all of the children are resolved.
      case p if p.expressions.exists(hasGroupingAttribute) =>
        failAnalysis(
          s"${VirtualColumn.hiveGroupingIdName} is deprecated; use grouping_id() instead")

      case Aggregate(Seq(c @ Cube(groupByExprs)), aggregateExpressions, child) =>
        GroupingSets(bitmasks(c), groupByExprs, child, aggregateExpressions)
      case Aggregate(Seq(r @ Rollup(groupByExprs)), aggregateExpressions, child) =>
        GroupingSets(bitmasks(r), groupByExprs, child, aggregateExpressions)

      // Ensure all the expressions have been resolved.
      case x: GroupingSets if x.expressions.forall(_.resolved) =>
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

        val expandedAttributes = groupByAliases.zipWithIndex.map { case (a, idx) =>
          a.toAttribute.withNullability((nonNullBitmask & 1 << idx) == 0)
        }

        val expand = Expand(x.bitmasks, groupByAliases, expandedAttributes, gid, x.child)
        val groupingAttrs = expand.output.drop(x.child.output.length)

        val aggregations: Seq[NamedExpression] = x.aggregations.map { case expr =>
          // collect all the found AggregateExpression, so we can check an expression is part of
          // any AggregateExpression or not.
          val aggsBuffer = ArrayBuffer[Expression]()
          // Returns whether the expression belongs to any expressions in `aggsBuffer` or not.
          def isPartOfAggregation(e: Expression): Boolean = {
            aggsBuffer.exists(a => a.find(_ eq e).isDefined)
          }
          replaceGroupingFunc(expr, x.groupByExprs, gid).transformDown {
            // AggregateExpression should be computed on the unmodified value of its argument
            // expressions, so we should not replace any references to grouping expression
            // inside it.
            case e: AggregateExpression =>
              aggsBuffer += e
              e
            case e if isPartOfAggregation(e) => e
            case e =>
              val index = groupByAliases.indexWhere(_.child.semanticEquals(e))
              if (index == -1) {
                e
              } else {
                groupingAttrs(index)
              }
          }.asInstanceOf[NamedExpression]
        }

        Aggregate(groupingAttrs, aggregations, expand)

      case f @ Filter(cond, child) if hasGroupingFunction(cond) =>
        val groupingExprs = findGroupingExprs(child)
        // The unresolved grouping id will be resolved by ResolveMissingReferences
        val newCond = replaceGroupingFunc(cond, groupingExprs, VirtualColumn.groupingIdAttribute)
        f.copy(condition = newCond)

      case s @ Sort(order, _, child) if order.exists(hasGroupingFunction) =>
        val groupingExprs = findGroupingExprs(child)
        val gid = VirtualColumn.groupingIdAttribute
        // The unresolved grouping id will be resolved by ResolveMissingReferences
        val newOrder = order.map(replaceGroupingFunc(_, groupingExprs, gid).asInstanceOf[SortOrder])
        s.copy(order = newOrder)
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
  }

  object ResolvePivot extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case p: Pivot if !p.childrenResolved | !p.aggregates.forall(_.resolved)
        | !p.groupByExprs.forall(_.resolved) | !p.pivotColumn.resolved => p
      case Pivot(groupByExprs, pivotColumn, pivotValues, aggregates, child) =>
        val singleAgg = aggregates.size == 1
        def outputName(value: Literal, aggregate: Expression): String = {
          if (singleAgg) value.toString else value + "_" + aggregate.sql
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
          val castPivotValues = pivotValues.map(Cast(_, pivotColumn.dataType).eval(EmptyRow))
          val pivotAggs = namedAggExps.map { a =>
            Alias(PivotFirst(namedPivotCol.toAttribute, a.toAttribute, castPivotValues)
              .toAggregateExpression()
            , "__pivot_" + a.sql)()
          }
          val secondAgg = Aggregate(groupByExprs, groupByExprs ++ pivotAggs, firstAgg)
          val pivotAggAttribute = pivotAggs.map(_.toAttribute)
          val pivotOutputs = pivotValues.zipWithIndex.flatMap { case (value, i) =>
            aggregates.zip(pivotAggAttribute).map { case (aggregate, pivotAtt) =>
              Alias(ExtractValue(pivotAtt, Literal(i), resolver), outputName(value, aggregate))()
            }
          }
          Project(groupByExprs ++ pivotOutputs, secondAgg)
        } else {
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
              }.transform {
                // We are duplicating aggregates that are now computing a different value for each
                // pivot value.
                // TODO: Don't construct the physical container until after analysis.
                case ae: AggregateExpression => ae.copy(resultId = NamedExpression.newExprId)
              }
              if (filteredAggregate.fastEquals(aggregate)) {
                throw new AnalysisException(
                  s"Aggregate expression required for pivot, found '$aggregate'")
              }
              Alias(filteredAggregate, outputName(value, aggregate))()
            }
          }
          Aggregate(groupByExprs, groupByExprs ++ pivotAggregates, child)
        }
    }
  }

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  object ResolveRelations extends Rule[LogicalPlan] {
    private def lookupTableFromCatalog(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"Table or view not found: ${u.tableName}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case i @ InsertIntoTable(u: UnresolvedRelation, parts, child, _, _) if child.resolved =>
        i.copy(table = EliminateSubqueryAliases(lookupTableFromCatalog(u)))
      case u: UnresolvedRelation =>
        val table = u.tableIdentifier
        if (table.database.isDefined && conf.runSQLonFile &&
            (!catalog.databaseExists(table.database.get) || !catalog.tableExists(table))) {
          // If the table does not exist, and the database part is specified, and we support
          // running SQL directly on files, then let's just return the original UnresolvedRelation.
          // It is possible we are matching a query like "select * from parquet.`/path/to/query`".
          // The plan will get resolved later.
          // Note that we are testing (!db_exists || !table_exists) because the catalog throws
          // an exception from tableExists if the database does not exist.
          u
        } else {
          lookupTableFromCatalog(u)
        }
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

        case oldVersion: Generate
            if oldVersion.generatedSet.intersect(conflictingAttributes).nonEmpty =>
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
          val newRight = right transformUp {
            case r if r == oldRelation => newRelation
          } transformUp {
            case other => other transformExpressions {
              case a: Attribute =>
                attributeRewrites.get(a).getOrElse(a).withQualifier(a.qualifier)
            }
          }
          newRight
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
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
      case j @ Join(left, right, _, _) if !j.duplicateResolved =>
        j.copy(right = dedupRight(left, right))
      case i @ Intersect(left, right) if !i.duplicateResolved =>
        i.copy(right = dedupRight(left, right))
      case i @ Except(left, right) if !i.duplicateResolved =>
        i.copy(right = dedupRight(left, right))

      // When resolve `SortOrder`s in Sort based on child, don't report errors as
      // we still have chance to resolve it based on its descendants
      case s @ Sort(ordering, global, child) if child.resolved && !s.resolved =>
        val newOrdering =
          ordering.map(order => resolveExpression(order, child).asInstanceOf[SortOrder])
        Sort(newOrdering, global, child)

      // A special case for Generate, because the output of Generate should not be resolved by
      // ResolveReferences. Attributes in the output will be resolved by ResolveGenerate.
      case g @ Generate(generator, _, _, _, _, _) if generator.resolved => g

      case g @ Generate(generator, join, outer, qualifier, output, child) =>
        val newG = resolveExpression(generator, child, throws = true)
        if (newG.fastEquals(generator)) {
          g
        } else {
          Generate(newG.asInstanceOf[Generator], join, outer, qualifier, output, child)
        }

      // Skips plan which contains deserializer expressions, as they should be resolved by another
      // rule: ResolveDeserializer.
      case plan if containsDeserializer(plan.expressions) => plan

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
        case a: Alias => Alias(a.child, a.name)(isGenerated = a.isGenerated)
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
        case c: CreateStruct if containsStar(c.children) =>
          c.copy(children = c.children.flatMap {
            case s: Star => s.expand(child, resolver)
            case o => o :: Nil
          })
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
        // count(*) has been replaced by count(1)
        case o if containsStar(o.children) =>
          failAnalysis(s"Invalid usage of '*' in expression '${o.prettyName}'")
      }
    }
  }

  private def containsDeserializer(exprs: Seq[Expression]): Boolean = {
    exprs.exists(_.find(_.isInstanceOf[UnresolvedDeserializer]).isDefined)
  }

  protected[sql] def resolveExpression(
      expr: Expression,
      plan: LogicalPlan,
      throws: Boolean = false) = {
    // Resolve expression in one round.
    // If throws == false or the desired attribute doesn't exist
    // (like try to resolve `a.b` but `a` doesn't exist), fail and return the origin one.
    // Else, throw exception.
    try {
      expr transformUp {
        case GetColumnByOrdinal(ordinal, _) => plan.output(ordinal)
        case u @ UnresolvedAttribute(nameParts) =>
          withPosition(u) { plan.resolve(nameParts, resolver).getOrElse(u) }
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
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p if !p.childrenResolved => p
      // Replace the index with the related attribute for ORDER BY,
      // which is a 1-base position of the projection list.
      case s @ Sort(orders, global, child)
        if orders.exists(_.child.isInstanceOf[UnresolvedOrdinal]) =>
        val newOrders = orders map {
          case s @ SortOrder(UnresolvedOrdinal(index), direction, nullOrder) =>
            if (index > 0 && index <= child.output.size) {
              SortOrder(child.output(index - 1), direction, nullOrder)
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
      case a @ Aggregate(groups, aggs, child) if aggs.forall(_.resolved) &&
        groups.exists(_.isInstanceOf[UnresolvedOrdinal]) =>
        val newGroups = groups.map {
          case ordinal @ UnresolvedOrdinal(index) if index > 0 && index <= aggs.size =>
            aggs(index - 1) match {
              case e if ResolveAggregateFunctions.containsAggregate(e) =>
                ordinal.failAnalysis(
                  s"GROUP BY position $index is an aggregate function, and " +
                    "aggregate functions are not allowed in GROUP BY")
              case o => o
            }
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
   * In many dialects of SQL it is valid to sort by attributes that are not present in the SELECT
   * clause.  This rule detects such queries and adds the required attributes to the original
   * projection, so that they will be available during sorting. Another projection is added to
   * remove these attributes after sorting.
   *
   * The HAVING clause could also used a grouping columns that is not presented in the SELECT.
   */
  object ResolveMissingReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      // Skip sort with aggregate. This will be handled in ResolveAggregateFunctions
      case sa @ Sort(_, _, child: Aggregate) => sa

      case s @ Sort(order, _, child) if child.resolved =>
        try {
          val newOrder = order.map(resolveExpressionRecursively(_, child).asInstanceOf[SortOrder])
          val requiredAttrs = AttributeSet(newOrder).filter(_.resolved)
          val missingAttrs = requiredAttrs -- child.outputSet
          if (missingAttrs.nonEmpty) {
            // Add missing attributes and then project them away after the sort.
            Project(child.output,
              Sort(newOrder, s.global, addMissingAttr(child, missingAttrs)))
          } else if (newOrder != order) {
            s.copy(order = newOrder)
          } else {
            s
          }
        } catch {
          // Attempting to resolve it might fail. When this happens, return the original plan.
          // Users will see an AnalysisException for resolution failure of missing attributes
          // in Sort
          case ae: AnalysisException => s
        }

      case f @ Filter(cond, child) if child.resolved =>
        try {
          val newCond = resolveExpressionRecursively(cond, child)
          val requiredAttrs = newCond.references.filter(_.resolved)
          val missingAttrs = requiredAttrs -- child.outputSet
          if (missingAttrs.nonEmpty) {
            // Add missing attributes and then project them away.
            Project(child.output,
              Filter(newCond, addMissingAttr(child, missingAttrs)))
          } else if (newCond != cond) {
            f.copy(condition = newCond)
          } else {
            f
          }
        } catch {
          // Attempting to resolve it might fail. When this happens, return the original plan.
          // Users will see an AnalysisException for resolution failure of missing attributes
          case ae: AnalysisException => f
        }
    }

    /**
     * Add the missing attributes into projectList of Project/Window or aggregateExpressions of
     * Aggregate.
     */
    private def addMissingAttr(plan: LogicalPlan, missingAttrs: AttributeSet): LogicalPlan = {
      if (missingAttrs.isEmpty) {
        return plan
      }
      plan match {
        case p: Project =>
          val missing = missingAttrs -- p.child.outputSet
          Project(p.projectList ++ missingAttrs, addMissingAttr(p.child, missing))
        case a: Aggregate =>
          // all the missing attributes should be grouping expressions
          // TODO: push down AggregateExpression
          missingAttrs.foreach { attr =>
            if (!a.groupingExpressions.exists(_.semanticEquals(attr))) {
              throw new AnalysisException(s"Can't add $attr to ${a.simpleString}")
            }
          }
          val newAggregateExpressions = a.aggregateExpressions ++ missingAttrs
          a.copy(aggregateExpressions = newAggregateExpressions)
        case g: Generate =>
          // If join is false, we will convert it to true for getting from the child the missing
          // attributes that its child might have or could have.
          val missing = missingAttrs -- g.child.outputSet
          g.copy(join = true, child = addMissingAttr(g.child, missing))
        case u: UnaryNode =>
          u.withNewChildren(addMissingAttr(u.child, missingAttrs) :: Nil)
        case other =>
          throw new AnalysisException(s"Can't add $missingAttrs to $other")
      }
    }

    /**
     * Resolve the expression on a specified logical plan and it's child (recursively), until
     * the expression is resolved or meet a non-unary node or Subquery.
     */
    @tailrec
    private def resolveExpressionRecursively(expr: Expression, plan: LogicalPlan): Expression = {
      val resolved = resolveExpression(expr, plan)
      if (resolved.resolved) {
        resolved
      } else {
        plan match {
          case u: UnaryNode if !u.isInstanceOf[SubqueryAlias] =>
            resolveExpressionRecursively(resolved, u.child)
          case other => resolved
        }
      }
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
                // DISTINCT is not meaningful for a Max or a Min.
                case max: Max if isDistinct =>
                  AggregateExpression(max, Complete, isDistinct = false)
                case min: Min if isDistinct =>
                  AggregateExpression(min, Complete, isDistinct = false)
                // AggregateWindowFunctions are AggregateFunctions that can only be evaluated within
                // the context of a Window clause. They do not need to be wrapped in an
                // AggregateExpression.
                case wf: AggregateWindowFunction => wf
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
      plan transformDown {
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
     * Pull out all (outer) correlated predicates from a given subquery. This method removes the
     * correlated predicates from subquery [[Filter]]s and adds the references of these predicates
     * to all intermediate [[Project]] and [[Aggregate]] clauses (if they are missing) in order to
     * be able to evaluate the predicates at the top level.
     *
     * This method returns the rewritten subquery and correlated predicates.
     */
    private def pullOutCorrelatedPredicates(sub: LogicalPlan): (LogicalPlan, Seq[Expression]) = {
      val predicateMap = scala.collection.mutable.Map.empty[LogicalPlan, Seq[Expression]]

      /** Make sure a plans' subtree does not contain a tagged predicate. */
      def failOnOuterReferenceInSubTree(p: LogicalPlan, msg: String): Unit = {
        if (p.collect(predicateMap).nonEmpty) {
          failAnalysis(s"Accessing outer query column is not allowed in $msg: $p")
        }
      }

      /** Helper function for locating outer references. */
      def containsOuter(e: Expression): Boolean = {
        e.find(_.isInstanceOf[OuterReference]).isDefined
      }

      /** Make sure a plans' expressions do not contain a tagged predicate. */
      def failOnOuterReference(p: LogicalPlan): Unit = {
        if (p.expressions.exists(containsOuter)) {
          failAnalysis(
            s"Correlated predicates are not supported outside of WHERE/HAVING clauses: $p")
        }
      }

      /** Determine which correlated predicate references are missing from this plan. */
      def missingReferences(p: LogicalPlan): AttributeSet = {
        val localPredicateReferences = p.collect(predicateMap)
          .flatten
          .map(_.references)
          .reduceOption(_ ++ _)
          .getOrElse(AttributeSet.empty)
        localPredicateReferences -- p.outputSet
      }

      // Simplify the predicates before pulling them out.
      val transformed = BooleanSimplification(sub) transformUp {
        case f @ Filter(cond, child) =>
          // Find all predicates with an outer reference.
          val (correlated, local) = splitConjunctivePredicates(cond).partition(containsOuter)

          // Rewrite the filter without the correlated predicates if any.
          correlated match {
            case Nil => f
            case xs if local.nonEmpty =>
              val newFilter = Filter(local.reduce(And), child)
              predicateMap += newFilter -> xs
              newFilter
            case xs =>
              predicateMap += child -> xs
              child
          }
        case p @ Project(expressions, child) =>
          failOnOuterReference(p)
          val referencesToAdd = missingReferences(p)
          if (referencesToAdd.nonEmpty) {
            Project(expressions ++ referencesToAdd, child)
          } else {
            p
          }
        case a @ Aggregate(grouping, expressions, child) =>
          failOnOuterReference(a)
          val referencesToAdd = missingReferences(a)
          if (referencesToAdd.nonEmpty) {
            Aggregate(grouping ++ referencesToAdd, expressions ++ referencesToAdd, child)
          } else {
            a
          }
        case j @ Join(left, _, RightOuter, _) =>
          failOnOuterReference(j)
          failOnOuterReferenceInSubTree(left, "a RIGHT OUTER JOIN")
          j
        case j @ Join(_, right, jt, _) if !jt.isInstanceOf[InnerLike] =>
          failOnOuterReference(j)
          failOnOuterReferenceInSubTree(right, "a LEFT (OUTER) JOIN")
          j
        case u: Union =>
          failOnOuterReferenceInSubTree(u, "a UNION")
          u
        case s: SetOperation =>
          failOnOuterReferenceInSubTree(s.right, "an INTERSECT/EXCEPT")
          s
        case e: Expand =>
          failOnOuterReferenceInSubTree(e, "an EXPAND")
          e
        case l : LocalLimit =>
          failOnOuterReferenceInSubTree(l, "a LIMIT")
          l
        // Since LIMIT <n> is represented as GlobalLimit(<n>, (LocalLimit (<n>, child))
        // and we are walking bottom up, we will fail on LocalLimit before
        // reaching GlobalLimit.
        // The code below is just a safety net.
        case g : GlobalLimit =>
          failOnOuterReferenceInSubTree(g, "a LIMIT")
          g
        case s : Sample =>
          failOnOuterReferenceInSubTree(s, "a TABLESAMPLE")
          s
        case p =>
          failOnOuterReference(p)
          p
      }
      (transformed, predicateMap.values.flatten.toSeq)
    }

    /**
     * Rewrite the subquery in a safe way by preventing that the subquery and the outer use the same
     * attributes.
     */
    private def rewriteSubQuery(
        sub: LogicalPlan,
        outer: Seq[LogicalPlan]): (LogicalPlan, Seq[Expression]) = {
      // Pull out the tagged predicates and rewrite the subquery in the process.
      val (basePlan, baseConditions) = pullOutCorrelatedPredicates(sub)

      // Make sure the inner and the outer query attributes do not collide.
      val outputSet = outer.map(_.outputSet).reduce(_ ++ _)
      val duplicates = basePlan.outputSet.intersect(outputSet)
      val (plan, deDuplicatedConditions) = if (duplicates.nonEmpty) {
        val aliasMap = AttributeMap(duplicates.map { dup =>
          dup -> Alias(dup, dup.toString)()
        }.toSeq)
        val aliasedExpressions = basePlan.output.map { ref =>
          aliasMap.getOrElse(ref, ref)
        }
        val aliasedProjection = Project(aliasedExpressions, basePlan)
        val aliasedConditions = baseConditions.map(_.transform {
          case ref: Attribute => aliasMap.getOrElse(ref, ref).toAttribute
        })
        (aliasedProjection, aliasedConditions)
      } else {
        (basePlan, baseConditions)
      }
      // Remove outer references from the correlated predicates. We wait with extracting
      // these until collisions between the inner and outer query attributes have been
      // solved.
      val conditions = deDuplicatedConditions.map(_.transform {
        case OuterReference(ref) => ref
      })
      (plan, conditions)
    }

    /**
     * Resolve and rewrite a subquery. The subquery is resolved using its outer plans. This method
     * will resolve the subquery by alternating between the regular analyzer and by applying the
     * resolveOuterReferences rule.
     *
     * All correlated conditions are pulled out of the subquery as soon as the subquery is resolved.
     */
    private def resolveSubQuery(
        e: SubqueryExpression,
        plans: Seq[LogicalPlan],
        requiredColumns: Int = 0)(
        f: (LogicalPlan, Seq[Expression]) => SubqueryExpression): SubqueryExpression = {
      // Step 1: Resolve the outer expressions.
      var previous: LogicalPlan = null
      var current = e.plan
      do {
        // Try to resolve the subquery plan using the regular analyzer.
        previous = current
        current = execute(current)

        // Use the outer references to resolve the subquery plan if it isn't resolved yet.
        val i = plans.iterator
        val afterResolve = current
        while (!current.resolved && current.fastEquals(afterResolve) && i.hasNext) {
          current = resolveOuterReferences(current, i.next())
        }
      } while (!current.resolved && !current.fastEquals(previous))

      // Step 2: Pull out the predicates if the plan is resolved.
      if (current.resolved) {
        // Make sure the resolved query has the required number of output columns. This is only
        // needed for Scalar and IN subqueries.
        if (requiredColumns > 0 && requiredColumns != current.output.size) {
          failAnalysis(s"The number of columns in the subquery (${current.output.size}) " +
            s"does not match the required number of columns ($requiredColumns)")
        }
        // Pullout predicates and construct a new plan.
        f.tupled(rewriteSubQuery(current, plans))
      } else {
        e.withNewPlan(current)
      }
    }

    /**
     * Resolve and rewrite all subqueries in a LogicalPlan. This method transforms IN and EXISTS
     * expressions into PredicateSubquery expression once the are resolved.
     */
    private def resolveSubQueries(plan: LogicalPlan, plans: Seq[LogicalPlan]): LogicalPlan = {
      plan transformExpressions {
        case s @ ScalarSubquery(sub, conditions, exprId)
            if sub.resolved && conditions.isEmpty && sub.output.size != 1 =>
          failAnalysis(s"Scalar subquery must return only one column, but got ${sub.output.size}")
        case s @ ScalarSubquery(sub, _, exprId) if !sub.resolved =>
          resolveSubQuery(s, plans, 1)(ScalarSubquery(_, _, exprId))
        case e @ Exists(sub, exprId) =>
          resolveSubQuery(e, plans)(PredicateSubquery(_, _, nullAware = false, exprId))
        case In(e, Seq(l @ ListQuery(_, exprId))) if e.resolved =>
          // Get the left hand side expressions.
          val expressions = e match {
            case CreateStruct(exprs) => exprs
            case expr => Seq(expr)
          }
          resolveSubQuery(l, plans, expressions.size) { (rewrite, conditions) =>
            // Construct the IN conditions.
            val inConditions = expressions.zip(rewrite.output).map(EqualTo.tupled)
            PredicateSubquery(rewrite, inConditions ++ conditions, nullAware = true, exprId)
          }
      }
    }

    /**
     * Resolve and rewrite all subqueries in an operator tree..
     */
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
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
   * Turns projections that contain aggregate expressions into aggregations.
   */
  object GlobalAggregates extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
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
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case filter @ Filter(havingCondition,
             aggregate @ Aggregate(grouping, originalAggExprs, child))
          if aggregate.resolved =>

        // Try resolving the condition of the filter as though it is in the aggregate clause
        try {
          val aggregatedCondition =
            Aggregate(
              grouping,
              Alias(havingCondition, "havingCondition")(isGenerated = true) :: Nil,
              child)
          val resolvedOperator = execute(aggregatedCondition)
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
                  !aggregate.output.exists(_.semanticEquals(e)) =>
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
              Project(aggregate.output,
                Filter(transformedAggregateFilter,
                  aggregate.copy(aggregateExpressions = originalAggExprs ++ aggregateExpressions)))
            } else {
              filter
            }
          } else {
            filter
          }
        } catch {
          // Attempting to resolve in the aggregate can result in ambiguity.  When this happens,
          // just return the original plan.
          case ae: AnalysisException => filter
        }

      case sort @ Sort(sortOrder, global, aggregate: Aggregate) if aggregate.resolved =>

        // Try resolving the ordering as though it is in the aggregate clause.
        try {
          val unresolvedSortOrders = sortOrder.filter(s => !s.resolved || containsAggregate(s))
          val aliasedOrdering =
            unresolvedSortOrders.map(o => Alias(o.child, "aggOrder")(isGenerated = true))
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

    def containsAggregate(condition: Expression): Boolean = {
      condition.find(_.isInstanceOf[AggregateExpression]).isDefined
    }
  }

  /**
   * Extracts [[Generator]] from the projectList of a [[Project]] operator and create [[Generate]]
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

    private def hasNestedGenerator(expr: NamedExpression): Boolean = expr match {
      case UnresolvedAlias(_: Generator, _) => false
      case Alias(_: Generator, _) => false
      case MultiAlias(_: Generator, _) => false
      case other => hasGenerator(other)
    }

    private def trimAlias(expr: NamedExpression): Expression = expr match {
      case UnresolvedAlias(child, _) => child
      case Alias(child, _) => child
      case MultiAlias(child, _) => child
      case _ => expr
    }

    /** Extracts a [[Generator]] expression and any names assigned by aliases to their output. */
    private object AliasedGenerator {
      def unapply(e: Expression): Option[(Generator, Seq[String])] = e match {
        case Alias(g: Generator, name) if g.resolved => Some((g, name :: Nil))
        case MultiAlias(g: Generator, names) if g.resolved => Some(g, names)
        case _ => None
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
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

        val newProjectList = projectList.flatMap {
          case AliasedGenerator(generator, names) if generator.childrenResolved =>
            // It's a sanity check, this should not happen as the previous case will throw
            // exception earlier.
            assert(resolvedGenerator == null, "More than one generator found in SELECT.")

            resolvedGenerator =
              Generate(
                generator,
                join = projectList.size > 1, // Only join if there are other expressions in SELECT.
                outer = false,
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
          "got: " + p.simpleString)
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
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
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
   * Fixes nullability of Attributes in a resolved LogicalPlan by using the nullability of
   * corresponding Attributes of its children output Attributes. This step is needed because
   * users can use a resolved AttributeReference in the Dataset API and outer joins
   * can change the nullability of an AttribtueReference. Without the fix, a nullable column's
   * nullable field can be actually set as non-nullable, which cause illegal optimization
   * (e.g., NULL propagation) and wrong answers.
   * See SPARK-13484 and SPARK-13801 for the concrete queries of this case.
   */
  object FixNullability extends Rule[LogicalPlan] {

    def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
      case p if !p.resolved => p // Skip unresolved nodes.
      case p: LogicalPlan if p.resolved =>
        val childrenOutput = p.children.flatMap(c => c.output).groupBy(_.exprId).flatMap {
          case (exprId, attributes) =>
            // If there are multiple Attributes having the same ExprId, we need to resolve
            // the conflict of nullable field. We do not really expect this happen.
            val nullable = attributes.exists(_.nullable)
            attributes.map(attr => attr.withNullability(nullable))
        }.toSeq
        // At here, we create an AttributeMap that only compare the exprId for the lookup
        // operation. So, we can find the corresponding input attribute's nullability.
        val attributeMap = AttributeMap[Attribute](childrenOutput.map(attr => attr -> attr))
        // For an Attribute used by the current LogicalPlan, if it is from its children,
        // we fix the nullable field by using the nullability setting of the corresponding
        // output Attribute from the children.
        p.transformExpressions {
          case attr: Attribute if attributeMap.contains(attr) =>
            attr.withNullability(attributeMap(attr).nullable)
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
              case _ => Alias(e, "_nondeterministic")(isGenerated = true)
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

  /**
   * Check and add proper window frames for all window functions.
   */
  object ResolveWindowFrame extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case logical: LogicalPlan => logical transformExpressions {
        case WindowExpression(wf: WindowFunction,
        WindowSpecDefinition(_, _, f: SpecifiedWindowFrame))
          if wf.frame != UnspecifiedFrame && wf.frame != f =>
          failAnalysis(s"Window Frame $f must match the required frame ${wf.frame}")
        case WindowExpression(wf: WindowFunction,
        s @ WindowSpecDefinition(_, o, UnspecifiedFrame))
          if wf.frame != UnspecifiedFrame =>
          WindowExpression(wf, s.copy(frameSpecification = wf.frame))
        case we @ WindowExpression(e, s @ WindowSpecDefinition(_, o, UnspecifiedFrame))
          if e.resolved =>
          val frame = SpecifiedWindowFrame.defaultWindowFrame(o.nonEmpty, acceptWindowFrame = true)
          we.copy(windowSpec = s.copy(frameSpecification = frame))
      }
    }
  }

  /**
   * Check and add order to [[AggregateWindowFunction]]s.
   */
  object ResolveWindowOrder extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case logical: LogicalPlan => logical transformExpressions {
        case WindowExpression(wf: WindowFunction, spec) if spec.orderSpec.isEmpty =>
          failAnalysis(s"Window function $wf requires window to be ordered, please add ORDER BY " +
            s"clause. For example SELECT $wf(value_expr) OVER (PARTITION BY window_partition " +
            s"ORDER BY window_ordering) from table")
        case WindowExpression(rank: RankLike, spec) if spec.resolved =>
          val order = spec.orderSpec.map(_.child)
          WindowExpression(rank.withOrder(order), spec)
      }
    }
  }

  /**
   * Removes natural or using joins by calculating output columns based on output from two sides,
   * Then apply a Project on a normal Join to eliminate natural or using join.
   */
  object ResolveNaturalAndUsingJoin extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case j @ Join(left, right, UsingJoin(joinType, usingCols), condition)
          if left.resolved && right.resolved && j.duplicateResolved =>
        // Resolve the column names referenced in using clause from both the legs of join.
        val lCols = usingCols.flatMap(col => left.resolveQuoted(col.name, resolver))
        val rCols = usingCols.flatMap(col => right.resolveQuoted(col.name, resolver))
        if ((lCols.length == usingCols.length) && (rCols.length == usingCols.length)) {
          val joinNames = lCols.map(exp => exp.name)
          commonNaturalJoinProcessing(left, right, joinType, joinNames, None)
        } else {
          j
        }
      case j @ Join(left, right, NaturalJoin(joinType), condition) if j.resolvedExceptNatural =>
        // find common column names from both sides
        val joinNames = left.output.map(_.name).intersect(right.output.map(_.name))
        commonNaturalJoinProcessing(left, right, joinType, joinNames, condition)
    }
  }

  private def commonNaturalJoinProcessing(
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      joinNames: Seq[String],
      condition: Option[Expression]) = {
    val leftKeys = joinNames.map { keyName =>
      val joinColumn = left.output.find(attr => resolver(attr.name, keyName))
      assert(
        joinColumn.isDefined,
        s"$keyName should exist in ${left.output.map(_.name).mkString(",")}")
      joinColumn.get
    }
    val rightKeys = joinNames.map { keyName =>
      val joinColumn = right.output.find(attr => resolver(attr.name, keyName))
      assert(
        joinColumn.isDefined,
        s"$keyName should exist in ${right.output.map(_.name).mkString(",")}")
      joinColumn.get
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
    Project(projectList, Join(left, right, joinType, newCondition))
  }

  /**
   * Replaces [[UnresolvedDeserializer]] with the deserialization expression that has been resolved
   * to the given input attributes.
   */
  object ResolveDeserializer extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
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
          val resolved = resolveExpression(
            deserializer, LocalRelation(inputs), throws = true)
          validateNestedTupleFields(resolved)
          resolved
      }
    }

    private def fail(schema: StructType, maxOrdinal: Int): Unit = {
      throw new AnalysisException(s"Try to map ${schema.simpleString} to Tuple${maxOrdinal + 1}, " +
        "but failed as the number of fields does not line up.")
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
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
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
      throw new AnalysisException(s"Cannot up cast ${from.sql} from " +
        s"${from.dataType.simpleString} to ${to.simpleString} as it may truncate\n" +
        "The type path of the target object is:\n" + walkedTypePath.mkString("", "\n", "\n") +
        "You can either add an explicit cast to the input data or choose a higher precision " +
        "type of the field in the target object")
    }

    private def illegalNumericPrecedence(from: DataType, to: DataType): Boolean = {
      val fromPrecedence = TypeCoercion.numericPrecedence.indexOf(from)
      val toPrecedence = TypeCoercion.numericPrecedence.indexOf(to)
      toPrecedence > 0 && fromPrecedence > toPrecedence
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p if !p.childrenResolved => p
      case p if p.resolved => p

      case p => p transformExpressions {
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
          case _ => Cast(child, dataType.asNullable)
        }
      }
    }
  }
}

/**
 * Removes [[SubqueryAlias]] operators from the plan. Subqueries are only required to provide
 * scoping information for attributes and can be removed once analysis is complete.
 */
object EliminateSubqueryAliases extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case SubqueryAlias(_, child, _) => child
  }
}

/**
 * Removes [[Union]] operators from the plan if it just has one child.
 */
object EliminateUnions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Union(children) if children.size == 1 => children.head
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
      a.withNewChildren(trimAliases(a.child) :: Nil)
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

    case w @ Window(windowExprs, partitionSpec, orderSpec, child) =>
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
 * Maps a time column to multiple time windows using the Expand operator. Since it's non-trivial to
 * figure out how many windows a time column can map to, we over-estimate the number of windows and
 * filter out the rows where the time column is not inside the time window.
 */
object TimeWindowing extends Rule[LogicalPlan] {
  import org.apache.spark.sql.catalyst.dsl.expressions._

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
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case p: LogicalPlan if p.children.size == 1 =>
      val child = p.children.head
      val windowExpressions =
        p.expressions.flatMap(_.collect { case t: TimeWindow => t }).distinct.toList // Not correct.

      // Only support a single window expression for now
      if (windowExpressions.size == 1 &&
          windowExpressions.head.timeColumn.resolved &&
          windowExpressions.head.checkInputDataTypes().isSuccess) {
        val window = windowExpressions.head
        val windowAttr = AttributeReference("window", window.dataType)()

        val maxNumOverlapping = math.ceil(window.windowDuration * 1.0 / window.slideDuration).toInt
        val windows = Seq.tabulate(maxNumOverlapping + 1) { i =>
          val windowId = Ceil((PreciseTimestamp(window.timeColumn) - window.startTime) /
            window.slideDuration)
          val windowStart = (windowId + i - maxNumOverlapping) *
              window.slideDuration + window.startTime
          val windowEnd = windowStart + window.windowDuration

          CreateNamedStruct(
            Literal(WINDOW_START) :: windowStart ::
            Literal(WINDOW_END) :: windowEnd :: Nil)
        }

        val projections = windows.map(_ +: p.children.head.output)

        val filterExpr =
          window.timeColumn >= windowAttr.getField(WINDOW_START) &&
          window.timeColumn < windowAttr.getField(WINDOW_END)

        val expandedPlan =
          Filter(filterExpr,
            Expand(projections, windowAttr +: child.output, child))

        val substitutedPlan = p transformExpressions {
          case t: TimeWindow => windowAttr
        }

        substitutedPlan.withNewChildren(expandedPlan :: Nil)
      } else if (windowExpressions.size > 1) {
        p.failAnalysis("Multiple time window expressions would result in a cartesian product " +
          "of rows, therefore they are not currently not supported.")
      } else {
        p // Return unchanged. Analyzer will throw exception later
      }
  }
}
