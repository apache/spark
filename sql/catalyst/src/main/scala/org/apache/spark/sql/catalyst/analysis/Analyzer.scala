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

import java.util
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Random, Success, Try}

import org.apache.spark.{SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.OuterScopes
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.optimizer.OptimizeUpdateFields
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{toPrettySQL, AUTO_GENERATED_ALIAS, CharVarcharUtils}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.connector.catalog.{View => _, _}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.TableChange.{After, ColumnPosition}
import org.apache.spark.sql.connector.catalog.functions.{AggregateFunction => V2AggregateFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{PartitionOverwriteMode, StoreAssignmentPolicy}
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.DAY
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._
/**
 * A trivial [[Analyzer]] with a dummy [[SessionCatalog]] and
 * [[EmptyTableFunctionRegistry]]. Used for testing when all relations are already filled
 * in and the analyzer needs only to resolve attribute references.
 *
 * Built-in function registry is set for Spark Connect project to test unresolved
 * functions.
 */
object SimpleAnalyzer extends Analyzer(
  new CatalogManager(
    FakeV2SessionCatalog,
    new SessionCatalog(
      new InMemoryCatalog,
      FunctionRegistry.builtin,
      TableFunctionRegistry.builtin) {
      override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {}
    })) {
  override def resolver: Resolver = caseSensitiveResolution
}

object FakeV2SessionCatalog extends TableCatalog with FunctionCatalog with SupportsNamespaces {
  private def fail() = throw SparkUnsupportedOperationException()
  override def listTables(namespace: Array[String]): Array[Identifier] = fail()
  override def loadTable(ident: Identifier): Table = {
    throw new NoSuchTableException(ident.asMultipartIdentifier)
  }
  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = fail()
  override def alterTable(ident: Identifier, changes: TableChange*): Table = fail()
  override def dropTable(ident: Identifier): Boolean = fail()
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = fail()
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  override def name(): String = CatalogManager.SESSION_CATALOG_NAME
  override def listFunctions(namespace: Array[String]): Array[Identifier] = fail()
  override def loadFunction(ident: Identifier): UnboundFunction = fail()
  override def listNamespaces(): Array[Array[String]] = fail()
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = fail()
  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    if (namespace.length == 1) {
      mutable.HashMap[String, String]().asJava
    } else {
      throw new NoSuchNamespaceException(namespace)
    }
  }
  override def createNamespace(
    namespace: Array[String], metadata: util.Map[String, String]): Unit = fail()
  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = fail()
  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = fail()
}

/**
 * Provides a way to keep state during the analysis, mostly for resolving views and subqueries.
 * This enables us to decouple the concerns of analysis environment from the catalog and resolve
 * star expressions in subqueries that reference the outer query plans.
 * The state that is kept here is per-query.
 *
 * Note this is thread local.
 *
 * @param catalogAndNamespace The catalog and namespace used in the view resolution. This overrides
 *                            the current catalog and namespace when resolving relations inside
 *                            views.
 * @param nestedViewDepth The nested depth in the view resolution, this enables us to limit the
 *                        depth of nested views.
 * @param maxNestedViewDepth The maximum allowed depth of nested view resolution.
 * @param relationCache A mapping from qualified table names and time travel spec to resolved
 *                      relations. This can ensure that the table is resolved only once if a table
 *                      is used multiple times in a query.
 * @param referredTempViewNames All the temp view names referred by the current view we are
 *                              resolving. It's used to make sure the relation resolution is
 *                              consistent between view creation and view resolution. For example,
 *                              if `t` was a permanent table when the current view was created, it
 *                              should still be a permanent table when resolving the current view,
 *                              even if a temp view `t` has been created.
 * @param outerPlan The query plan from the outer query that can be used to resolve star
 *                  expressions in a subquery.
 */
case class AnalysisContext(
    catalogAndNamespace: Seq[String] = Nil,
    nestedViewDepth: Int = 0,
    maxNestedViewDepth: Int = -1,
    relationCache: mutable.Map[(Seq[String], Option[TimeTravelSpec]), LogicalPlan] =
      mutable.Map.empty,
    referredTempViewNames: Seq[Seq[String]] = Seq.empty,
    // 1. If we are resolving a view, this field will be restored from the view metadata,
    //    by calling `AnalysisContext.withAnalysisContext(viewDesc)`.
    // 2. If we are not resolving a view, this field will be updated everytime the analyzer
    //    lookup a temporary function. And export to the view metadata.
    referredTempFunctionNames: mutable.Set[String] = mutable.Set.empty,
    referredTempVariableNames: Seq[Seq[String]] = Seq.empty,
    outerPlan: Option[LogicalPlan] = None)

object AnalysisContext {
  private val value = new ThreadLocal[AnalysisContext]() {
    override def initialValue: AnalysisContext = AnalysisContext()
  }

  def get: AnalysisContext = value.get()
  def reset(): Unit = value.remove()

  private def set(context: AnalysisContext): Unit = value.set(context)

  def withAnalysisContext[A](viewDesc: CatalogTable)(f: => A): A = {
    val originContext = value.get()
    val maxNestedViewDepth = if (originContext.maxNestedViewDepth == -1) {
      // Here we start to resolve views, get `maxNestedViewDepth` from configs.
      SQLConf.get.maxNestedViewDepth
    } else {
      originContext.maxNestedViewDepth
    }
    val context = AnalysisContext(
      viewDesc.viewCatalogAndNamespace,
      originContext.nestedViewDepth + 1,
      maxNestedViewDepth,
      originContext.relationCache,
      viewDesc.viewReferredTempViewNames,
      mutable.Set(viewDesc.viewReferredTempFunctionNames: _*),
      viewDesc.viewReferredTempVariableNames)
    set(context)
    try f finally { set(originContext) }
  }

  def withNewAnalysisContext[A](f: => A): A = {
    val originContext = value.get()
    reset()
    try f finally { set(originContext) }
  }

  def withOuterPlan[A](outerPlan: LogicalPlan)(f: => A): A = {
    val originContext = value.get()
    val context = originContext.copy(outerPlan = Some(outerPlan))
    set(context)
    try f finally { set(originContext) }
  }
}

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a [[SessionCatalog]].
 */
class Analyzer(override val catalogManager: CatalogManager) extends RuleExecutor[LogicalPlan]
  with CheckAnalysis with SQLConfHelper with ColumnResolutionHelper {

  private val v1SessionCatalog: SessionCatalog = catalogManager.v1SessionCatalog

  override protected def validatePlanChanges(
      previousPlan: LogicalPlan,
      currentPlan: LogicalPlan): Option[String] = {
    LogicalPlanIntegrity.validateExprIdUniqueness(currentPlan)
  }

  override def isView(nameParts: Seq[String]): Boolean = v1SessionCatalog.isView(nameParts)

  // Only for tests.
  def this(catalog: SessionCatalog) = {
    this(new CatalogManager(FakeV2SessionCatalog, catalog))
  }

  def executeAndCheck(plan: LogicalPlan, tracker: QueryPlanningTracker): LogicalPlan = {
    if (plan.analyzed) return plan
    AnalysisHelper.markInAnalyzer {
      val analyzed = executeAndTrack(plan, tracker)
      checkAnalysis(analyzed)
      analyzed
    }
  }

  override def execute(plan: LogicalPlan): LogicalPlan = {
    AnalysisContext.withNewAnalysisContext {
      executeSameContext(plan)
    }
  }

  private def executeSameContext(plan: LogicalPlan): LogicalPlan = super.execute(plan)

  def resolver: Resolver = conf.resolver

  /**
   * If the plan cannot be resolved within maxIterations, analyzer will throw exception to inform
   * user to increase the value of SQLConf.ANALYZER_MAX_ITERATIONS.
   */
  protected def fixedPoint =
    FixedPoint(
      conf.analyzerMaxIterations,
      errorOnExceed = true,
      maxIterationsSetting = SQLConf.ANALYZER_MAX_ITERATIONS.key)

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

  private def typeCoercionRules(): List[Rule[LogicalPlan]] = if (conf.ansiEnabled) {
    AnsiTypeCoercion.typeCoercionRules
  } else {
    TypeCoercion.typeCoercionRules
  }

  private def earlyBatches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      new SubstituteExecuteImmediate(catalogManager),
      // This rule optimizes `UpdateFields` expression chains so looks more like optimization rule.
      // However, when manipulating deeply nested schema, `UpdateFields` expression tree could be
      // very complex and make analysis impossible. Thus we need to optimize `UpdateFields` early
      // at the beginning of analysis.
      OptimizeUpdateFields,
      CTESubstitution,
      WindowsSubstitution,
      EliminateUnions,
      SubstituteUnresolvedOrdinals),
    Batch("Disable Hints", Once,
      new ResolveHints.DisableHints),
    Batch("Hints", fixedPoint,
      ResolveHints.ResolveJoinStrategyHints,
      ResolveHints.ResolveCoalesceHints),
    Batch("Simple Sanity Check", Once,
      LookupFunctions),
    Batch("Keep Legacy Outputs", Once,
      KeepLegacyOutputs)
  )

  override def batches: Seq[Batch] = earlyBatches ++ Seq(
    Batch("Resolution", fixedPoint,
      new ResolveCatalogs(catalogManager) ::
      ResolveInsertInto ::
      ResolveRelations ::
      ResolvePartitionSpec ::
      ResolveFieldNameAndPosition ::
      AddMetadataColumns ::
      DeduplicateRelations ::
      new ResolveReferences(catalogManager) ::
      // Please do not insert any other rules in between. See the TODO comments in rule
      // ResolveLateralColumnAliasReference for more details.
      ResolveLateralColumnAliasReference ::
      ResolveExpressionsWithNamePlaceholders ::
      ResolveDeserializer ::
      ResolveNewInstance ::
      ResolveUpCast ::
      ResolveGroupingAnalytics ::
      ResolvePivot ::
      ResolveUnpivot ::
      ResolveTranspose ::
      ResolveOrdinalInOrderByAndGroupBy ::
      ExtractGenerator ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveTableSpec ::
      ResolveAliases ::
      ResolveSubquery ::
      ResolveSubqueryColumnAliases ::
      ResolveWindowOrder ::
      ResolveWindowFrame ::
      ResolveNaturalAndUsingJoin ::
      ResolveOutputRelation ::
      new ResolveDataFrameDropColumns(catalogManager) ::
      new ResolveSetVariable(catalogManager) ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      TimeWindowing ::
      SessionWindowing ::
      ResolveWindowTime ::
      ResolveInlineTables ::
      ResolveLambdaVariables ::
      ResolveTimeZone ::
      ResolveRandomSeed ::
      ResolveBinaryArithmetic ::
      new ResolveIdentifierClause(earlyBatches) ::
      ResolveUnion ::
      ResolveRowLevelCommandAssignments ::
      MoveParameterizedQueriesDown ::
      BindParameters ::
      typeCoercionRules() ++
      Seq(
        ResolveWithCTE,
        ExtractDistributedSequenceID) ++
      Seq(ResolveUpdateEventTimeWatermarkColumn) ++
      extendedResolutionRules : _*),
    Batch("Remove TempResolvedColumn", Once, RemoveTempResolvedColumn),
    Batch("Post-Hoc Resolution", Once,
      Seq(ResolveCommandsWithIfExists) ++
      postHocResolutionRules: _*),
    Batch("Remove Unresolved Hints", Once,
      new ResolveHints.RemoveAllHints),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("ScalaUDF Null Handling", fixedPoint,
      // `HandleNullInputsForUDF` may wrap the `ScalaUDF` with `If` expression to return null for
      // null inputs, so the result can be null even if `ScalaUDF#nullable` is false. We need to
      // run `UpdateAttributeNullability` to update nullability of the UDF output attribute in
      // downstream operators. After updating attribute nullability, `ScalaUDF`s in downstream
      // operators may need null handling as well, so we should run these two rules repeatedly.
      HandleNullInputsForUDF,
      UpdateAttributeNullability),
    Batch("UDF", Once,
      ResolveEncodersInUDF),
    // The rewrite rules might move resolved query plan into subquery. Once the resolved plan
    // contains ScalaUDF, their encoders won't be resolved if `ResolveEncodersInUDF` is not
    // applied before the rewrite rules. So we need to apply `ResolveEncodersInUDF` before the
    // rewrite rules.
    Batch("DML rewrite", fixedPoint,
      RewriteDeleteFromTable,
      RewriteUpdateTable,
      RewriteMergeIntoTable),
    Batch("Subquery", Once,
      UpdateOuterReferences),
    Batch("Cleanup", fixedPoint,
      CleanupAliases),
    Batch("HandleSpecialCommand", Once,
      HandleSpecialCommand),
    Batch("Remove watermark for batch query", Once,
      EliminateEventTimeWatermark)
  )

  /**
   * For [[Add]]:
   * 1. if both side are interval, stays the same;
   * 2. else if one side is date and the other is interval,
   *    turns it to [[DateAddInterval]];
   * 3. else if one side is interval, turns it to [[TimeAdd]];
   * 4. else if one side is date, turns it to [[DateAdd]] ;
   * 5. else stays the same.
   *
   * For [[Subtract]]:
   * 1. if both side are interval, stays the same;
   * 2. else if the left side is date and the right side is interval,
   *    turns it to [[DateAddInterval(l, -r)]];
   * 3. else if the right side is an interval, turns it to [[TimeAdd(l, -r)]];
   * 4. else if one side is timestamp, turns it to [[SubtractTimestamps]];
   * 5. else if the right side is date, turns it to [[DateDiff]]/[[SubtractDates]];
   * 6. else if the left side is date, turns it to [[DateSub]];
   * 7. else turns it to stays the same.
   *
   * For [[Multiply]]:
   * 1. If one side is interval, turns it to [[MultiplyInterval]];
   * 2. otherwise, stays the same.
   *
   * For [[Divide]]:
   * 1. If the left side is interval, turns it to [[DivideInterval]];
   * 2. otherwise, stays the same.
   */
  object ResolveBinaryArithmetic extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan =
      plan.resolveExpressionsUpWithPruning(_.containsPattern(BINARY_ARITHMETIC), ruleId) {
        case expr @ (_: Add | _: Subtract | _: Multiply | _: Divide)
          if expr.childrenResolved => resolve(expr)
      }

    def resolve(expr: Expression): Expression = expr match {
      case a @ Add(l, r, mode) => (l.dataType, r.dataType) match {
        case (DateType, DayTimeIntervalType(DAY, DAY)) => DateAdd(l, ExtractANSIIntervalDays(r))
        case (DateType, _: DayTimeIntervalType) => TimeAdd(Cast(l, TimestampType), r)
        case (DayTimeIntervalType(DAY, DAY), DateType) => DateAdd(r, ExtractANSIIntervalDays(l))
        case (_: DayTimeIntervalType, DateType) => TimeAdd(Cast(r, TimestampType), l)
        case (DateType, _: YearMonthIntervalType) => DateAddYMInterval(l, r)
        case (_: YearMonthIntervalType, DateType) => DateAddYMInterval(r, l)
        case (TimestampType | TimestampNTZType, _: YearMonthIntervalType) =>
          TimestampAddYMInterval(l, r)
        case (_: YearMonthIntervalType, TimestampType | TimestampNTZType) =>
          TimestampAddYMInterval(r, l)
        case (CalendarIntervalType, CalendarIntervalType) |
             (_: DayTimeIntervalType, _: DayTimeIntervalType) => a
        case (_: NullType, _: AnsiIntervalType) =>
          a.copy(left = Cast(a.left, a.right.dataType))
        case (_: AnsiIntervalType, _: NullType) =>
          a.copy(right = Cast(a.right, a.left.dataType))
        case (DateType, CalendarIntervalType) =>
          DateAddInterval(l, r, ansiEnabled = mode == EvalMode.ANSI)
        case (_, CalendarIntervalType | _: DayTimeIntervalType) => Cast(TimeAdd(l, r), l.dataType)
        case (CalendarIntervalType, DateType) =>
          DateAddInterval(r, l, ansiEnabled = mode == EvalMode.ANSI)
        case (CalendarIntervalType | _: DayTimeIntervalType, _) => Cast(TimeAdd(r, l), r.dataType)
        case (DateType, dt) if dt != StringType => DateAdd(l, r)
        case (dt, DateType) if dt != StringType => DateAdd(r, l)
        case _ => a
      }
      case s @ Subtract(l, r, mode) => (l.dataType, r.dataType) match {
        case (DateType, DayTimeIntervalType(DAY, DAY)) =>
          DateAdd(l, UnaryMinus(ExtractANSIIntervalDays(r), mode == EvalMode.ANSI))
        case (DateType, _: DayTimeIntervalType) =>
          DatetimeSub(l, r, TimeAdd(Cast(l, TimestampType), UnaryMinus(r, mode == EvalMode.ANSI)))
        case (DateType, _: YearMonthIntervalType) =>
          DatetimeSub(l, r, DateAddYMInterval(l, UnaryMinus(r, mode == EvalMode.ANSI)))
        case (TimestampType | TimestampNTZType, _: YearMonthIntervalType) =>
          DatetimeSub(l, r, TimestampAddYMInterval(l, UnaryMinus(r, mode == EvalMode.ANSI)))
        case (CalendarIntervalType, CalendarIntervalType) |
             (_: DayTimeIntervalType, _: DayTimeIntervalType) => s
        case (_: NullType, _: AnsiIntervalType) =>
          s.copy(left = Cast(s.left, s.right.dataType))
        case (_: AnsiIntervalType, _: NullType) =>
          s.copy(right = Cast(s.right, s.left.dataType))
        case (DateType, CalendarIntervalType) =>
          DatetimeSub(l, r, DateAddInterval(l,
            UnaryMinus(r, mode == EvalMode.ANSI), ansiEnabled = mode == EvalMode.ANSI))
        case (_, CalendarIntervalType | _: DayTimeIntervalType) =>
          Cast(DatetimeSub(l, r, TimeAdd(l, UnaryMinus(r, mode == EvalMode.ANSI))), l.dataType)
        case _ if AnyTimestampTypeExpression.unapply(l) ||
          AnyTimestampTypeExpression.unapply(r) => SubtractTimestamps(l, r)
        case (_, DateType) => SubtractDates(l, r)
        case (DateType, dt) if dt != StringType => DateSub(l, r)
        case _ => s
      }
      case m @ Multiply(l, r, mode) => (l.dataType, r.dataType) match {
        case (CalendarIntervalType, _) => MultiplyInterval(l, r, mode == EvalMode.ANSI)
        case (_, CalendarIntervalType) => MultiplyInterval(r, l, mode == EvalMode.ANSI)
        case (_: YearMonthIntervalType, _) => MultiplyYMInterval(l, r)
        case (_, _: YearMonthIntervalType) => MultiplyYMInterval(r, l)
        case (_: DayTimeIntervalType, _) => MultiplyDTInterval(l, r)
        case (_, _: DayTimeIntervalType) => MultiplyDTInterval(r, l)
        case _ => m
      }
      case d @ Divide(l, r, mode) => (l.dataType, r.dataType) match {
        case (CalendarIntervalType, _) => DivideInterval(l, r, mode == EvalMode.ANSI)
        case (_: YearMonthIntervalType, _) => DivideYMInterval(l, r)
        case (_: DayTimeIntervalType, _) => DivideDTInterval(l, r)
        case _ => d
      }
    }
  }

  /**
   * Substitute child plan with WindowSpecDefinitions.
   */
  object WindowsSubstitution extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsAnyPattern(WITH_WINDOW_DEFINITION, UNRESOLVED_WINDOW_EXPRESSION), ruleId) {
      // Lookup WindowSpecDefinitions. This rule works with unresolved children.
      case WithWindowDefinition(windowDefinitions, child) => child.resolveExpressions {
        case UnresolvedWindowExpression(c, WindowSpecReference(windowName)) =>
          val windowSpecDefinition = windowDefinitions.getOrElse(windowName,
            throw QueryCompilationErrors.windowSpecificationNotDefinedError(windowName))
          WindowExpression(c, windowSpecDefinition)
      }
    }
  }

  /**
   * Replaces [[UnresolvedAlias]]s with concrete aliases.
   */
  object ResolveAliases extends Rule[LogicalPlan] {
    private def assignAliases(exprs: Seq[NamedExpression]) = {
      exprs.map(_.transformUpWithPruning(_.containsPattern(UNRESOLVED_ALIAS)) {
          case u: UnresolvedAlias => resolve(u)
        }
      ).asInstanceOf[Seq[NamedExpression]]
    }

    private[analysis] def resolve(u: UnresolvedAlias): Expression = {
      val UnresolvedAlias(child, optGenAliasFunc) = u
      child match {
        case ne: NamedExpression => ne
        case go @ GeneratorOuter(g: Generator) if g.resolved => MultiAlias(go, Nil)
        case e if !e.resolved => u
        case g: Generator => MultiAlias(g, Nil)
        case c @ Cast(ne: NamedExpression, _, _, _) => Alias(c, ne.name)()
        case e: ExtractValue if extractOnly(e) => Alias(e, toPrettySQL(e))()
        case e if optGenAliasFunc.isDefined =>
          Alias(child, optGenAliasFunc.get.apply(e))()
        case l: Literal => Alias(l, toPrettySQL(l))()
        case e =>
          val metaForAutoGeneratedAlias = new MetadataBuilder()
            .putString(AUTO_GENERATED_ALIAS, "true")
            .build()
          Alias(e, toPrettySQL(e))(explicitMetadata = Some(metaForAutoGeneratedAlias))
      }
    }

    private def extractOnly(e: Expression): Boolean = e match {
      case _: ExtractValue => e.children.forall(extractOnly)
      case _: Literal => true
      case _: Attribute => true
      case _ => false
    }

    private def hasUnresolvedAlias(exprs: Seq[NamedExpression]) =
      exprs.exists(_.exists(_.isInstanceOf[UnresolvedAlias]))

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(UNRESOLVED_ALIAS), ruleId) {
      case Aggregate(groups, aggs, child) if child.resolved && hasUnresolvedAlias(aggs) =>
        Aggregate(groups, assignAliases(aggs), child)

      case Pivot(groupByOpt, pivotColumn, pivotValues, aggregates, child)
        if child.resolved && groupByOpt.isDefined && hasUnresolvedAlias(groupByOpt.get) =>
        Pivot(Some(assignAliases(groupByOpt.get)), pivotColumn, pivotValues, aggregates, child)

      case up: Unpivot if up.child.resolved &&
        (up.ids.exists(hasUnresolvedAlias) || up.values.exists(_.exists(hasUnresolvedAlias))) =>
        up.copy(ids = up.ids.map(assignAliases), values = up.values.map(_.map(assignAliases)))

      case Project(projectList, child) if child.resolved && hasUnresolvedAlias(projectList) =>
        Project(assignAliases(projectList), child)

      case c: CollectMetrics if c.child.resolved && hasUnresolvedAlias(c.metrics) =>
        c.copy(metrics = assignAliases(c.metrics))
    }
  }

  object ResolveGroupingAnalytics extends Rule[LogicalPlan] {
    private[analysis] def hasGroupingFunction(e: Expression): Boolean = {
      e.exists (g => g.isInstanceOf[Grouping] || g.isInstanceOf[GroupingID])
    }

    private def replaceGroupingFunc(
        expr: Expression,
        groupByExprs: Seq[Expression],
        gid: Expression): Expression = {
      expr transform {
        case e: GroupingID =>
          if (e.groupByExprs.isEmpty ||
              e.groupByExprs.map(_.canonicalized) == groupByExprs.map(_.canonicalized)) {
            Alias(gid, toPrettySQL(e))()
          } else {
            throw QueryCompilationErrors.groupingIDMismatchError(e, groupByExprs)
          }
        case e @ Grouping(col: Expression) =>
          val idx = groupByExprs.indexWhere(_.semanticEquals(col))
          if (idx >= 0) {
            Alias(Cast(BitwiseAnd(ShiftRight(gid, Literal(groupByExprs.length - 1 - idx)),
              Literal(1L)), ByteType), toPrettySQL(e))()
          } else {
            throw QueryCompilationErrors.groupingColInvalidError(col, groupByExprs)
          }
      }
    }

    /*
     * Create new alias for all group by expressions for `Expand` operator.
     */
    private def constructGroupByAlias(groupByExprs: Seq[Expression]): Seq[Alias] = {
      groupByExprs.map {
        case e: NamedExpression => Alias(e, e.name)(qualifier = e.qualifier)
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
      // should change the nullability of b to be TRUE.
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
            throw QueryCompilationErrors.selectExprNotInGroupByError(expr, groupByAliases))
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
        gid: Attribute): Seq[NamedExpression] = {
      def replaceExprs(e: Expression): Expression = e match {
        case e: AggregateExpression => e
        case e =>
          // Replace expression by expand output attribute.
          val index = groupByAliases.indexWhere(_.child.semanticEquals(e))
          if (index == -1) {
            e.mapChildren(replaceExprs)
          } else {
            groupingAttrs(index)
          }
      }
      aggregations
        .map(replaceGroupingFunc(_, groupByExprs, gid))
        .map(replaceExprs)
        .map(_.asInstanceOf[NamedExpression])
    }

    /*
     * Construct [[Aggregate]] operator from Cube/Rollup/GroupingSets.
     */
    private def constructAggregate(
        selectedGroupByExprs: Seq[Seq[Expression]],
        groupByExprs: Seq[Expression],
        aggregationExprs: Seq[NamedExpression],
        child: LogicalPlan): LogicalPlan = {

      if (groupByExprs.size > GroupingID.dataType.defaultSize * 8) {
        throw QueryCompilationErrors.groupingSizeTooLargeError(GroupingID.dataType.defaultSize * 8)
      }

      // Expand works by setting grouping expressions to null as determined by the
      // `selectedGroupByExprs`. To prevent these null values from being used in an aggregate
      // instead of the original value we need to create new aliases for all group by expressions
      // that will only be used for the intended purpose.
      val groupByAliases = constructGroupByAlias(groupByExprs)

      val gid = AttributeReference(VirtualColumn.groupingIdName, GroupingID.dataType, false)()
      val expand = constructExpand(selectedGroupByExprs, child, groupByAliases, gid)
      val groupingAttrs = expand.output.drop(child.output.length)

      val aggregations = constructAggregateExprs(
        groupByExprs, aggregationExprs, groupByAliases, groupingAttrs, gid)

      Aggregate(groupingAttrs, aggregations, expand)
    }

    private def findGroupingExprs(plan: LogicalPlan): Seq[Expression] = {
      plan.collectFirst {
        case a: Aggregate =>
          // this Aggregate should have grouping id as the last grouping key.
          val gid = a.groupingExpressions.last
          if (!gid.isInstanceOf[AttributeReference]
            || gid.asInstanceOf[AttributeReference].name != VirtualColumn.groupingIdName) {
            throw QueryCompilationErrors.groupingMustWithGroupingSetsOrCubeOrRollupError()
          }
          a.groupingExpressions.take(a.groupingExpressions.length - 1)
      }.getOrElse {
        throw QueryCompilationErrors.groupingMustWithGroupingSetsOrCubeOrRollupError()
      }
    }

    private def tryResolveHavingCondition(
        h: UnresolvedHaving,
        aggregate: Aggregate,
        selectedGroupByExprs: Seq[Seq[Expression]],
        groupByExprs: Seq[Expression]): LogicalPlan = {
      // For CUBE/ROLLUP expressions, to avoid resolving repeatedly, here we delete them from
      // groupingExpressions for condition resolving.
      val aggForResolving = aggregate.copy(groupingExpressions = groupByExprs)
      // HACK ALTER! Ideally we should only resolve GROUPING SETS + HAVING when the having condition
      // is fully resolved, similar to the rule `ResolveAggregateFunctions`. However, Aggregate
      // with GROUPING SETS is marked as unresolved and many analyzer rules can't apply to
      // UnresolvedHaving because its child is not resolved. Here we explicitly resolve columns
      // and subqueries of UnresolvedHaving so that the rewrite works in most cases.
      // TODO: mark Aggregate as resolved even if it has GROUPING SETS. We can expand it at the end
      //       of the analysis phase.
      val colResolved = h.mapExpressions { e =>
        resolveExpressionByPlanOutput(
          resolveColWithAgg(e, aggForResolving), aggForResolving, includeLastResort = true)
      }
      val cond = if (SubqueryExpression.hasSubquery(colResolved.havingCondition)) {
        val fake = Project(Alias(colResolved.havingCondition, "fake")() :: Nil, aggregate.child)
        ResolveSubquery(fake).asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
      } else {
        colResolved.havingCondition
      }
      // Try resolving the condition of the filter as though it is in the aggregate clause
      val (extraAggExprs, Seq(resolvedHavingCond)) =
        ResolveAggregateFunctions.resolveExprsWithAggregate(Seq(cond), aggForResolving)

      // Push the aggregate expressions into the aggregate (if any).
      val newChild = constructAggregate(selectedGroupByExprs, groupByExprs,
        aggregate.aggregateExpressions ++ extraAggExprs, aggregate.child)

      // Since the output exprId will be changed in the constructed aggregate, here we build an
      // attrMap to resolve the condition again.
      val attrMap = AttributeMap((aggForResolving.output ++ extraAggExprs.map(_.toAttribute))
        .zip(newChild.output))
      val newCond = resolvedHavingCond.transform {
        case a: AttributeReference => attrMap.getOrElse(a, a)
      }

      if (extraAggExprs.isEmpty) {
        Filter(newCond, newChild)
      } else {
        Project(newChild.output.dropRight(extraAggExprs.length),
          Filter(newCond, newChild))
      }
    }

    // This require transformDown to resolve having condition when generating aggregate node for
    // CUBE/ROLLUP/GROUPING SETS. This also replace grouping()/grouping_id() in resolved
    // Filter/Sort.
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDownWithPruning(
      _.containsPattern(GROUPING_ANALYTICS), ruleId) {
      case h @ UnresolvedHaving(_, agg @ Aggregate(
        GroupingAnalytics(selectedGroupByExprs, groupByExprs), aggExprs, _))
        if agg.childrenResolved && aggExprs.forall(_.resolved) =>
        tryResolveHavingCondition(h, agg, selectedGroupByExprs, groupByExprs)

      // Make sure all of the children are resolved.
      // We can't put this at the beginning, because `Aggregate` with GROUPING SETS is unresolved
      // but we need to resolve `UnresolvedHaving` above it.
      case a if !a.childrenResolved => a

      // Ensure group by expressions and aggregate expressions have been resolved.
      case Aggregate(GroupingAnalytics(selectedGroupByExprs, groupByExprs), aggExprs, child)
        if aggExprs.forall(_.resolved) =>
        constructAggregate(selectedGroupByExprs, groupByExprs, aggExprs, child)

      // We should make sure all expressions in condition have been resolved.
      case f @ Filter(cond, child) if hasGroupingFunction(cond) && cond.resolved =>
        val groupingExprs = findGroupingExprs(child)
        // The unresolved grouping id will be resolved by ResolveReferences
        val newCond = replaceGroupingFunc(cond, groupingExprs, VirtualColumn.groupingIdAttribute)
        f.copy(condition = newCond)

      // We should make sure all [[SortOrder]]s have been resolved.
      case s @ Sort(order, _, child)
        if order.exists(hasGroupingFunction) && order.forall(_.resolved) =>
        val groupingExprs = findGroupingExprs(child)
        val gid = VirtualColumn.groupingIdAttribute
        // The unresolved grouping id will be resolved by ResolveReferences
        val newOrder = order.map(replaceGroupingFunc(_, groupingExprs, gid).asInstanceOf[SortOrder])
        s.copy(order = newOrder)
    }
  }

  object ResolvePivot extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      _.containsPattern(PIVOT), ruleId) {
      case p: Pivot if !p.childrenResolved || !p.aggregates.forall(_.resolved)
        || (p.groupByExprsOpt.isDefined && !p.groupByExprsOpt.get.forall(_.resolved))
        || !p.pivotColumn.resolved || !p.pivotValues.forall(_.resolved) => p
      case Pivot(groupByExprsOpt, pivotColumn, pivotValues, aggregates, child) =>
        if (!RowOrdering.isOrderable(pivotColumn.dataType)) {
          throw QueryCompilationErrors.unorderablePivotColError(pivotColumn)
        }
        // Check all aggregate expressions.
        aggregates.foreach(checkValidAggregateExpression)
        // Check all pivot values are literal and match pivot column data type.
        val evalPivotValues = pivotValues.map { value =>
          val foldable = trimAliases(value).foldable
          if (!foldable) {
            throw QueryCompilationErrors.nonLiteralPivotValError(value)
          }
          if (!Cast.canCast(value.dataType, pivotColumn.dataType)) {
            throw QueryCompilationErrors.pivotValDataTypeMismatchError(value, pivotColumn)
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
                  First(ifExpr(expr), true)
                case Last(expr, _) =>
                  Last(ifExpr(expr), true)
                case a: ApproximatePercentile =>
                  // ApproximatePercentile takes two literals for accuracy and percentage which
                  // should not be wrapped by if-else.
                  a.withNewChildren(ifExpr(a.first) :: a.second :: a.third :: Nil)
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
      case a: AggregateExpression =>
        if (a.aggregateFunction.isInstanceOf[PythonUDAF]) {
          throw QueryCompilationErrors.pandasUDFAggregateNotSupportedInPivotError()
        } else {
          // OK and leave the argument check to CheckAnalysis.
        }
      case e: Attribute =>
        throw QueryCompilationErrors.aggregateExpressionRequiredForPivotError(e.sql)
      case e => e.children.foreach(checkValidAggregateExpression)
    }
  }

  object ResolveUnpivot extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      _.containsPattern(UNPIVOT), ruleId) {

      // once children are resolved, we can determine values from ids and vice versa
      // if only either is given, and only AttributeReference are given
      case up @ Unpivot(Some(ids), None, _, _, _, _) if up.childrenResolved &&
        ids.forall(_.resolved) &&
        ids.forall(_.isInstanceOf[AttributeReference]) =>
        val idAttrs = AttributeSet(up.ids.get)
        val values = up.child.output.filterNot(idAttrs.contains)
        up.copy(values = Some(values.map(Seq(_))))
      case up @ Unpivot(None, Some(values), _, _, _, _) if up.childrenResolved &&
        values.forall(_.forall(_.resolved)) &&
        values.forall(_.forall(_.isInstanceOf[AttributeReference])) =>
        val valueAttrs = AttributeSet(up.values.get.flatten)
        val ids = up.child.output.filterNot(valueAttrs.contains)
        up.copy(ids = Some(ids))

      case up: Unpivot if !up.childrenResolved || !up.ids.exists(_.forall(_.resolved)) ||
        !up.values.exists(_.nonEmpty) || !up.values.exists(_.forall(_.forall(_.resolved))) ||
        !up.values.get.forall(_.length == up.valueColumnNames.length) ||
        !up.valuesTypeCoercioned => up

      // TypeCoercionBase.UnpivotCoercion determines valueType
      // and casts values once values are set and resolved
      case Unpivot(Some(ids), Some(values), aliases, variableColumnName, valueColumnNames, child) =>

        def toString(values: Seq[NamedExpression]): String =
          values.map(v => v.name).mkString("_")

        // construct unpivot expressions for Expand
        val exprs: Seq[Seq[Expression]] =
          values.zip(aliases.getOrElse(values.map(_ => None))).map {
            case (vals, Some(alias)) => (ids :+ Literal(alias)) ++ vals
            case (Seq(value), None) => (ids :+ Literal(value.name)) :+ value
            // there are more than one value in vals
            case (vals, None) => (ids :+ Literal(toString(vals))) ++ vals
          }

        // construct output attributes
        val variableAttr = AttributeReference(variableColumnName, StringType, nullable = false)()
        val valueAttrs = valueColumnNames.zipWithIndex.map {
          case (valueColumnName, idx) =>
            AttributeReference(
              valueColumnName,
              values.head(idx).dataType,
              values.map(_(idx)).exists(_.nullable))()
        }
        val output = (ids.map(_.toAttribute) :+ variableAttr) ++ valueAttrs

        // expand the unpivot expressions
        Expand(exprs, output, child)
    }
  }

  private def isResolvingView: Boolean = AnalysisContext.get.catalogAndNamespace.nonEmpty
  private def isReferredTempViewName(nameParts: Seq[String]): Boolean = {
    AnalysisContext.get.referredTempViewNames.exists { n =>
      (n.length == nameParts.length) && n.zip(nameParts).forall {
        case (a, b) => resolver(a, b)
      }
    }
  }

  // If we are resolving database objects (relations, functions, etc.) insides views, we may need to
  // expand single or multi-part identifiers with the current catalog and namespace of when the
  // view was created.
  private def expandIdentifier(nameParts: Seq[String]): Seq[String] = {
    if (!isResolvingView || isReferredTempViewName(nameParts)) return nameParts

    if (nameParts.length == 1) {
      AnalysisContext.get.catalogAndNamespace :+ nameParts.head
    } else if (catalogManager.isCatalogRegistered(nameParts.head)) {
      nameParts
    } else {
      AnalysisContext.get.catalogAndNamespace.head +: nameParts
    }
  }

  /**
   * Adds metadata columns to output for child relations when nodes are missing resolved attributes.
   *
   * References to metadata columns are resolved using columns from [[LogicalPlan.metadataOutput]],
   * but the relation's output does not include the metadata columns until the relation is replaced.
   * Unless this rule adds metadata to the relation's output, the analyzer will detect that nothing
   * produces the columns.
   *
   * This rule only adds metadata columns when a node is resolved but is missing input from its
   * children. This ensures that metadata columns are not added to the plan unless they are used. By
   * checking only resolved nodes, this ensures that * expansion is already done so that metadata
   * columns are not accidentally selected by *. This rule resolves operators downwards to avoid
   * projecting away metadata columns prematurely.
   */
  object AddMetadataColumns extends Rule[LogicalPlan] {
    import org.apache.spark.sql.catalyst.util._

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDownWithPruning(
      AlwaysProcess.fn, ruleId) {
      case hint: UnresolvedHint => hint
      // Add metadata output to all node types
      case node if node.children.nonEmpty && node.resolved && hasMetadataCol(node) =>
        val inputAttrs = AttributeSet(node.children.flatMap(_.output))
        val metaCols = getMetadataAttributes(node).filterNot(inputAttrs.contains)
        if (metaCols.isEmpty) {
          node
        } else {
          val newNode = node.mapChildren(addMetadataCol(_, metaCols.map(_.exprId).toSet))
          // We should not change the output schema of the plan. We should project away the extra
          // metadata columns if necessary.
          if (newNode.sameOutput(node)) {
            newNode
          } else {
            Project(node.output, newNode)
          }
        }
    }

    private def getMetadataAttributes(plan: LogicalPlan): Seq[Attribute] = {
      plan.expressions.flatMap(_.collect {
        case a: Attribute if a.isMetadataCol => a
        case a: Attribute
          if plan.children.exists(c => c.metadataOutput.exists(_.exprId == a.exprId)) =>
          plan.children.collectFirst {
            case c if c.metadataOutput.exists(_.exprId == a.exprId) =>
              c.metadataOutput.find(_.exprId == a.exprId).get
          }.get
      })
    }

    private def hasMetadataCol(plan: LogicalPlan): Boolean = {
      plan.expressions.exists(_.exists {
        case a: Attribute =>
          // If an attribute is resolved before being labeled as metadata
          // (i.e. from the originating Dataset), we check with expression ID
          a.isMetadataCol ||
            plan.children.exists(c => c.metadataOutput.exists(_.exprId == a.exprId))
        case _ => false
      })
    }

    private def addMetadataCol(
        plan: LogicalPlan,
        requiredAttrIds: Set[ExprId]): LogicalPlan = plan match {
      case s: ExposesMetadataColumns if s.metadataOutput.exists( a =>
        requiredAttrIds.contains(a.exprId)) =>
        s.withMetadataColumns()
      case p: Project if p.metadataOutput.exists(a => requiredAttrIds.contains(a.exprId)) =>
        val newProj = p.copy(
          // Do not leak the qualified-access-only restriction to normal plan outputs.
          projectList = p.projectList ++ p.metadataOutput.map(_.markAsAllowAnyAccess()),
          child = addMetadataCol(p.child, requiredAttrIds))
        newProj.copyTagsFrom(p)
        newProj
      case _ => plan.withNewChildren(plan.children.map(addMetadataCol(_, requiredAttrIds)))
    }
  }

  /**
   * Replaces unresolved relations (tables and views) with concrete relations from the catalog.
   */
  object ResolveRelations extends Rule[LogicalPlan] {
    // The current catalog and namespace may be different from when the view was created, we must
    // resolve the view logical plan here, with the catalog and namespace stored in view metadata.
    // This is done by keeping the catalog and namespace in `AnalysisContext`, and analyzer will
    // look at `AnalysisContext.catalogAndNamespace` when resolving relations with single-part name.
    // If `AnalysisContext.catalogAndNamespace` is non-empty, analyzer will expand single-part names
    // with it, instead of current catalog and namespace.
    private def resolveViews(plan: LogicalPlan): LogicalPlan = plan match {
      // The view's child should be a logical plan parsed from the `desc.viewText`, the variable
      // `viewText` should be defined, or else we throw an error on the generation of the View
      // operator.
      case view @ View(desc, isTempView, child) if !child.resolved =>
        // Resolve all the UnresolvedRelations and Views in the child.
        val newChild = AnalysisContext.withAnalysisContext(desc) {
          val nestedViewDepth = AnalysisContext.get.nestedViewDepth
          val maxNestedViewDepth = AnalysisContext.get.maxNestedViewDepth
          if (nestedViewDepth > maxNestedViewDepth) {
            throw QueryCompilationErrors.viewDepthExceedsMaxResolutionDepthError(
              desc.identifier, maxNestedViewDepth, view)
          }
          SQLConf.withExistingConf(View.effectiveSQLConf(desc.viewSQLConfigs, isTempView)) {
            executeSameContext(child)
          }
        }
        // Fail the analysis eagerly because outside AnalysisContext, the unresolved operators
        // inside a view maybe resolved incorrectly.
        checkAnalysis(newChild)
        view.copy(child = newChild)
      case p @ SubqueryAlias(_, view: View) =>
        p.copy(child = resolveViews(view))
      case _ => plan
    }

    private def unwrapRelationPlan(plan: LogicalPlan): LogicalPlan = {
      EliminateSubqueryAliases(plan) match {
        case v: View if v.isTempViewStoringAnalyzedPlan => v.child
        case other => other
      }
    }

    def apply(plan: LogicalPlan)
        : LogicalPlan = plan.resolveOperatorsUpWithPruning(AlwaysProcess.fn, ruleId) {
      case i @ InsertIntoStatement(table, _, _, _, _, _, _) =>
        val relation = table match {
          case u: UnresolvedRelation if !u.isStreaming =>
            resolveRelation(u).getOrElse(u)
          case other => other
        }

        // Inserting into a file-based temporary view is allowed.
        // (e.g., spark.read.parquet("path").createOrReplaceTempView("t").
        // Thus, we need to look at the raw plan if `relation` is a temporary view.
        unwrapRelationPlan(relation) match {
          case v: View =>
            throw QueryCompilationErrors.insertIntoViewNotAllowedError(v.desc.identifier, table)
          case other => i.copy(table = other)
        }

      // TODO (SPARK-27484): handle streaming write commands when we have them.
      case write: V2WriteCommand =>
        write.table match {
          case u: UnresolvedRelation if !u.isStreaming =>
            resolveRelation(u).map(unwrapRelationPlan).map {
              case v: View => throw QueryCompilationErrors.writeIntoViewNotAllowedError(
                v.desc.identifier, write)
              case r: DataSourceV2Relation => write.withNewTable(r)
              case u: UnresolvedCatalogRelation =>
                throw QueryCompilationErrors.writeIntoV1TableNotAllowedError(
                  u.tableMeta.identifier, write)
              case other =>
                throw QueryCompilationErrors.writeIntoTempViewNotAllowedError(
                  u.multipartIdentifier.quoted)
            }.getOrElse(write)
          case _ => write
        }

      case u: UnresolvedRelation =>
        resolveRelation(u).map(resolveViews).getOrElse(u)

      case r @ RelationTimeTravel(u: UnresolvedRelation, timestamp, version)
          if timestamp.forall(ts => ts.resolved && !SubqueryExpression.hasSubquery(ts)) =>
        val timeTravelSpec = TimeTravelSpec.create(timestamp, version, conf.sessionLocalTimeZone)
        resolveRelation(u, timeTravelSpec).getOrElse(r)

      case u @ UnresolvedTable(identifier, cmd, suggestAlternative) =>
        lookupTableOrView(identifier).map {
          case v: ResolvedPersistentView =>
            val nameParts = v.catalog.name() +: v.identifier.asMultipartIdentifier
            throw QueryCompilationErrors.expectTableNotViewError(
              nameParts, cmd, suggestAlternative, u)
          case _: ResolvedTempView =>
            throw QueryCompilationErrors.expectTableNotViewError(
              identifier, cmd, suggestAlternative, u)
          case table => table
        }.getOrElse(u)

      case u @ UnresolvedView(identifier, cmd, allowTemp, suggestAlternative) =>
        lookupTableOrView(identifier, viewOnly = true).map {
          case _: ResolvedTempView if !allowTemp =>
            throw QueryCompilationErrors.expectPermanentViewNotTempViewError(
              identifier, cmd, u)
          case t: ResolvedTable =>
            val nameParts = t.catalog.name() +: t.identifier.asMultipartIdentifier
            throw QueryCompilationErrors.expectViewNotTableError(
              nameParts, cmd, suggestAlternative, u)
          case other => other
        }.getOrElse(u)

      case u @ UnresolvedTableOrView(identifier, cmd, allowTempView) =>
        lookupTableOrView(identifier).map {
          case _: ResolvedTempView if !allowTempView =>
            throw QueryCompilationErrors.expectPermanentViewNotTempViewError(
              identifier, cmd, u)
          case other => other
        }.getOrElse(u)
    }

    private def lookupTempView(identifier: Seq[String]): Option[TemporaryViewRelation] = {
      // We are resolving a view and this name is not a temp view when that view was created. We
      // return None earlier here.
      if (isResolvingView && !isReferredTempViewName(identifier)) return None
      v1SessionCatalog.getRawLocalOrGlobalTempView(identifier)
    }

    private def resolveTempView(
        identifier: Seq[String],
        isStreaming: Boolean = false,
        isTimeTravel: Boolean = false): Option[LogicalPlan] = {
      lookupTempView(identifier).map { v =>
        val tempViewPlan = v1SessionCatalog.getTempViewRelation(v)
        if (isStreaming && !tempViewPlan.isStreaming) {
          throw QueryCompilationErrors.readNonStreamingTempViewError(identifier.quoted)
        }
        if (isTimeTravel) {
          throw QueryCompilationErrors.timeTravelUnsupportedError(toSQLId(identifier))
        }
        tempViewPlan
      }
    }

    /**
     * Resolves relations to `ResolvedTable` or `Resolved[Temp/Persistent]View`. This is
     * for resolving DDL and misc commands.
     */
    private def lookupTableOrView(
        identifier: Seq[String],
        viewOnly: Boolean = false): Option[LogicalPlan] = {
      lookupTempView(identifier).map { tempView =>
        ResolvedTempView(identifier.asIdentifier, tempView.tableMeta)
      }.orElse {
        expandIdentifier(identifier) match {
          case CatalogAndIdentifier(catalog, ident) =>
            if (viewOnly && !CatalogV2Util.isSessionCatalog(catalog)) {
              throw QueryCompilationErrors.catalogOperationNotSupported(catalog, "views")
            }
            CatalogV2Util.loadTable(catalog, ident).map {
              case v1Table: V1Table if CatalogV2Util.isSessionCatalog(catalog) &&
                v1Table.v1Table.tableType == CatalogTableType.VIEW =>
                val v1Ident = v1Table.catalogTable.identifier
                val v2Ident = Identifier.of(v1Ident.database.toArray, v1Ident.identifier)
                ResolvedPersistentView(
                  catalog, v2Ident, v1Table.catalogTable)
              case table =>
                ResolvedTable.create(catalog.asTableCatalog, ident, table)
            }
          case _ => None
        }
      }
    }

    private def createRelation(
        catalog: CatalogPlugin,
        ident: Identifier,
        table: Option[Table],
        options: CaseInsensitiveStringMap,
        isStreaming: Boolean): Option[LogicalPlan] = {
      table.map {
        // To utilize this code path to execute V1 commands, e.g. INSERT,
        // either it must be session catalog, or tracksPartitionsInCatalog
        // must be false so it does not require use catalog to manage partitions.
        // Obviously we cannot execute V1Table by V1 code path if the table
        // is not from session catalog and the table still requires its catalog
        // to manage partitions.
        case v1Table: V1Table if CatalogV2Util.isSessionCatalog(catalog)
          || !v1Table.catalogTable.tracksPartitionsInCatalog =>
          if (isStreaming) {
            if (v1Table.v1Table.tableType == CatalogTableType.VIEW) {
              throw QueryCompilationErrors.permanentViewNotSupportedByStreamingReadingAPIError(
                ident.quoted)
            }
            SubqueryAlias(
              catalog.name +: ident.asMultipartIdentifier,
              UnresolvedCatalogRelation(v1Table.v1Table, options, isStreaming = true))
          } else {
            v1SessionCatalog.getRelation(v1Table.v1Table, options)
          }

        case table =>
          if (isStreaming) {
            val v1Fallback = table match {
              case withFallback: V2TableWithV1Fallback =>
                Some(UnresolvedCatalogRelation(withFallback.v1Table, isStreaming = true))
              case _ => None
            }
            SubqueryAlias(
              catalog.name +: ident.asMultipartIdentifier,
              StreamingRelationV2(None, table.name, table, options, table.columns.toAttributes,
                Some(catalog), Some(ident), v1Fallback))
          } else {
            SubqueryAlias(
              catalog.name +: ident.asMultipartIdentifier,
              DataSourceV2Relation.create(table, Some(catalog), Some(ident), options))
          }
      }
    }

    /**
     * Resolves relations to v1 relation if it's a v1 table from the session catalog, or to v2
     * relation. This is for resolving DML commands and SELECT queries.
     */
    private def resolveRelation(
        u: UnresolvedRelation,
        timeTravelSpec: Option[TimeTravelSpec] = None): Option[LogicalPlan] = {
      val timeTravelSpecFromOptions = TimeTravelSpec.fromOptions(
        u.options,
        conf.getConf(SQLConf.TIME_TRAVEL_TIMESTAMP_KEY),
        conf.getConf(SQLConf.TIME_TRAVEL_VERSION_KEY),
        conf.sessionLocalTimeZone
      )
      if (timeTravelSpec.nonEmpty && timeTravelSpecFromOptions.nonEmpty) {
        throw new AnalysisException("MULTIPLE_TIME_TRAVEL_SPEC", Map.empty[String, String])
      }
      val finalTimeTravelSpec = timeTravelSpec.orElse(timeTravelSpecFromOptions)
      resolveTempView(u.multipartIdentifier, u.isStreaming, finalTimeTravelSpec.isDefined).orElse {
        expandIdentifier(u.multipartIdentifier) match {
          case CatalogAndIdentifier(catalog, ident) =>
            val key =
              ((catalog.name +: ident.namespace :+ ident.name).toImmutableArraySeq,
              finalTimeTravelSpec)
            AnalysisContext.get.relationCache.get(key).map { cache =>
              val cachedRelation = cache.transform {
                case multi: MultiInstanceRelation =>
                  val newRelation = multi.newInstance()
                  newRelation.copyTagsFrom(multi)
                  newRelation
              }
              u.getTagValue(LogicalPlan.PLAN_ID_TAG).map { planId =>
                val cachedConnectRelation = cachedRelation.clone()
                cachedConnectRelation.setTagValue(LogicalPlan.PLAN_ID_TAG, planId)
                cachedConnectRelation
              }.getOrElse(cachedRelation)
            }.orElse {
              val writePrivilegesString =
                Option(u.options.get(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES))
              val table = CatalogV2Util.loadTable(
                catalog, ident, finalTimeTravelSpec, writePrivilegesString)
              val loaded = createRelation(
                catalog, ident, table, u.clearWritePrivileges.options, u.isStreaming)
              loaded.foreach(AnalysisContext.get.relationCache.update(key, _))
              u.getTagValue(LogicalPlan.PLAN_ID_TAG).map { planId =>
                loaded.map { loadedRelation =>
                  val loadedConnectRelation = loadedRelation.clone()
                  loadedConnectRelation.setTagValue(LogicalPlan.PLAN_ID_TAG, planId)
                  loadedConnectRelation
                }
              }.getOrElse(loaded)
            }
          case _ => None
        }
      }
    }

    /** Consumes an unresolved relation and resolves it to a v1 or v2 relation or temporary view. */
    def resolveRelationOrTempView(u: UnresolvedRelation): LogicalPlan = {
      EliminateSubqueryAliases(resolveRelation(u).getOrElse(u))
    }
  }

  /** Handle INSERT INTO for DSv2 */
  object ResolveInsertInto extends ResolveInsertionBase {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      AlwaysProcess.fn, ruleId) {
      case i @ InsertIntoStatement(r: DataSourceV2Relation, _, _, _, _, _, _)
          if i.query.resolved =>
        // ifPartitionNotExists is append with validation, but validation is not supported
        if (i.ifPartitionNotExists) {
          throw QueryCompilationErrors.unsupportedIfNotExistsError(r.table.name)
        }

        // Create a project if this is an INSERT INTO BY NAME query.
        val projectByName = if (i.userSpecifiedCols.nonEmpty) {
          Some(createProjectForByNameQuery(r.table.name, i))
        } else {
          None
        }
        val isByName = projectByName.nonEmpty || i.byName

        val partCols = partitionColumnNames(r.table)
        validatePartitionSpec(partCols, i.partitionSpec)

        val staticPartitions = i.partitionSpec.filter(_._2.isDefined).transform((_, v) => v.get)
        val query = addStaticPartitionColumns(r, projectByName.getOrElse(i.query), staticPartitions,
          isByName)

        if (!i.overwrite) {
          if (isByName) {
            AppendData.byName(r, query)
          } else {
            AppendData.byPosition(r, query)
          }
        } else if (conf.partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC) {
          if (isByName) {
            OverwritePartitionsDynamic.byName(r, query)
          } else {
            OverwritePartitionsDynamic.byPosition(r, query)
          }
        } else {
          if (isByName) {
            OverwriteByExpression.byName(r, query, staticDeleteExpression(r, staticPartitions))
          } else {
            OverwriteByExpression.byPosition(r, query, staticDeleteExpression(r, staticPartitions))
          }
        }
    }

    private def partitionColumnNames(table: Table): Seq[String] = {
      // get partition column names. in v2, partition columns are columns that are stored using an
      // identity partition transform because the partition values and the column values are
      // identical. otherwise, partition values are produced by transforming one or more source
      // columns and cannot be set directly in a query's PARTITION clause.
      table.partitioning.flatMap {
        case IdentityTransform(FieldReference(Seq(name))) => Some(name)
        case _ => None
      }.toImmutableArraySeq
    }

    private def validatePartitionSpec(
        partitionColumnNames: Seq[String],
        partitionSpec: Map[String, Option[String]]): Unit = {
      // check that each partition name is a partition column. otherwise, it is not valid
      partitionSpec.keySet.foreach { partitionName =>
        partitionColumnNames.find(name => conf.resolver(name, partitionName)) match {
          case Some(_) =>
          case None =>
            throw QueryCompilationErrors.nonPartitionColError(partitionName)
        }
      }
    }

    private def addStaticPartitionColumns(
        relation: DataSourceV2Relation,
        query: LogicalPlan,
        staticPartitions: Map[String, String],
        isByName: Boolean): LogicalPlan = {

      if (staticPartitions.isEmpty) {
        query

      } else {
        // add any static value as a literal column
        val withStaticPartitionValues = {
          // for each static name, find the column name it will replace and check for unknowns.
          val outputNameToStaticName = staticPartitions.keySet.map { staticName =>
            if (isByName) {
              // If this is INSERT INTO BY NAME, the query output's names will be the user specified
              // column names. We need to make sure the static partition column name doesn't appear
              // there to catch the following ambiguous query:
              // INSERT OVERWRITE t PARTITION (c='1') (c) VALUES ('2')
              if (query.output.exists(col => conf.resolver(col.name, staticName))) {
                throw QueryCompilationErrors.staticPartitionInUserSpecifiedColumnsError(staticName)
              }
            }
            relation.output.find(col => conf.resolver(col.name, staticName)) match {
              case Some(attr) =>
                attr.name -> staticName
              case _ =>
                throw QueryCompilationErrors.missingStaticPartitionColumn(staticName)
            }
          }.toMap

          val queryColumns = query.output.iterator

          // for each output column, add the static value as a literal, or use the next input
          // column. this does not fail if input columns are exhausted and adds remaining columns
          // at the end. both cases will be caught by ResolveOutputRelation and will fail the
          // query with a helpful error message.
          relation.output.flatMap { col =>
            outputNameToStaticName.get(col.name).flatMap(staticPartitions.get) match {
              case Some(staticValue) =>
                // SPARK-30844: try our best to follow StoreAssignmentPolicy for static partition
                // values but not completely follow because we can't do static type checking due to
                // the reason that the parser has erased the type info of static partition values
                // and converted them to string.
                val cast = Cast(Literal(staticValue), col.dataType, ansiEnabled = true)
                cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
                Some(Alias(cast, col.name)())
              case _ if queryColumns.hasNext =>
                Some(queryColumns.next())
              case _ =>
                None
            }
          } ++ queryColumns
        }

        Project(withStaticPartitionValues, query)
      }
    }

    private def staticDeleteExpression(
        relation: DataSourceV2Relation,
        staticPartitions: Map[String, String]): Expression = {
      if (staticPartitions.isEmpty) {
        Literal(true)
      } else {
        staticPartitions.map { case (name, value) =>
          relation.output.find(col => conf.resolver(col.name, name)) match {
            case Some(attr) =>
              // the delete expression must reference the table's column names, but these attributes
              // are not available when CheckAnalysis runs because the relation is not a child of
              // the logical operation. instead, expressions are resolved after
              // ResolveOutputRelation runs, using the query's column names that will match the
              // table names at that point. because resolution happens after a future rule, create
              // an UnresolvedAttribute.
              EqualNullSafe(
                UnresolvedAttribute.quoted(attr.name),
                Cast(Literal(value), attr.dataType))
            case None =>
              throw QueryCompilationErrors.missingStaticPartitionColumn(name)
          }
        }.reduce(And)
      }
    }
  }

  /**
   * Resolves column references in the query plan. Basically it transform the query plan tree bottom
   * up, and only try to resolve references for a plan node if all its children nodes are resolved,
   * and there is no conflicting attributes between the children nodes (see `hasConflictingAttrs`
   * for details).
   *
   * The general workflow to resolve references:
   * 1. Expands the star in Project/Aggregate/Generate.
   * 2. Resolves the columns to [[AttributeReference]] with the output of the children plans. This
   *    includes metadata columns as well.
   * 3. Resolves the columns to literal function which is allowed to be invoked without braces,
   *    e.g. `SELECT col, current_date FROM t`.
   * 4. Resolves the columns to outer references with the outer plan if we are resolving subquery
   *    expressions.
   * 5. Resolves the columns to SQL variables.
   *
   * Some plan nodes have special column reference resolution logic, please read these sub-rules for
   * details:
   *  - [[ResolveReferencesInAggregate]]
   *  - [[ResolveReferencesInUpdate]]
   *  - [[ResolveReferencesInSort]]
   *
   * Note: even if we use a single rule to resolve columns, it's still non-trivial to have a
   *       reliable column resolution order, as the rule will be executed multiple times, with other
   *       rules in the same batch. We should resolve columns with the next option only if all the
   *       previous options are permanently not applicable. If the current option can be applicable
   *       in the next iteration (other rules update the plan), we should not try the next option.
   */
  class ResolveReferences(val catalogManager: CatalogManager)
    extends Rule[LogicalPlan] with ColumnResolutionHelper {

    private val resolveColumnDefaultInCommandInputQuery =
      new ResolveColumnDefaultInCommandInputQuery(catalogManager)
    private val resolveReferencesInAggregate =
      new ResolveReferencesInAggregate(catalogManager)
    private val resolveReferencesInUpdate =
      new ResolveReferencesInUpdate(catalogManager)
    private val resolveReferencesInSort =
      new ResolveReferencesInSort(catalogManager)

    /**
     * Return true if there're conflicting attributes among children's outputs of a plan
     *
     * The children logical plans may output columns with conflicting attribute IDs. This may happen
     * in cases such as self-join. We should wait for the rule [[DeduplicateRelations]] to eliminate
     * conflicting attribute IDs, otherwise we can't resolve columns correctly due to ambiguity.
     */
    def hasConflictingAttrs(p: LogicalPlan): Boolean = {
      p.children.length > 1 && {
        // Note that duplicated attributes are allowed within a single node,
        // e.g., df.select($"a", $"a"), so we should only check conflicting
        // attributes between nodes.
        val uniqueAttrs = mutable.HashSet[ExprId]()
        p.children.head.outputSet.foreach(a => uniqueAttrs.add(a.exprId))
        p.children.tail.exists { child =>
          val uniqueSize = uniqueAttrs.size
          val childSize = child.outputSet.size
          child.outputSet.foreach(a => uniqueAttrs.add(a.exprId))
          uniqueSize + childSize > uniqueAttrs.size
        }
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      // Don't wait other rules to resolve the child plans of `InsertIntoStatement` as we need
      // to resolve column "DEFAULT" in the child plans so that they must be unresolved.
      case i: InsertIntoStatement => resolveColumnDefaultInCommandInputQuery(i)

      // Don't wait other rules to resolve the child plans of `SetVariable` as we need
      // to resolve column "DEFAULT" in the child plans so that they must be unresolved.
      case s: SetVariable => resolveColumnDefaultInCommandInputQuery(s)

      // Wait for other rules to resolve child plans first
      case p: LogicalPlan if !p.childrenResolved => p

      // Wait for the rule `DeduplicateRelations` to resolve conflicting attrs first.
      case p: LogicalPlan if hasConflictingAttrs(p) => p

      // If the projection list contains Stars, expand it.
      case p: Project if containsStar(p.projectList) =>
        p.copy(projectList = buildExpandedProjectList(p.projectList, p.child))
      // If the filter list contains Stars, expand it.
      case p: Filter if containsStar(Seq(p.condition)) =>
        p.copy(expandStarExpression(p.condition, p.child))
      // If the aggregate function argument contains Stars, expand it.
      case a: Aggregate if containsStar(a.aggregateExpressions) =>
        if (a.groupingExpressions.exists(_.isInstanceOf[UnresolvedOrdinal])) {
          throw QueryCompilationErrors.starNotAllowedWhenGroupByOrdinalPositionUsedError()
        } else {
          a.copy(aggregateExpressions = buildExpandedProjectList(a.aggregateExpressions, a.child))
        }
      case c: CollectMetrics if containsStar(c.metrics) =>
        c.copy(metrics = buildExpandedProjectList(c.metrics, c.child))
      case g: Generate if containsStar(g.generator.children) =>
        throw QueryCompilationErrors.invalidStarUsageError("explode/json_tuple/UDTF",
          extractStar(g.generator.children))
      // If the Unpivot ids or values contain Stars, expand them.
      case up: Unpivot if up.ids.exists(containsStar) ||
        // Only expand Stars in one-dimensional values
        up.values.exists(values => values.exists(_.length == 1) && values.exists(containsStar)) =>
        up.copy(
          ids = up.ids.map(buildExpandedProjectList(_, up.child)),
          // The inner exprs in Option[[exprs] is one-dimensional, e.g. Optional[[["*"]]].
          // The single NamedExpression turns into multiple, which we here have to turn into
          // Optional[[["col1"], ["col2"]]]
          values = up.values.map(_.flatMap(buildExpandedProjectList(_, up.child)).map(Seq(_)))
        )

      case u @ Union(children, _, _)
        // if there are duplicate output columns, give them unique expr ids
          if children.exists(c => c.output.map(_.exprId).distinct.length < c.output.length) =>
        val newChildren = children.map { c =>
          if (c.output.map(_.exprId).distinct.length < c.output.length) {
            val existingExprIds = mutable.HashSet[ExprId]()
            val projectList = c.output.map { attr =>
              if (existingExprIds.contains(attr.exprId)) {
                // replace non-first duplicates with aliases and tag them
                val newMetadata = new MetadataBuilder().withMetadata(attr.metadata)
                  .putNull("__is_duplicate").build()
                Alias(attr, attr.name)(explicitMetadata = Some(newMetadata))
              } else {
                // leave first duplicate alone
                existingExprIds.add(attr.exprId)
                attr
              }
            }
            Project(projectList, c)
          } else {
            c
          }
        }
        u.withNewChildren(newChildren)

      // A special case for Generate, because the output of Generate should not be resolved by
      // ResolveReferences. Attributes in the output will be resolved by ResolveGenerate.
      case g @ Generate(generator, _, _, _, _, _) if generator.resolved => g

      case g @ Generate(generator, join, outer, qualifier, output, child) =>
        val newG = resolveExpressionByPlanOutput(
          generator, child, throws = true, includeLastResort = true)
        if (newG.fastEquals(generator)) {
          g
        } else {
          Generate(newG.asInstanceOf[Generator], join, outer, qualifier, output, child)
        }

      case mg: MapGroups if mg.dataOrder.exists(!_.resolved) =>
        // Resolve against `AppendColumns`'s children, instead of `AppendColumns`,
        // because `AppendColumns`'s serializer might produce conflict attribute
        // names leading to ambiguous references exception.
        val planForResolve = mg.child match {
          case appendColumns: AppendColumns => appendColumns.child
          case plan => plan
        }
        val resolvedOrder = mg.dataOrder
          .map(resolveExpressionByPlanOutput(_, planForResolve).asInstanceOf[SortOrder])
        mg.copy(dataOrder = resolvedOrder)

      // Left and right sort expression have to be resolved against the respective child plan only
      case cg: CoGroup if cg.leftOrder.exists(!_.resolved) || cg.rightOrder.exists(!_.resolved) =>
        // Resolve against `AppendColumns`'s children, instead of `AppendColumns`,
        // because `AppendColumns`'s serializer might produce conflict attribute
        // names leading to ambiguous references exception.
        val (leftPlanForResolve, rightPlanForResolve) = Seq(cg.left, cg.right).map {
          case appendColumns: AppendColumns => appendColumns.child
          case plan => plan
        } match {
          case Seq(left, right) => (left, right)
        }

        val resolvedLeftOrder = cg.leftOrder
          .map(resolveExpressionByPlanOutput(_, leftPlanForResolve).asInstanceOf[SortOrder])
        val resolvedRightOrder = cg.rightOrder
          .map(resolveExpressionByPlanOutput(_, rightPlanForResolve).asInstanceOf[SortOrder])

        cg.copy(leftOrder = resolvedLeftOrder, rightOrder = resolvedRightOrder)

      // Skips plan which contains deserializer expressions, as they should be resolved by another
      // rule: ResolveDeserializer.
      case plan if containsDeserializer(plan.expressions) => plan

      case a: Aggregate => resolveReferencesInAggregate(a)

      // Special case for Project as it supports lateral column alias.
      case p: Project =>
        val resolvedBasic = p.projectList.map(resolveExpressionByPlanChildren(_, p))
        // Lateral column alias has higher priority than outer reference.
        val resolvedWithLCA = resolveLateralColumnAlias(resolvedBasic)
        val resolvedFinal = resolvedWithLCA.map(resolveColsLastResort)
        p.copy(projectList = resolvedFinal.map(_.asInstanceOf[NamedExpression]))

      case o: OverwriteByExpression if o.table.resolved =>
        // The delete condition of `OverwriteByExpression` will be passed to the table
        // implementation and should be resolved based on the table schema.
        o.copy(deleteExpr = resolveExpressionByPlanOutput(o.deleteExpr, o.table))

      case u: UpdateTable => resolveReferencesInUpdate(u)

      case m @ MergeIntoTable(targetTable, sourceTable, _, _, _, _, _)
        if !m.resolved && targetTable.resolved && sourceTable.resolved =>

        EliminateSubqueryAliases(targetTable) match {
          case r: NamedRelation if r.skipSchemaResolution =>
            // Do not resolve the expression if the target table accepts any schema.
            // This allows data sources to customize their own resolution logic using
            // custom resolution rules.
            m

          case _ =>
            val newMatchedActions = m.matchedActions.map {
              case DeleteAction(deleteCondition) =>
                val resolvedDeleteCondition = deleteCondition.map(
                  resolveExpressionByPlanChildren(_, m))
                DeleteAction(resolvedDeleteCondition)
              case UpdateAction(updateCondition, assignments) =>
                val resolvedUpdateCondition = updateCondition.map(
                  resolveExpressionByPlanChildren(_, m))
                UpdateAction(
                  resolvedUpdateCondition,
                  // The update value can access columns from both target and source tables.
                  resolveAssignments(assignments, m, MergeResolvePolicy.BOTH))
              case UpdateStarAction(updateCondition) =>
                val assignments = targetTable.output.map { attr =>
                  Assignment(attr, UnresolvedAttribute(Seq(attr.name)))
                }
                UpdateAction(
                  updateCondition.map(resolveExpressionByPlanChildren(_, m)),
                  // For UPDATE *, the value must be from source table.
                  resolveAssignments(assignments, m, MergeResolvePolicy.SOURCE))
              case o => o
            }
            val newNotMatchedActions = m.notMatchedActions.map {
              case InsertAction(insertCondition, assignments) =>
                // The insert action is used when not matched, so its condition and value can only
                // access columns from the source table.
                val resolvedInsertCondition = insertCondition.map(
                  resolveExpressionByPlanOutput(_, m.sourceTable))
                InsertAction(
                  resolvedInsertCondition,
                  resolveAssignments(assignments, m, MergeResolvePolicy.SOURCE))
              case InsertStarAction(insertCondition) =>
                // The insert action is used when not matched, so its condition and value can only
                // access columns from the source table.
                val resolvedInsertCondition = insertCondition.map(
                  resolveExpressionByPlanOutput(_, m.sourceTable))
                val assignments = targetTable.output.map { attr =>
                  Assignment(attr, UnresolvedAttribute(Seq(attr.name)))
                }
                InsertAction(
                  resolvedInsertCondition,
                  resolveAssignments(assignments, m, MergeResolvePolicy.SOURCE))
              case o => o
            }
            val newNotMatchedBySourceActions = m.notMatchedBySourceActions.map {
              case DeleteAction(deleteCondition) =>
                val resolvedDeleteCondition = deleteCondition.map(
                  resolveExpressionByPlanOutput(_, targetTable))
                DeleteAction(resolvedDeleteCondition)
              case UpdateAction(updateCondition, assignments) =>
                val resolvedUpdateCondition = updateCondition.map(
                  resolveExpressionByPlanOutput(_, targetTable))
                UpdateAction(
                  resolvedUpdateCondition,
                  // The update value can access columns from the target table only.
                  resolveAssignments(assignments, m, MergeResolvePolicy.TARGET))
              case o => o
            }

            val resolvedMergeCondition = resolveExpressionByPlanChildren(m.mergeCondition, m)
            m.copy(mergeCondition = resolvedMergeCondition,
              matchedActions = newMatchedActions,
              notMatchedActions = newNotMatchedActions,
              notMatchedBySourceActions = newNotMatchedBySourceActions)
        }

      // UnresolvedHaving can host grouping expressions and aggregate functions. We should resolve
      // columns with `agg.output` and the rule `ResolveAggregateFunctions` will push them down to
      // Aggregate later.
      case u @ UnresolvedHaving(cond, agg: Aggregate) if !cond.resolved =>
        u.mapExpressions { e =>
          // Columns in HAVING should be resolved with `agg.child.output` first, to follow the SQL
          // standard. See more details in SPARK-31519.
          val resolvedWithAgg = resolveColWithAgg(e, agg)
          resolveExpressionByPlanChildren(resolvedWithAgg, u, includeLastResort = true)
        }

      // RepartitionByExpression can host missing attributes that are from a descendant node.
      // For example, `spark.table("t").select($"a").repartition($"b")`. We can resolve `b` with
      // table `t` even if there is a Project node between the table scan node and Sort node.
      // We also need to propagate the missing attributes from the descendant node to the current
      // node, and project them way at the end via an extra Project.
      case r @ RepartitionByExpression(partitionExprs, child, _, _)
        if !r.resolved || r.missingInput.nonEmpty =>
        val resolvedBasic = partitionExprs.map(resolveExpressionByPlanChildren(_, r))
        val (newPartitionExprs, newChild) = resolveExprsAndAddMissingAttrs(resolvedBasic, child)
        // Missing columns should be resolved right after basic column resolution.
        // See the doc of `ResolveReferences`.
        val resolvedFinal = newPartitionExprs.map(resolveColsLastResort)
        if (child.output == newChild.output) {
          r.copy(resolvedFinal, newChild)
        } else {
          Project(child.output, r.copy(resolvedFinal, newChild))
        }

      // Filter can host both grouping expressions/aggregate functions and missing attributes.
      // The grouping expressions/aggregate functions resolution takes precedence over missing
      // attributes. See the classdoc of `ResolveReferences` for details.
      case f @ Filter(cond, child) if !cond.resolved || f.missingInput.nonEmpty =>
        val resolvedBasic = resolveExpressionByPlanChildren(cond, f)
        val resolvedWithAgg = resolveColWithAgg(resolvedBasic, child)
        val (newCond, newChild) = resolveExprsAndAddMissingAttrs(Seq(resolvedWithAgg), child)
        // Missing columns should be resolved right after basic column resolution.
        // See the doc of `ResolveReferences`.
        val resolvedFinal = resolveColsLastResort(newCond.head)
        if (child.output == newChild.output) {
          f.copy(condition = resolvedFinal)
        } else {
          // Add missing attributes and then project them away.
          val newFilter = Filter(resolvedFinal, newChild)
          Project(child.output, newFilter)
        }

      case s: Sort if !s.resolved || s.missingInput.nonEmpty =>
        resolveReferencesInSort(s)

      case u: UnresolvedWithCTERelations =>
        UnresolvedWithCTERelations(this.apply(u.unresolvedPlan), u.cteRelations)

      case q: LogicalPlan =>
        logTrace(s"Attempting to resolve ${q.simpleString(conf.maxToStringFields)}")
        q.mapExpressions(resolveExpressionByPlanChildren(_, q, includeLastResort = true))
    }

    private object MergeResolvePolicy extends Enumeration {
      val BOTH, SOURCE, TARGET = Value
    }

    def resolveAssignments(
        assignments: Seq[Assignment],
        mergeInto: MergeIntoTable,
        resolvePolicy: MergeResolvePolicy.Value): Seq[Assignment] = {
      assignments.map { assign =>
        val resolvedKey = assign.key match {
          case c if !c.resolved =>
            resolveMergeExprOrFail(c, Project(Nil, mergeInto.targetTable))
          case o => o
        }
        val resolvedValue = assign.value match {
          case c if !c.resolved =>
            val resolvePlan = resolvePolicy match {
              case MergeResolvePolicy.BOTH => mergeInto
              case MergeResolvePolicy.SOURCE => Project(Nil, mergeInto.sourceTable)
              case MergeResolvePolicy.TARGET => Project(Nil, mergeInto.targetTable)
            }
            val resolvedExpr = resolveExprInAssignment(c, resolvePlan)
            val withDefaultResolved = if (conf.enableDefaultColumns) {
              resolveColumnDefaultInAssignmentValue(
                resolvedKey,
                resolvedExpr,
                QueryCompilationErrors
                  .defaultReferencesNotAllowedInComplexExpressionsInMergeInsertsOrUpdates())
            } else {
              resolvedExpr
            }
            checkResolvedMergeExpr(withDefaultResolved, resolvePlan)
            withDefaultResolved
          case o => o
        }
        Assignment(resolvedKey, resolvedValue)
      }
    }

    private def resolveMergeExprOrFail(e: Expression, p: LogicalPlan): Expression = {
      val resolved = resolveExprInAssignment(e, p)
      checkResolvedMergeExpr(resolved, p)
      resolved
    }

    private def checkResolvedMergeExpr(e: Expression, p: LogicalPlan): Unit = {
      e.references.filter(!_.resolved).foreach { a =>
        // Note: This will throw error only on unresolved attribute issues,
        // not other resolution errors like mismatched data types.
        val cols = p.inputSet.toSeq.map(attr => toSQLId(attr.name)).mkString(", ")
        a.failAnalysis(
          errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          messageParameters = Map(
            "objectName" -> toSQLId(a.name),
            "proposal" -> cols))
      }
    }

    // Expand the star expression using the input plan first. If failed, try resolve
    // the star expression using the outer query plan and wrap the resolved attributes
    // in outer references. Otherwise throw the original exception.
    private def expand(s: Star, plan: LogicalPlan): Seq[NamedExpression] = {
      withPosition(s) {
        try {
          s.expand(plan, resolver)
        } catch {
          case e: AnalysisException =>
            AnalysisContext.get.outerPlan.map {
              // Only Project, Aggregate, CollectMetrics can host star expressions.
              case u @ (_: Project | _: Aggregate | _: CollectMetrics) =>
                Try(s.expand(u.children.head, resolver)) match {
                  case Success(expanded) => expanded.map(wrapOuterReference)
                  case Failure(_) => throw e
                }
              // Do not use the outer plan to resolve the star expression
              // since the star usage is invalid.
              case _ => throw e
            }.getOrElse { throw e }
        }
      }
    }

    /**
     * Build a project list for Project/Aggregate and expand the star if possible
     */
    private def buildExpandedProjectList(
      exprs: Seq[NamedExpression],
      child: LogicalPlan): Seq[NamedExpression] = {
      exprs.flatMap {
        // Using Dataframe/Dataset API: testData2.groupBy($"a", $"b").agg($"*")
        case s: Star => expand(s, child)
        // Using SQL API without running ResolveAlias: SELECT * FROM testData2 group by a, b
        case UnresolvedAlias(s: Star, _) => expand(s, child)
        case o if containsStar(o :: Nil) => expandStarExpression(o, child) :: Nil
        case o => o :: Nil
      }.map(_.asInstanceOf[NamedExpression])
    }

    /**
     * Returns true if `exprs` contains a [[Star]].
     */
    def containsStar(exprs: Seq[Expression]): Boolean =
      exprs.exists(_.collect { case _: Star => true }.nonEmpty)

    private def extractStar(exprs: Seq[Expression]): Seq[Star] =
      exprs.flatMap(_.collect { case s: Star => s })

    private def isCountStarExpansionAllowed(arguments: Seq[Expression]): Boolean = arguments match {
      case Seq(UnresolvedStar(None)) => true
      case Seq(_: ResolvedStar) => true
      case _ => false
    }

    /**
     * Expands the matching attribute.*'s in `child`'s output.
     */
    def expandStarExpression(expr: Expression, child: LogicalPlan): Expression = {
      expr.transformUp {
        case f0: UnresolvedFunction if !f0.isDistinct &&
          f0.nameParts.map(_.toLowerCase(Locale.ROOT)) == Seq("count") &&
          isCountStarExpansionAllowed(f0.arguments) =>
          // Transform COUNT(*) into COUNT(1).
          f0.copy(nameParts = Seq("count"), arguments = Seq(Literal(1)))
        case f1: UnresolvedFunction if containsStar(f1.arguments) =>
          // SPECIAL CASE: We want to block count(tblName.*) because in spark, count(tblName.*) will
          // be expanded while count(*) will be converted to count(1). They will produce different
          // results and confuse users if there are any null values. For count(t1.*, t2.*), it is
          // still allowed, since it's well-defined in spark.
          if (!conf.allowStarWithSingleTableIdentifierInCount &&
              f1.nameParts == Seq("count") &&
              f1.arguments.length == 1) {
            f1.arguments.foreach {
              case u: UnresolvedStar if u.isQualifiedByTable(child, resolver) =>
                throw QueryCompilationErrors
                  .singleTableStarInCountNotAllowedError(u.target.get.mkString("."))
              case _ => // do nothing
            }
          }
          f1.copy(arguments = f1.arguments.flatMap {
            case s: Star => expand(s, child)
            case o => o :: Nil
          })
        case c: CreateNamedStruct if containsStar(c.valExprs) =>
          val newChildren = c.children.grouped(2).flatMap {
            case Seq(k, s : Star) => CreateStruct(expand(s, child)).children
            case kv => kv
          }
          c.copy(children = newChildren.toList )
        case c: CreateArray if containsStar(c.children) =>
          c.copy(children = c.children.flatMap {
            case s: Star => expand(s, child)
            case o => o :: Nil
          })
        case p: Murmur3Hash if containsStar(p.children) =>
          p.copy(children = p.children.flatMap {
            case s: Star => expand(s, child)
            case o => o :: Nil
          })
        case p: XxHash64 if containsStar(p.children) =>
          p.copy(children = p.children.flatMap {
            case s: Star => expand(s, child)
            case o => o :: Nil
          })
        case p: In if containsStar(p.children) =>
          p.copy(list = p.list.flatMap {
            case s: Star => expand(s, child)
            case o => o :: Nil
          })
        // count(*) has been replaced by count(1)
        case o if containsStar(o.children) =>
          throw QueryCompilationErrors.invalidStarUsageError(s"expression `${o.prettyName}`",
            extractStar(o.children))
      }
    }
  }

  private def containsDeserializer(exprs: Seq[Expression]): Boolean = {
    exprs.exists(_.exists(_.isInstanceOf[UnresolvedDeserializer]))
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
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(UNRESOLVED_ORDINAL), ruleId) {
      case p if !p.childrenResolved => p
      // Replace the index with the related attribute for ORDER BY,
      // which is a 1-base position of the projection list.
      case Sort(orders, global, child)
        if orders.exists(_.child.isInstanceOf[UnresolvedOrdinal]) =>
        val newOrders = orders map {
          case s @ SortOrder(UnresolvedOrdinal(index), direction, nullOrdering, _) =>
            if (index > 0 && index <= child.output.size) {
              SortOrder(child.output(index - 1), direction, nullOrdering, Seq.empty)
            } else {
              throw QueryCompilationErrors.orderByPositionRangeError(index, child.output.size, s)
            }
          case o => o
        }
        Sort(newOrders, global, child)

      // Replace the index with the corresponding expression in aggregateExpressions. The index is
      // a 1-base position of aggregateExpressions, which is output columns (select expression)
      case Aggregate(groups, aggs, child) if aggs.forall(_.resolved) &&
        groups.exists(containUnresolvedOrdinal) =>
        val newGroups = groups.map(resolveGroupByExpressionOrdinal(_, aggs))
        Aggregate(newGroups, aggs, child)
    }

    private def containUnresolvedOrdinal(e: Expression): Boolean = e match {
      case _: UnresolvedOrdinal => true
      case gs: BaseGroupingSets => gs.children.exists(containUnresolvedOrdinal)
      case _ => false
    }

    private def resolveGroupByExpressionOrdinal(
        expr: Expression,
        aggs: Seq[Expression]): Expression = expr match {
      case ordinal @ UnresolvedOrdinal(index) =>
        withPosition(ordinal) {
          if (index > 0 && index <= aggs.size) {
            val ordinalExpr = aggs(index - 1)
            if (ordinalExpr.exists(_.isInstanceOf[AggregateExpression])) {
              throw QueryCompilationErrors.groupByPositionRefersToAggregateFunctionError(
                index, ordinalExpr)
            } else {
              trimAliases(ordinalExpr) match {
                // HACK ALERT: If the ordinal expression is also an integer literal, don't use it
                //             but still keep the ordinal literal. The reason is we may repeatedly
                //             analyze the plan. Using a different integer literal may lead to
                //             a repeat GROUP BY ordinal resolution which is wrong. GROUP BY
                //             constant is meaningless so whatever value does not matter here.
                // TODO: (SPARK-45932) GROUP BY ordinal should pull out grouping expressions to
                //       a Project, then the resolved ordinal expression is always
                //       `AttributeReference`.
                case Literal(_: Int, IntegerType) =>
                  Literal(index)
                case _ => ordinalExpr
              }
            }
          } else {
            throw QueryCompilationErrors.groupByPositionRangeError(index, aggs.size)
          }
        }
      case gs: BaseGroupingSets =>
        gs.withNewChildren(gs.children.map(resolveGroupByExpressionOrdinal(_, aggs)))
      case others => others
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
      val externalFunctionNameSet = new mutable.HashSet[Seq[String]]()

      plan.resolveExpressionsWithPruning(_.containsAnyPattern(UNRESOLVED_FUNCTION)) {
        case f @ UnresolvedFunction(nameParts, _, _, _, _, _, _) =>
          if (ResolveFunctions.lookupBuiltinOrTempFunction(nameParts, Some(f)).isDefined) {
            f
          } else {
            val CatalogAndIdentifier(catalog, ident) = expandIdentifier(nameParts)
            val fullName =
              normalizeFuncName((catalog.name +: ident.namespace :+ ident.name).toImmutableArraySeq)
            if (externalFunctionNameSet.contains(fullName)) {
              f
            } else if (catalog.asFunctionCatalog.functionExists(ident)) {
              externalFunctionNameSet.add(fullName)
              f
            } else {
              val catalogPath = (catalog.name() +: catalogManager.currentNamespace).mkString(".")
              throw QueryCompilationErrors.unresolvedRoutineError(
                nameParts,
                Seq("system.builtin", "system.session", catalogPath),
                f.origin)
            }
          }
      }
    }

    def normalizeFuncName(name: Seq[String]): Seq[String] = {
      if (conf.caseSensitiveAnalysis) {
        name
      } else {
        name.map(_.toLowerCase(Locale.ROOT))
      }
    }
  }

  /**
   * Replaces [[UnresolvedFunctionName]]s with concrete [[LogicalPlan]]s.
   * Replaces [[UnresolvedFunction]]s with concrete [[Expression]]s.
   * Replaces [[UnresolvedGenerator]]s with concrete [[Expression]]s.
   * Replaces [[UnresolvedTableValuedFunction]]s with concrete [[LogicalPlan]]s.
   */
  object ResolveFunctions extends Rule[LogicalPlan] {
    val trimWarningEnabled = new AtomicBoolean(true)

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsAnyPattern(UNRESOLVED_FUNC, UNRESOLVED_FUNCTION, GENERATOR,
        UNRESOLVED_TABLE_VALUED_FUNCTION, UNRESOLVED_TVF_ALIASES), ruleId) {
      // Resolve functions with concrete relations from v2 catalog.
      case u @ UnresolvedFunctionName(nameParts, cmd, requirePersistentFunc, mismatchHint, _) =>
        lookupBuiltinOrTempFunction(nameParts, None)
          .orElse(lookupBuiltinOrTempTableFunction(nameParts)).map { info =>
          if (requirePersistentFunc) {
            throw QueryCompilationErrors.expectPersistentFuncError(
              nameParts.head, cmd, mismatchHint, u)
          } else {
            ResolvedNonPersistentFunc(nameParts.head, V1Function(info))
          }
        }.getOrElse {
          val CatalogAndIdentifier(catalog, ident) = expandIdentifier(nameParts)
          val fullName = catalog.name +: ident.namespace :+ ident.name
          CatalogV2Util.loadFunction(catalog, ident).map { func =>
            ResolvedPersistentFunc(catalog.asFunctionCatalog, ident, func)
          }.getOrElse(u.copy(possibleQualifiedName = Some(fullName.toImmutableArraySeq)))
        }

      // Resolve table-valued function references.
      case u: UnresolvedTableValuedFunction if u.functionArgs.forall(_.resolved) =>
        withPosition(u) {
          try {
            val resolvedFunc = resolveBuiltinOrTempTableFunction(u.name, u.functionArgs).getOrElse {
              val CatalogAndIdentifier(catalog, ident) = expandIdentifier(u.name)
              if (CatalogV2Util.isSessionCatalog(catalog)) {
                v1SessionCatalog.resolvePersistentTableFunction(
                  ident.asFunctionIdentifier, u.functionArgs)
              } else {
                throw QueryCompilationErrors.missingCatalogAbilityError(
                  catalog, "table-valued functions")
              }
            }
            resolvedFunc.transformAllExpressionsWithPruning(
              _.containsPattern(FUNCTION_TABLE_RELATION_ARGUMENT_EXPRESSION))  {
              case t: FunctionTableSubqueryArgumentExpression =>
                resolvedFunc match {
                  case Generate(_: PythonUDTF, _, _, _, _, _) =>
                  case Generate(_: UnresolvedPolymorphicPythonUDTF, _, _, _, _, _) =>
                  case _ =>
                    assert(!t.hasRepartitioning,
                      "Cannot evaluate the table-valued function call because it included the " +
                        "PARTITION BY clause, but only Python table functions support this " +
                        "clause")
                }
                t
            }
          } catch {
            case _: NoSuchFunctionException =>
              u.failAnalysis(
                errorClass = "UNRESOLVABLE_TABLE_VALUED_FUNCTION",
                messageParameters = Map("name" -> toSQLId(u.name)))
          }
        }

      // Resolve table-valued functions' output column aliases.
      case u: UnresolvedTVFAliases if u.child.resolved =>
        // Add `Project` with the aliases.
        val outputAttrs = u.child.output
        // Checks if the number of the aliases is equal to expected one
        if (u.outputNames.size != outputAttrs.size) {
          u.failAnalysis(
            errorClass = "NUM_TABLE_VALUE_ALIASES_MISMATCH",
            messageParameters = Map(
              "funcName" -> toSQLId(u.name),
              "aliasesNum" -> u.outputNames.size.toString,
              "outColsNum" -> outputAttrs.size.toString))
        }
        val aliases = outputAttrs.zip(u.outputNames).map {
          case (attr, name) => Alias(attr, name)()
        }
        Project(aliases, u.child)

      case p: LogicalPlan
          if p.resolved && p.containsPattern(FUNCTION_TABLE_RELATION_ARGUMENT_EXPRESSION) =>
        withPosition(p) {
          val tableArgs =
            mutable.ArrayBuffer.empty[(FunctionTableSubqueryArgumentExpression, LogicalPlan)]

          val tvf = p.transformExpressionsWithPruning(
            _.containsPattern(FUNCTION_TABLE_RELATION_ARGUMENT_EXPRESSION)) {
            case t: FunctionTableSubqueryArgumentExpression =>
              val alias = SubqueryAlias.generateSubqueryName(s"_${tableArgs.size}")
              tableArgs.append((t, SubqueryAlias(alias, t.evaluable)))
              UnresolvedAttribute(Seq(alias, "c"))
          }

          assert(tableArgs.nonEmpty)
          if (!conf.tvfAllowMultipleTableArguments && tableArgs.size > 1) {
            throw QueryCompilationErrors.tableValuedFunctionTooManyTableArgumentsError(
              tableArgs.size)
          }
          val alias = SubqueryAlias.generateSubqueryName(s"_${tableArgs.size}")

          // Propagate the column indexes for TABLE arguments to the PythonUDTF instance.
          val f: FunctionTableSubqueryArgumentExpression = tableArgs.head._1
          val tvfWithTableColumnIndexes = tvf match {
            case g @ Generate(pyudtf: PythonUDTF, _, _, _, _, _)
                if f.extraProjectedPartitioningExpressions.nonEmpty =>
              val partitionColumnIndexes = if (f.selectedInputExpressions.isEmpty) {
                PythonUDTFPartitionColumnIndexes(f.partitioningExpressionIndexes)
              } else {
                // If the UDTF specified 'select' expression(s), we added a projection to compute
                // them plus the 'partitionBy' expression(s) afterwards.
                PythonUDTFPartitionColumnIndexes(
                  (0 until f.extraProjectedPartitioningExpressions.length)
                    .map(_ + f.selectedInputExpressions.length))
              }
              g.copy(generator = pyudtf.copy(
                pythonUDTFPartitionColumnIndexes = Some(partitionColumnIndexes)))
            case _ => tvf
          }

          Project(
            Seq(UnresolvedStar(Some(Seq(alias)))),
            LateralJoin(
              tableArgs.map(_._2).reduceLeft(Join(_, _, Inner, None, JoinHint.NONE)),
              LateralSubquery(SubqueryAlias(alias, tvfWithTableColumnIndexes)), Inner, None)
          )
        }

      case q: LogicalPlan =>
        q.transformExpressionsUpWithPruning(
          _.containsAnyPattern(UNRESOLVED_FUNCTION, GENERATOR),
          ruleId) {
          case u @ UnresolvedFunction(nameParts, arguments, _, _, _, _, _)
              if hasLambdaAndResolvedArguments(arguments) => withPosition(u) {
            resolveBuiltinOrTempFunction(nameParts, arguments, u).map {
              case func: HigherOrderFunction => func
              case other => other.failAnalysis(
                errorClass = "INVALID_LAMBDA_FUNCTION_CALL.NON_HIGHER_ORDER_FUNCTION",
                messageParameters = Map(
                  "class" -> other.getClass.getCanonicalName))
            }.getOrElse {
              throw QueryCompilationErrors.unresolvedRoutineError(
                nameParts,
                // We don't support persistent high-order functions yet.
                Seq("system.builtin", "system.session"),
                u.origin)
            }
          }

          case u if !u.childrenResolved => u // Skip until children are resolved.

          case u @ UnresolvedGenerator(name, arguments) => withPosition(u) {
            // For generator function, the parser only accepts v1 function name and creates
            // `FunctionIdentifier`.
            v1SessionCatalog.lookupFunction(name, arguments) match {
              case generator: Generator => generator
              case other => throw QueryCompilationErrors.generatorNotExpectedError(
                name, other.getClass.getCanonicalName)
            }
          }

          case u: UnresolvedFunction => resolveFunction(u)

          case u: UnresolvedPolymorphicPythonUDTF => withPosition(u) {
            // Check if this is a call to a Python user-defined table function whose polymorphic
            // 'analyze' method returned metadata indicated requested partitioning and/or
            // ordering properties of the input relation. In that event, make sure that the UDTF
            // call did not include any explicit PARTITION BY and/or ORDER BY clauses for the
            // corresponding TABLE argument, and then update the TABLE argument representation
            // to apply the requested partitioning and/or ordering.
            val analyzeResult = u.resolveElementMetadata(u.func, u.children)
            val newChildren = u.children.map {
              case NamedArgumentExpression(key, t: FunctionTableSubqueryArgumentExpression) =>
                NamedArgumentExpression(key, analyzeResult.applyToTableArgument(u.name, t))
              case t: FunctionTableSubqueryArgumentExpression =>
                analyzeResult.applyToTableArgument(u.name, t)
              case c => c
            }
            PythonUDTF(
              u.name, u.func, analyzeResult.schema, Some(analyzeResult.pickledAnalyzeResult),
              newChildren, u.evalType, u.udfDeterministic, u.resultId)
          }
        }
    }

    private[analysis] def resolveFunction(u: UnresolvedFunction): Expression = {
      withPosition(u) {
        resolveBuiltinOrTempFunction(u.nameParts, u.arguments, u).getOrElse {
          val CatalogAndIdentifier(catalog, ident) = expandIdentifier(u.nameParts)
          if (CatalogV2Util.isSessionCatalog(catalog)) {
            resolveV1Function(ident.asFunctionIdentifier, u.arguments, u)
          } else {
            resolveV2Function(catalog.asFunctionCatalog, ident, u.arguments, u)
          }
        }
      }
    }

    /**
     * Check if the arguments of a function are either resolved or a lambda function.
     */
    private def hasLambdaAndResolvedArguments(expressions: Seq[Expression]): Boolean = {
      val (lambdas, others) = expressions.partition(_.isInstanceOf[LambdaFunction])
      lambdas.nonEmpty && others.forall(_.resolved)
    }

    def lookupBuiltinOrTempFunction(
        name: Seq[String],
        u: Option[UnresolvedFunction]): Option[ExpressionInfo] = {
      if (name.size == 1 && u.exists(_.isInternal)) {
        FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name.head))
      } else if (name.size == 1) {
        v1SessionCatalog.lookupBuiltinOrTempFunction(name.head)
      } else {
        None
      }
    }

    def lookupBuiltinOrTempTableFunction(name: Seq[String]): Option[ExpressionInfo] = {
      if (name.length == 1) {
        v1SessionCatalog.lookupBuiltinOrTempTableFunction(name.head)
      } else {
        None
      }
    }

    private def resolveBuiltinOrTempFunction(
        name: Seq[String],
        arguments: Seq[Expression],
        u: UnresolvedFunction): Option[Expression] = {
      val expression = if (name.size == 1  && u.isInternal) {
        Option(FunctionRegistry.internal.lookupFunction(FunctionIdentifier(name.head), arguments))
      } else if (name.size == 1) {
        v1SessionCatalog.resolveBuiltinOrTempFunction(name.head, arguments)
      } else {
        None
      }
      expression.map { func =>
        validateFunction(func, arguments.length, u)
      }
    }

    private def resolveBuiltinOrTempTableFunction(
        name: Seq[String],
        arguments: Seq[Expression]): Option[LogicalPlan] = {
      if (name.length == 1) {
        v1SessionCatalog.resolveBuiltinOrTempTableFunction(name.head, arguments)
      } else {
        None
      }
    }

    private def resolveV1Function(
        ident: FunctionIdentifier,
        arguments: Seq[Expression],
        u: UnresolvedFunction): Expression = {
      val func = v1SessionCatalog.resolvePersistentFunction(ident, arguments)
      validateFunction(func, arguments.length, u)
    }

    private def validateFunction(
        func: Expression,
        numArgs: Int,
        u: UnresolvedFunction): Expression = {
      func match {
        case owg: SupportsOrderingWithinGroup if u.isDistinct =>
          throw QueryCompilationErrors.distinctInverseDistributionFunctionUnsupportedError(
            owg.prettyName)
        case owg: SupportsOrderingWithinGroup
          if !owg.orderingFilled && u.orderingWithinGroup.isEmpty =>
          throw QueryCompilationErrors.inverseDistributionFunctionMissingWithinGroupError(
            owg.prettyName)
        case owg: SupportsOrderingWithinGroup
          if owg.orderingFilled && u.orderingWithinGroup.nonEmpty =>
          throw QueryCompilationErrors.wrongNumOrderingsForInverseDistributionFunctionError(
            owg.prettyName, 0, u.orderingWithinGroup.length)
        case f
          if !f.isInstanceOf[SupportsOrderingWithinGroup] && u.orderingWithinGroup.nonEmpty =>
          throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
            func.prettyName, "WITHIN GROUP (ORDER BY ...)")
        // AggregateWindowFunctions are AggregateFunctions that can only be evaluated within
        // the context of a Window clause. They do not need to be wrapped in an
        // AggregateExpression.
        case wf: AggregateWindowFunction =>
          if (u.isDistinct) {
            throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
              wf.prettyName, "DISTINCT")
          } else if (u.filter.isDefined) {
            throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
              wf.prettyName, "FILTER clause")
          } else if (u.ignoreNulls) {
            wf match {
              case nthValue: NthValue =>
                nthValue.copy(ignoreNulls = u.ignoreNulls)
              case _ =>
                throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
                  wf.prettyName, "IGNORE NULLS")
            }
          } else {
            wf
          }
        case owf: FrameLessOffsetWindowFunction =>
          if (u.isDistinct) {
            throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
              owf.prettyName, "DISTINCT")
          } else if (u.filter.isDefined) {
            throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
              owf.prettyName, "FILTER clause")
          } else if (u.ignoreNulls) {
            owf match {
              case lead: Lead =>
                lead.copy(ignoreNulls = u.ignoreNulls)
              case lag: Lag =>
                lag.copy(ignoreNulls = u.ignoreNulls)
            }
          } else {
            owf
          }
        // We get an aggregate function, we need to wrap it in an AggregateExpression.
        case agg: AggregateFunction =>
          // Note: PythonUDAF does not support these advanced clauses.
          if (agg.isInstanceOf[PythonUDAF]) checkUnsupportedAggregateClause(agg, u)
          // After parse, the inverse distribution functions not set the ordering within group yet.
          val newAgg = agg match {
            case owg: SupportsOrderingWithinGroup
              if !owg.orderingFilled && u.orderingWithinGroup.nonEmpty =>
              owg.withOrderingWithinGroup(u.orderingWithinGroup)
            case _ =>
              agg
          }

          u.filter match {
            case Some(filter) if !filter.deterministic =>
              throw QueryCompilationErrors.nonDeterministicFilterInAggregateError(
                filterExpr = filter)
            case Some(filter) if filter.dataType != BooleanType =>
              throw QueryCompilationErrors.nonBooleanFilterInAggregateError(
                filterExpr = filter)
            case Some(filter) if filter.exists(_.isInstanceOf[AggregateExpression]) =>
              throw QueryCompilationErrors.aggregateInAggregateFilterError(
                filterExpr = filter,
                aggExpr = filter.find(_.isInstanceOf[AggregateExpression]).get)
            case Some(filter) if filter.exists(_.isInstanceOf[WindowExpression]) =>
              throw QueryCompilationErrors.windowFunctionInAggregateFilterError(
                filterExpr = filter,
                windowExpr = filter.find(_.isInstanceOf[WindowExpression]).get)
            case _ =>
          }
          if (u.ignoreNulls) {
            val aggFunc = newAgg match {
              case first: First => first.copy(ignoreNulls = u.ignoreNulls)
              case last: Last => last.copy(ignoreNulls = u.ignoreNulls)
              case any_value: AnyValue => any_value.copy(ignoreNulls = u.ignoreNulls)
              case _ =>
                throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
                  newAgg.prettyName, "IGNORE NULLS")
            }
            aggFunc.toAggregateExpression(u.isDistinct, u.filter)
          } else {
            newAgg.toAggregateExpression(u.isDistinct, u.filter)
          }
        // This function is not an aggregate function, just return the resolved one.
        case other =>
          checkUnsupportedAggregateClause(other, u)
          if (other.isInstanceOf[String2TrimExpression] && numArgs == 2) {
            if (trimWarningEnabled.get) {
              log.warn("Two-parameter TRIM/LTRIM/RTRIM function signatures are deprecated." +
                " Use SQL syntax `TRIM((BOTH | LEADING | TRAILING)? trimStr FROM str)`" +
                " instead.")
              trimWarningEnabled.set(false)
            }
          }
          other
      }
    }

    private def checkUnsupportedAggregateClause(func: Expression, u: UnresolvedFunction): Unit = {
      if (u.isDistinct) {
        throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
          func.prettyName, "DISTINCT")
      }
      if (u.filter.isDefined) {
        throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
          func.prettyName, "FILTER clause")
      }
      if (u.ignoreNulls) {
        throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
          func.prettyName, "IGNORE NULLS")
      }
    }

    private def resolveV2Function(
        catalog: FunctionCatalog,
        ident: Identifier,
        arguments: Seq[Expression],
        u: UnresolvedFunction): Expression = {
      val unbound = catalog.loadFunction(ident)
      val inputType = StructType(arguments.zipWithIndex.map {
        case (exp, pos) => StructField(s"_$pos", exp.dataType, exp.nullable)
      })
      val bound = try {
        unbound.bind(inputType)
      } catch {
        case unsupported: UnsupportedOperationException =>
          throw QueryCompilationErrors.functionCannotProcessInputError(
            unbound, arguments, unsupported)
      }

      if (bound.inputTypes().length != arguments.length) {
        throw QueryCompilationErrors.v2FunctionInvalidInputTypeLengthError(
          bound, arguments)
      }

      bound match {
        case scalarFunc: ScalarFunction[_] =>
          processV2ScalarFunction(scalarFunc, arguments, u)
        case aggFunc: V2AggregateFunction[_, _] =>
          processV2AggregateFunction(aggFunc, arguments, u)
        case _ =>
          failAnalysis(
            errorClass = "INVALID_UDF_IMPLEMENTATION",
            messageParameters = Map("funcName" -> toSQLId(bound.name())))
      }
    }

    private def processV2ScalarFunction(
        scalarFunc: ScalarFunction[_],
        arguments: Seq[Expression],
        u: UnresolvedFunction): Expression = {
      if (u.isDistinct) {
        throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
          scalarFunc.name(), "DISTINCT")
      } else if (u.filter.isDefined) {
        throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
          scalarFunc.name(), "FILTER clause")
      } else if (u.ignoreNulls) {
        throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
          scalarFunc.name(), "IGNORE NULLS")
      } else {
        V2ExpressionUtils.resolveScalarFunction(scalarFunc, arguments)
      }
    }

    private def processV2AggregateFunction(
        aggFunc: V2AggregateFunction[_, _],
        arguments: Seq[Expression],
        u: UnresolvedFunction): Expression = {
      if (u.ignoreNulls) {
        throw QueryCompilationErrors.functionWithUnsupportedSyntaxError(
          aggFunc.name(), "IGNORE NULLS")
      }
      val aggregator = V2Aggregator(aggFunc, arguments)
      aggregator.toAggregateExpression(u.isDistinct, u.filter)
    }
  }

  /**
   * This rule resolves and rewrites subqueries inside expressions.
   *
   * Note: CTEs are handled in CTESubstitution.
   */
  object ResolveSubquery extends Rule[LogicalPlan] {
    /**
     * Resolves the subquery plan that is referenced in a subquery expression, by invoking the
     * entire analyzer recursively. We set outer plan in `AnalysisContext`, so that the analyzer
     * can resolve outer references.
     *
     * Outer references of the subquery are updated as children of Subquery expression.
     */
    private def resolveSubQuery(
        e: SubqueryExpression,
        outer: LogicalPlan)(
        f: (LogicalPlan, Seq[Expression]) => SubqueryExpression): SubqueryExpression = {
      val newSubqueryPlan = AnalysisContext.withOuterPlan(outer) {
        executeSameContext(e.plan)
      }

      // If the subquery plan is fully resolved, pull the outer references and record
      // them as children of SubqueryExpression.
      if (newSubqueryPlan.resolved) {
        // Record the outer references as children of subquery expression.
        f(newSubqueryPlan, SubExprUtils.getOuterReferences(newSubqueryPlan))
      } else {
        e.withNewPlan(newSubqueryPlan)
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
    private def resolveSubQueries(plan: LogicalPlan, outer: LogicalPlan): LogicalPlan = {
      plan.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION), ruleId) {
        case s @ ScalarSubquery(sub, _, exprId, _, _, _) if !sub.resolved =>
          resolveSubQuery(s, outer)(ScalarSubquery(_, _, exprId))
        case e @ Exists(sub, _, exprId, _, _) if !sub.resolved =>
          resolveSubQuery(e, outer)(Exists(_, _, exprId))
        case InSubquery(values, l @ ListQuery(_, _, exprId, _, _, _))
            if values.forall(_.resolved) && !l.resolved =>
          val expr = resolveSubQuery(l, outer)((plan, exprs) => {
            ListQuery(plan, exprs, exprId, plan.output.length)
          })
          InSubquery(values, expr.asInstanceOf[ListQuery])
        case s @ LateralSubquery(sub, _, exprId, _, _) if !sub.resolved =>
          resolveSubQuery(s, outer)(LateralSubquery(_, _, exprId))
        case a: FunctionTableSubqueryArgumentExpression if !a.plan.resolved =>
          resolveSubQuery(a, outer)(
            (plan, outerAttrs) => a.copy(plan = plan, outerAttrs = outerAttrs))
      }
    }

    /**
     * Resolve and rewrite all subqueries in an operator tree..
     */
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(PLAN_EXPRESSION), ruleId) {
      case j: LateralJoin if j.left.resolved =>
        // We can't pass `LateralJoin` as the outer plan, as its right child is not resolved yet
        // and we can't call `LateralJoin.resolveChildren` to resolve outer references. Here we
        // create a fake Project node as the outer plan.
        resolveSubQueries(j, Project(Nil, j.left))
      // Only a few unary nodes (Project/Filter/Aggregate) can contain subqueries.
      case q: UnaryNode if q.childrenResolved =>
        resolveSubQueries(q, q)
      case r: RelationTimeTravel =>
        resolveSubQueries(r, r)
      case j: Join if j.childrenResolved && j.duplicateResolved =>
        resolveSubQueries(j, j)
      case tvf: UnresolvedTableValuedFunction =>
        resolveSubQueries(tvf, tvf)
      case s: SupportsSubquery if s.childrenResolved =>
        resolveSubQueries(s, s)
    }
  }

  /**
   * Replaces unresolved column aliases for a subquery with projections.
   */
  object ResolveSubqueryColumnAliases extends Rule[LogicalPlan] {

     def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
       _.containsPattern(UNRESOLVED_SUBQUERY_COLUMN_ALIAS), ruleId) {
      case u @ UnresolvedSubqueryColumnAliases(columnNames, child) if child.resolved =>
        // Resolves output attributes if a query has alias names in its subquery:
        // e.g., SELECT * FROM (SELECT 1 AS a, 1 AS b) t(col1, col2)
        val outputAttrs = child.output
        // Checks if the number of the aliases equals to the number of output columns
        // in the subquery.
        if (columnNames.size != outputAttrs.size) {
          throw QueryCompilationErrors.aliasNumberNotMatchColumnNumberError(
            columnNames.size, outputAttrs.size, u)
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
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      t => t.containsAnyPattern(AGGREGATE_EXPRESSION, PYTHON_UDF) && t.containsPattern(PROJECT),
      ruleId) {
      case Project(projectList, child) if containsAggregates(projectList) =>
        Aggregate(Nil, projectList, child)
    }

    def containsAggregates(exprs: Seq[Expression]): Boolean = {
      // Collect all Windowed Aggregate Expressions.
      val windowedAggExprs: Set[Expression] = exprs.flatMap { expr =>
        expr.collect {
          case WindowExpression(ae: AggregateExpression, _) => ae
          case UnresolvedWindowExpression(ae: AggregateExpression, _) => ae
        }
      }.toSet

      // Find the first Aggregate Expression that is not Windowed.
      exprs.exists(_.exists {
        case ae: AggregateExpression => !windowedAggExprs.contains(ae)
        case _ => false
      })
    }
  }

  /**
   * This rule finds aggregate expressions that are not in an aggregate operator.  For example,
   * those in a HAVING clause or ORDER BY clause.  These expressions are pushed down to the
   * underlying aggregate operator and then projected away after the original operator.
   *
   * We need to make sure the expressions all fully resolved before looking for aggregate functions
   * and group by expressions from them.
   */
  object ResolveAggregateFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(AGGREGATE), ruleId) {
      case UnresolvedHaving(cond, agg: Aggregate) if agg.resolved && cond.resolved =>
        resolveOperatorWithAggregate(Seq(cond), agg, (newExprs, newChild) => {
          val newCond = newExprs.head
          if (newCond.resolved) {
            Filter(newCond, newChild)
          } else {
            // The condition can be unresolved after the resolution, as we may mark
            // `TempResolvedColumn` as unresolved if it's not aggregate function inputs or grouping
            // expressions. We should remain `UnresolvedHaving` as the rule `ResolveReferences` can
            // re-resolve `TempResolvedColumn` and `UnresolvedHaving` has a special column
            // resolution order.
            UnresolvedHaving(newCond, newChild)
          }
        })

      case Filter(cond, agg: Aggregate) if agg.resolved && cond.resolved =>
        resolveOperatorWithAggregate(Seq(cond), agg, (newExprs, newChild) => {
          Filter(newExprs.head, newChild)
        })

      case s @ Sort(_, _, agg: Aggregate) if agg.resolved && s.order.forall(_.resolved) =>
        resolveOperatorWithAggregate(s.order.map(_.child), agg, (newExprs, newChild) => {
          val newSortOrder = s.order.zip(newExprs).map {
            case (sortOrder, expr) => sortOrder.copy(child = expr)
          }
          s.copy(order = newSortOrder, child = newChild)
        })

      case s @ Sort(_, _, f @ Filter(cond, agg: Aggregate))
          if agg.resolved && cond.resolved && s.order.forall(_.resolved) =>
        resolveOperatorWithAggregate(s.order.map(_.child), agg, (newExprs, newChild) => {
          val newSortOrder = s.order.zip(newExprs).map {
            case (sortOrder, expr) => sortOrder.copy(child = expr)
          }
          s.copy(order = newSortOrder, child = f.copy(child = newChild))
        })
    }

    /**
     * Resolves the given expressions as if they are in the given Aggregate operator, which means
     * the column can be resolved using `agg.child` and aggregate functions/grouping columns are
     * allowed. It returns a list of named expressions that need to be appended to
     * `agg.aggregateExpressions`, and the list of resolved expressions.
     */
    def resolveExprsWithAggregate(
        exprs: Seq[Expression],
        agg: Aggregate): (Seq[NamedExpression], Seq[Expression]) = {
      val extraAggExprs = ArrayBuffer.empty[NamedExpression]
      val transformed = exprs.map { e =>
        if (!e.resolved) {
          e
        } else {
          buildAggExprList(e, agg, extraAggExprs)
        }
      }
      (extraAggExprs.toSeq, transformed)
    }

    private def buildAggExprList(
        expr: Expression,
        agg: Aggregate,
        aggExprList: ArrayBuffer[NamedExpression]): Expression = {
      // Avoid adding an extra aggregate expression if it's already present in
      // `agg.aggregateExpressions`.
      val index = agg.aggregateExpressions.indexWhere {
        case Alias(child, _) => child semanticEquals expr
        case other => other semanticEquals expr
      }
      if (index >= 0) {
        agg.aggregateExpressions(index).toAttribute
      } else {
        expr match {
          case ae: AggregateExpression =>
            val cleaned = trimTempResolvedColumn(ae)
            val alias = Alias(cleaned, cleaned.toString)()
            aggExprList += alias
            alias.toAttribute
          case grouping: Expression if agg.groupingExpressions.exists(grouping.semanticEquals) =>
            trimTempResolvedColumn(grouping) match {
              case ne: NamedExpression =>
                aggExprList += ne
                ne.toAttribute
              case other =>
                val alias = Alias(other, other.toString)()
                aggExprList += alias
                alias.toAttribute
            }
          case t: TempResolvedColumn =>
            if (t.child.isInstanceOf[Attribute]) {
              // This column is neither inside aggregate functions nor a grouping column. It
              // shouldn't be resolved with `agg.child.output`. Mark it as "hasTried", so that it
              // can be re-resolved later or go back to `UnresolvedAttribute` at the end.
              withOrigin(t.origin)(t.copy(hasTried = true))
            } else {
              // This is a nested column, we still have a chance to match grouping expressions with
              // the top-level column. Here we wrap the underlying `Attribute` with
              // `TempResolvedColumn` and try again.
              val childWithTempCol = t.child.transformUp {
                case a: Attribute => TempResolvedColumn(a, Seq(a.name))
              }
              val newChild = buildAggExprList(childWithTempCol, agg, aggExprList)
              if (newChild.containsPattern(TEMP_RESOLVED_COLUMN)) {
                withOrigin(t.origin)(t.copy(hasTried = true))
              } else {
                newChild
              }
            }
          case other =>
            other.withNewChildren(other.children.map(buildAggExprList(_, agg, aggExprList)))
        }
      }
    }

    private def trimTempResolvedColumn(input: Expression): Expression = input.transform {
      case t: TempResolvedColumn => t.child
    }

    def resolveOperatorWithAggregate(
        exprs: Seq[Expression],
        agg: Aggregate,
        buildOperator: (Seq[Expression], Aggregate) => LogicalPlan): LogicalPlan = {
      val (extraAggExprs, resolvedExprs) = resolveExprsWithAggregate(exprs, agg)
      if (extraAggExprs.isEmpty) {
        buildOperator(resolvedExprs, agg)
      } else {
        Project(agg.output, buildOperator(resolvedExprs, agg.copy(
          aggregateExpressions = agg.aggregateExpressions ++ extraAggExprs)))
      }
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
    def hasGenerator(expr: Expression): Boolean = {
      expr.exists(_.isInstanceOf[Generator])
    }

    private def hasNestedGenerator(expr: NamedExpression): Boolean = {
      @scala.annotation.tailrec
      def hasInnerGenerator(g: Generator): Boolean = g match {
        // Since `GeneratorOuter` is just a wrapper of generators, we skip it here
        case go: GeneratorOuter =>
          hasInnerGenerator(go.child)
        case _ =>
          g.children.exists { _.exists {
            case _: Generator => true
            case _ => false
          } }
      }
      trimNonTopLevelAliases(expr) match {
        case UnresolvedAlias(g: Generator, _) => hasInnerGenerator(g)
        case Alias(g: Generator, _) => hasInnerGenerator(g)
        case MultiAlias(g: Generator, _) => hasInnerGenerator(g)
        case other => hasGenerator(other)
      }
    }

    private def hasAggFunctionInGenerator(ne: Seq[NamedExpression]): Boolean = {
      ne.exists(_.exists {
        case g: Generator =>
          g.children.exists(_.exists(_.isInstanceOf[AggregateFunction]))
        case _ =>
          false
      })
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

    // We must wait until all expressions except for generator functions are resolved before
    // rewriting generator functions in Project/Aggregate. This is necessary to make this rule
    // stable for different execution orders of analyzer rules. See also SPARK-47241.
    private def canRewriteGenerator(namedExprs: Seq[NamedExpression]): Boolean = {
      namedExprs.forall { ne =>
        ne.resolved || {
          trimNonTopLevelAliases(ne) match {
            case AliasedGenerator(_, _, _) => true
            case _ => false
          }
        }
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(GENERATOR), ruleId) {
      case Project(projectList, _) if projectList.exists(hasNestedGenerator) =>
        val nestedGenerator = projectList.find(hasNestedGenerator).get
        throw QueryCompilationErrors.nestedGeneratorError(trimAlias(nestedGenerator))

      case Aggregate(_, aggList, _) if aggList.exists(hasNestedGenerator) =>
        val nestedGenerator = aggList.find(hasNestedGenerator).get
        throw QueryCompilationErrors.nestedGeneratorError(trimAlias(nestedGenerator))

      case Aggregate(_, aggList, _) if aggList.count(hasGenerator) > 1 =>
        val generators = aggList.filter(hasGenerator).map(trimAlias)
        throw QueryCompilationErrors.moreThanOneGeneratorError(generators)

      case Aggregate(groupList, aggList, child) if canRewriteGenerator(aggList) &&
          aggList.exists(hasGenerator) =>
        // If generator in the aggregate list was visited, set the boolean flag true.
        var generatorVisited = false

        val projectExprs = Array.ofDim[NamedExpression](aggList.length)
        val newAggList = aggList
          .toIndexedSeq
          .map(trimNonTopLevelAliases)
          .zipWithIndex
          .flatMap {
            case (AliasedGenerator(generator, names, outer), idx) =>
              // It's a sanity check, this should not happen as the previous case will throw
              // exception earlier.
              assert(!generatorVisited, "More than one generator found in aggregate.")
              generatorVisited = true

              val newGenChildren: Seq[Expression] = generator.children.zipWithIndex.map {
                case (e, idx) => if (e.foldable) e else Alias(e, s"_gen_input_${idx}")()
              }
              val newGenerator = {
                val g = generator.withNewChildren(newGenChildren.map { e =>
                  if (e.foldable) e else e.asInstanceOf[Alias].toAttribute
                }).asInstanceOf[Generator]
                if (outer) GeneratorOuter(g) else g
              }
              val newAliasedGenerator = if (names.length == 1) {
                Alias(newGenerator, names(0))()
              } else {
                MultiAlias(newGenerator, names)
              }
              projectExprs(idx) = newAliasedGenerator
              newGenChildren.filter(!_.foldable).asInstanceOf[Seq[NamedExpression]]
            case (other, idx) =>
              projectExprs(idx) = other.toAttribute
              other :: Nil
          }

        val newAgg = Aggregate(groupList, newAggList, child)
        Project(projectExprs.toList, newAgg)

      case p @ Project(projectList, _) if hasAggFunctionInGenerator(projectList) =>
        // If a generator has any aggregate function, we need to apply the `GlobalAggregates` rule
        // first for replacing `Project` with `Aggregate`.
        p

      case p @ Project(projectList, child) if canRewriteGenerator(projectList) &&
          projectList.exists(hasGenerator) =>
        val (resolvedGenerator, newProjectList) = projectList
          .map(trimNonTopLevelAliases)
          .foldLeft((None: Option[Generate], Nil: Seq[NamedExpression])) { (res, e) =>
            e match {
              // If there are more than one generator, we only rewrite the first one and wait for
              // the next analyzer iteration to rewrite the next one.
              case AliasedGenerator(generator, names, outer) if res._1.isEmpty &&
                  generator.childrenResolved =>
                val g = Generate(
                  generator,
                  unrequiredChildIndex = Nil,
                  outer = outer,
                  qualifier = None,
                  generatorOutput = ResolveGenerate.makeGeneratorOutput(generator, names),
                  child)
                (Some(g), res._2 ++ g.nullableOutput)
              case other =>
                (res._1, res._2 :+ other)
            }
          }

        if (resolvedGenerator.isDefined) {
          Project(newProjectList, resolvedGenerator.get)
        } else {
          p
        }

      case g @ Generate(GeneratorOuter(generator), _, _, _, _, _) =>
        g.copy(generator = generator, outer = true)

      case g: Generate => g

      case u: UnresolvedTableValuedFunction => u

      case p: Project => p

      case a: Aggregate => a

      case p if p.expressions.exists(hasGenerator) =>
        throw QueryCompilationErrors.generatorOutsideSelectError(p)
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
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(GENERATE), ruleId) {
      case g: Generate if !g.child.resolved || !g.generator.resolved => g
      case g: Generate if !g.resolved => withPosition(g) {
        // Check nested generators.
        if (g.generator.children.exists(ExtractGenerator.hasGenerator)) {
          throw QueryCompilationErrors.nestedGeneratorError(g.generator)
        }
        g.copy(generatorOutput = makeGeneratorOutput(g.generator, g.generatorOutput.map(_.name)))
      }
    }

    /**
     * Construct the output attributes for a [[Generator]], given a list of names.  If the list of
     * names is empty names are assigned from field names in generator.
     */
    private[analysis] def makeGeneratorOutput(
        generator: Generator,
        names: Seq[String]): Seq[Attribute] = {
      val elementAttrs = DataTypeUtils.toAttributes(generator.elementSchema)

      if (names.length == elementAttrs.length) {
        names.zip(elementAttrs).map {
          case (name, attr) => attr.withName(name)
        }
      } else if (names.isEmpty) {
        elementAttrs
      } else {
        throw QueryCompilationErrors.aliasesNumberNotMatchUDTFOutputError(
          elementAttrs.size, names.mkString(","))
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
   * Note: [[ResolveLateralColumnAliasReference]] rule is applied before this rule. To guarantee
   * this order, we make sure this rule applies only when the [[Project]] or [[Aggregate]] doesn't
   * contain any [[LATERAL_COLUMN_ALIAS_REFERENCE]].
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
    type Spec = (Seq[Expression], Seq[SortOrder], WindowFunctionType)

    private def hasWindowFunction(exprs: Seq[Expression]): Boolean =
      exprs.exists(hasWindowFunction)

    private def hasWindowFunction(expr: Expression): Boolean = {
      expr.exists {
        case window: WindowExpression => true
        case _ => false
      }
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
      val extractedExprMap = mutable.LinkedHashMap.empty[Expression, NamedExpression]
      def getOrExtract(key: Expression, value: Expression): Expression = {
        extractedExprMap.getOrElseUpdate(key.canonicalized,
          Alias(value, s"_w${extractedExprMap.size}")()).toAttribute
      }
      def extractExpr(expr: Expression): Expression = expr match {
        case ne: NamedExpression =>
          // If a named expression is not in regularExpressions, add it to
          // extractedExprMap and replace it with an AttributeReference.
          val missingExpr =
            AttributeSet(Seq(expr)) -- (regularExpressions ++ extractedExprMap.values)
          if (missingExpr.nonEmpty) {
            extractedExprMap += ne.canonicalized -> ne
          }
          // alias will be cleaned in the rule CleanupAliases
          ne
        case e: Expression if e.foldable =>
          e // No need to create an attribute reference if it will be evaluated as a Literal.
        case e: NamedArgumentExpression =>
          // For NamedArgumentExpression, we extract the value and replace it with
          // an AttributeReference (with an internal column name, e.g. "_w0").
          NamedArgumentExpression(e.key, getOrExtract(e, e.value))
        case e: Expression =>
          // For other expressions, we extract it and replace it with an AttributeReference (with
          // an internal column name, e.g. "_w0").
          getOrExtract(e, e)
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

          case WindowExpression(ae: AggregateExpression, _) if ae.filter.isDefined =>
            throw QueryCompilationErrors.windowAggregateFunctionWithFilterNotSupportedError()

          // Extract Windowed AggregateExpression
          case we @ WindowExpression(
              ae @ AggregateExpression(function, _, _, _, _),
              spec: WindowSpecDefinition) =>
            val newChildren = function.children.map(extractExpr)
            val newFunction = function.withNewChildren(newChildren).asInstanceOf[AggregateFunction]
            val newAgg = ae.copy(aggregateFunction = newFunction)
            seenWindowAggregates += newAgg
            WindowExpression(newAgg, spec)

          case AggregateExpression(aggFunc, _, _, _, _) if hasWindowFunction(aggFunc.children) =>
            throw QueryCompilationErrors.windowFunctionInsideAggregateFunctionNotAllowedError()

          // Extracts AggregateExpression. For example, for SUM(x) - Sum(y) OVER (...),
          // we need to extract SUM(x).
          case agg: AggregateExpression if !seenWindowAggregates.contains(agg) =>
            extractedExprMap.getOrElseUpdate(agg.canonicalized,
              Alias(agg, s"_w${extractedExprMap.size}")()).toAttribute

          // Extracts other attributes
          case attr: Attribute => extractExpr(attr)

        }.asInstanceOf[NamedExpression]
      }

      (newExpressionsWithWindowFunctions, regularExpressions ++ extractedExprMap.values)
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

      // SPARK-32616: Use a linked hash map to maintains the insertion order of the Window
      // operators, so the query with multiple Window operators can have the determined plan.
      val groupedWindowExpressions = mutable.LinkedHashMap.empty[Spec, ArrayBuffer[NamedExpression]]
      // Second, we group extractedWindowExprBuffer based on their Partition and Order Specs.
      extractedWindowExprBuffer.foreach { expr =>
        val distinctWindowSpec = expr.collect {
          case window: WindowExpression => window.windowSpec
        }.distinct

        // We do a final check and see if we only have a single Window Spec defined in an
        // expressions.
        if (distinctWindowSpec.isEmpty) {
          throw QueryCompilationErrors.expressionWithoutWindowExpressionError(expr)
        } else if (distinctWindowSpec.length > 1) {
          // newExpressionsWithWindowFunctions only have expressions with a single
          // WindowExpression. If we reach here, we have a bug.
          throw QueryCompilationErrors.expressionWithMultiWindowExpressionsError(
            expr, distinctWindowSpec)
        } else {
          val spec = distinctWindowSpec.head
          val specKey = (spec.partitionSpec, spec.orderSpec, WindowFunctionType.functionType(expr))
          val windowExprs = groupedWindowExpressions
            .getOrElseUpdate(specKey, new ArrayBuffer[NamedExpression])
          windowExprs += expr
        }
      }

      // Third, we aggregate them by adding each Window operator for each Window Spec and then
      // setting this to the child of the next Window operator.
      val windowOps =
        groupedWindowExpressions.foldLeft(child) {
          case (last, ((partitionSpec, orderSpec, _), windowExpressions)) =>
            Window(windowExpressions.toSeq, partitionSpec, orderSpec, last)
        }

      // Finally, we create a Project to output windowOps's output
      // newExpressionsWithWindowFunctions.
      Project(windowOps.output ++ newExpressionsWithWindowFunctions, windowOps)
    } // end of addWindow

    // We have to use transformDown at here to make sure the rule of
    // "Aggregate with Having clause" will be triggered.
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDownWithPruning(
      _.containsPattern(WINDOW_EXPRESSION), ruleId) {

      case Filter(condition, _) if hasWindowFunction(condition) =>
        throw QueryCompilationErrors.windowFunctionNotAllowedError("WHERE")

      case UnresolvedHaving(condition, _) if hasWindowFunction(condition) =>
        throw QueryCompilationErrors.windowFunctionNotAllowedError("HAVING")

      // Aggregate with Having clause. This rule works with an unresolved Aggregate because
      // a resolved Aggregate will not have Window Functions.
      case f @ UnresolvedHaving(condition, a @ Aggregate(groupingExprs, aggregateExprs, child))
        if child.resolved &&
          hasWindowFunction(aggregateExprs) &&
          a.expressions.forall(_.resolved) =>
        aggregateExprs.foreach(_.transformDownWithPruning(
          _.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
          case lcaRef: LateralColumnAliasReference =>
            throw QueryCompilationErrors.lateralColumnAliasInAggWithWindowAndHavingUnsupportedError(
              lcaRef.nameParts)
        })
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
      // Make sure the lateral column aliases are properly handled first.
      case a @ Aggregate(groupingExprs, aggregateExprs, child)
        if hasWindowFunction(aggregateExprs) &&
          a.expressions.forall(_.resolved) &&
          !aggregateExprs.exists(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) =>
        val (windowExpressions, aggregateExpressions) = extract(aggregateExprs)
        // Create an Aggregate operator to evaluate aggregation functions.
        val withAggregate = Aggregate(groupingExprs, aggregateExpressions, child)
        // Add Window operators.
        val withWindow = addWindow(windowExpressions, withAggregate)

        // Finally, generate output columns according to the original projectList.
        val finalProjectList = aggregateExprs.map(_.toAttribute)
        Project(finalProjectList, withWindow)

      // We only extract Window Expressions after all expressions of the Project
      // have been resolved, and lateral column aliases are properly handled first.
      case p @ Project(projectList, child)
        if hasWindowFunction(projectList) &&
          p.expressions.forall(_.resolved) &&
          !projectList.exists(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) =>
        val (windowExpressions, regularExpressions) = extract(projectList.toIndexedSeq)
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
   * Set the seed for random number generation.
   */
  object ResolveRandomSeed extends Rule[LogicalPlan] {
    private lazy val random = new Random()

    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(EXPRESSION_WITH_RANDOM_SEED), ruleId) {
      case p if p.resolved => p
      case p => p.transformExpressionsUpWithPruning(
        _.containsPattern(EXPRESSION_WITH_RANDOM_SEED), ruleId) {
        case e: ExpressionWithRandomSeed if e.seedExpression == UnresolvedSeed =>
          e.withNewSeed(random.nextLong())
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
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(SCALA_UDF)) {
      case p if !p.resolved => p // Skip unresolved nodes.

      case p => p.transformExpressionsUpWithPruning(_.containsPattern(SCALA_UDF)) {

        case udf: ScalaUDF if udf.inputPrimitives.contains(true) =>
          // Otherwise, add special handling of null for fields that can't accept null.
          // The result of operations like this, when passed null, is generally to return null.
          assert(udf.inputPrimitives.length == udf.children.length)

          val inputPrimitivesPair = udf.inputPrimitives.zip(udf.children)
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
   * Resolve the encoders for the UDF by explicitly given the attributes. We give the
   * attributes explicitly in order to handle the case where the data type of the input
   * value is not the same with the internal schema of the encoder, which could cause
   * data loss. For example, the encoder should not cast the input value to Decimal(38, 18)
   * if the actual data type is Decimal(30, 0).
   *
   * The resolved encoders then will be used to deserialize the internal row to Scala value.
   */
  object ResolveEncodersInUDF extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(SCALA_UDF), ruleId) {
      case p if !p.resolved => p // Skip unresolved nodes.

      case p => p.transformExpressionsUpWithPruning(_.containsPattern(SCALA_UDF), ruleId) {

        case udf: ScalaUDF if udf.inputEncoders.nonEmpty =>
          val boundEncoders = udf.inputEncoders.zipWithIndex.map { case (encOpt, i) =>
            val dataType = udf.children(i).dataType
            encOpt.map { enc =>
              val attrs = if (enc.isSerializedAsStructForTopLevel) {
                // Value class that has been replaced with its underlying type
                if (enc.schema.fields.length == 1 && enc.schema.fields.head.dataType == dataType) {
                  DataTypeUtils.toAttributes(enc.schema)
                } else {
                  DataTypeUtils.toAttributes(dataType.asInstanceOf[StructType])
                }
              } else {
                // the field name doesn't matter here, so we use
                // a simple literal to avoid any overhead
                DataTypeUtils.toAttribute(StructField("input", dataType)) :: Nil
              }
              enc.resolveAndBind(attrs)
            }
          }
          udf.copy(inputEncoders = boundEncoders)
      }
    }
  }

  /**
   * Check and add proper window frames for all window functions.
   */
  object ResolveWindowFrame extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressionsWithPruning(
      _.containsPattern(WINDOW_EXPRESSION), ruleId) {
      case WindowExpression(wf: FrameLessOffsetWindowFunction,
        WindowSpecDefinition(_, _, f: SpecifiedWindowFrame)) if wf.frame != f =>
        throw QueryCompilationErrors.cannotSpecifyWindowFrameError(wf.prettyName)
      case WindowExpression(wf: WindowFunction, WindowSpecDefinition(_, _, f: SpecifiedWindowFrame))
          if wf.frame != UnspecifiedFrame && wf.frame != f =>
        throw QueryCompilationErrors.windowFrameNotMatchRequiredFrameError(f, wf.frame)
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
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressionsWithPruning(
      _.containsPattern(WINDOW_EXPRESSION), ruleId) {
      case WindowExpression(wf: WindowFunction, spec) if spec.orderSpec.isEmpty =>
        throw QueryCompilationErrors.windowFunctionWithWindowFrameNotOrderedError(wf)
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
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(NATURAL_LIKE_JOIN), ruleId) {
      case j @ Join(left, right, UsingJoin(joinType, usingCols), _, hint)
          if left.resolved && right.resolved && j.duplicateResolved =>
        val project = commonNaturalJoinProcessing(
          left, right, joinType, usingCols, None, hint)
        j.getTagValue(LogicalPlan.PLAN_ID_TAG)
          .foreach(project.setTagValue(LogicalPlan.PLAN_ID_TAG, _))
        project
      case j @ Join(left, right, NaturalJoin(joinType), condition, hint)
          if j.resolvedExceptNatural =>
        // find common column names from both sides
        val joinNames = left.output.map(_.name).intersect(right.output.map(_.name))
        val project = commonNaturalJoinProcessing(
          left, right, joinType, joinNames, condition, hint)
        j.getTagValue(LogicalPlan.PLAN_ID_TAG)
          .foreach(project.setTagValue(LogicalPlan.PLAN_ID_TAG, _))
        project
    }
  }

  /**
   * Resolves columns of an output table from the data in a logical plan. This rule will:
   *
   * - Reorder columns when the write is by name
   * - Insert casts when data types do not match
   * - Insert aliases when column names do not match
   * - Detect plans that are not compatible with the output table and throw AnalysisException
   */
  object ResolveOutputRelation extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      _.containsPattern(COMMAND), ruleId) {
      case v2Write: V2WriteCommand
          if v2Write.table.resolved && v2Write.query.resolved && !v2Write.outputResolved =>
        validateStoreAssignmentPolicy()
        TableOutputResolver.suitableForByNameCheck(v2Write.isByName,
          expected = v2Write.table.output, queryOutput = v2Write.query.output)
        val projection = TableOutputResolver.resolveOutputColumns(
          v2Write.table.name, v2Write.table.output, v2Write.query, v2Write.isByName, conf)
        if (projection != v2Write.query) {
          val cleanedTable = v2Write.table match {
            case r: DataSourceV2Relation =>
              r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata))
            case other => other
          }
          v2Write.withNewQuery(projection).withNewTable(cleanedTable)
        } else {
          v2Write
        }
    }
  }

  private def validateStoreAssignmentPolicy(): Unit = {
    // SPARK-28730: LEGACY store assignment policy is disallowed in data source v2.
    if (conf.storeAssignmentPolicy == StoreAssignmentPolicy.LEGACY) {
      throw QueryCompilationErrors.legacyStoreAssignmentPolicyError()
    }
  }

  private def commonNaturalJoinProcessing(
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      joinNames: Seq[String],
      condition: Option[Expression],
      hint: JoinHint): LogicalPlan = {
    import org.apache.spark.sql.catalyst.util._

    val leftKeys = joinNames.map { keyName =>
      left.output.find(attr => resolver(attr.name, keyName)).getOrElse {
        throw QueryCompilationErrors.unresolvedUsingColForJoinError(
          keyName, left.schema.fieldNames.sorted.map(toSQLId).mkString(", "), "left")
      }
    }
    val rightKeys = joinNames.map { keyName =>
      right.output.find(attr => resolver(attr.name, keyName)).getOrElse {
        throw QueryCompilationErrors.unresolvedUsingColForJoinError(
          keyName, right.schema.fieldNames.sorted.map(toSQLId).mkString(", "), "right")
      }
    }
    val joinPairs = leftKeys.zip(rightKeys)

    val newCondition = (condition ++ joinPairs.map(EqualTo.tupled)).reduceOption(And)

    // columns not in joinPairs
    val lUniqueOutput = left.output.filterNot(att => leftKeys.contains(att))
    val rUniqueOutput = right.output.filterNot(att => rightKeys.contains(att))

    // the output list looks like: join keys, columns from left, columns from right
    val (projectList, hiddenList) = joinType match {
      case LeftOuter =>
        (leftKeys ++ lUniqueOutput ++ rUniqueOutput.map(_.withNullability(true)),
          rightKeys.map(_.withNullability(true)))
      case LeftExistence(_) =>
        (leftKeys ++ lUniqueOutput, Seq.empty)
      case RightOuter =>
        (rightKeys ++ lUniqueOutput.map(_.withNullability(true)) ++ rUniqueOutput,
          leftKeys.map(_.withNullability(true)))
      case FullOuter =>
        // In full outer join, we should return non-null values for the join columns
        // if either side has non-null values for those columns. Therefore, for each
        // join column pair, add a coalesce to return the non-null value, if it exists.
        val joinedCols = joinPairs.map { case (l, r) =>
          // Since this is a full outer join, either side could be null, so we explicitly
          // set the nullability to true for both sides.
          Alias(Coalesce(Seq(l.withNullability(true), r.withNullability(true))), l.name)()
        }
        (joinedCols ++
          lUniqueOutput.map(_.withNullability(true)) ++
          rUniqueOutput.map(_.withNullability(true)),
          leftKeys.map(_.withNullability(true)) ++
          rightKeys.map(_.withNullability(true)))
      case _ : InnerLike =>
        (leftKeys ++ lUniqueOutput ++ rUniqueOutput, rightKeys)
      case _ =>
        throw QueryExecutionErrors.unsupportedNaturalJoinTypeError(joinType)
    }

    // use Project to hide duplicated common keys
    // propagate hidden columns from nested USING/NATURAL JOINs
    val project = Project(projectList, Join(left, right, joinType, newCondition, hint))
    project.setTagValue(
      Project.hiddenOutputTag,
      hiddenList.map(_.markAsQualifiedAccessOnly()) ++
        project.child.metadataOutput.filter(_.qualifiedAccessOnly))
    project
  }

  /**
   * Replaces [[UnresolvedDeserializer]] with the deserialization expression that has been resolved
   * to the given input attributes.
   */
  object ResolveDeserializer extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(UNRESOLVED_DESERIALIZER), ruleId) {
      case p if !p.childrenResolved => p
      case p if p.resolved => p

      case p => p.transformExpressionsWithPruning(
        _.containsPattern(UNRESOLVED_DESERIALIZER), ruleId) {
        case UnresolvedDeserializer(deserializer, inputAttributes) =>
          val inputs = if (inputAttributes.isEmpty) {
            p.children.flatMap(_.output)
          } else {
            inputAttributes
          }

          validateTopLevelTupleFields(deserializer, inputs)
          val resolved = resolveExpressionByPlanOutput(
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
                  throw QueryCompilationErrors.dataTypeMismatchForDeserializerError(other,
                    "array")
              }
            case u: UnresolvedCatalystToExternalMap if u.child.resolved =>
              u.child.dataType match {
                case _: MapType =>
                  CatalystToExternalMap(u) transformUp {
                    case UnresolvedExtractValue(child, fieldName) if child.resolved =>
                      ExtractValue(child, fieldName, resolver)
                  }
                case other =>
                  throw QueryCompilationErrors.dataTypeMismatchForDeserializerError(other, "map")
              }
          }
          validateNestedTupleFields(result)
          result
      }
    }

    private def fail(schema: StructType, maxOrdinal: Int): Unit = {
      throw QueryCompilationErrors.fieldNumberMismatchForDeserializerError(schema, maxOrdinal)
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
        .transform((_, v) => v.map(_.ordinal).distinct.sorted)

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
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(NEW_INSTANCE), ruleId) {
      case p if !p.childrenResolved => p
      case p if p.resolved => p

      case p => p.transformExpressionsUpWithPruning(_.containsPattern(NEW_INSTANCE), ruleId) {
        case n: NewInstance if n.childrenResolved && !n.resolved =>
          val outer = OuterScopes.getOuterScope(n.cls)
          if (outer == null) {
            throw QueryCompilationErrors.outerScopeFailureForNewInstanceError(n.cls.getName)
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
      throw QueryCompilationErrors.upCastFailureError(fromStr, from, to, walkedTypePath)
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
      _.containsPattern(UP_CAST), ruleId) {
      case p if !p.childrenResolved => p
      case p if p.resolved => p

      case p => p.transformExpressionsWithPruning(_.containsPattern(UP_CAST), ruleId) {
        case u @ UpCast(child, _, _) if !child.resolved => u

        case UpCast(_, target, _) if target != DecimalType && !target.isInstanceOf[DataType] =>
          throw SparkException.internalError(
            s"UpCast only supports DecimalType as AbstractDataType yet, but got: $target")

        case UpCast(child, target, walkedTypePath) if target == DecimalType
          && child.dataType.isInstanceOf[DecimalType] =>
          assert(walkedTypePath.nonEmpty,
            "object DecimalType should only be used inside ExpressionEncoder")

          // SPARK-31750: if we want to upcast to the general decimal type, and the `child` is
          // already decimal type, we can remove the `Upcast` and accept any precision/scale.
          // This can happen for cases like `spark.read.parquet("/tmp/file").as[BigDecimal]`.
          child

        case UpCast(child, target: AtomicType, _)
            if conf.getConf(SQLConf.LEGACY_LOOSE_UPCAST) &&
              child.dataType == StringType =>
          Cast(child, target.asNullable)

        case u @ UpCast(child, _, walkedTypePath) if !Cast.canUpCast(child.dataType, u.dataType) =>
          fail(child, u.dataType, walkedTypePath)

        case u @ UpCast(child, _, _) => Cast(child, u.dataType)
      }
    }
  }

  /**
   * Rule to resolve, normalize and rewrite field names based on case sensitivity for commands.
   */
  object ResolveFieldNameAndPosition extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case cmd: CreateIndex if cmd.table.resolved &&
          cmd.columns.exists(_._1.isInstanceOf[UnresolvedFieldName]) =>
        val table = cmd.table.asInstanceOf[ResolvedTable]
        cmd.copy(columns = cmd.columns.map {
          case (u: UnresolvedFieldName, prop) => resolveFieldNames(table, u.name, u) -> prop
          case other => other
        })

      case a: DropColumns if a.table.resolved && hasUnresolvedFieldName(a) && a.ifExists =>
        // for DropColumn with IF EXISTS clause, we should resolve and ignore missing column errors
        val table = a.table.asInstanceOf[ResolvedTable]
        val columnsToDrop = a.columnsToDrop
        a.copy(columnsToDrop = columnsToDrop.flatMap(c => resolveFieldNamesOpt(table, c.name, c)))

      case a: AlterTableCommand if a.table.resolved && hasUnresolvedFieldName(a) =>
        val table = a.table.asInstanceOf[ResolvedTable]
        a.transformExpressions {
          case u: UnresolvedFieldName => resolveFieldNames(table, u.name, u)
        }

      case a @ AddColumns(r: ResolvedTable, cols) if !a.resolved =>
        // 'colsToAdd' keeps track of new columns being added. It stores a mapping from a
        // normalized parent name of fields to field names that belong to the parent.
        // For example, if we add columns "a.b.c", "a.b.d", and "a.c", 'colsToAdd' will become
        // Map(Seq("a", "b") -> Seq("c", "d"), Seq("a") -> Seq("c")).
        val colsToAdd = mutable.Map.empty[Seq[String], Seq[String]]
        def resolvePosition(
            col: QualifiedColType,
            parentSchema: StructType,
            resolvedParentName: Seq[String]): Option[FieldPosition] = {
          val fieldsAdded = colsToAdd.getOrElse(resolvedParentName, Nil)
          val resolvedPosition = col.position.map {
            case u: UnresolvedFieldPosition => u.position match {
              case after: After =>
                val allFields = parentSchema.fieldNames ++ fieldsAdded
                allFields.find(n => conf.resolver(n, after.column())) match {
                  case Some(colName) =>
                    ResolvedFieldPosition(ColumnPosition.after(colName))
                  case None =>
                    throw QueryCompilationErrors.referenceColNotFoundForAlterTableChangesError(
                      col.colName, allFields)
                }
              case _ => ResolvedFieldPosition(u.position)
            }
            case resolved => resolved
          }
          colsToAdd(resolvedParentName) = fieldsAdded :+ col.colName
          resolvedPosition
        }
        val schema = r.table.columns.asSchema
        val resolvedCols = cols.map { col =>
          col.path match {
            case Some(parent: UnresolvedFieldName) =>
              // Adding a nested field, need to resolve the parent column and position.
              val resolvedParent = resolveFieldNames(r, parent.name, parent)
              val parentSchema = resolvedParent.field.dataType match {
                case s: StructType => s
                case _ => throw QueryCompilationErrors.invalidFieldName(
                  col.name, parent.name, parent.origin)
              }
              val resolvedPosition = resolvePosition(col, parentSchema, resolvedParent.name)
              col.copy(path = Some(resolvedParent), position = resolvedPosition)
            case _ =>
              // Adding to the root. Just need to resolve position.
              val resolvedPosition = resolvePosition(col, schema, Nil)
              col.copy(position = resolvedPosition)
          }
        }
        val resolved = a.copy(columnsToAdd = resolvedCols)
        resolved.copyTagsFrom(a)
        resolved

      case a @ AlterColumn(
          table: ResolvedTable, ResolvedFieldName(path, field), dataType, _, _, position, _) =>
        val newDataType = dataType.flatMap { dt =>
          // Hive style syntax provides the column type, even if it may not have changed.
          val existing = CharVarcharUtils.getRawType(field.metadata).getOrElse(field.dataType)
          if (existing == dt) None else Some(dt)
        }
        val newPosition = position map {
          case u @ UnresolvedFieldPosition(after: After) =>
            // TODO: since the field name is already resolved, it's more efficient if
            //       `ResolvedFieldName` carries the parent struct and we resolve column position
            //       based on the parent struct, instead of re-resolving the entire column path.
            val resolved = resolveFieldNames(table, path :+ after.column(), u)
            ResolvedFieldPosition(ColumnPosition.after(resolved.field.name))
          case u: UnresolvedFieldPosition => ResolvedFieldPosition(u.position)
          case other => other
        }
        val resolved = a.copy(dataType = newDataType, position = newPosition)
        resolved.copyTagsFrom(a)
        resolved
    }

    /**
     * Returns the resolved field name if the field can be resolved, returns None if the column is
     * not found. An error will be thrown in CheckAnalysis for columns that can't be resolved.
     */
    private def resolveFieldNames(
        table: ResolvedTable,
        fieldName: Seq[String],
        context: Expression): ResolvedFieldName = {
      resolveFieldNamesOpt(table, fieldName, context)
        .getOrElse {
          throw QueryCompilationErrors.unresolvedColumnError(fieldName, table.schema.fieldNames,
            context.origin)
        }
    }

    private def resolveFieldNamesOpt(
        table: ResolvedTable,
        fieldName: Seq[String],
        context: Expression): Option[ResolvedFieldName] = {
      table.schema.findNestedField(
        fieldName, includeCollections = true, conf.resolver, context.origin
      ).map {
        case (path, field) => ResolvedFieldName(path, field)
      }
    }

    private def hasUnresolvedFieldName(a: AlterTableCommand): Boolean = {
      a.expressions.exists(_.exists(_.isInstanceOf[UnresolvedFieldName]))
    }
  }

  /**
   * A rule to handle special commands that need to be notified when analysis is done. This rule
   * should run after all other analysis rules are run.
   */
  object HandleSpecialCommand extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      _.containsPattern(COMMAND)) {
      case c: AnalysisOnlyCommand if c.resolved =>
        checkAnalysis(c)
        c.markAsAnalyzed(AnalysisContext.get)
      case c: KeepAnalyzedQuery if c.resolved =>
        c.storeAnalyzedQuery()
    }
  }

  object ResolveTranspose extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      _.containsPattern(TRANSPOSE), ruleId) {
      // scalastyle:off println
      case t @ Transpose(firstColumnValues, child) =>
        val firstColumn = child.output.head
        val firstColumnNamedExpr = firstColumn.asInstanceOf[NamedExpression]

        val unpivot = Unpivot(
          ids = Some(Seq(firstColumnNamedExpr)),
          values = None,
          aliases = None,
          variableColumnName = "key",
          valueColumnNames = Seq("value"),
          child = child
        )

        val keyExpr = UnresolvedAttribute("key")
        val valueExpr = UnresolvedAttribute("value")

        val aggExpression = First(
          valueExpr, ignoreNulls = true).toAggregateExpression(isDistinct = true)

        val pivot = Pivot(
          groupByExprsOpt = Some(Seq(keyExpr)),
          pivotColumn = firstColumnNamedExpr,
          pivotValues = Seq(Literal("dotNET"), Literal("Java")),
          aggregates = Seq(aggExpression),
          child = unpivot
        )

        pivot
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
    plan.transformUpWithPruning(AlwaysProcess.fn, ruleId) {
      case SubqueryAlias(_, child) => child
    }
  }
}

/**
 * Removes [[Union]] operators from the plan if it just has one child.
 */
object EliminateUnions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(UNION), ruleId) {
    case u: Union if u.children.size == 1 => u.children.head
  }
}

/**
 * Cleans up unnecessary Aliases inside the plan. Basically we only need Alias as a top level
 * expression in Project(project list) or Aggregate(aggregate expressions) or
 * Window(window expressions). Notice that if an expression has other expression parameters which
 * are not in its `children`, e.g. `RuntimeReplaceable`, the transformation for Aliases in this
 * rule can't work for those parameters.
 */
object CleanupAliases extends Rule[LogicalPlan] with AliasHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    // trimNonTopLevelAliases can transform Alias and MultiAlias.
    _.containsAnyPattern(ALIAS, MULTI_ALIAS)) {
    case Project(projectList, child) =>
      val cleanedProjectList = projectList.map(trimNonTopLevelAliases)
      Project(cleanedProjectList, child)

    case Aggregate(grouping, aggs, child) =>
      val cleanedAggs = aggs.map(trimNonTopLevelAliases)
      Aggregate(grouping.map(trimAliases), cleanedAggs, child)

    case Window(windowExprs, partitionSpec, orderSpec, child) =>
      val cleanedWindowExprs = windowExprs.map(trimNonTopLevelAliases)
      Window(cleanedWindowExprs, partitionSpec.map(trimAliases),
        orderSpec.map(trimAliases(_).asInstanceOf[SortOrder]), child)

    case CollectMetrics(name, metrics, child, dataframeId) =>
      val cleanedMetrics = metrics.map(trimNonTopLevelAliases)
      CollectMetrics(name, cleanedMetrics, child, dataframeId)

    case Unpivot(ids, values, aliases, variableColumnName, valueColumnNames, child) =>
      val cleanedIds = ids.map(_.map(trimNonTopLevelAliases))
      val cleanedValues = values.map(_.map(_.map(trimNonTopLevelAliases)))
      Unpivot(
        cleanedIds,
        cleanedValues,
        aliases,
        variableColumnName,
        valueColumnNames,
        child)

    // Operators that operate on objects should only have expressions from encoders, which should
    // never have extra aliases.
    case o: ObjectConsumer => o
    case o: ObjectProducer => o
    case a: AppendColumns => a

    case other =>
      other.transformExpressionsDownWithPruning(_.containsAnyPattern(ALIAS, MULTI_ALIAS)) {
        case Alias(child, _) => child
      }
  }
}

/**
 * Ignore event time watermark in batch query, which is only supported in Structured Streaming.
 * TODO: add this rule into analyzer rule list.
 */
object EliminateEventTimeWatermark extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
    _.containsPattern(EVENT_TIME_WATERMARK)) {
    case EventTimeWatermark(_, _, child) if child.resolved && !child.isStreaming => child
    case UpdateEventTimeWatermarkColumn(_, _, child) if child.resolved && !child.isStreaming =>
      child
  }
}

/**
 * Resolve expressions if they contains [[NamePlaceholder]]s.
 */
object ResolveExpressionsWithNamePlaceholders extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressionsWithPruning(
    _.containsAnyPattern(ARRAYS_ZIP, CREATE_NAMED_STRUCT), ruleId) {
    case e: ArraysZip if !e.resolved =>
      val names = e.children.zip(e.names).map {
        case (e: NamedExpression, NamePlaceholder) if e.resolved =>
          Literal(e.name)
        case (_, other) => other
      }
      ArraysZip(e.children, names)

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
    plan.resolveOperatorsWithPruning(
      _.containsAllPatterns(PLAN_EXPRESSION, FILTER, AGGREGATE), ruleId) {
      case f @ Filter(_, a: Aggregate) if f.resolved =>
        f.transformExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION), ruleId) {
          case s: SubqueryExpression if s.children.nonEmpty =>
            // Collect the aliases from output of aggregate.
            val outerAliases = a.aggregateExpressions collect { case a: Alias => a }
            // Update the subquery plan to record the OuterReference to point to outer query plan.
            s.withNewPlan(updateOuterReferenceInSubquery(s.plan, outerAliases))
      }
    }
  }
}

/**
 * The rule `ResolveReferences` in the main resolution batch creates [[TempResolvedColumn]] in
 * UnresolvedHaving/Filter/Sort to hold the temporarily resolved column with `agg.child`.
 *
 * If the expression hosting [[TempResolvedColumn]] is fully resolved, the rule
 * `ResolveAggregationFunctions` will
 * - Replace [[TempResolvedColumn]] with [[AttributeReference]] if it's inside aggregate functions
 *   or grouping expressions.
 * - Mark [[TempResolvedColumn]] as `hasTried` if not inside aggregate functions or grouping
 *   expressions, hoping other rules can re-resolve it.
 * `ResolveReferences` will re-resolve [[TempResolvedColumn]] if `hasTried` is true, and keep it
 * unchanged if the resolution fails. We should turn it back to [[UnresolvedAttribute]] so that the
 * analyzer can report missing column error later.
 *
 * If the expression hosting [[TempResolvedColumn]] is not resolved, [[TempResolvedColumn]] will
 * remain with `hasTried` as false. We should strip [[TempResolvedColumn]], so that users can see
 * the reason why the expression is not resolved, e.g. type mismatch.
 */
object RemoveTempResolvedColumn extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveExpressionsWithPruning(_.containsPattern(TEMP_RESOLVED_COLUMN)) {
      case t: TempResolvedColumn =>
        if (t.hasTried) {
          UnresolvedAttribute(t.nameParts)
        } else {
          t.child
        }
    }
  }
}
