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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.Locale

import org.apache.spark.sql.catalyst.{
  FunctionIdentifier,
  QueryPlanningTracker,
  SQLConfHelper
}
import org.apache.spark.sql.catalyst.analysis.{
  ExpressionWithUnresolvedIdentifier,
  FakeLocalCatalog,
  FakeSystemCatalog,
  FunctionRegistry,
  FunctionResolution,
  GetViewColumnByNameAndOrdinal,
  MultiAlias,
  NamedParameter,
  NameParameterizedQuery,
  PlanWithUnresolvedIdentifier,
  PosParameterizedQuery,
  ResolvedInlineTable,
  Star,
  TableFunctionRegistry,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedDataFrameStar,
  UnresolvedExtractValue,
  UnresolvedFunction,
  UnresolvedHaving,
  UnresolvedInlineTable,
  UnresolvedOrdinal,
  UnresolvedPipeAggregateOrdinal,
  UnresolvedRelation,
  UnresolvedStarBase,
  UnresolvedStarExceptOrReplace,
  UnresolvedSubqueryColumnAliases,
  UnresolvedTableValuedFunction,
  UnresolvedTVFAliases,
  VariableResolution
}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.NamePlaceholder
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  AnyValue,
  First,
  Last
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, UNRESOLVED_WITH}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.internal.SQLConf.HiveCaseSensitiveInferenceMode

/**
 * [[ResolverGuard]] is a class that checks if the operator that is yet to be analyzed
 * only consists of operators and expressions that are currently supported by the
 * single-pass analyzer.
 *
 * Queries containing a hint operator whose name is not one of the known built-in hint names
 * (see [[ResolverGuard.BUILTIN_HINT_NAMES]]) are rejected so they fall back to the fixed-point
 * analyzer. Unknown hint names may be handled by custom hint resolution rules registered via
 * [[SparkSessionExtensions.injectHintResolutionRule]], which are placed in the fixed-point
 * resolution batch and applied iteratively over multiple passes - a pattern that does not fit
 * the single-pass analyzer's bottom-up resolution model.
 *
 * Operators that are not known to the guard are offered to the registered
 * [[ResolverGuardExtension]]s before they are rejected.
 *
 * This is a one-shot object and should not be reused after [[apply]] call.
 */
class ResolverGuard(
    catalogManager: CatalogManager,
    guardExtensions: Seq[ResolverGuardExtension] = Seq.empty,
    tracker: Option[QueryPlanningTracker] = None
) extends SQLConfHelper {
  private val v1SessionCatalog = catalogManager.v1SessionCatalog
  private lazy val variableResolution = new VariableResolution(
    tempVariableManager = catalogManager.tempVariableManager,
    catalogManager = catalogManager
  )

  /**
   * Check the top level operator of the parsed operator.
   */
  def apply(operator: LogicalPlan): ResolverGuardResult = {
    val unsupportedConf = detectUnsupportedConf()

    val unsupportedReason = if (unsupportedConf.isDefined) {
      Some(s"configuration: ${unsupportedConf.get}")
    } else {
      checkOperator(operator)
    }

    tryThrowUnsupportedSinglePassAnalyzerFeature(unsupportedReason)

    ResolverGuardResult(unsupportedReason)
  }

  /**
   * Check if all the operators are supported. For implemented ones, recursively check
   * their children. For unimplemented ones, return Some("reason").
   */
  private def checkOperator(operator: LogicalPlan): Option[String] = {
    operator match {
      case unresolvedWith: UnresolvedWith =>
        checkUnresolvedWith(unresolvedWith)
      case withCte: WithCTE =>
        checkWithCte(withCte)
      case project: Project =>
        checkProject(project)
      case aggregate: Aggregate =>
        checkAggregate(aggregate)
      case filter: Filter =>
        checkFilter(filter)
      case join: Join =>
        checkJoin(join)
      case asOfJoin: AsOfJoin
          if conf.getConf(
            SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION
          ) =>
        checkAsOfJoin(asOfJoin)
      case unresolvedSubqueryColumnAliases: UnresolvedSubqueryColumnAliases =>
        checkUnresolvedSubqueryColumnAliases(unresolvedSubqueryColumnAliases)
      case subqueryAlias: SubqueryAlias =>
        checkSubqueryAlias(subqueryAlias)
      case globalLimit: GlobalLimit =>
        checkGlobalLimit(globalLimit)
      case localLimit: LocalLimit =>
        checkLocalLimit(localLimit)
      case limitAll: LimitAll =>
        checkLimitAll(limitAll)
      case offset: Offset =>
        checkOffset(offset)
      case tail: Tail =>
        checkTail(tail)
      case distinct: Distinct =>
        checkDistinct(distinct)
      case view: View =>
        checkView(view)
      case unresolvedRelation: UnresolvedRelation =>
        checkUnresolvedRelation(unresolvedRelation)
      case unresolvedCteRelationRef: UnresolvedCteRelationRef =>
        checkUnresolvedCteRelationRef(unresolvedCteRelationRef)
      case unresolvedInlineTable: UnresolvedInlineTable =>
        checkUnresolvedInlineTable(unresolvedInlineTable)
      case resolvedInlineTable: ResolvedInlineTable =>
        checkResolvedInlineTable(resolvedInlineTable)
      case localRelation: LocalRelation =>
        checkLocalRelation(localRelation)
      case range: Range =>
        checkRange(range)
      case oneRowRelation: OneRowRelation =>
        checkOneRowRelation(oneRowRelation)
      case cteRelationDef: CTERelationDef =>
        checkCteRelationDef(cteRelationDef)
      case cteRelationRef: CTERelationRef =>
        checkCteRelationRef(cteRelationRef)
      case union: Union =>
        checkUnion(union)
      case setOperation: SetOperation =>
        checkSetOperation(setOperation)
      case sort: Sort =>
        checkSort(sort)
      case supervisingCommand: SupervisingCommand =>
        None
      case setVariable: SetVariable =>
        checkSetVariable(setVariable)
      case repartition: Repartition =>
        checkRepartition(repartition)
      case unresolvedHint: UnresolvedHint =>
        checkUnresolvedHint(unresolvedHint)
      case resolvedHint: ResolvedHint =>
        checkResolvedHint(resolvedHint)
      case having: UnresolvedHaving =>
        checkHaving(having)
      case sample: Sample =>
        checkSample(sample)
      case unresolvedTVF: UnresolvedTableValuedFunction =>
        checkUnresolvedTableValuedFunction(unresolvedTVF)
      case unresolvedTVFAliases: UnresolvedTVFAliases =>
        checkUnresolvedTVFAliases(unresolvedTVFAliases)
      case nameParameterizedQuery: NameParameterizedQuery =>
        checkNameParameterizedQuery(nameParameterizedQuery)
      case posParameterizedQuery: PosParameterizedQuery =>
        checkPosParameterizedQuery(posParameterizedQuery)
      case repartitionByExpression: RepartitionByExpression =>
        checkRepartitionByExpression(repartitionByExpression)
      case pipeOperator: PipeOperator =>
        checkPipeOperator(pipeOperator)
      case pivot: Pivot =>
        checkPivot(pivot)
      case unpivot: Unpivot =>
        checkUnpivot(unpivot)
      case planWithUnresolvedIdentifier: PlanWithUnresolvedIdentifier =>
        checkPlanWithUnresolvedIdentifier(planWithUnresolvedIdentifier)
      case lateralJoin: LateralJoin =>
        checkLateralJoin(lateralJoin)
      case unresolvedBinBy: UnresolvedBinBy =>
        checkBinBy(unresolvedBinBy)
      case _ =>
        tryDelegateToGuardExtension(operator)
    }
  }

  /** Try to delegate the guard check to one of the registered [[ResolverGuardExtension]]s. */
  private def tryDelegateToGuardExtension(operator: LogicalPlan): Option[String] = {
    guardExtensions.iterator.map(_.checkOperator(operator, tracker)).collectFirst {
      case GuardExtensionResult.Supported => None
      case GuardExtensionResult.NotSupported =>
        Some(s"${operator.getClass} operator resolution")
    }.getOrElse(Some(s"${operator.getClass} operator resolution"))
  }

  private object CheckOperator {
    def unapply(operator: LogicalPlan): Option[String] = checkOperator(operator)
  }

  /**
   * Method used to check if expressions are supported by the new analyzer.
   * For LeafNode types, we return None or Some("reason"). For other ones, check their children.
   */
  private def checkExpression(expression: Expression): Option[String] = {
    expression match {
      case alias: Alias =>
        checkAlias(alias)
      case multiAlias: MultiAlias =>
        checkMultiAlias(multiAlias)
      case unresolvedConditionalExpression: ConditionalExpression =>
        checkUnresolvedConditionalExpression(unresolvedConditionalExpression)
      case unresolvedCast: Cast =>
        checkUnresolvedCast(unresolvedCast)
      case unresolvedUpCast: UpCast =>
        checkUnresolvedUpCast(unresolvedUpCast)
      case unresolvedAlias: UnresolvedAlias =>
        checkUnresolvedAlias(unresolvedAlias)
      case unresolvedAttribute: UnresolvedAttribute =>
        checkUnresolvedAttribute(unresolvedAttribute)
      case literal: Literal =>
        checkLiteral(literal)
      case unresolvedOrdinal: UnresolvedOrdinal =>
        checkUnresolvedOrdinal(unresolvedOrdinal)
      case _: UnresolvedPipeAggregateOrdinal =>
        None
      case unresolvedPredicate: Predicate =>
        checkUnresolvedPredicate(unresolvedPredicate)
      case scalarSubquery: ScalarSubquery =>
        checkScalarSubquery(scalarSubquery)
      case listQuery: ListQuery =>
        checkListQuery(listQuery)
      case outerReference: OuterReference =>
        checkOuterReference(outerReference)
      case attributeReference: AttributeReference =>
        checkAttributeReference(attributeReference)
      case createNamedStruct: CreateNamedStruct =>
        checkCreateNamedStruct(createNamedStruct)
      case unresolvedFunction: UnresolvedFunction =>
        checkUnresolvedFunction(unresolvedFunction)
      // A routine may own a routed constructor, so check it like the flat call.
      case routedJsonConstructor: RoutedJsonConstructor =>
        checkUnresolvedFunction(routedJsonConstructor.toUnresolvedFunction)
      case getViewColumnByNameAndOrdinal: GetViewColumnByNameAndOrdinal =>
        checkGetViewColumnByNameAndOrdinal(getViewColumnByNameAndOrdinal)
      case semiStructuredExtract: SemiStructuredExtract =>
        checkSemiStructuredExtract(semiStructuredExtract)
      case unresolvedExtractValue: UnresolvedExtractValue =>
        checkUnresolvedExtractValue(unresolvedExtractValue)
      case star: Star =>
        checkStar(star)
      case _: UnresolvedDataFrameStar =>
        checkPlanIdTaggedReference()
      case namedArgumentExpression: NamedArgumentExpression =>
        checkNamedArgumentExpression(namedArgumentExpression)
      case namedParameter: NamedParameter =>
        checkNamedParameter(namedParameter)
      case getStructField: GetStructField =>
        checkGetStructField(getStructField)
      case getMapValue: GetMapValue =>
        checkGetMapValue(getMapValue)
      case _: JsonTable =>
        Some("JSON_TABLE")
      case lambdaFunction: LambdaFunction =>
        checkLambdaFunction(lambdaFunction)
      case unresolvedNamedLambdaVariable: UnresolvedNamedLambdaVariable =>
        checkUnresolvedNamedLambdaVariable(unresolvedNamedLambdaVariable)
      case namedLambdaVariable: NamedLambdaVariable =>
        checkNamedLambdaVariable(namedLambdaVariable)
      case NamePlaceholder =>
        checkNamePlaceholder()
      case baseGroupingSets: BaseGroupingSets =>
        checkBaseGroupingSets(baseGroupingSets)
      case pipeExpression: PipeExpression =>
        checkPipeExpression(pipeExpression)
      case expressionWithUnresolvedIdentifier: ExpressionWithUnresolvedIdentifier =>
        checkExpressionWithUnresolvedIdentifier(expressionWithUnresolvedIdentifier)
      case expression if isGenerallySupportedExpression(expression) =>
        expression.children.collectFirst { case CheckExpression(reason) => reason }
      case _ =>
        Some(s"${expression.getClass} expression resolution")
    }
  }

  private object CheckExpression {
    def unapply(expression: Expression): Option[String] = checkExpression(expression)
  }

  private object CheckExpressionSeq {
    def unapply(expressions: Seq[Expression]): Option[String] = expressions.collectFirst {
      case CheckExpression(reason) => reason
    }
  }

  private def checkUnresolvedWith(unresolvedWith: UnresolvedWith) = {
    unresolvedWith.cteRelations
      .map(_.plan)
      .collectFirst { case CheckOperator(reason) => reason }
      .orElse(checkOperator(unresolvedWith.child))
  }

  private def checkWithCte(withCte: WithCTE) = {
    withCte.children.collectFirst { case CheckOperator(reason) => reason }
  }

  private def checkProject(project: Project) = {
    checkProjectHiddenOutputTag(project)
      .orElse(checkOperator(project.child))
      .orElse {
        project.projectList.collectFirst { case CheckExpression(reason) => reason }
      }
  }

  private def checkAggregate(aggregate: Aggregate) = {
    checkOperator(aggregate.child)
      .orElse {
        aggregate.groupingExpressions.collectFirst { case CheckExpression(reason) => reason }
      }
      .orElse {
        aggregate.aggregateExpressions.collectFirst { case CheckExpression(reason) => reason }
      }
  }

  private def checkJoin(join: Join) = {
    checkOperator(join.left)
      .orElse {
        checkOperator(join.right)
      }
      .orElse {
        join.condition match {
          case Some(condition) => checkExpression(condition)
          case None => None
        }
      }
  }

  private def checkLateralJoin(lateralJoin: LateralJoin) = {
    checkOperator(lateralJoin.left)
      .orElse {
        checkOperator(lateralJoin.right.plan)
      }
      .orElse {
        lateralJoin.condition match {
          case Some(condition) => checkExpression(condition)
          case None => None
        }
      }
  }

  private def checkAsOfJoin(asOfJoin: AsOfJoin) = {
    checkOperator(asOfJoin.left)
      .orElse {
        checkOperator(asOfJoin.right)
      }
      .orElse {
        asOfJoin.condition match {
          case Some(condition) => checkExpression(condition)
          case None => None
        }
      }
      .orElse {
        asOfJoin.matchLeftOperand match {
          case Some(expression) => checkExpression(expression)
          case None => None
        }
      }
      .orElse {
        asOfJoin.matchRightOperand match {
          case Some(expression) => checkExpression(expression)
          case None => None
        }
      }
      .orElse {
        checkExpression(asOfJoin.asOfCondition)
      }
      .orElse {
        asOfJoin.orderExpression.flatMap(checkExpression)
      }
      .orElse {
        asOfJoin.tolerance match {
          case Some(expression) => checkExpression(expression)
          case None => None
        }
      }
      .orElse {
        checkExpressions(asOfJoin.leftSortExprs)
      }
      .orElse {
        checkExpressions(asOfJoin.rightSortExprs)
      }
  }

  private def checkExpressions(expressions: Seq[Expression]): Option[String] =
    expressions.iterator.map(checkExpression).collectFirst { case Some(reason) => reason }

  private def checkFilter(unresolvedFilter: Filter) =
    checkOperator(unresolvedFilter.child).orElse(checkExpression(unresolvedFilter.condition))

  private def checkUnresolvedSubqueryColumnAliases(
      unresolvedSubqueryColumnAliases: UnresolvedSubqueryColumnAliases) =
    checkOperator(unresolvedSubqueryColumnAliases.child)

  private def checkSubqueryAlias(subqueryAlias: SubqueryAlias) =
    checkOperator(subqueryAlias.child)

  private def checkGlobalLimit(globalLimit: GlobalLimit) =
    checkOperator(globalLimit.child).orElse(checkExpression(globalLimit.limitExpr))

  private def checkLocalLimit(localLimit: LocalLimit) =
    checkOperator(localLimit.child).orElse(checkExpression(localLimit.limitExpr))

  private def checkLimitAll(limitAll: LimitAll) =
    checkOperator(limitAll.child)

  private def checkOffset(offset: Offset) =
    checkOperator(offset.child).orElse(checkExpression(offset.offsetExpr))

  private def checkTail(tail: Tail) =
    checkOperator(tail.child).orElse(checkExpression(tail.limitExpr))

  private def checkDistinct(distinct: Distinct) =
    checkOperator(distinct.child)

  private def checkView(view: View) = checkOperator(view.child)

  private def checkUnresolvedInlineTable(unresolvedInlineTable: UnresolvedInlineTable) = {
    unresolvedInlineTable.rows.collectFirst { case CheckExpressionSeq(reason) => reason }
  }

  private def checkUnresolvedRelation(unresolvedRelation: UnresolvedRelation) = {
    if (unresolvedRelation.isStreaming) {
      Some("streaming relation")
    } else {
      None
    }
  }

  /** Accepts a resolver-owned CTE reference introduced by [[PlanPreprocessor]]. */
  private def checkUnresolvedCteRelationRef(
      unresolvedCteRelationRef: UnresolvedCteRelationRef) = None

  private def checkResolvedInlineTable(resolvedInlineTable: ResolvedInlineTable) = {
    resolvedInlineTable.rows.collectFirst { case CheckExpressionSeq(reason) => reason }
  }

  // Usually we don't check outputs of operators in unresolved plans, but in this case
  // [[LocalRelation]] is resolved in the parser.
  private def checkLocalRelation(localRelation: LocalRelation) =
    localRelation.output.collectFirst { case CheckExpression(reason) => reason }

  private def checkRange(range: Range) = None

  private def checkUnion(union: Union) = {
    if (union.byName) {
      Some("union by name")
    } else if (union.allowMissingCol) {
      Some("union allow missing col")
    } else {
      union.children.collectFirst { case CheckOperator(reason) => reason }
    }
  }

  private def checkSetOperation(setOperation: SetOperation) =
    setOperation.children.collectFirst { case CheckOperator(reason) => reason }

  private def checkSort(sort: Sort) =
    checkOperator(sort.child).orElse {
      sort.order.collectFirst { case CheckExpression(reason) => reason }
    }

  private def checkOneRowRelation(oneRowRelation: OneRowRelation) = None

  private def checkCteRelationDef(cteRelationDef: CTERelationDef) = {
    checkOperator(cteRelationDef.child)
  }

  private def checkCteRelationRef(cteRelationRef: CTERelationRef) = None

  private def checkAlias(alias: Alias) = checkExpression(alias.child)

  private def checkMultiAlias(multiAlias: MultiAlias) = checkExpression(multiAlias.child)

  private def checkUnresolvedConditionalExpression(
      unresolvedConditionalExpression: ConditionalExpression) =
    unresolvedConditionalExpression.children.collectFirst { case CheckExpression(reason) => reason }

  private def checkUnresolvedCast(cast: Cast) = checkExpression(cast.child)

  private def checkUnresolvedUpCast(upCast: UpCast) = checkExpression(upCast.child)

  private def checkUnresolvedExtractValue(unresolvedExtractValue: UnresolvedExtractValue) =
    unresolvedExtractValue.children.collectFirst { case CheckExpression(reason) => reason }

  /**
   * Recursively check the children of the [[Star]] expression.
   * [[UnresolvedStarExceptOrReplace]] is handled separately, because it's a leaf expression,
   * but it has replacement expressions, that could be non-trivial.
   */
  private def checkStar(star: Star) =
    checkStarTarget(star).orElse {
      star match {
        case starExceptOrReplace: UnresolvedStarExceptOrReplace =>
          starExceptOrReplace.replacements.collectFirst {
            case CheckExpressionSeq(reason) => reason
          }
        case star =>
          star.children.collectFirst { case CheckExpression(reason) => reason }
      }
    }

  private def checkStarTarget(star: Star): Option[String] = star match {
    case starBase: UnresolvedStarBase =>
      starBase.target.flatMap { targetParts =>
        targetParts
          .find(name => ResolverGuard.UNSUPPORTED_ATTRIBUTE_NAMES.contains(name))
          .map(unsupportedName =>
            s"unsupported attribute name '${unsupportedName.toLowerCase(Locale.ROOT)}'"
          )
      }
    case _ => None
  }

  private def checkNamedArgumentExpression(
      namedArgumentExpression: NamedArgumentExpression) =
    checkExpression(namedArgumentExpression.value)

  private def checkNamedParameter(namedParameter: NamedParameter) = None

  private def checkGetStructField(getStructField: GetStructField) = {
    checkExpression(getStructField.child)
  }

  private def checkGetMapValue(getMapValue: GetMapValue) = {
    getMapValue.children.collectFirst { case CheckExpression(reason) => reason }
  }

  private def checkLambdaFunction(lambdaFunction: LambdaFunction) = {
    lambdaFunction.children.collectFirst { case CheckExpression(reason) => reason }
  }

  private def checkUnresolvedNamedLambdaVariable(
      unresolvedNamedLambdaVariable: UnresolvedNamedLambdaVariable) = None

  private def checkNamedLambdaVariable(namedLambdaVariable: NamedLambdaVariable) = None

  private def checkNamePlaceholder() = None

  private def checkBaseGroupingSets(baseGroupingSets: BaseGroupingSets) = {
    baseGroupingSets.children.collectFirst { case CheckExpression(reason) => reason }
  }

  private def checkUnresolvedAlias(unresolvedAlias: UnresolvedAlias) =
    checkExpression(unresolvedAlias.child)

  /**
   * Checks whether the provided attribute is supported. It's unsupported if:
   *  - Any of its name parts is in the [[ResolverGuard.UNSUPPORTED_ATTRIBUTE_NAMES]] list.
   *  - It has the `PLAN_ID_TAG` tag and is not supported by [[checkPlanIdTaggedReference]].
   */
  private def checkUnresolvedAttribute(unresolvedAttribute: UnresolvedAttribute) = {
    val unsupportedNameOption =
      unresolvedAttribute.nameParts.find(name =>
        ResolverGuard.UNSUPPORTED_ATTRIBUTE_NAMES.contains(name)
      )

    unsupportedNameOption match {
      case Some(unsupportedName) =>
        Some(
          s"unsupported attribute name " +
          s"'${unsupportedName.toLowerCase(Locale.ROOT)}'"
        )
      case None if unresolvedAttribute.containsTag(LogicalPlan.PLAN_ID_TAG) =>
        checkPlanIdTaggedReference()
      case None =>
        None
    }
  }

  /**
   * DataFrame column references carrying `PLAN_ID_TAG` (including `df("*")`) are resolved against
   * the plan-id targets visible from the current scope, falling back to name resolution when the
   * target does not produce the column. This matches the fixed-point analyzer only when
   * [[SQLConf.STRICT_DATAFRAME_COLUMN_RESOLUTION]] is disabled.
   */
  private def checkPlanIdTaggedReference(): Option[String] = {
    if (conf.getConf(SQLConf.STRICT_DATAFRAME_COLUMN_RESOLUTION)) {
      Some("PLAN_ID_TAG with strict DataFrame column resolution")
    } else {
      None
    }
  }

  private def checkUnresolvedPredicate(unresolvedPredicate: Predicate) = unresolvedPredicate match {
    case inSubquery: InSubquery =>
      checkInSubquery(inSubquery)
    case exists: Exists =>
      checkExists(exists)
    case _ =>
      unresolvedPredicate.children.collectFirst { case CheckExpression(reason) => reason }
  }

  private def checkAttributeReference(attributeReference: AttributeReference) = None

  private def checkCreateNamedStruct(createNamedStruct: CreateNamedStruct) = {
    createNamedStruct.children.collectFirst { case CheckExpression(reason) => reason }
  }

  /**
   * There are several type of unsupported functions:
   *   - Multi-part function names, including qualified builtin/session names
   *     (e.g., `builtin.abs`, `system.builtin.abs`). These are recognized as valid
   *     qualifications but not yet supported by the single-pass resolver.
   *   - Subset of built-in functions defined in:
   *     - [[ResolverGuard.UNSUPPORTED_FUNCTION_NAMES]]
   *   - Non-builtin functions, see [[isBuiltinFunction]].
   */
  private def checkUnresolvedFunction(unresolvedFunction: UnresolvedFunction) = {
    val nameParts = unresolvedFunction.nameParts
    val funcName = nameParts.last.toLowerCase(Locale.ROOT)
    if (nameParts.size == 1) {
      // Unqualified: reject if unsupported, else non-builtin or check children (same as master)
      if (isUnsupportedFunction(funcName)) {
        Some(s"unsupported function ${funcName}")
      } else if (!isBuiltinFunction(funcName)) {
        Some("non-builtin function")
      } else {
        unresolvedFunction.children.collectFirst { case CheckExpression(reason) => reason }
      }
    } else if (FunctionResolution.sessionNamespaceKind(nameParts)
        .contains(org.apache.spark.sql.catalyst.catalog.SessionCatalog.Builtin)) {
      // Explicitly builtin-qualified: reject if unsupported, else check children
      if (isUnsupportedFunction(funcName)) {
        Some(s"unsupported function ${funcName}")
      } else {
        unresolvedFunction.children.collectFirst { case CheckExpression(reason) => reason }
      }
    } else if (FunctionResolution.sessionNamespaceKind(nameParts).isDefined) {
      Some("session-qualified user-defined function")
    } else {
      Some("multi-part function name")
    }
  }

  private def checkLiteral(literal: Literal) = None

  private def checkUnresolvedOrdinal(unresolvedOrdinal: UnresolvedOrdinal) = None

  private def checkScalarSubquery(scalarSubquery: ScalarSubquery) =
    checkOperator(scalarSubquery.plan)

  private def checkInSubquery(inSubquery: InSubquery) =
    inSubquery.values
      .collectFirst { case CheckExpression(reason) => reason }
      .orElse(
        checkExpression(inSubquery.query)
      )

  private def checkListQuery(listQuery: ListQuery) = checkOperator(listQuery.plan)

  private def checkExists(exists: Exists) = checkOperator(exists.plan)

  private def checkOuterReference(outerReference: OuterReference) =
    checkExpression(outerReference.e)

  private def checkGetViewColumnByNameAndOrdinal(
      getViewColumnByNameAndOrdinal: GetViewColumnByNameAndOrdinal) = None

  private def checkSemiStructuredExtract(semiStructuredExtract: SemiStructuredExtract) =
    checkExpression(semiStructuredExtract.child)

  private def checkRepartition(repartition: Repartition) = {
    checkOperator(repartition.child)
  }

  /**
   * Rejects [[UnresolvedHint]] operators whose name is not one of the known built-in hint names
   * (see [[ResolverGuard.BUILTIN_HINT_NAMES]]). An unknown hint name may be handled by a custom
   * hint resolution rule registered via [[SparkSessionExtensions.injectHintResolutionRule]]. Such
   * rules rely on being placed in the fixed-point resolution batch where they can be applied
   * repeatedly across multiple resolution passes, which is incompatible with single-pass
   * bottom-up resolution. When the hint name is unrecognized, the query falls back to the
   * fixed-point analyzer where any such custom rules can run as designed. The reason is a constant
   * because the hint name is user text.
   */
  private def checkUnresolvedHint(unresolvedHint: UnresolvedHint) = {
    if (!ResolverGuard.BUILTIN_HINT_NAMES.contains(
        unresolvedHint.name.toUpperCase(Locale.ROOT))) {
      Some("user defined hint")
    } else {
      checkOperator(unresolvedHint.child)
        .orElse {
          unresolvedHint.parameters.collectFirst { case CheckExpression(reason) => reason }
        }
    }
  }

  /**
   * Checks the child of a [[ResolvedHint]] operator. A [[ResolvedHint]] has already been resolved
   * (e.g. by a DataFrame program), so there is no hint name left to resolve and no custom hint
   * resolution rule needs to run; we simply recurse into the child.
   */
  private def checkResolvedHint(resolvedHint: ResolvedHint) = {
    checkOperator(resolvedHint.child)
  }

  private def checkHaving(having: UnresolvedHaving) =
    checkExpression(having.havingCondition).orElse(checkOperator(having.child))

  private def checkSample(sample: Sample) = {
    checkOperator(sample.child)
  }

  private def checkUnresolvedTableValuedFunction(unresolvedTVF: UnresolvedTableValuedFunction) = {
    if (unresolvedTVF.isStreaming) {
      Some("streaming table valued function")
    } else if (unresolvedTVF.name.size != 1) {
      Some("multi-part table valued function name")
    } else if (ResolverGuard.UNSUPPORTED_TABLE_VALUED_FUNCTIONS.contains(unresolvedTVF.name.head)) {
      Some(s"unsupported table valued function ${unresolvedTVF.name.head.toLowerCase(Locale.ROOT)}")
    } else if (ResolverGuard.GENERATOR_FUNCTION_NAMES.contains(unresolvedTVF.name.head)) {
      // Generator table-valued functions resolve to a [[Generate]] operator, which is not
      // supported yet.
      Some(s"generator table valued function ${unresolvedTVF.name.head.toLowerCase(Locale.ROOT)}")
    } else if (!isBuiltinTableFunction(unresolvedTVF.name.head)) {
      Some("UDTF")
    } else {
      unresolvedTVF.functionArgs.collectFirst { case CheckExpression(reason) => reason }
    }
  }

  private def checkUnresolvedTVFAliases(unresolvedTVFAliases: UnresolvedTVFAliases) = {
    checkOperator(unresolvedTVFAliases.child)
  }

  /**
   * Checks the [[SetVariable]] target variables and source query. Only session and scripting
   * (local) variables are supported as targets. A source query with CTEs is not supported.
   */
  private def checkSetVariable(setVariable: SetVariable): Option[String] = {
    setVariable.targetVariables.iterator
      .map(checkSetVariableTarget)
      .collectFirst { case Some(reason) => reason }
      .orElse {
        if (setVariable.sourceQuery.containsAnyPattern(UNRESOLVED_WITH, CTE)) {
          Some("SET VARIABLE with a CTE source")
        } else {
          checkOperator(setVariable.sourceQuery)
        }
      }
  }

  private def checkSetVariableTarget(target: Expression): Option[String] = target match {
    case unresolvedAttribute: UnresolvedAttribute =>
      variableResolution
        .lookupVariable(nameParts = unresolvedAttribute.nameParts)
        .flatMap { variableReference =>
          variableReference.catalog match {
            case FakeLocalCatalog | FakeSystemCatalog =>
              None
            case _ =>
              Some("SET VARIABLE with an unsupported target catalog")
          }
        }
    case _ =>
      None
  }

  /**
   * [[PosParameterizedQuery]] may wrap an unresolved plan with already-bound parameters that
   * need to be removed during analysis. If the child plan contains [[PosParameter]] nodes
   * (i.e., unbound positional parameters), the guard will reject the expression and we fall
   * back to the fixed-point analyzer. Note that when
   * [[SQLConf.LEGACY_PARAMETER_SUBSTITUTION_CONSTANTS_ONLY]] is true, positional parameters
   * are not bound before analysis, so the child plan will contain [[PosParameter]] nodes and
   * the guard will reject on the expression-level check instead.
   */
  private def checkPosParameterizedQuery(posParameterizedQuery: PosParameterizedQuery) = {
    checkOperator(posParameterizedQuery.child)
      .orElse {
        posParameterizedQuery.args.collectFirst { case CheckExpression(reason) => reason }
      }
  }

  private def checkBinBy(unresolvedBinBy: UnresolvedBinBy): Option[String] = {
    checkOperator(unresolvedBinBy.child)
      .orElse(checkExpression(unresolvedBinBy.binWidthExpr))
      .orElse(checkExpression(unresolvedBinBy.rangeStartCol))
      .orElse(checkExpression(unresolvedBinBy.rangeEndCol))
      .orElse(unresolvedBinBy.originExpr.flatMap(checkExpression))
      .orElse(unresolvedBinBy.distributeColumns match {
        case CheckExpressionSeq(reason) => Some(reason)
        case _ => None
      })
  }

  private def checkNameParameterizedQuery(nameParameterizedQuery: NameParameterizedQuery) = {
    checkOperator(nameParameterizedQuery.child)
      .orElse {
        nameParameterizedQuery.argValues.collectFirst { case CheckExpression(reason) => reason }
      }
  }

  private def checkRepartitionByExpression(repartitionByExpression: RepartitionByExpression) = {
    checkOperator(repartitionByExpression.child)
      .orElse {
        repartitionByExpression.partitionExpressions.collectFirst {
          case CheckExpression(reason) => reason
        }
      }
  }

  private def checkPipeOperator(pipeOperator: PipeOperator) = {
    checkOperator(pipeOperator.child)
  }

  private def checkPipeExpression(pipeExpression: PipeExpression) = {
    checkExpression(pipeExpression.child)
  }

  private def checkPivot(pivot: Pivot) = {
    checkOperator(pivot.child)
      .orElse {
        checkExpression(pivot.pivotColumn)
      }
      .orElse {
        pivot.pivotValues.collectFirst { case CheckExpression(reason) => reason }
      }
      .orElse {
        pivot.aggregates.collectFirst { case CheckExpression(reason) => reason }
      }
      .orElse {
        pivot.groupByExprsOpt.flatMap { groupByExprs =>
          groupByExprs.collectFirst { case CheckExpression(reason) => reason }
        }
      }
  }

  private def checkUnpivot(unpivot: Unpivot) = {
    checkOperator(unpivot.child)
      .orElse {
        unpivot.ids.flatMap { ids =>
          ids.collectFirst { case CheckExpression(reason) => reason }
        }
      }
      .orElse {
        unpivot.values.flatMap { values =>
          values.collectFirst { case CheckExpressionSeq(reason) => reason }
        }
      }
  }

  private def checkPlanWithUnresolvedIdentifier(
      planWithUnresolvedIdentifier: PlanWithUnresolvedIdentifier): Option[String] = {
    checkExpression(planWithUnresolvedIdentifier.identifierExpr).orElse {
      planWithUnresolvedIdentifier.children.collectFirst {
        case CheckOperator(reason) => reason
      }
    }
  }

  private def checkExpressionWithUnresolvedIdentifier(
      expressionWithUnresolvedIdentifier: ExpressionWithUnresolvedIdentifier): Option[String] = {
    checkExpression(expressionWithUnresolvedIdentifier.identifierExpr).orElse {
      expressionWithUnresolvedIdentifier.otherExprs.collectFirst {
        case CheckExpression(reason) => reason
      }
    }
  }

  /**
   * Most of the expressions come from resolving the [[UnresolvedFunction]], but here we have some
   * popular expressions allowlist for two reasons:
   *   1. Some of them are allocated in the Parser;
   *   2. To allow the resolution of resolved DataFrame subtrees.
   */
  private def isGenerallySupportedExpression(expression: Expression): Boolean = {
    expression match {
      // Math
      case _: UnaryMinus | _: BinaryArithmetic | _: LeafMathExpression | _: UnaryMathExpression |
          _: UnaryLogExpression | _: BinaryMathExpression | _: BitShiftOperation | _: RoundCeil |
          _: Conv | _: RoundBase | _: Factorial | _: Bin | _: Hex | _: Unhex | _: WidthBucket |
          _: UnaryPositive | _: BitwiseNot =>
        true
      // Strings
      case _: Collate | _: Collation | _: ResolvedCollation | _: UnresolvedCollation | _: Concat |
          _: Mask | _: ConcatWs | _: Elt | _: Upper | _: Lower | _: BinaryPredicate |
          _: StringPredicate | _: IsValidUTF8 | _: MakeValidUTF8 | _: ValidateUTF8 |
          _: TryValidateUTF8 | _: StringReplace | _: Overlay | _: StringTranslate | _: FindInSet |
          _: String2TrimExpression | _: StringTrimBoth | _: StringInstr | _: SubstringIndex |
          _: StringLocate | _: StringLPad | _: BinaryPad | _: StringRPad | _: FormatString |
          _: InitCap | _: StringRepeat | _: StringSpace | _: Substring | _: Right | _: Left |
          _: Length | _: BitLength | _: OctetLength | _: Levenshtein | _: SoundEx | _: Ascii |
          _: Chr | _: Base64 | _: UnBase64 | _: Decode | _: StringDecode | _: Encode | _: ToBinary |
          _: FormatNumber | _: Sentences | _: StringSplitSQL | _: SplitPart | _: Empty2Null |
          _: Luhncheck | _: StringInstrWithOccurrence =>
        true
      // Datetime
      case _: CurrentTime | _: CurrentTimestampLike | _: TimeZoneAwareExpression =>
        true
      // Decimal
      case _: UnscaledValue | _: MakeDecimal | _: CheckOverflow | _: CheckOverflowInSum |
          _: DecimalAddNoOverflowCheck | _: DecimalSubtractNoOverflowCheck |
          _: DecimalDivideWithOverflowCheck =>
        true
      // Interval
      case _: ExtractIntervalPart[_] | _: IntervalNumOperation | _: MultiplyInterval |
          _: DivideInterval | _: TryMakeInterval | _: MakeInterval | _: MakeDTInterval |
          _: MakeYMInterval | _: MultiplyYMInterval | _: MultiplyDTInterval | _: DivideYMInterval |
          _: DivideDTInterval =>
        true
      // Number format
      case _: ToNumber | _: TryToNumber | _: ToCharacter =>
        true
      // Random
      case _: Rand | _: Randn | _: Uniform | _: RandStr =>
        true
      // Regexp
      case _: Like | _: ILike | _: LikeAll | _: NotLikeAll | _: LikeAny | _: NotLikeAny | _: RLike |
          _: StringSplit | _: RegExpReplace | _: RegExpExtract | _: RegExpExtractAll |
          _: RegExpCount | _: RegExpSubStr | _: RegExpInStr =>
        true
      // JSON
      case _: GetJsonObject | _: JsonTuple | _: JsonToStructs | _: StructsToJson |
          _: SchemaOfJson | _: JsonObjectKeys | _: LengthOfJsonArray | _: JsonArray |
          _: JsonExists | _: JsonQuery | _: JsonValue | _: JsonObjectExpr |
          _: StockJsonConstructor =>
        true
      // CSV
      case _: SchemaOfCsv | _: StructsToCsv | _: CsvToStructs =>
        true
      // URL
      case _: TryParseUrl | _: ParseUrl | _: UrlEncode | _: UrlDecode | _: TryUrlDecode =>
        true
      // XML
      case _: XmlToStructs | _: SchemaOfXml | _: StructsToXml =>
        true
      // Complex type creation
      case _: CreateArray =>
        true
      // Misc
      case _: SortOrder | _: TaggingExpression =>
        true
      // Aggregate
      case _: AggregateExpression | _: AnyValue | _: First | _: Last =>
        true
      case _ =>
        false
    }
  }

  private def detectUnsupportedConf(): Option[String] = {
    if (conf.caseSensitiveAnalysis) {
      Some("caseSensitiveAnalysis")
    } else if (conf.caseSensitiveInferenceMode != HiveCaseSensitiveInferenceMode.NEVER_INFER) {
      Some("hiveCaseSensitiveInferenceMode")
    } else if (conf.getConf(SQLConf.LEGACY_INLINE_CTE_IN_COMMANDS)) {
      Some("legacyInlineCTEInCommands")
    } else if (conf.getConf(SQLConf.LEGACY_CTE_PRECEDENCE_POLICY) !=
      LegacyBehaviorPolicy.CORRECTED) {
      Some("legacyCTEPrecedencePolicy")
    } else if (!conf.getConf(SQLConf.PRIORITIZE_ORDINAL_RESOLUTION_IN_SORT)) {
      Some("prioritizeOrdinalResolutionInSortDisabled")
    } else if (conf.getConf(SQLConf.PERSISTENT_CATALOG_FIRST)) {
      Some("persistentCatalogFirst")
    } else {
      None
    }
  }

  private def isUnsupportedFunction(name: String): Boolean = {
    ResolverGuard.UNSUPPORTED_FUNCTION_NAMES.contains(name) ||
    ResolverGuard.GENERATOR_FUNCTION_NAMES.contains(name)
  }

  private def isBuiltinFunction(singlePartName: String) = {
    FunctionRegistry.functionSet.contains(
      FunctionRegistry.builtinFunctionIdentifier(singlePartName)) && v1SessionCatalog
      .lookupBuiltinOrTempFunction(singlePartName)
      .exists(info => info.getSource == "built-in")
  }

  private def isBuiltinTableFunction(singlePartName: String): Boolean = {
    val name = singlePartName.toLowerCase(Locale.ROOT)
    val onePart = FunctionIdentifier(name)
    val threePart = FunctionRegistry.builtinFunctionIdentifier(name)
    TableFunctionRegistry.functionSet.contains(onePart) ||
      TableFunctionRegistry.functionSet.contains(threePart)
  }

  private def tryThrowUnsupportedSinglePassAnalyzerFeature(reason: Option[String]): Unit = {
    reason match {
      case Some(reason)
          if conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_THROW_FROM_RESOLVER_GUARD) =>
        throw QueryCompilationErrors.unsupportedSinglePassAnalyzerFeature(reason)
      case _ =>
    }
  }

  /**
   * Check if the [[Project]] was created during [[NaturalJoin]] resolution.
   *
   * We currently do not support a second resolution because information about the hidden output of
   * [[NaturalJoin]] is lost during the first resolution.
   */
  private def checkProjectHiddenOutputTag(project: Project): Option[String] = {
    project.getTagValue(Project.hiddenOutputTag) match {
      case Some(_) => Some("NaturalJoin second resolution")
      case None => None
    }
  }
}

/**
 * Creates [[ResolverGuard]]s that share the same configuration. A [[ResolverGuard]] is a one-shot
 * object, so a fresh instance is created for every check.
 */
class ResolverGuardFactory(
    catalogManager: CatalogManager,
    resolverExtensions: Seq[ResolverExtension],
    tracker: Option[QueryPlanningTracker] = None) {

  def create(): ResolverGuard = {
    ResolverGuard(
      catalogManager = catalogManager,
      resolverExtensions = resolverExtensions,
      tracker = tracker
    )
  }
}

object ResolverGuard {

  /** Creates a guard configured from resolver extensions. */
  def apply(
      catalogManager: CatalogManager,
      resolverExtensions: Seq[ResolverExtension],
      tracker: Option[QueryPlanningTracker] = None): ResolverGuard = {
    new ResolverGuard(
      catalogManager = catalogManager,
      guardExtensions = resolverExtensions.collect {
        case extension: ResolverGuardExtension => extension
      },
      tracker = tracker
    )
  }

  private val UNSUPPORTED_ATTRIBUTE_NAMES = {
    val map = new IdentifierMap[Unit]()

    // Not supported until we support their ''real'' function counterparts.
    map += ("current_user", ())
    map += ("user", ())
    map += ("session_user", ())

    map
  }

  /**
   * Generator functions.
   */
  private val GENERATOR_FUNCTION_NAMES = {
    val map = new IdentifierMap[Unit]()
    map += ("collations", ())
    map += ("explode", ())
    map += ("explode_outer", ())
    map += ("inline", ())
    map += ("inline_outer", ())
    map += ("json_tuple", ())
    map += ("posexplode", ())
    map += ("posexplode_outer", ())
    map += ("stack", ())
    map += ("sql_keywords", ())
    map += ("variant_explode", ())
    map += ("variant_explode_outer", ())
    map
  }

  private val UNSUPPORTED_FUNCTION_NAMES = {
    val map = new IdentifierMap[Unit]()
    // User info functions are not supported.
    map += ("current_user", ())
    map += ("session_user", ())
    map += ("user", ())
    // Functions that require session/time window resolution.
    map += ("session_window", ())
    map += ("window", ())
    map += ("window_time", ())
    // Functions that produce wrong schemas/plans because of alias assignment.
    map += ("from_json", ())
  }

  /**
   * The names of all built-in (non user-defined) SQL query hints that the single-pass analyzer
   * knows how to resolve, dispatched on by [[HintResolver]]. Names are upper-cased so lookups
   * must upper-case the candidate name with [[java.util.Locale.ROOT]] before checking membership.
   * A hint whose name is absent from this set is rejected by [[checkUnresolvedHint]] so the query
   * falls back to the fixed-point analyzer (where custom hint resolution rules can run). Kept in
   * alphabetical order.
   */
  val BUILTIN_HINT_NAMES: Set[String] = Set(
    "BROADCAST",
    "BROADCASTJOIN",
    "COALESCE",
    "MAPJOIN",
    "MERGE",
    "MERGEJOIN",
    "REBALANCE",
    "REBALANCE_BY_SIZE",
    "REPARTITION",
    "REPARTITION_BY_RANGE",
    "SHUFFLE_HASH",
    "SHUFFLE_MERGE",
    "SHUFFLE_REPLICATE_NL"
  )

  private val UNSUPPORTED_TABLE_VALUED_FUNCTIONS = {
    val map = new IdentifierMap[Unit]()
    map += ("python_worker_logs", ())
    map
  }
}
