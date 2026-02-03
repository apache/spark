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

import scala.util.control.NonFatal

import com.databricks.sql.DatabricksSQLConf
import com.databricks.sql.acl.TrustedPlan
import com.databricks.sql.catalyst.TemporalTableIdentifier
import com.databricks.sql.catalyst.catalog.SessionTempTableCatalogEdgeInterface
import com.databricks.sql.catalyst.expressions.ai.{AIFunctionBase, AIGen, AIQueryBase}
import com.databricks.sql.catalyst.plans.logical.ExplainResult
import com.databricks.sql.expressions.GetSecretImplementation

import org.apache.spark.sql.catalyst.{
  FunctionIdentifier,
  MetricKey,
  QueryPlanningTracker,
  SQLConfHelper,
  SqlScriptingContextManager
}
import org.apache.spark.sql.catalyst.analysis.{
  DummyMultiStatementTransactionAccessor,
  FunctionRegistry,
  GetViewColumnByNameAndOrdinal,
  MultiStatementTransactionAccessor,
  NamedParameter,
  NameParameterizedQuery,
  ResolvedInlineTable,
  Star,
  TableFunctionRegistry,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedExtractValue,
  UnresolvedFunction,
  UnresolvedHaving,
  UnresolvedInlineTable,
  UnresolvedOrdinal,
  UnresolvedRelation,
  UnresolvedStar,
  UnresolvedStarExceptOrReplace,
  UnresolvedSubqueryColumnAliases,
  UnresolvedTableValuedFunction
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
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.internal.SQLConf.HiveCaseSensitiveInferenceMode

/**
 * [[ResolverGuard]] is a class that checks if the operator that is yet to be analyzed
 * only consists of operators and expressions that are currently supported by the
 * single-pass analyzer.
 *
 * This is a one-shot object and should not be reused after [[apply]] call.
 */
class ResolverGuard(
    catalogManager: CatalogManager,
    // BEGIN-EDGE
    multiStatementTransactionAccessor: MultiStatementTransactionAccessor =
      new DummyMultiStatementTransactionAccessor,
    // END-EDGE
    tracker: Option[QueryPlanningTracker] = None
) extends SQLConfHelper {
  private val v1SessionCatalog = catalogManager.v1SessionCatalog

  /**
   * Check the top level operator of the parsed operator.
   */
  def apply(operator: LogicalPlan): ResolverGuardResult = {
    val unsupportedConf = detectUnsupportedConf()

    val unsupportedReason = if (unsupportedConf.isDefined) {
      Some(s"configuration: ${unsupportedConf.get}")
    } else if (!checkTempVariables()) {
      Some("temp variables")
    } else if (!checkScriptingVariables()) {
      Some("scripting variables")
      // BEGIN-EDGE
    } else if (multiStatementTransactionAccessor.isActive) {
      Some("multi-statement transaction")
    } else if (conf.getConf(DatabricksSQLConf.SQL_TEMP_TABLE_CREATE_ENABLED)
      && sessionTempTableNamespaceCreated()) {
      Some("temp tables")
      // END-EDGE
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
    checkOperatorSecondPassAnalysis(operator)

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
      case unresolvedSubqueryColumnAliases: UnresolvedSubqueryColumnAliases =>
        checkUnresolvedSubqueryColumnAliases(unresolvedSubqueryColumnAliases)
      case subqueryAlias: SubqueryAlias =>
        checkSubqueryAlias(subqueryAlias)
      case globalLimit: GlobalLimit =>
        checkGlobalLimit(globalLimit)
      case localLimit: LocalLimit =>
        checkLocalLimit(localLimit)
      case limitAll: LimitAll
        if conf.getConf(
          DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_RECURSIVE_CTE_RESOLUTION
        ) =>
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
      // BEGIN-EDGE
      case explainNode: ExplainResult
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_EXPLAIN_NODE_RESOLUTION
          ) =>
        recordExperimentalFeatureUsed("ExplainNode")
        checkExplainNode(explainNode)
      case signalStatement: SignalStatement =>
        checkSignalStatement(signalStatement)
      // END-EDGE
      case repartition: Repartition =>
        checkRepartition(repartition)
      case having: UnresolvedHaving =>
        checkHaving(having)
      case sample: Sample =>
        checkSample(sample)
      case unresolvedTVF: UnresolvedTableValuedFunction
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_TVF_RESOLUTION
          ) =>
        recordExperimentalFeatureUsed("UnresolvedTableValuedFunction")
        checkUnresolvedTableValuedFunction(unresolvedTVF)
      // BEGIN-EDGE
      case trustedPlan: TrustedPlan
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_TRUSTED_PLAN_RESOLUTION
          ) =>
        recordExperimentalFeatureUsed("TrustedPlan")
        checkTrustedPlan(trustedPlan)
      // END-EDGE
      case nameParameterizedQuery: NameParameterizedQuery
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_PARAMETER_RESOLUTION
          ) =>
        recordExperimentalFeatureUsed("NameParameterizedQuery")
        checkNameParameterizedQuery(nameParameterizedQuery)
      case repartitionByExpression: RepartitionByExpression
          if conf.getConf(
            // scalastyle:off line.size.limit
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_REPARTITION_BY_EXPRESSION_RESOLUTION
            // scalastyle:on line.size.limit
          ) =>
        checkRepartitionByExpression(repartitionByExpression)
      case pivot: Pivot
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_PIVOT_RESOLUTION
          ) =>
        checkPivot(pivot)
      case unpivot: Unpivot
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_UNPIVOT_RESOLUTION
          ) =>
        checkUnpivot(unpivot)
      case _ =>
        Some(s"${operator.getClass} operator resolution")
    }
  }

  private object CheckOperator {
    def unapply(operator: LogicalPlan): Option[String] = checkOperator(operator)
  }

  private def checkOperatorSecondPassAnalysis(operator: LogicalPlan): Unit = {
    if (operator.analyzed) {
      tracker match {
        case Some(tracker) =>
          tracker.setNumericMetric(
            MetricKey.SINGLE_PASS_ANALYZER_RESOLVER_GUARD_DETECTED_SECOND_PASS_ANALYSIS,
            1.0
          )
        case _ =>
      }
    }
  }

  /**
   * Method used to check if expressions are supported by the new analyzer.
   * For LeafNode types, we return None or Some("reason"). For other ones, check their children.
   */
  private def checkExpression(expression: Expression): Option[String] = {
    expression match {
      case alias: Alias =>
        checkAlias(alias)
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
      case getViewColumnByNameAndOrdinal: GetViewColumnByNameAndOrdinal =>
        checkGetViewColumnByNameAndOrdinal(getViewColumnByNameAndOrdinal)
      case semiStructuredExtract: SemiStructuredExtract =>
        checkSemiStructuredExtract(semiStructuredExtract)
      case unresolvedExtractValue: UnresolvedExtractValue
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_EXTRACT_VALUE_RESOLUTION
          ) =>
        recordExperimentalFeatureUsed("UnresolvedExtractValue")
        checkUnresolvedExtractValue(unresolvedExtractValue)
      case star: Star
          if conf.getConf(
            // scalastyle:off line.size.limit
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_EXTENDED_STAR_USE_CASES_RESOLUTION
            // scalastyle:on line.size.limit
          ) =>
        recordExperimentalFeatureUsed("Star")
        checkStar(star)
      case namedParameter: NamedParameter
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_PARAMETER_RESOLUTION
          ) =>
        recordExperimentalFeatureUsed("NamedParameter")
        checkNamedParameter(namedParameter)
      case getStructField: GetStructField
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_GET_STRUCT_FIELD_RESOLUTION
          ) =>
        checkGetStructField(getStructField)
      case windowExpression: WindowExpression
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_WINDOW_RESOLUTION
          ) =>
        recordExperimentalFeatureUsed("WindowExpression")
        checkWindowExpression(windowExpression)
      case windowSpecDefinition: WindowSpecDefinition
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_WINDOW_RESOLUTION
          ) =>
        recordExperimentalFeatureUsed("WindowSpecDefinition")
        checkWindowSpecDefinition(windowSpecDefinition)
      case windowFrame: WindowFrame
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_WINDOW_RESOLUTION
          ) =>
        recordExperimentalFeatureUsed("WindowFrame")
        checkWindowFrame(windowFrame)
      case lambdaFunction: LambdaFunction
          if conf.getConf(
            // scalastyle:off line.size.limit
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_HIGHER_ORDER_FUNCTIONS_RESOLUTION
            // scalastyle:on line.size.limit
          ) =>
        checkLambdaFunction(lambdaFunction)
      case unresolvedNamedLambdaVariable: UnresolvedNamedLambdaVariable
          if conf.getConf(
            // scalastyle:off line.size.limit
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_HIGHER_ORDER_FUNCTIONS_RESOLUTION
            // scalastyle:on line.size.limit
          ) =>
        checkUnresolvedNamedLambdaVariable(unresolvedNamedLambdaVariable)
      case namedLambdaVariable: NamedLambdaVariable
          if conf.getConf(
            // scalastyle:off line.size.limit
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_HIGHER_ORDER_FUNCTIONS_RESOLUTION
            // scalastyle:on line.size.limit
          ) =>
        checkNamedLambdaVariable(namedLambdaVariable)
      case NamePlaceholder
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_NAME_PLACEHOLDER_RESOLUTION
          ) =>
        checkNamePlaceholder()
      case baseGroupingSets: BaseGroupingSets
          if conf.getConf(
            DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_GROUPING_ANALYTICS_RESOLUTION
          ) =>
        checkBaseGroupingSets(baseGroupingSets)
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
    if (unresolvedWith.allowRecursion && !conf.getConf(
        DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_RECURSIVE_CTE_RESOLUTION
      )) {
      Some("Recursive CTE")
    } else {
      unresolvedWith.cteRelations
        .map(cteDefinition => cteDefinition._2)
        .collectFirst { case CheckOperator(reason) => reason }
        .orElse(checkOperator(unresolvedWith.child))
    }
  }

  private def checkWithCte(withCte: WithCTE) = {
    withCte.children.collectFirst { case CheckOperator(reason) => reason }
  }

  private def checkProject(project: Project) = {
    checkProjectHiddenOutputTag(project)
      .orElse(checkOperator(project.child))
      .orElse {
        project.projectList.filterNot(_.isInstanceOf[UnresolvedStar]).collectFirst {
          case CheckExpression(reason) => reason
        }
      }
  }

  private def checkAggregate(aggregate: Aggregate) = {
    checkOperator(aggregate.child)
      .orElse {
        aggregate.groupingExpressions.collectFirst { case CheckExpression(reason) => reason }
      }
      .orElse {
        aggregate.aggregateExpressions.filterNot(_.isInstanceOf[UnresolvedStar]).collectFirst {
          case CheckExpression(reason) => reason
        }
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
      // BEGIN-EDGE
    } else if (isTimeTravel(unresolvedRelation)) {
      Some("time travel")
      // END-EDGE
    } else {
      None
    }
  }

  // BEGIN-EDGE
  private def isTimeTravel(unresolvedRelation: UnresolvedRelation): Boolean = {
    unresolvedRelation.options.containsKey("versionAsOf") ||
    unresolvedRelation.options.containsKey("timestampAsOf") || {
      unresolvedRelation.multipartIdentifier match {
        case TemporalTableIdentifier(_) => true
        case _ => false
      }
    }
  }
  // END-EDGE

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
  private def checkStar(star: Star) = star match {
    case starExceptOrReplace: UnresolvedStarExceptOrReplace =>
      starExceptOrReplace.replacements.collectFirst { case CheckExpressionSeq(reason) => reason }
    case star =>
      star.children.collectFirst { case CheckExpression(reason) => reason }
  }

  private def checkNamedParameter(namedParameter: NamedParameter) = None

  private def checkGetStructField(getStructField: GetStructField) = {
    checkExpression(getStructField.child)
  }

  private def checkWindowExpression(windowExpression: WindowExpression) =
    windowExpression.children.collectFirst { case CheckExpression(reason) => reason }

  private def checkWindowSpecDefinition(windowSpecDefinition: WindowSpecDefinition) =
    windowSpecDefinition.children.collectFirst { case CheckExpression(reason) => reason }

  private def checkWindowFrame(windowFrame: WindowFrame) =
    windowFrame.children.collectFirst { case CheckExpression(reason) => reason }

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
   *  - It has the `PLAN_ID_TAG` tag.
   */
  private def checkUnresolvedAttribute(unresolvedAttribute: UnresolvedAttribute) = {
    val unsupportedNameOption =
      unresolvedAttribute.nameParts.find(ResolverGuard.UNSUPPORTED_ATTRIBUTE_NAMES.contains)

    unsupportedNameOption match {
      case Some(unsupportedName) =>
        Some(
          s"unsupported attribute name " +
          s"'${unsupportedName.toLowerCase(Locale.ROOT)}'"
        )
      case None if unresolvedAttribute.containsTag(LogicalPlan.PLAN_ID_TAG) =>
        Some("PLAN_ID_TAG")
      case None =>
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
   *   - Multi-part function names.
   *   - Subset of built-in functions defined in:
   *     - [[ResolverGuard.UNSUPPORTED_FUNCTION_NAMES]]
   *     - [[ResolverGuard.SUPPORTED_EXPERIMENTAL_FUNCTION_NAMES]] when the experimental functions
   *       flag (`DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_EXPERIMENTAL_FUNCTIONS`)
   *       is disabled.
   *     - [[ResolverGuard.HIGHER_ORDER_FUNCTIONS]] when the experimental functions flag guarding
   *       higher order functions is disabled. See
   *       `ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_HIGHER_ORDER_FUNCTIONS_RESOLUTION` in the
   *       [[DatabricksSQLConf]].
   *   - Non-builtin functions, see [[isBuiltinFunction]].
   */
  private def checkUnresolvedFunction(unresolvedFunction: UnresolvedFunction) = {
    val singlePartName = unresolvedFunction.nameParts.head.toLowerCase(Locale.ROOT)

    if (unresolvedFunction.nameParts.size != 1) {
      Some("multi-part function name")
    } else if (isUnsupportedFunction(singlePartName)) {
      Some(s"unsupported function ${singlePartName}")
    } else if (!isBuiltinFunction(singlePartName)) {
      Some("non-builtin function")
    } else {
      unresolvedFunction.children.collectFirst { case CheckExpression(reason) => reason }
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
  // BEGIN-EDGE

  private def checkExplainNode(explainNode: ExplainResult) = {
    checkOperator(explainNode.logicalPlan)
  }

  private def checkSignalStatement(signalStatement: SignalStatement) = {
    checkExpression(signalStatement.messageExpr).orElse {
      signalStatement.messageArgumentsExpr.collectFirst { case CheckExpression(reason) => reason }
    }
  }
  // END-EDGE

  private def checkRepartition(repartition: Repartition) = {
    checkOperator(repartition.child)
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
    } else if (!TableFunctionRegistry.functionSet.contains(
        FunctionIdentifier(unresolvedTVF.name.head.toLowerCase(Locale.ROOT))
      )) {
      Some("UDTF")
    } else if (!ResolverGuard.SUPPORTED_TABLE_VALUED_FUNCTIONS.contains(
        unresolvedTVF.name.head
      )) {
      Some(s"unsupported table valued function ${unresolvedTVF.name.head.toLowerCase(Locale.ROOT)}")
    } else {
      unresolvedTVF.functionArgs.collectFirst { case CheckExpression(reason) => reason }
    }
  }
  // BEGIN-EDGE

  private def checkTrustedPlan(trustedPlan: TrustedPlan) = {
    checkOperator(trustedPlan.child)
  }
  // END-EDGE

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
          _: Luhncheck =>
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
      case _: GetJsonObjectBase | _: JsonTupleBase | _: JsonToStructs | _: StructsToJson |
          _: SchemaOfJson | _: JsonObjectKeys | _: LengthOfJsonArray =>
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
      // Misc
      case _: SortOrder | _: TaggingExpression | _: GetSecretImplementation =>
        true
      // Aggregate
      case _: AggregateExpression | _: AnyValue | _: First | _: Last =>
        true
      // AI functions
      case _: AIFunctionBase | _: AIGen | _: AIQueryBase =>
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
    } else if (conf.getConfString("pipelines.id", null) != null) {
      Some("dlt")
    } else {
      None
    }
  }

  private def checkTempVariables() = {
    conf.getConf(DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_TEMP_VARIABLE_RESOLUTION) ||
    catalogManager.tempVariableManager.isEmpty
  }

  private def checkScriptingVariables() = {
    conf.getConf(
      DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_SCRIPTING_VARIABLE_RESOLUTION
    ) ||
    SqlScriptingContextManager.get().map(_.getVariableManager).forall(_.isEmpty)
  }

  private def isUnsupportedFunction(name: String): Boolean = {
    val isUnsupportedFunction = ResolverGuard.UNSUPPORTED_FUNCTION_NAMES.contains(name)
    val isExperimentalFunction = ResolverGuard.SUPPORTED_EXPERIMENTAL_FUNCTION_NAMES.contains(name)
    val experimentalFunctionsDisabled = !conf.getConf(
      DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_EXPERIMENTAL_FUNCTIONS
    )
    val isHigherOrderFunction = ResolverGuard.HIGHER_ORDER_FUNCTIONS.contains(name)
    val higherOrderFunctionsDisabled = !conf.getConf(
      DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_HIGHER_ORDER_FUNCTIONS_RESOLUTION
    )

    isUnsupportedFunction ||
    (isExperimentalFunction && experimentalFunctionsDisabled) ||
    (isHigherOrderFunction && higherOrderFunctionsDisabled)
  }

  private def isBuiltinFunction(singlePartName: String) = {
    FunctionRegistry.functionSet.contains(FunctionIdentifier(singlePartName)) && v1SessionCatalog
      .lookupBuiltinOrTempFunction(singlePartName)
      .exists(info => info.getSource == "built-in")
  }

  // BEGIN-EDGE
  private def sessionTempTableNamespaceCreated(): Boolean = {
    try {
      catalogManager.sessionTempTableCatalog
        .asInstanceOf[SessionTempTableCatalogEdgeInterface]
        .sessionTempTableNamespaceExists()
    } catch {
      // Exceptions thrown during this process e.g. UC disabled or Delta disabled, implies no temp
      // table exists.
      case NonFatal(e) =>
        false
    }
  }
  // END-EDGE

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

  /**
   * Record an experimental feature. Do that by setting the
   * `MetricKey.SINGLE_PASS_ANALYZER_EXPERIMENTAL_FEATURE` metric to the provided reason
   * (experimental feature name).
   */
  private def recordExperimentalFeatureUsed(reason: String): Unit = {
    tracker match {
      case Some(tracker) =>
        tracker.recordStringMetric(
          MetricKey.SINGLE_PASS_ANALYZER_EXPERIMENTAL_FEATURE.toString,
          reason
        )
      case None =>
    }
  }
}

object ResolverGuard {

  private val UNSUPPORTED_ATTRIBUTE_NAMES = {
    val map = new IdentifierMap[Unit]()

    // Not supported until we support their ''real'' function counterparts.
    map += ("current_user", ())
    map += ("user", ())
    map += ("session_user", ())

    // Not supported until we support GroupingSets/Cube/Rollup.
    map += ("grouping__id", ())

    /**
     * Metadata column resolution is not supported for now
     */
    map += ("_metadata", ())

    map
  }

  private val UNSUPPORTED_FUNCTION_NAMES = {
    val map = new IdentifierMap[Unit]()
    // User info functions are not supported.
    // [[InlineUserInfoExpressions]] cannot be invoked as a post-hoc rule as it produces
    // inconsistent aliases based on the table type (because of the rule ordering in fixed-point).
    // BEGIN-EDGE
    map += ("current_oauth_custom_identity_claim", ())
    map += ("current_metastore", ())
    map += ("current_recipient", ())
    map += ("is_account_group_member", ())
    map += ("is_member", ())
    map += ("user_home_catalog", ())
    // END-EDGE
    map += ("current_user", ())
    map += ("session_user", ())
    map += ("user", ())
    // Functions that require generator support.
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
    // Functions that require session/time window resolution.
    map += ("session_window", ())
    map += ("window", ())
    map += ("window_time", ())
    // Functions that are not resolved properly.
    // Functions that produce wrong schemas/plans because of alias assignment.
    map += ("ai_query", ()) // EDGE
    map += ("from_json", ())
    // BEGIN-EDGE
    // Functions that expose secrets require running the
    // [[RedactSecretValuesFromQueryResultsInAnalyzer]] rule which is not supported. After migrating
    // to the [[RedactSecretValuesFromQueryResultsInOptimizer]] rule, those functions can be safely
    // removed from this list. See [[DatabricksSQLConf.SQL_REDACT_SECRETS_IN_OPTIMIZER]]
    map += ("secret", ())
    map += ("try_secret", ())
    // END-EDGE
  }

  /**
   * Functions supported under the
   * `DatabricksSQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_EXPERIMENTAL_FUNCTIONS` flag.
   */
  private val SUPPORTED_EXPERIMENTAL_FUNCTION_NAMES = {
    val map = new IdentifierMap[Unit]()
    map += ("ai_complete", ())
    map += ("ai_embed", ())
    map += ("collate", ())
    map += ("schema_of_json", ())
    map += ("schema_of_xml", ())
  }

  /**
   * Higher order functions that are supported but guarded under the
   * `ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_HIGHER_ORDER_FUNCTIONS_RESOLUTION` flag.
   */
  private val HIGHER_ORDER_FUNCTIONS = {
    val map = new IdentifierMap[Unit]()
    map += ("aggregate", ())
    map += ("array_sort", ())
    map += ("exists", ())
    map += ("filter", ())
    map += ("forall", ())
    map += ("map_filter", ())
    map += ("map_zip_with", ())
    map += ("reduce", ())
    map += ("transform", ())
    map += ("transform_keys", ())
    map += ("transform_values", ())
    map += ("zip_with", ())
    map
  }

  private val SUPPORTED_TABLE_VALUED_FUNCTIONS = {
    val map = new IdentifierMap[Unit]()
    map += ("collations", ())
    map += ("explode", ())
    map += ("explode_outer", ())
    map += ("get_warmup_tracing", ()) // EDGE
    map += ("inline", ())
    map += ("inline_outer", ())
    map += ("json_tuple", ())
    map += ("list_secrets", ()) // EDGE
    map += ("posexplode", ())
    map += ("posexplode_outer", ())
    map += ("range", ())
    map += ("sql_keywords", ())
    map += ("stack", ())
    map += ("variant_explode", ())
    map += ("variant_explode_outer", ())
    map
  }
}
