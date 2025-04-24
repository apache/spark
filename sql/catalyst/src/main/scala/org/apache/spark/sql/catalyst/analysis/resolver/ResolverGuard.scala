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
  SQLConfHelper,
  SqlScriptingLocalVariableManager
}
import org.apache.spark.sql.catalyst.analysis.{
  FunctionRegistry,
  GetViewColumnByNameAndOrdinal,
  ResolvedInlineTable,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedInlineTable,
  UnresolvedRelation,
  UnresolvedStar,
  UnresolvedSubqueryColumnAliases
}
import org.apache.spark.sql.catalyst.expressions._
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
class ResolverGuard(catalogManager: CatalogManager) extends SQLConfHelper {

  /**
   * Check the top level operator of the parsed operator.
   */
  def apply(operator: LogicalPlan): Boolean = {
    val unsupportedConf = detectUnsupportedConf()
    if (unsupportedConf.isDefined) {
      tryThrowUnsupportedSinglePassAnalyzerFeature(s"configuration: ${unsupportedConf.get}")
    }

    val areTempVariablesSupported = checkTempVariables()
    if (!areTempVariablesSupported) {
      tryThrowUnsupportedSinglePassAnalyzerFeature("temp variables")
    }

    val areScriptingVariablesSupported = checkScriptingVariables()
    if (!areScriptingVariablesSupported) {
      tryThrowUnsupportedSinglePassAnalyzerFeature("scripting variables")
    }

    !unsupportedConf.isDefined &&
    areTempVariablesSupported &&
    areScriptingVariablesSupported &&
    checkOperator(operator)
  }

  /**
   * Check if all the operators are supported. For implemented ones, recursively check
   * their children. For unimplemented ones, return false.
   */
  private def checkOperator(operator: LogicalPlan): Boolean = {
    val isSupported = operator match {
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
      case _ =>
        false
    }

    if (!isSupported) {
      tryThrowUnsupportedSinglePassAnalyzerFeature(operator)
    }

    isSupported
  }

  /**
   * Method used to check if expressions are supported by the new analyzer.
   * For LeafNode types, we return true or false. For other ones, check their children.
   */
  private def checkExpression(expression: Expression): Boolean = {
    val isSupported = expression match {
      case alias: Alias =>
        checkAlias(alias)
      case unresolvedConditionalExpression: ConditionalExpression =>
        checkUnresolvedConditionalExpression(unresolvedConditionalExpression)
      case unresolvedCast: Cast =>
        checkUnresolvedCast(unresolvedCast)
      case unresolvedUpCast: UpCast =>
        checkUnresolvedUpCast(unresolvedUpCast)
      case unresolvedStar: UnresolvedStar =>
        checkUnresolvedStar(unresolvedStar)
      case unresolvedAlias: UnresolvedAlias =>
        checkUnresolvedAlias(unresolvedAlias)
      case unresolvedAttribute: UnresolvedAttribute =>
        checkUnresolvedAttribute(unresolvedAttribute)
      case literal: Literal =>
        checkLiteral(literal)
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
        checkGetViewColumnBynameAndOrdinal(getViewColumnByNameAndOrdinal)
      case expression if isGenerallySupportedExpression(expression) =>
        expression.children.forall(checkExpression)
      case _ =>
        false
    }

    if (!isSupported) {
      tryThrowUnsupportedSinglePassAnalyzerFeature(expression)
    }

    isSupported
  }

  private def checkUnresolvedWith(unresolvedWith: UnresolvedWith) = {
    !unresolvedWith.allowRecursion && unresolvedWith.cteRelations.forall {
      case (cteName, ctePlan) =>
        checkOperator(ctePlan)
    } && checkOperator(unresolvedWith.child)
  }

  private def checkWithCte(withCte: WithCTE) = {
    withCte.children.forall(checkOperator)
  }

  private def checkProject(project: Project) = {
    checkOperator(project.child) && project.projectList.forall(checkExpression)
  }

  private def checkAggregate(aggregate: Aggregate) = {
    checkOperator(aggregate.child) &&
    aggregate.groupingExpressions.forall(checkExpression) &&
    aggregate.aggregateExpressions.forall(checkExpression)
  }

  private def checkJoin(join: Join) = {
    checkOperator(join.left) && checkOperator(join.right) && {
      join.condition match {
        case Some(condition) => checkExpression(condition)
        case None => true
      }
    }
  }

  private def checkFilter(unresolvedFilter: Filter) =
    checkOperator(unresolvedFilter.child) && checkExpression(unresolvedFilter.condition)

  private def checkUnresolvedSubqueryColumnAliases(
      unresolvedSubqueryColumnAliases: UnresolvedSubqueryColumnAliases) =
    checkOperator(unresolvedSubqueryColumnAliases.child)

  private def checkSubqueryAlias(subqueryAlias: SubqueryAlias) =
    checkOperator(subqueryAlias.child)

  private def checkGlobalLimit(globalLimit: GlobalLimit) =
    checkOperator(globalLimit.child) && checkExpression(globalLimit.limitExpr)

  private def checkLocalLimit(localLimit: LocalLimit) =
    checkOperator(localLimit.child) && checkExpression(localLimit.limitExpr)

  private def checkOffset(offset: Offset) =
    checkOperator(offset.child) && checkExpression(offset.offsetExpr)

  private def checkTail(tail: Tail) =
    checkOperator(tail.child) && checkExpression(tail.limitExpr)

  private def checkDistinct(distinct: Distinct) =
    checkOperator(distinct.child)

  private def checkView(view: View) = checkOperator(view.child)

  private def checkUnresolvedInlineTable(unresolvedInlineTable: UnresolvedInlineTable) =
    unresolvedInlineTable.rows.forall(_.forall(checkExpression))

  private def checkUnresolvedRelation(unresolvedRelation: UnresolvedRelation) = true

  private def checkResolvedInlineTable(resolvedInlineTable: ResolvedInlineTable) =
    resolvedInlineTable.rows.forall(_.forall(checkExpression))

  // Usually we don't check outputs of operators in unresolved plans, but in this case
  // [[LocalRelation]] is resolved in the parser.
  private def checkLocalRelation(localRelation: LocalRelation) =
    localRelation.output.forall(checkExpression)

  private def checkRange(range: Range) = true

  private def checkUnion(union: Union) =
    !union.byName && !union.allowMissingCol && union.children.forall(checkOperator)

  private def checkSetOperation(setOperation: SetOperation) =
    setOperation.children.forall(checkOperator)

  private def checkSort(sort: Sort) = {
    checkOperator(sort.child) && sort.order.forall(
      sortOrder => checkExpression(sortOrder.child)
    )
  }

  private def checkOneRowRelation(oneRowRelation: OneRowRelation) = true

  private def checkCteRelationDef(cteRelationDef: CTERelationDef) = {
    checkOperator(cteRelationDef.child)
  }

  private def checkCteRelationRef(cteRelationRef: CTERelationRef) = true

  private def checkAlias(alias: Alias) = checkExpression(alias.child)

  private def checkUnresolvedConditionalExpression(
      unresolvedConditionalExpression: ConditionalExpression) =
    unresolvedConditionalExpression.children.forall(checkExpression)

  private def checkUnresolvedCast(cast: Cast) = checkExpression(cast.child)

  private def checkUnresolvedUpCast(upCast: UpCast) = checkExpression(upCast.child)

  private def checkUnresolvedStar(unresolvedStar: UnresolvedStar) = true

  private def checkUnresolvedAlias(unresolvedAlias: UnresolvedAlias) =
    checkExpression(unresolvedAlias.child)

  private def checkUnresolvedAttribute(unresolvedAttribute: UnresolvedAttribute) =
    !ResolverGuard.UNSUPPORTED_ATTRIBUTE_NAMES.contains(unresolvedAttribute.nameParts.head) &&
    !unresolvedAttribute.getTagValue(LogicalPlan.PLAN_ID_TAG).isDefined

  private def checkUnresolvedPredicate(unresolvedPredicate: Predicate) = unresolvedPredicate match {
    case inSubquery: InSubquery =>
      checkInSubquery(inSubquery)
    case exists: Exists =>
      checkExists(exists)
    case _ =>
      unresolvedPredicate.children.forall(checkExpression)
  }

  private def checkAttributeReference(attributeReference: AttributeReference) = true

  private def checkCreateNamedStruct(createNamedStruct: CreateNamedStruct) = {
    createNamedStruct.children.forall(checkExpression)
  }

  private def checkUnresolvedFunction(unresolvedFunction: UnresolvedFunction) =
    !ResolverGuard.UNSUPPORTED_FUNCTION_NAMES.contains(unresolvedFunction.nameParts.head) &&
    // UDFs are not supported
    FunctionRegistry.functionSet.contains(
      FunctionIdentifier(unresolvedFunction.nameParts.head.toLowerCase(Locale.ROOT))
    ) &&
    unresolvedFunction.children.forall(checkExpression)

  private def checkLiteral(literal: Literal) = true

  private def checkScalarSubquery(scalarSubquery: ScalarSubquery) =
    checkOperator(scalarSubquery.plan)

  private def checkInSubquery(inSubquery: InSubquery) =
    inSubquery.values.forall(checkExpression) && checkExpression(inSubquery.query)

  private def checkListQuery(listQuery: ListQuery) = checkOperator(listQuery.plan)

  private def checkExists(exists: Exists) = checkOperator(exists.plan)

  private def checkOuterReference(outerReference: OuterReference) =
    checkExpression(outerReference.e)

  private def checkGetViewColumnBynameAndOrdinal(
      getViewColumnByNameAndOrdinal: GetViewColumnByNameAndOrdinal) = true

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
          _: Conv | _: RoundBase | _: Factorial | _: Bin | _: Hex | _: Unhex | _: WidthBucket =>
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
      case _: TimeZoneAwareExpression =>
        true
      // Decimal
      case _: UnscaledValue | _: MakeDecimal | _: CheckOverflow | _: CheckOverflowInSum |
          _: DecimalAddNoOverflowCheck |
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
      case _: JsonToStructs | _: StructsToJson |
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
      case _: TaggingExpression =>
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
    } else {
      None
    }
  }

  private def checkTempVariables() =
    catalogManager.tempVariableManager.isEmpty

  private def checkScriptingVariables() =
    SqlScriptingLocalVariableManager.get().forall(_.isEmpty)

  private def tryThrowUnsupportedSinglePassAnalyzerFeature(operator: LogicalPlan): Unit = {
    tryThrowUnsupportedSinglePassAnalyzerFeature(s"${operator.getClass} operator resolution")
  }

  private def tryThrowUnsupportedSinglePassAnalyzerFeature(expression: Expression): Unit = {
    tryThrowUnsupportedSinglePassAnalyzerFeature(s"${expression.getClass} expression resolution")
  }

  private def tryThrowUnsupportedSinglePassAnalyzerFeature(feature: String): Unit = {
    if (conf.getConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_THROW_FROM_RESOLVER_GUARD)) {
      throw QueryCompilationErrors.unsupportedSinglePassAnalyzerFeature(feature)
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
    map += ("current_user", ())
    map += ("session_user", ())
    map += ("user", ())
    // Functions that require lambda support.
    map += ("array_sort", ())
    map += ("transform", ())
    // Functions that require generator support.
    map += ("explode", ())
    map += ("explode_outer", ())
    map += ("inline", ())
    map += ("inline_outer", ())
    map += ("posexplode", ())
    map += ("posexplode_outer", ())
    // Functions that require session/time window resolution.
    map += ("session_window", ())
    map += ("window", ())
    map += ("window_time", ())
    // Functions that are not resolved properly.
    map += ("collate", ())
    map += ("json_tuple", ())
    // Functions that produce wrong schemas/plans because of alias assignment.
    map += ("from_json", ())
    map += ("schema_of_json", ())
    // Function for which we don't handle exceptions properly.
    map += ("schema_of_xml", ())
  }
}
