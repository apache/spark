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

package org.apache.spark.sql.errors

import org.antlr.v4.runtime.ParserRuleContext

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.StringType

/**
 * Object for grouping all error messages of the query parsing.
 * Currently it includes all ParseException.
 */
private[sql] object QueryParsingErrors extends QueryErrorsBase {

  def invalidInsertIntoError(ctx: InsertIntoContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0001", ctx)
  }

  def insertOverwriteDirectoryUnsupportedError(ctx: InsertIntoContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0002", ctx)
  }

  def columnAliasInOperationNotAllowedError(op: String, ctx: TableAliasContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0003",
      messageParameters = Map("op" -> op),
      ctx.identifierList())
  }

  def emptySourceForMergeError(ctx: MergeIntoTableContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0004", ctx.source)
  }

  def insertedValueNumberNotMatchFieldNumberError(ctx: NotMatchedClauseContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0006", ctx.notMatchedAction())
  }

  def mergeStatementWithoutWhenClauseError(ctx: MergeIntoTableContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0008", ctx)
  }

  def nonLastMatchedClauseOmitConditionError(ctx: MergeIntoTableContext): Throwable = {
    new ParseException(errorClass = "NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION", ctx)
  }

  def nonLastNotMatchedClauseOmitConditionError(ctx: MergeIntoTableContext): Throwable = {
    new ParseException(errorClass = "NON_LAST_NOT_MATCHED_BY_TARGET_CLAUSE_OMIT_CONDITION", ctx)
  }

  def nonLastNotMatchedBySourceClauseOmitConditionError(ctx: MergeIntoTableContext): Throwable = {
    new ParseException(errorClass = "NON_LAST_NOT_MATCHED_BY_SOURCE_CLAUSE_OMIT_CONDITION", ctx)
  }

  def emptyPartitionKeyError(key: String, ctx: PartitionSpecContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" -> s"Partition key ${toSQLId(key)} must set value (can't be empty)."),
      ctx)
  }

  def combinationQueryResultClausesUnsupportedError(ctx: QueryOrganizationContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0011", ctx)
  }

  def distributeByUnsupportedError(ctx: QueryOrganizationContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0012", ctx)
  }

  def transformNotSupportQuantifierError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.TRANSFORM_DISTINCT_ALL",
      messageParameters = Map.empty,
      ctx)
  }

  def transformWithSerdeUnsupportedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.TRANSFORM_NON_HIVE",
      messageParameters = Map.empty,
      ctx)
  }

  def unpivotWithPivotInFromClauseNotAllowedError(ctx: ParserRuleContext): Throwable = {
    new ParseException("UNPIVOT cannot be used together with PIVOT in FROM clause", ctx)
  }

  def lateralWithPivotInFromClauseNotAllowedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0013", ctx)
  }

  def lateralWithUnpivotInFromClauseNotAllowedError(ctx: ParserRuleContext): Throwable = {
    new ParseException("LATERAL cannot be used together with UNPIVOT in FROM clause", ctx)
  }

  def lateralJoinWithNaturalJoinUnsupportedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.LATERAL_NATURAL_JOIN",
      messageParameters = Map.empty,
      ctx)
  }

  def lateralJoinWithUsingJoinUnsupportedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.LATERAL_JOIN_USING",
      messageParameters = Map.empty,
      ctx)
  }

  def unsupportedLateralJoinTypeError(ctx: ParserRuleContext, joinType: String): Throwable = {
    new ParseException(
      errorClass = "INVALID_LATERAL_JOIN_TYPE",
      messageParameters = Map("joinType" -> toSQLStmt(joinType)),
      ctx)
  }

  def invalidLateralJoinRelationError(ctx: RelationPrimaryContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" -> s"${toSQLStmt("LATERAL")} can only be used with subquery."),
      ctx)
  }

  def repetitiveWindowDefinitionError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" -> s"The definition of window ${toSQLId(name)} is repetitive."),
      ctx)
  }

  def invalidWindowReferenceError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" -> s"Window reference ${toSQLId(name)} is not a window specification."),
      ctx)
  }

  def cannotResolveWindowReferenceError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" -> s"Cannot resolve window reference ${toSQLId(name)}."),
      ctx)
  }

  def naturalCrossJoinUnsupportedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.NATURAL_CROSS_JOIN",
      messageParameters = Map.empty,
      ctx = ctx)
  }

  def emptyInputForTableSampleError(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0014", ctx)
  }

  def tableSampleByBytesUnsupportedError(msg: String, ctx: SampleMethodContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0015",
      messageParameters = Map("msg" -> msg),
      ctx)
  }

  def invalidByteLengthLiteralError(bytesStr: String, ctx: SampleByBytesContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0016",
      messageParameters = Map("bytesStr" -> bytesStr),
      ctx)
  }

  def invalidEscapeStringError(ctx: PredicateContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0017", ctx)
  }

  def trimOptionUnsupportedError(trimOption: Int, ctx: TrimContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0018",
      messageParameters = Map("trimOption" -> trimOption.toString),
      ctx)
  }

  def functionNameUnsupportedError(functionName: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" -> s"Unsupported function name ${toSQLId(functionName)}"),
      ctx)
  }

  def cannotParseValueTypeError(
      valueType: String, value: String, ctx: TypeConstructorContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_TYPED_LITERAL",
      messageParameters = Map(
        "valueType" -> toSQLType(valueType),
        "value" -> toSQLValue(value, StringType)
      ),
      ctx)
  }

  def literalValueTypeUnsupportedError(
      unsupportedType: String,
      supportedTypes: Seq[String],
      ctx: TypeConstructorContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_TYPED_LITERAL",
      messageParameters = Map(
        "unsupportedType" -> toSQLType(unsupportedType),
        "supportedTypes" -> supportedTypes.map(toSQLType).mkString(", ")),
      ctx)
  }

  def invalidNumericLiteralRangeError(rawStrippedQualifier: String, minValue: BigDecimal,
      maxValue: BigDecimal, typeName: String, ctx: NumberContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0023",
      messageParameters = Map(
        "rawStrippedQualifier" -> rawStrippedQualifier,
        "minValue" -> minValue.toString(),
        "maxValue" -> maxValue.toString(),
        "typeName" -> typeName),
      ctx)
  }

  def moreThanOneFromToUnitInIntervalLiteralError(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0024", ctx)
  }

  def invalidIntervalLiteralError(ctx: IntervalContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0025", ctx)
  }

  def invalidIntervalFormError(value: String, ctx: MultiUnitsIntervalContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0026",
      messageParameters = Map("value" -> value),
      ctx)
  }

  def invalidFromToUnitValueError(ctx: IntervalValueContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0027", ctx)
  }

  def fromToIntervalUnsupportedError(
      from: String, to: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0028",
      messageParameters = Map("from" -> from, "to" -> to),
      ctx)
  }

  def mixedIntervalUnitsError(literal: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0029",
      messageParameters = Map("literal" -> literal),
      ctx)
  }

  def dataTypeUnsupportedError(dataType: String, ctx: PrimitiveDataTypeContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_DATATYPE",
      messageParameters = Map("typeName" -> toSQLType(dataType)),
      ctx)
  }

  def charTypeMissingLengthError(dataType: String, ctx: PrimitiveDataTypeContext): Throwable = {
    new ParseException(
      errorClass = "DATATYPE_MISSING_SIZE",
      messageParameters = Map("type" -> toSQLType(dataType)),
      ctx)
  }

  def partitionTransformNotExpectedError(
      name: String, describe: String, ctx: ApplyTransformContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" ->
          s"Expected a column reference for transform ${toSQLId(name)}: $describe"),
      ctx)
  }

  def tooManyArgumentsForTransformError(name: String, ctx: ApplyTransformContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" -> s"Too many arguments for transform ${toSQLId(name)}"),
      ctx)
  }

  def invalidBucketsNumberError(describe: String, ctx: ApplyTransformContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0031",
      messageParameters = Map("describe" -> describe),
      ctx)
  }

  def cannotCleanReservedNamespacePropertyError(
      property: String, ctx: ParserRuleContext, msg: String): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY",
      messageParameters = Map("property" -> property, "msg" -> msg),
      ctx)
  }

  def propertiesAndDbPropertiesBothSpecifiedError(ctx: CreateNamespaceContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.SET_PROPERTIES_AND_DBPROPERTIES",
      messageParameters = Map.empty,
      ctx
    )
  }

  def cannotCleanReservedTablePropertyError(
      property: String, ctx: ParserRuleContext, msg: String): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY",
      messageParameters = Map("property" -> property, "msg" -> msg),
      ctx)
  }

  def duplicatedTablePathsFoundError(
      pathOne: String, pathTwo: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0032",
      messageParameters = Map(
        "pathOne" -> pathOne,
        "pathTwo" -> pathTwo),
      ctx)
  }

  def storedAsAndStoredByBothSpecifiedError(ctx: CreateFileFormatContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0033", ctx)
  }

  def operationInHiveStyleCommandUnsupportedError(operation: String,
      command: String, ctx: StatementContext, msgOpt: Option[String] = None): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0034",
      messageParameters = Map(
        "operation" -> operation,
        "command" -> command,
        "msg" -> msgOpt.map(m => s", $m").getOrElse("")
      ),
      ctx)
  }

  def operationNotAllowedError(message: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0035",
      messageParameters = Map("message" -> message),
      ctx)
  }

  def descColumnForPartitionUnsupportedError(ctx: DescribeRelationContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.DESC_TABLE_COLUMN_PARTITION",
      messageParameters = Map.empty,
      ctx)
  }

  def incompletePartitionSpecificationError(
      key: String, ctx: DescribeRelationContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" -> s"PARTITION specification is incomplete: ${toSQLId(key)}"),
      ctx)
  }

  def computeStatisticsNotExpectedError(ctx: IdentifierContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0036",
      messageParameters = Map("ctx" -> ctx.getText),
      ctx)
  }

  def addCatalogInCacheTableAsSelectNotAllowedError(
      quoted: String, ctx: CacheTableContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0037",
      messageParameters = Map("quoted" -> quoted),
      ctx)
  }

  def showFunctionsUnsupportedError(identifier: String, ctx: IdentifierContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" ->
          s"${toSQLStmt("SHOW")} ${toSQLId(identifier)} ${toSQLStmt("FUNCTIONS")} not supported"),
      ctx)
  }

  def showFunctionsInvalidPatternError(pattern: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" ->
          (s"Invalid pattern in ${toSQLStmt("SHOW FUNCTIONS")}: ${toSQLId(pattern)}. " +
          s"It must be a ${toSQLType(StringType)} literal.")),
      ctx)
  }

  def duplicateCteDefinitionNamesError(duplicateNames: String, ctx: CtesContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0038",
      messageParameters = Map("duplicateNames" -> duplicateNames),
      ctx)
  }

  def sqlStatementUnsupportedError(sqlText: String, position: Origin): Throwable = {
    new ParseException(Option(sqlText), "Unsupported SQL statement", position, position,
      Some("_LEGACY_ERROR_TEMP_0039"))
  }

  def invalidIdentifierError(ident: String, ctx: ErrorIdentContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_IDENTIFIER",
      messageParameters = Map("ident" -> ident),
      ctx)
  }

  def duplicateClausesError(clauseName: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0041",
      messageParameters = Map("clauseName" -> clauseName),
      ctx)
  }

  def duplicateKeysError(key: String, ctx: ParserRuleContext): Throwable = {
    // Found duplicate keys '$key'
    new ParseException(
      errorClass = "DUPLICATE_KEY",
      messageParameters = Map("keyColumn" -> toSQLId(key)),
      ctx)
  }

  def unexpectedFormatForSetConfigurationError(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0042", ctx)
  }

  def invalidPropertyKeyForSetQuotedConfigurationError(
      keyCandidate: String, valueStr: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_PROPERTY_KEY",
      messageParameters = Map(
        "key" -> toSQLConf(keyCandidate),
        "value" -> toSQLConf(valueStr)),
      ctx)
  }

  def invalidPropertyValueForSetQuotedConfigurationError(
      valueCandidate: String, keyStr: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_PROPERTY_VALUE",
      messageParameters = Map(
        "value" -> toSQLConf(valueCandidate),
        "key" -> toSQLConf(keyStr)),
      ctx)
  }

  def unexpectedFormatForResetConfigurationError(ctx: ResetConfigurationContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0043", ctx)
  }

  def intervalValueOutOfRangeError(ctx: IntervalContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0044", ctx)
  }

  def invalidTimeZoneDisplacementValueError(ctx: SetTimeZoneContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0045", ctx)
  }

  def createTempTableNotSpecifyProviderError(ctx: CreateTableContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0046", ctx)
  }

  def rowFormatNotUsedWithStoredAsError(ctx: CreateTableLikeContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0047", ctx)
  }

  def useDefinedRecordReaderOrWriterClassesError(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0048", ctx)
  }

  def directoryPathAndOptionsPathBothSpecifiedError(ctx: InsertOverwriteDirContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0049", ctx)
  }

  def unsupportedLocalFileSchemeError(ctx: InsertOverwriteDirContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0050", ctx)
  }

  def invalidGroupingSetError(element: String, ctx: GroupingAnalyticsContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0051",
      messageParameters = Map("element" -> element),
      ctx)
  }

  def createViewWithBothIfNotExistsAndReplaceError(ctx: CreateViewContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0052", ctx)
  }

  def defineTempViewWithIfNotExistsError(ctx: CreateViewContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0053", ctx)
  }

  def notAllowedToAddDBPrefixForTempViewError(
      nameParts: Seq[String],
      ctx: CreateViewContext): Throwable = {
    new ParseException(
      errorClass = "TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS",
      messageParameters = Map("actualName" -> toSQLId(nameParts)),
      ctx)
  }

  def createFuncWithBothIfNotExistsAndReplaceError(ctx: CreateFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" ->
          (s"${toSQLStmt("CREATE FUNCTION")} with both ${toSQLStmt("IF NOT EXISTS")} " +
          s"and ${toSQLStmt("REPLACE")} is not allowed.")),
      ctx)
  }

  def defineTempFuncWithIfNotExistsError(ctx: CreateFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" ->
          (s"It is not allowed to define a ${toSQLStmt("TEMPORARY FUNCTION")}" +
          s" with ${toSQLStmt("IF NOT EXISTS")}.")),
      ctx)
  }

  def unsupportedFunctionNameError(funcName: Seq[String], ctx: CreateFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" -> s"Unsupported function name ${toSQLId(funcName)}"),
      ctx)
  }

  def specifyingDBInCreateTempFuncError(
      databaseName: String,
      ctx: CreateFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" ->
          (s"Specifying a database in ${toSQLStmt("CREATE TEMPORARY FUNCTION")} is not allowed: " +
          toSQLId(databaseName))),
      ctx)
  }

  def invalidTableValuedFunctionNameError(
      name: Seq[String],
      ctx: TableValuedFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" ->
          ("table valued function cannot specify database name: " + toSQLId(name))),
      ctx)
  }

  def unclosedBracketedCommentError(command: String, start: Origin, stop: Origin): Throwable = {
    new ParseException(
      command = Some(command),
      start = start,
      stop = stop,
      errorClass = "UNCLOSED_BRACKETED_COMMENT",
      messageParameters = Map.empty)
  }

  def invalidTimeTravelSpec(reason: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0056",
      messageParameters = Map("reason" -> reason),
      ctx)
  }

  def invalidNameForDropTempFunc(name: Seq[String], ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Map(
        "inputString" ->
          (s"${toSQLStmt("DROP TEMPORARY FUNCTION")} requires a single part name but got: " +
          toSQLId(name))),
      ctx)
  }

  def defaultColumnNotImplementedYetError(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0057", ctx)
  }

  def defaultColumnNotEnabledError(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0058", ctx)
  }

  def defaultColumnReferencesNotAllowedInPartitionSpec(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0059", ctx)
  }

  def duplicateCreateTableColumnOption(
      ctx: ParserRuleContext,
      columnName: String,
      optionName: String): Throwable = {
    new ParseException(
      errorClass = "CREATE_TABLE_COLUMN_OPTION_DUPLICATE",
      messageParameters = Map(
        "columnName" -> columnName,
        "optionName" -> optionName),
      ctx)
  }
}
