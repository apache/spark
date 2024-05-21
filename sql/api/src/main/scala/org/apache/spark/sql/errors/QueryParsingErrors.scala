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

import java.util.Locale

import org.antlr.v4.runtime.ParserRuleContext

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.trees.Origin

/**
 * Object for grouping all error messages of the query parsing.
 * Currently it includes all ParseException.
 */
private[sql] object QueryParsingErrors extends DataTypeErrorsBase {

  def invalidInsertIntoError(ctx: InsertIntoContext): Throwable = {
    new ParseException(errorClass = "_LEGACY_ERROR_TEMP_0001", ctx)
  }

  def parserStackOverflow(parserRuleContext: ParserRuleContext): Throwable = {
    throw new ParseException(
      errorClass = "FAILED_TO_PARSE_TOO_COMPLEX",
      ctx = parserRuleContext)
  }

  def insertOverwriteDirectoryUnsupportedError(): Throwable = {
    SparkException.internalError("INSERT OVERWRITE DIRECTORY is not supported.")
  }

  def columnAliasInOperationNotAllowedError(op: String, ctx: TableAliasContext): Throwable = {
    new ParseException(
      errorClass = "COLUMN_ALIASES_NOT_ALLOWED",
      messageParameters = Map("op" -> toSQLStmt(op)),
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
      errorClass = "INVALID_SQL_SYNTAX.EMPTY_PARTITION_VALUE",
      messageParameters = Map("partKey" -> toSQLId(key)),
      ctx)
  }

  def combinationQueryResultClausesUnsupportedError(ctx: QueryOrganizationContext): Throwable = {
    new ParseException(errorClass = "UNSUPPORTED_FEATURE.COMBINATION_QUERY_RESULT_CLAUSES", ctx)
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
    new ParseException(errorClass = "NOT_ALLOWED_IN_FROM.UNPIVOT_WITH_PIVOT", ctx)
  }

  def lateralWithPivotInFromClauseNotAllowedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "NOT_ALLOWED_IN_FROM.LATERAL_WITH_PIVOT", ctx)
  }

  def lateralWithUnpivotInFromClauseNotAllowedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "NOT_ALLOWED_IN_FROM.LATERAL_WITH_UNPIVOT", ctx)
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
      errorClass = "INVALID_SQL_SYNTAX.LATERAL_WITHOUT_SUBQUERY_OR_TABLE_VALUED_FUNC",
      ctx)
  }

  def repetitiveWindowDefinitionError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.REPETITIVE_WINDOW_DEFINITION",
      messageParameters = Map("windowName" -> toSQLId(name)),
      ctx)
  }

  def invalidWindowReferenceError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.INVALID_WINDOW_REFERENCE",
      messageParameters = Map("windowName" -> toSQLId(name)),
      ctx)
  }

  def cannotResolveWindowReferenceError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.UNRESOLVED_WINDOW_REFERENCE",
      messageParameters = Map("windowName" -> toSQLId(name)),
      ctx)
  }

  def incompatibleJoinTypesError(
      joinType1: String, joinType2: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INCOMPATIBLE_JOIN_TYPES",
      messageParameters = Map(
        "joinType1" -> joinType1.toUpperCase(Locale.ROOT),
        "joinType2" -> joinType2.toUpperCase(Locale.ROOT)),
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

  def invalidEscapeStringError(invalidEscape: String, ctx: PredicateContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_ESC",
      messageParameters = Map("invalidEscape" -> toSQLValue(invalidEscape)),
      ctx)
  }

  def trimOptionUnsupportedError(trimOption: Int, ctx: TrimContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0018",
      messageParameters = Map("trimOption" -> trimOption.toString),
      ctx)
  }

  def functionNameUnsupportedError(functionName: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.UNSUPPORTED_FUNC_NAME",
      messageParameters = Map("funcName" -> toSQLId(functionName)),
      ctx)
  }

  def cannotParseValueTypeError(
      valueType: String, value: String, ctx: TypeConstructorContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_TYPED_LITERAL",
      messageParameters = Map(
        "valueType" -> toSQLType(valueType),
        "value" -> toSQLValue(value)
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
      errorClass = "INVALID_NUMERIC_LITERAL_RANGE",
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

  def nestedTypeMissingElementTypeError(
      dataType: String, ctx: PrimitiveDataTypeContext): Throwable = {
    dataType.toUpperCase(Locale.ROOT) match {
      case "ARRAY" =>
        new ParseException(
          errorClass = "INCOMPLETE_TYPE_DEFINITION.ARRAY",
          messageParameters = Map("elementType" -> "<INT>"),
          ctx)
      case "STRUCT" =>
        new ParseException(
          errorClass = "INCOMPLETE_TYPE_DEFINITION.STRUCT",
          messageParameters = Map.empty,
          ctx)
      case "MAP" =>
        new ParseException(
          errorClass = "INCOMPLETE_TYPE_DEFINITION.MAP",
          messageParameters = Map.empty,
          ctx)
    }
  }

  def partitionTransformNotExpectedError(
      name: String, expr: String, ctx: ApplyTransformContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.INVALID_COLUMN_REFERENCE",
      messageParameters = Map(
        "transform" -> toSQLId(name),
        "expr" -> expr),
      ctx)
  }

  def wrongNumberArgumentsForTransformError(
      name: String, actualNum: Int, ctx: ApplyTransformContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.TRANSFORM_WRONG_NUM_ARGS",
      messageParameters = Map(
        "transform" -> toSQLId(name),
      "expectedNum" -> "1",
      "actualNum" -> actualNum.toString),
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

  def invalidStatementError(operation: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_STATEMENT_OR_CLAUSE",
      messageParameters = Map("operation" -> toSQLStmt(operation)),
      ctx)
  }

  def descColumnForPartitionUnsupportedError(ctx: DescribeRelationContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE.DESC_TABLE_COLUMN_PARTITION",
      messageParameters = Map.empty,
      ctx)
  }

  def computeStatisticsNotExpectedError(ctx: IdentifierContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.ANALYZE_TABLE_UNEXPECTED_NOSCAN",
      messageParameters = Map("ctx" -> toSQLStmt(ctx.getText)),
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
      errorClass = "INVALID_SQL_SYNTAX.SHOW_FUNCTIONS_INVALID_SCOPE",
      messageParameters = Map("scope" -> toSQLId(identifier)),
      ctx)
  }

  def showFunctionsInvalidPatternError(pattern: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.SHOW_FUNCTIONS_INVALID_PATTERN",
      messageParameters = Map("pattern" -> toSQLId(pattern)),
      ctx)
  }

  def duplicateCteDefinitionNamesError(duplicateNames: String, ctx: CtesContext): Throwable = {
    new ParseException(
      errorClass = "_LEGACY_ERROR_TEMP_0038",
      messageParameters = Map("duplicateNames" -> duplicateNames),
      ctx)
  }

  def sqlStatementUnsupportedError(sqlText: String, position: Origin): Throwable = {
    new ParseException(
      command = Option(sqlText),
      start = position,
      stop = position,
      errorClass = "_LEGACY_ERROR_TEMP_0039",
      messageParameters = Map.empty)
  }

  def invalidIdentifierError(ident: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_IDENTIFIER",
      messageParameters = Map("ident" -> ident),
      ctx)
  }

  def duplicateClausesError(clauseName: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "DUPLICATE_CLAUSES",
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
    new ParseException(errorClass = "INVALID_SET_SYNTAX", ctx)
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

  def unsupportedLocalFileSchemeError(
      ctx: InsertOverwriteDirContext,
      actualSchema: String): Throwable = {
    new ParseException(
      errorClass = "LOCAL_MUST_WITH_SCHEMA_FILE",
      messageParameters = Map("actualSchema" -> actualSchema),
      ctx)
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

  def temporaryViewWithSchemaBindingMode(ctx: StatementContext): Throwable = {
    new ParseException(errorClass = "UNSUPPORTED_FEATURE.TEMPORARY_VIEW_WITH_SCHEMA_BINDING_MODE",
      messageParameters = Map.empty,
      ctx)
  }

  def parameterMarkerNotAllowed(statement: String, origin: Origin): Throwable = {
    new ParseException(
      command = origin.sqlText,
      start = origin,
      stop = origin,
      errorClass = "UNSUPPORTED_FEATURE.PARAMETER_MARKER_IN_UNEXPECTED_STATEMENT",
      messageParameters = Map("statement" -> statement))
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
      errorClass = "INVALID_SQL_SYNTAX.CREATE_ROUTINE_WITH_IF_NOT_EXISTS_AND_REPLACE",
      ctx)
  }

  def defineTempFuncWithIfNotExistsError(ctx: CreateFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.CREATE_TEMP_FUNC_WITH_IF_NOT_EXISTS",
      ctx)
  }

  def unsupportedFunctionNameError(funcName: Seq[String], ctx: CreateFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.MULTI_PART_NAME",
      messageParameters = Map(
        "statement" -> toSQLStmt("CREATE TEMPORARY FUNCTION"),
        "funcName" -> toSQLId(funcName)),
      ctx)
  }

  def specifyingDBInCreateTempFuncError(
      databaseName: String,
      ctx: CreateFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.CREATE_TEMP_FUNC_WITH_DATABASE",
      messageParameters = Map("database" -> toSQLId(databaseName)),
      ctx)
  }

  def invalidTableValuedFunctionNameError(
      name: Seq[String],
      ctx: TableValuedFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX.INVALID_TABLE_VALUED_FUNC_NAME",
      messageParameters = Map("funcName" -> toSQLId(name)),
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
      errorClass = "INVALID_SQL_SYNTAX.MULTI_PART_NAME",
      messageParameters = Map(
        "statement" -> toSQLStmt("DROP TEMPORARY FUNCTION"),
        "funcName" -> toSQLId(name)),
      ctx)
  }

  def defaultColumnNotImplementedYetError(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "UNSUPPORTED_DEFAULT_VALUE.WITHOUT_SUGGESTION", ctx)
  }

  def defaultColumnNotEnabledError(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "UNSUPPORTED_DEFAULT_VALUE.WITH_SUGGESTION", ctx)
  }

  def defaultColumnReferencesNotAllowedInPartitionSpec(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "REF_DEFAULT_VALUE_IS_NOT_ALLOWED_IN_PARTITION", ctx)
  }

  def duplicateArgumentNamesError(
      arguments: Seq[String],
      ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "EXEC_IMMEDIATE_DUPLICATE_ARGUMENT_ALIASES",
      messageParameters = Map("aliases" -> arguments.map(toSQLId).mkString(", ")),
      ctx)
  }

  def duplicateTableColumnDescriptor(
      ctx: ParserRuleContext,
      columnName: String,
      optionName: String,
      isCreate: Boolean = true,
      alterType: String = "ADD"): Throwable = {
    val errorClass =
      if (isCreate) {
        "CREATE_TABLE_COLUMN_DESCRIPTOR_DUPLICATE"
      } else {
        "ALTER_TABLE_COLUMN_DESCRIPTOR_DUPLICATE"
      }
    val alterTypeMap: Map[String, String] =
      if (isCreate) {
        Map.empty
      } else {
        Map("type" -> alterType)
      }
    new ParseException(
      errorClass = errorClass,
      messageParameters = alterTypeMap ++ Map(
          "columnName" -> columnName,
          "optionName" -> optionName
        ),
      ctx
    )
  }

  def invalidDatetimeUnitError(
      ctx: ParserRuleContext,
      functionName: String,
      invalidValue: String): Throwable = {
    new ParseException(
      errorClass = "INVALID_PARAMETER_VALUE.DATETIME_UNIT",
      messageParameters = Map(
        "functionName" -> toSQLId(functionName),
        "parameter" -> toSQLId("unit"),
        "invalidValue" -> invalidValue),
      ctx
    )
  }

  def invalidTableFunctionIdentifierArgumentMissingParentheses(
      ctx: ParserRuleContext, argumentName: String): Throwable = {
    new ParseException(
      errorClass =
        "INVALID_SQL_SYNTAX.INVALID_TABLE_FUNCTION_IDENTIFIER_ARGUMENT_MISSING_PARENTHESES",
      messageParameters = Map(
        "argumentName" -> toSQLId(argumentName)),
      ctx
    )
  }

  def clusterByWithPartitionedBy(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED", ctx)
  }

  def clusterByWithBucketing(ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED", ctx)
  }
}
