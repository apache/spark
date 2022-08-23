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
    new ParseException("Invalid InsertIntoContext", ctx)
  }

  def insertOverwriteDirectoryUnsupportedError(ctx: InsertIntoContext): Throwable = {
    new ParseException("INSERT OVERWRITE DIRECTORY is not supported", ctx)
  }

  def columnAliasInOperationNotAllowedError(op: String, ctx: TableAliasContext): Throwable = {
    new ParseException(s"Columns aliases are not allowed in $op.", ctx.identifierList())
  }

  def emptySourceForMergeError(ctx: MergeIntoTableContext): Throwable = {
    new ParseException("Empty source for merge: you should specify a source" +
      " table/subquery in merge.", ctx.source)
  }

  def unrecognizedMatchedActionError(ctx: MatchedClauseContext): Throwable = {
    new ParseException(s"Unrecognized matched action: ${ctx.matchedAction().getText}",
      ctx.matchedAction())
  }

  def insertedValueNumberNotMatchFieldNumberError(ctx: NotMatchedClauseContext): Throwable = {
    new ParseException("The number of inserted values cannot match the fields.",
      ctx.notMatchedAction())
  }

  def unrecognizedNotMatchedActionError(ctx: NotMatchedClauseContext): Throwable = {
    new ParseException(s"Unrecognized not matched action: ${ctx.notMatchedAction().getText}",
      ctx.notMatchedAction())
  }

  def mergeStatementWithoutWhenClauseError(ctx: MergeIntoTableContext): Throwable = {
    new ParseException("There must be at least one WHEN clause in a MERGE statement", ctx)
  }

  def nonLastMatchedClauseOmitConditionError(ctx: MergeIntoTableContext): Throwable = {
    new ParseException("When there are more than one MATCHED clauses in a MERGE " +
      "statement, only the last MATCHED clause can omit the condition.", ctx)
  }

  def nonLastNotMatchedClauseOmitConditionError(ctx: MergeIntoTableContext): Throwable = {
    new ParseException("When there are more than one NOT MATCHED clauses in a MERGE " +
      "statement, only the last NOT MATCHED clause can omit the condition.", ctx)
  }

  def emptyPartitionKeyError(key: String, ctx: PartitionSpecContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters =
        Array(s"Partition key ${toSQLId(key)} must set value (can't be empty)."),
      ctx)
  }

  def combinationQueryResultClausesUnsupportedError(ctx: QueryOrganizationContext): Throwable = {
    new ParseException(
      "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported", ctx)
  }

  def distributeByUnsupportedError(ctx: QueryOrganizationContext): Throwable = {
    new ParseException("DISTRIBUTE BY is not supported", ctx)
  }

  def transformNotSupportQuantifierError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "TRANSFORM_DISTINCT_ALL",
        messageParameters = Array[String](),
      ctx)
  }

  def transformWithSerdeUnsupportedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "TRANSFORM_NON_HIVE",
        messageParameters = Array[String](),
      ctx)
  }

  def lateralWithPivotInFromClauseNotAllowedError(ctx: FromClauseContext): Throwable = {
    new ParseException("LATERAL cannot be used together with PIVOT in FROM clause", ctx)
  }

  def lateralJoinWithNaturalJoinUnsupportedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "LATERAL_NATURAL_JOIN",
        messageParameters = Array[String](),
      ctx)
  }

  def lateralJoinWithUsingJoinUnsupportedError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "LATERAL_JOIN_USING",
        messageParameters = Array[String](),
      ctx)
  }

  def unsupportedLateralJoinTypeError(ctx: ParserRuleContext, joinType: String): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "LATERAL_JOIN_OF_TYPE",
      messageParameters = Array(s"${toSQLStmt(joinType)}"),
      ctx)
  }

  def invalidLateralJoinRelationError(ctx: RelationPrimaryContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Array(s"${toSQLStmt("LATERAL")} can only be used with subquery."),
      ctx)
  }

  def repetitiveWindowDefinitionError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException("INVALID_SQL_SYNTAX",
      Array(s"The definition of window ${toSQLId(name)} is repetitive."), ctx)
  }

  def invalidWindowReferenceError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException("INVALID_SQL_SYNTAX",
      Array(s"Window reference ${toSQLId(name)} is not a window specification."), ctx)
  }

  def cannotResolveWindowReferenceError(name: String, ctx: WindowClauseContext): Throwable = {
    new ParseException("INVALID_SQL_SYNTAX",
      Array(s"Cannot resolve window reference ${toSQLId(name)}."), ctx)
  }

  def naturalCrossJoinUnsupportedError(ctx: RelationContext): Throwable = {
    new ParseException(errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "NATURAL_CROSS_JOIN",
        messageParameters = Array[String](),
      ctx = ctx)
  }

  def emptyInputForTableSampleError(ctx: ParserRuleContext): Throwable = {
    new ParseException("TABLESAMPLE does not accept empty inputs.", ctx)
  }

  def tableSampleByBytesUnsupportedError(msg: String, ctx: SampleMethodContext): Throwable = {
    new ParseException(s"TABLESAMPLE($msg) is not supported", ctx)
  }

  def invalidByteLengthLiteralError(bytesStr: String, ctx: SampleByBytesContext): Throwable = {
    new ParseException(s"$bytesStr is not a valid byte length literal, " +
        "expected syntax: DIGIT+ ('B' | 'K' | 'M' | 'G')", ctx)
  }

  def invalidEscapeStringError(ctx: PredicateContext): Throwable = {
    new ParseException("Invalid escape string. Escape string must contain only one character.", ctx)
  }

  def trimOptionUnsupportedError(trimOption: Int, ctx: TrimContext): Throwable = {
    new ParseException("Function trim doesn't support with " +
      s"type $trimOption. Please use BOTH, LEADING or TRAILING as trim type", ctx)
  }

  def functionNameUnsupportedError(functionName: String, ctx: ParserRuleContext): Throwable = {
    new ParseException("INVALID_SQL_SYNTAX",
      Array(s"Unsupported function name ${toSQLId(functionName)}"), ctx)
  }

  def cannotParseValueTypeError(
      valueType: String, value: String, ctx: TypeConstructorContext): Throwable = {
    new ParseException(s"Cannot parse the $valueType value: $value", ctx)
  }

  def cannotParseIntervalValueError(value: String, ctx: TypeConstructorContext): Throwable = {
    new ParseException(s"Cannot parse the INTERVAL value: $value", ctx)
  }

  def literalValueTypeUnsupportedError(
      valueType: String, ctx: TypeConstructorContext): Throwable = {
    new ParseException(s"Literals of type '$valueType' are currently not supported.", ctx)
  }

  def parsingValueTypeError(
      e: IllegalArgumentException, valueType: String, ctx: TypeConstructorContext): Throwable = {
    val message = Option(e.getMessage).getOrElse(s"Exception parsing $valueType")
    new ParseException(message, ctx)
  }

  def invalidNumericLiteralRangeError(rawStrippedQualifier: String, minValue: BigDecimal,
      maxValue: BigDecimal, typeName: String, ctx: NumberContext): Throwable = {
    new ParseException(s"Numeric literal $rawStrippedQualifier does not " +
      s"fit in range [$minValue, $maxValue] for type $typeName", ctx)
  }

  def moreThanOneFromToUnitInIntervalLiteralError(ctx: ParserRuleContext): Throwable = {
    new ParseException("Can only have a single from-to unit in the interval literal syntax", ctx)
  }

  def invalidIntervalLiteralError(ctx: IntervalContext): Throwable = {
    new ParseException("at least one time unit should be given for interval literal", ctx)
  }

  def invalidIntervalFormError(value: String, ctx: MultiUnitsIntervalContext): Throwable = {
    new ParseException("Can only use numbers in the interval value part for" +
      s" multiple unit value pairs interval form, but got invalid value: $value", ctx)
  }

  def invalidFromToUnitValueError(ctx: IntervalValueContext): Throwable = {
    new ParseException("The value of from-to unit must be a string", ctx)
  }

  def fromToIntervalUnsupportedError(
      from: String, to: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Intervals FROM $from TO $to are not supported.", ctx)
  }

  def mixedIntervalUnitsError(literal: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Cannot mix year-month and day-time fields: $literal", ctx)
  }

  def dataTypeUnsupportedError(dataType: String, ctx: PrimitiveDataTypeContext): Throwable = {
    new ParseException(s"DataType $dataType is not supported.", ctx)
  }

  def charTypeMissingLengthError(dataType: String, ctx: PrimitiveDataTypeContext): Throwable = {
    new ParseException("PARSE_CHAR_MISSING_LENGTH", Array(dataType, dataType), ctx)
  }

  def partitionTransformNotExpectedError(
      name: String, describe: String, ctx: ApplyTransformContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters =
        Array(s"Expected a column reference for transform ${toSQLId(name)}: $describe"),
      ctx)
  }

  def tooManyArgumentsForTransformError(name: String, ctx: ApplyTransformContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Array(s"Too many arguments for transform ${toSQLId(name)}"),
      ctx)
  }

  def invalidBucketsNumberError(describe: String, ctx: ApplyTransformContext): Throwable = {
    new ParseException(s"Invalid number of buckets: $describe", ctx)
  }

  def cannotCleanReservedNamespacePropertyError(
      property: String, ctx: ParserRuleContext, msg: String): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "SET_NAMESPACE_PROPERTY",
      messageParameters = Array(property, msg),
      ctx)
  }

  def propertiesAndDbPropertiesBothSpecifiedError(ctx: CreateNamespaceContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "SET_PROPERTIES_AND_DBPROPERTIES",
        messageParameters = Array[String](),
      ctx
    )
  }

  def cannotCleanReservedTablePropertyError(
      property: String, ctx: ParserRuleContext, msg: String): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "SET_TABLE_PROPERTY",
      messageParameters = Array(property, msg),
      ctx)
  }

  def duplicatedTablePathsFoundError(
      pathOne: String, pathTwo: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Duplicated table paths found: '$pathOne' and '$pathTwo'. LOCATION" +
      s" and the case insensitive key 'path' in OPTIONS are all used to indicate the custom" +
      s" table path, you can only specify one of them.", ctx)
  }

  def storedAsAndStoredByBothSpecifiedError(ctx: CreateFileFormatContext): Throwable = {
    new ParseException("Expected either STORED AS or STORED BY, not both", ctx)
  }

  def operationInHiveStyleCommandUnsupportedError(operation: String,
      command: String, ctx: StatementContext, msgOpt: Option[String] = None): Throwable = {
    val basicError = s"$operation is not supported in Hive-style $command"
    val msg = if (msgOpt.isDefined) {
      s"$basicError, ${msgOpt.get}."
    } else {
      basicError
    }
    new ParseException(msg, ctx)
  }

  def operationNotAllowedError(message: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Operation not allowed: $message", ctx)
  }

  def descColumnForPartitionUnsupportedError(ctx: DescribeRelationContext): Throwable = {
    new ParseException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "DESC_TABLE_COLUMN_PARTITION",
      messageParameters = Array[String](),
      ctx)
  }

  def incompletePartitionSpecificationError(
      key: String, ctx: DescribeRelationContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Array(s"PARTITION specification is incomplete: ${toSQLId(key)}"),
      ctx)
  }

  def computeStatisticsNotExpectedError(ctx: IdentifierContext): Throwable = {
    new ParseException(s"Expected `NOSCAN` instead of `${ctx.getText}`", ctx)
  }

  def addCatalogInCacheTableAsSelectNotAllowedError(
      quoted: String, ctx: CacheTableContext): Throwable = {
    new ParseException(s"It is not allowed to add catalog/namespace prefix $quoted to " +
      "the table name in CACHE TABLE AS SELECT", ctx)
  }

  def showFunctionsUnsupportedError(identifier: String, ctx: IdentifierContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Array(
        s"${toSQLStmt("SHOW")} ${toSQLId(identifier)} ${toSQLStmt("FUNCTIONS")} not supported"),
      ctx)
  }

  def showFunctionsInvalidPatternError(pattern: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Array(
        s"Invalid pattern in ${toSQLStmt("SHOW FUNCTIONS")}: ${toSQLId(pattern)}. " +
        s"It must be a ${toSQLType(StringType)} literal."),
      ctx)
  }

  def duplicateCteDefinitionNamesError(duplicateNames: String, ctx: CtesContext): Throwable = {
    new ParseException(s"CTE definition can't have duplicate names: $duplicateNames.", ctx)
  }

  def sqlStatementUnsupportedError(sqlText: String, position: Origin): Throwable = {
    new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
  }

  def unquotedIdentifierError(ident: String, ctx: ErrorIdentContext): Throwable = {
    new ParseException(s"Possibly unquoted identifier $ident detected. " +
      s"Please consider quoting it with back-quotes as `$ident`", ctx)
  }

  def duplicateClausesError(clauseName: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Found duplicate clauses: $clauseName", ctx)
  }

  def duplicateKeysError(key: String, ctx: ParserRuleContext): Throwable = {
    // Found duplicate keys '$key'
    new ParseException(errorClass = "DUPLICATE_KEY", messageParameters = Array(toSQLId(key)), ctx)
  }

  def unexpectedFormatForSetConfigurationError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      "Expected format is 'SET', 'SET key', or 'SET key=value'. If you want to include " +
      "special characters in key, or include semicolon in value, please use quotes, " +
      "e.g., SET `key`=`value`.", ctx)
  }

  def invalidPropertyKeyForSetQuotedConfigurationError(
      keyCandidate: String, valueStr: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "INVALID_PROPERTY_KEY",
      messageParameters = Array(toSQLConf(keyCandidate),
        toSQLConf(keyCandidate), toSQLConf(valueStr)),
      ctx)
  }

  def invalidPropertyValueForSetQuotedConfigurationError(
      valueCandidate: String, keyStr: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(errorClass = "INVALID_PROPERTY_VALUE",
      messageParameters = Array(toSQLConf(valueCandidate),
        toSQLConf(keyStr), toSQLConf(valueCandidate)),
      ctx)
  }

  def unexpectedFormatForResetConfigurationError(ctx: ResetConfigurationContext): Throwable = {
    new ParseException(
      "Expected format is 'RESET' or 'RESET key'. If you want to include special characters " +
      "in key, please use quotes, e.g., RESET `key`.", ctx)
  }

  def intervalValueOutOfRangeError(ctx: IntervalContext): Throwable = {
    new ParseException("The interval value must be in the range of [-18, +18] hours" +
      " with second precision", ctx)
  }

  def invalidTimeZoneDisplacementValueError(ctx: SetTimeZoneContext): Throwable = {
    new ParseException("Invalid time zone displacement value", ctx)
  }

  def createTempTableNotSpecifyProviderError(ctx: CreateTableContext): Throwable = {
    new ParseException("CREATE TEMPORARY TABLE without a provider is not allowed.", ctx)
  }

  def rowFormatNotUsedWithStoredAsError(ctx: CreateTableLikeContext): Throwable = {
    new ParseException("'ROW FORMAT' must be used with 'STORED AS'", ctx)
  }

  def useDefinedRecordReaderOrWriterClassesError(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      "Unsupported operation: Used defined record reader/writer classes.", ctx)
  }

  def directoryPathAndOptionsPathBothSpecifiedError(ctx: InsertOverwriteDirContext): Throwable = {
    new ParseException(
      "Directory path and 'path' in OPTIONS should be specified one, but not both", ctx)
  }

  def unsupportedLocalFileSchemeError(ctx: InsertOverwriteDirContext): Throwable = {
    new ParseException("LOCAL is supported only with file: scheme", ctx)
  }

  def invalidGroupingSetError(element: String, ctx: GroupingAnalyticsContext): Throwable = {
    new ParseException(s"Empty set in $element grouping sets is not supported.", ctx)
  }

  def createViewWithBothIfNotExistsAndReplaceError(ctx: CreateViewContext): Throwable = {
    new ParseException("CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed.", ctx)
  }

  def defineTempViewWithIfNotExistsError(ctx: CreateViewContext): Throwable = {
    new ParseException("It is not allowed to define a TEMPORARY view with IF NOT EXISTS.", ctx)
  }

  def notAllowedToAddDBPrefixForTempViewError(
      database: String,
      ctx: CreateViewContext): Throwable = {
    new ParseException(
      s"It is not allowed to add database prefix `$database` for the TEMPORARY view name.", ctx)
  }

  def createFuncWithBothIfNotExistsAndReplaceError(ctx: CreateFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Array(
        s"${toSQLStmt("CREATE FUNCTION")} with both ${toSQLStmt("IF NOT EXISTS")} " +
        s"and ${toSQLStmt("REPLACE")} is not allowed."),
      ctx)
  }

  def defineTempFuncWithIfNotExistsError(ctx: CreateFunctionContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Array(
        s"It is not allowed to define a ${toSQLStmt("TEMPORARY FUNCTION")}" +
        s" with ${toSQLStmt("IF NOT EXISTS")}."), ctx)
  }

  def unsupportedFunctionNameError(funcName: Seq[String], ctx: CreateFunctionContext): Throwable = {
    new ParseException("INVALID_SQL_SYNTAX",
      Array(s"Unsupported function name ${toSQLId(funcName)}"), ctx)
  }

  def specifyingDBInCreateTempFuncError(
      databaseName: String,
      ctx: CreateFunctionContext): Throwable = {
    new ParseException(
      "INVALID_SQL_SYNTAX",
      Array(
        s"Specifying a database in ${toSQLStmt("CREATE TEMPORARY FUNCTION")} is not allowed: " +
        toSQLId(databaseName)),
      ctx)
  }

  def invalidTableValuedFunctionNameError(
      name: Seq[String],
      ctx: TableValuedFunctionContext): Throwable = {
    new ParseException(
      "INVALID_SQL_SYNTAX",
      Array("table valued function cannot specify database name ", toSQLId(name)), ctx)
  }

  def unclosedBracketedCommentError(command: String, position: Origin): Throwable = {
    new ParseException(Some(command), "Unclosed bracketed comment", position, position)
  }

  def invalidTimeTravelSpec(reason: String, ctx: ParserRuleContext): Throwable = {
    new ParseException(s"Invalid time travel spec: $reason.", ctx)
  }

  def invalidNameForDropTempFunc(name: Seq[String], ctx: ParserRuleContext): Throwable = {
    new ParseException(
      errorClass = "INVALID_SQL_SYNTAX",
      messageParameters = Array(
        s"${toSQLStmt("DROP TEMPORARY FUNCTION")} requires a single part name but got: " +
        toSQLId(name)),
      ctx)
  }

  def defaultColumnNotImplementedYetError(ctx: ParserRuleContext): Throwable = {
    new ParseException("Support for DEFAULT column values is not implemented yet", ctx)
  }

  def defaultColumnNotEnabledError(ctx: ParserRuleContext): Throwable = {
    new ParseException("Support for DEFAULT column values is not allowed", ctx)
  }

  def defaultColumnReferencesNotAllowedInPartitionSpec(ctx: ParserRuleContext): Throwable = {
    new ParseException(
      "References to DEFAULT column values are not allowed within the PARTITION clause", ctx)
  }
}
