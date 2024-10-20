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

import org.apache.hadoop.fs.Path

import org.apache.spark.{SPARK_DOC_ROOT, SparkException, SparkRuntimeException, SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{ExtendedAnalysisException, FunctionIdentifier, InternalRow, QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, FunctionAlreadyExistsException, NamespaceAlreadyExistsException, NoSuchFunctionException, NoSuchNamespaceException, NoSuchPartitionException, NoSuchTableException, Star, TableAlreadyExistsException, UnresolvedRegex}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, InvalidUDFClassException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, CreateMap, CreateStruct, Expression, GroupingID, NamedExpression, SpecifiedWindowFrame, WindowFrame, WindowFunction, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.expressions.aggregate.AnyValue
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, InputParameter, Join, LogicalPlan, SerdeInfo, Window}
import org.apache.spark.sql.catalyst.trees.{Origin, TreeNode}
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LEGACY_CTE_PRECEDENCE_POLICY
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * Object for grouping error messages from exceptions thrown during query compilation.
 * As commands are executed eagerly, this also includes errors thrown during the execution of
 * commands, which users can see immediately.
 */
private[sql] object QueryCompilationErrors extends QueryErrorsBase with CompilationErrors {

  def unexpectedRequiredParameter(
      routineName: String, parameters: Seq[InputParameter]): Throwable = {
    val errorMessage = s"Routine ${toSQLId(routineName)} has an unexpected required argument for" +
      s" the provided routine signature ${parameters.mkString("[", ", ", "]")}." +
      s" All required arguments should come before optional arguments."
    SparkException.internalError(errorMessage)
  }

  def namedArgumentsNotSupported(functionName: String) : Throwable = {
    new AnalysisException(
      errorClass = "NAMED_PARAMETERS_NOT_SUPPORTED",
      messageParameters = Map("functionName" -> toSQLId(functionName))
    )
  }

  def positionalAndNamedArgumentDoubleReference(
      routineName: String, parameterName: String): Throwable = {
    val errorClass =
      "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.BOTH_POSITIONAL_AND_NAMED"
    new AnalysisException(
      errorClass = errorClass,
      messageParameters = Map(
        "routineName" -> toSQLId(routineName),
        "parameterName" -> toSQLId(parameterName))
    )
  }

  def doubleNamedArgumentReference(
      routineName: String, parameterName: String): Throwable = {
    val errorClass =
      "DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE"
    new AnalysisException(
      errorClass = errorClass,
      messageParameters = Map(
        "routineName" -> toSQLId(routineName),
        "parameterName" -> toSQLId(parameterName))
    )
  }

  def requiredParameterNotFound(
      routineName: String, parameterName: String, index: Int) : Throwable = {
    new AnalysisException(
      errorClass = "REQUIRED_PARAMETER_NOT_FOUND",
      messageParameters = Map(
        "routineName" -> toSQLId(routineName),
        "parameterName" -> toSQLId(parameterName),
        "index" -> index.toString)
    )
  }

  def unrecognizedParameterName(
      routineName: String, argumentName: String, candidates: Seq[String]): Throwable = {
    import org.apache.spark.sql.catalyst.util.StringUtils.orderSuggestedIdentifiersBySimilarity

    val inputs = candidates.map(candidate => Seq(candidate))
    val recommendations = orderSuggestedIdentifiersBySimilarity(argumentName, inputs)
      .take(3)
    new AnalysisException(
      errorClass = "UNRECOGNIZED_PARAMETER_NAME",
      messageParameters = Map(
        "routineName" -> toSQLId(routineName),
        "argumentName" -> toSQLId(argumentName),
        "proposal" -> recommendations.mkString(" "))
    )
  }

  def unexpectedPositionalArgument(
      routineName: String,
      precedingNamedArgument: String): Throwable = {
    new AnalysisException(
      errorClass = "UNEXPECTED_POSITIONAL_ARGUMENT",
      messageParameters = Map(
        "routineName" -> toSQLId(routineName),
        "parameterName" -> toSQLId(precedingNamedArgument))
    )
  }

  def groupingIDMismatchError(groupingID: GroupingID, groupByExprs: Seq[Expression]): Throwable = {
    new AnalysisException(
      errorClass = "GROUPING_ID_COLUMN_MISMATCH",
      messageParameters = Map(
        "groupingIdColumn" -> groupingID.groupByExprs.mkString(","),
        "groupByColumns" -> groupByExprs.mkString(",")))
  }

  def groupingColInvalidError(groupingCol: Expression, groupByExprs: Seq[Expression]): Throwable = {
    new AnalysisException(
      errorClass = "GROUPING_COLUMN_MISMATCH",
      messageParameters = Map(
        "grouping" -> groupingCol.toString,
        "groupingColumns" -> groupByExprs.mkString(",")))
  }

  def groupingSizeTooLargeError(sizeLimit: Int): Throwable = {
    new AnalysisException(
      errorClass = "GROUPING_SIZE_LIMIT_EXCEEDED",
      messageParameters = Map("maxSize" -> sizeLimit.toString))
  }

  def zeroArgumentIndexError(): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_PARAMETER_VALUE.ZERO_INDEX",
      messageParameters = Map(
        "parameter" -> toSQLId("strfmt"),
        "functionName" -> toSQLId("format_string")))
  }

  def binaryFormatError(funcName: String, invalidFormat: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_PARAMETER_VALUE.BINARY_FORMAT",
      messageParameters = Map(
        "parameter" -> toSQLId("format"),
        "functionName" -> toSQLId(funcName),
        "invalidFormat" -> toSQLValue(invalidFormat, StringType)))
  }

  def nullArgumentError(funcName: String, parameter: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_PARAMETER_VALUE.NULL",
      messageParameters = Map(
        "parameter" -> toSQLId(parameter),
        "functionName" -> toSQLId(funcName)))
  }

  def invalidRandomSeedParameter(functionName: String, invalidValue: Expression): Throwable = {
    invalidParameter("LONG", functionName, "seed", invalidValue)
  }

  def invalidReverseParameter(invalidValue: Expression): Throwable = {
    invalidParameter("BOOLEAN", "collect_top_k", "reverse", invalidValue)
  }

  def invalidNumParameter(invalidValue: Expression): Throwable = {
    invalidParameter("INTEGER", "collect_top_k", "num", invalidValue)
  }

  def invalidIgnoreNullsParameter(functionName: String, invalidValue: Expression): Throwable = {
    invalidParameter("BOOLEAN", functionName, "ignoreNulls", invalidValue)
  }

  def invalidIgnoreNAParameter(functionName: String, invalidValue: Expression): Throwable = {
    invalidParameter("BOOLEAN", functionName, "ignoreNA", invalidValue)
  }

  def invalidDdofParameter(functionName: String, invalidValue: Expression): Throwable = {
    invalidParameter("INTEGER", functionName, "ddof", invalidValue)
  }

  def invalidAlphaParameter(invalidValue: Expression): Throwable = {
    invalidParameter("DOUBLE", "ewm", "alpha", invalidValue)
  }

  def invalidStringParameter(
      functionName: String,
      parameter: String,
      invalidValue: Expression): Throwable = {
    invalidParameter("STRING", functionName, parameter, invalidValue)
  }

  def invalidParameter(
      subClass: String,
      functionName: String,
      parameter: String,
      invalidValue: Expression): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_PARAMETER_VALUE." + subClass,
      messageParameters = Map(
        "functionName" -> toSQLId(functionName),
        "parameter" -> toSQLId(parameter),
        "invalidValue" -> toSQLExpr(invalidValue)))
  }

  def nullDataSourceOption(option: String): Throwable = {
    new AnalysisException(
      errorClass = "NULL_DATA_SOURCE_OPTION",
      messageParameters = Map("option" -> option)
    )
  }

  def unorderablePivotColError(pivotCol: Expression): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPARABLE_PIVOT_COLUMN",
      messageParameters = Map("columnName" -> toSQLId(pivotCol.sql)))
  }

  def nonLiteralPivotValError(pivotVal: Expression): Throwable = {
    new AnalysisException(
      errorClass = "NON_LITERAL_PIVOT_VALUES",
      messageParameters = Map("expression" -> toSQLExpr(pivotVal)))
  }

  def pivotValDataTypeMismatchError(pivotVal: Expression, pivotCol: Expression): Throwable = {
    new AnalysisException(
      errorClass = "PIVOT_VALUE_DATA_TYPE_MISMATCH",
      messageParameters = Map(
        "value" -> pivotVal.toString,
        "valueType" -> pivotVal.dataType.simpleString,
        "pivotType" -> pivotCol.dataType.catalogString))
  }

  // Wrap `given` in backticks due to it will become a keyword in Scala 3.
  def unpivotRequiresAttributes(
      `given`: String,
      empty: String,
      expressions: Seq[NamedExpression]): Throwable = {
    val nonAttributes = expressions.filterNot(_.isInstanceOf[Attribute]).map(toSQLExpr)
    new AnalysisException(
      errorClass = "UNPIVOT_REQUIRES_ATTRIBUTES",
      messageParameters = Map(
        "given" -> `given`,
        "empty" -> empty,
        "expressions" -> nonAttributes.mkString(", ")))
  }

  def unpivotRequiresValueColumns(): Throwable = {
    new AnalysisException(
      errorClass = "UNPIVOT_REQUIRES_VALUE_COLUMNS",
      messageParameters = Map.empty)
  }

  def unpivotValueSizeMismatchError(names: Int): Throwable = {
    new AnalysisException(
      errorClass = "UNPIVOT_VALUE_SIZE_MISMATCH",
      messageParameters = Map("names" -> names.toString))
  }

  def unpivotValueDataTypeMismatchError(values: Seq[Seq[NamedExpression]]): Throwable = {
    val dataTypes = values.map {
      case Seq(value) => value
      // wrap multiple values into a struct to get a nice name for them
      case seq => Some(CreateStruct(seq)).map(e => Alias(e, e.sql)()).get
    }
      .groupBy(_.dataType)
      .transform((_, values) => values.map(value => toSQLId(value.name)).sorted)
      .transform((_, values) => if (values.length > 3) values.take(3) :+ "..." else values)
      .toList.sortBy(_._1.sql)
      .map { case (dataType, values) => s"${toSQLType(dataType)} (${values.mkString(", ")})" }

    new AnalysisException(
      errorClass = "UNPIVOT_VALUE_DATA_TYPE_MISMATCH",
      messageParameters = Map("types" -> dataTypes.mkString(", ")))
  }

  def unsupportedIfNotExistsError(tableName: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.INSERT_PARTITION_SPEC_IF_NOT_EXISTS",
      messageParameters = Map("tableName" -> toSQLId(tableName)))
  }

  def nonPartitionColError(partitionName: String): Throwable = {
    new AnalysisException(
      errorClass = "NON_PARTITION_COLUMN",
      messageParameters = Map("columnName" -> toSQLId(partitionName)))
  }

  def missingStaticPartitionColumn(staticName: String): Throwable = {
    SparkException.internalError(s"Unknown static partition column: $staticName.")
  }

  def staticPartitionInUserSpecifiedColumnsError(staticName: String): Throwable = {
    new AnalysisException(
      errorClass = "STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST",
      messageParameters = Map("staticName" -> staticName))
  }

  def nestedGeneratorError(trimmedNestedGenerator: Expression): Throwable = {
    new AnalysisException(errorClass = "UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS",
      messageParameters = Map("expression" -> toSQLExpr(trimmedNestedGenerator)))
  }

  def moreThanOneGeneratorError(generators: Seq[Expression]): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_GENERATOR.MULTI_GENERATOR",
      messageParameters = Map(
        "num" -> generators.size.toString,
        "generators" -> generators.map(toSQLExpr).mkString(", ")))
  }

  def generatorOutsideSelectError(plan: LogicalPlan): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_GENERATOR.OUTSIDE_SELECT",
      messageParameters = Map("plan" -> plan.simpleString(SQLConf.get.maxToStringFields)))
  }

  def legacyStoreAssignmentPolicyError(): Throwable = {
    val configKey = SQLConf.STORE_ASSIGNMENT_POLICY.key
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1000",
      messageParameters = Map("configKey" -> configKey))
  }

  def namedArgumentsNotEnabledError(functionName: String, argumentName: String): Throwable = {
    new AnalysisException(
      errorClass = "NAMED_PARAMETER_SUPPORT_DISABLED",
      messageParameters = Map(
        "functionName" -> toSQLId(functionName),
        "argument" -> toSQLId(argumentName))
    )
  }

  def trimCollationNotEnabledError(): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.TRIM_COLLATION",
      messageParameters = Map.empty
    )
  }

  def trailingCommaInSelectError(origin: Origin): Throwable = {
    new AnalysisException(
      errorClass = "TRAILING_COMMA_IN_SELECT",
      messageParameters = Map.empty,
      origin = origin
    )
  }

  def unresolvedUsingColForJoinError(
      colName: String, suggestion: String, side: String): Throwable = {
    new AnalysisException(
      errorClass = "UNRESOLVED_USING_COLUMN_FOR_JOIN",
      messageParameters = Map(
        "colName" -> toSQLId(colName),
        "side" -> side,
        "suggestion" -> suggestion))
  }

  def unresolvedAttributeError(
      errorClass: String,
      colName: String,
      candidates: Seq[String],
      origin: Origin): Throwable = {
    val commonParam = Map("objectName" -> toSQLId(colName))
    val proposalParam = if (candidates.isEmpty) {
        Map.empty[String, String]
      } else {
        Map("proposal" -> candidates.take(5).map(toSQLId).mkString(", "))
      }
    val errorSubClass = if (candidates.isEmpty) "WITHOUT_SUGGESTION" else "WITH_SUGGESTION"
    new AnalysisException(
      errorClass = s"$errorClass.$errorSubClass",
      messageParameters = commonParam ++ proposalParam,
      origin = origin
    )
  }

  def unresolvedColumnError(columnName: String, proposal: Seq[String]): Throwable = {
    val commonParam = Map("objectName" -> toSQLId(columnName))
    val proposalParam = if (proposal.isEmpty) {
      Map.empty[String, String]
    } else {
      Map("proposal" -> proposal.take(5).map(toSQLId).mkString(", "))
    }
    val errorSubClass = if (proposal.isEmpty) "WITHOUT_SUGGESTION" else "WITH_SUGGESTION"
    new AnalysisException(
      errorClass = s"UNRESOLVED_COLUMN.$errorSubClass",
      messageParameters = commonParam ++ proposalParam)
  }

  def unresolvedFieldError(
      fieldName: String,
      columnPath: Seq[String],
      proposal: Seq[String]): Throwable = {
    val commonParams = Map(
      "fieldName" -> toSQLId(fieldName),
      "columnPath" -> toSQLId(columnPath))
    val proposalParam = if (proposal.isEmpty) {
        Map.empty[String, String]
      } else {
        Map("proposal" -> proposal.map(toSQLId).mkString(", "))
      }
    val errorSubClass = if (proposal.isEmpty) "WITHOUT_SUGGESTION" else "WITH_SUGGESTION"
    new AnalysisException(
      errorClass = s"UNRESOLVED_FIELD.$errorSubClass",
      messageParameters = commonParams ++ proposalParam)
  }

  def dataTypeMismatchForDeserializerError(
      dataType: DataType, desiredType: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_DESERIALIZER.DATA_TYPE_MISMATCH",
      messageParameters = Map(
        "desiredType" -> toSQLType(desiredType),
        "dataType" -> toSQLType(dataType)))
  }

  def fieldNumberMismatchForDeserializerError(
      schema: StructType, maxOrdinal: Int): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_DESERIALIZER.FIELD_NUMBER_MISMATCH",
      messageParameters = Map(
        "schema" -> toSQLType(schema),
        "ordinal" -> (maxOrdinal + 1).toString))
  }

  def upCastFailureError(
      fromStr: String, from: Expression, to: DataType, walkedTypePath: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_UP_CAST_DATATYPE",
      messageParameters = Map(
        "expression" -> fromStr,
        "sourceType" -> toSQLType(from.dataType),
        "targetType" ->  toSQLType(to),
        "details" -> (s"The type path of the target object is:\n" +
          walkedTypePath.mkString("", "\n", "\n") +
          "You can either add an explicit cast to the input data or choose a higher precision " +
          "type of the field in the target object"))
    )
  }

  def outerScopeFailureForNewInstanceError(className: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1002",
      messageParameters = Map("className" -> className))
  }

  def referenceColNotFoundForAlterTableChangesError(
      fieldName: String, fields: Array[String]): Throwable = {
    new AnalysisException(
      errorClass = "FIELD_NOT_FOUND",
      messageParameters = Map(
        "fieldName" -> toSQLId(fieldName),
        "fields" -> fields.mkString(", ")))
  }

  def windowSpecificationNotDefinedError(windowName: String): Throwable = {
    new AnalysisException(
      errorClass = "MISSING_WINDOW_SPECIFICATION",
      messageParameters = Map(
        "windowName" -> windowName,
        "docroot" -> SPARK_DOC_ROOT)
      )
  }

  def selectExprNotInGroupByError(expr: Expression, groupByAliases: Seq[Alias]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1005",
      messageParameters = Map(
        "expr" -> expr.toString,
        "groupByAliases" -> groupByAliases.toString()))
  }

  def groupingMustWithGroupingSetsOrCubeOrRollupError(): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_GROUPING_EXPRESSION",
      messageParameters = Map.empty)
  }

  def pandasUDFAggregateNotSupportedInPivotError(): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.PANDAS_UDAF_IN_PIVOT",
      messageParameters = Map.empty)
  }

  def aggregateExpressionRequiredForPivotError(sql: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1006",
      messageParameters = Map("sql" -> sql))
  }

  def writeIntoTempViewNotAllowedError(quoted: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1007",
      messageParameters = Map("quoted" -> quoted))
  }

  def readNonStreamingTempViewError(quoted: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1008",
      messageParameters = Map("quoted" -> quoted))
  }

  def viewDepthExceedsMaxResolutionDepthError(
      identifier: TableIdentifier, maxNestedDepth: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "VIEW_EXCEED_MAX_NESTED_DEPTH",
      messageParameters = Map(
        "viewName" -> toSQLId(identifier.nameParts),
        "maxNestedDepth" -> maxNestedDepth.toString),
      origin = t.origin)
  }

  def insertIntoViewNotAllowedError(identifier: TableIdentifier, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
      messageParameters = Map(
        "viewName" -> toSQLId(identifier.nameParts),
        "operation" -> "INSERT"),
      origin = t.origin)
  }

  def writeIntoViewNotAllowedError(identifier: TableIdentifier, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1011",
      messageParameters = Map("identifier" -> identifier.toString),
      origin = t.origin)
  }

  def writeIntoV1TableNotAllowedError(identifier: TableIdentifier, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1012",
      messageParameters = Map("identifier" -> identifier.toString),
      origin = t.origin)
  }

  def expectTableNotViewError(
      nameParts: Seq[String],
      cmd: String,
      suggestAlternative: Boolean,
      t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = if (suggestAlternative) {
        "EXPECT_TABLE_NOT_VIEW.USE_ALTER_VIEW"
      } else {
        "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE"
      },
      messageParameters = Map(
        "viewName" -> toSQLId(nameParts),
        "operation" -> cmd),
      origin = t.origin)
  }

  def expectViewNotTableError(
      nameParts: Seq[String],
      cmd: String,
      suggestAlternative: Boolean,
      t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = if (suggestAlternative) {
        "EXPECT_VIEW_NOT_TABLE.USE_ALTER_TABLE"
      } else {
        "EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE"
      },
      messageParameters = Map(
        "tableName" -> toSQLId(nameParts),
        "operation" -> cmd),
      origin = t.origin)
  }

  def expectPermanentViewNotTempViewError(
      nameParts: Seq[String],
      cmd: String,
      t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "EXPECT_PERMANENT_VIEW_NOT_TEMP",
      messageParameters = Map(
        "viewName" -> toSQLId(nameParts),
        "operation" -> cmd),
      origin = t.origin)
  }

  def expectPersistentFuncError(
      name: String, cmd: String, mismatchHint: Option[String], t: TreeNode[_]): Throwable = {
    val hintStr = mismatchHint.map(" " + _).getOrElse("")
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1017",
      messageParameters = Map(
        "name" -> name,
        "cmd" -> cmd,
        "hintStr" -> hintStr),
      origin = t.origin)
  }

  def permanentViewNotSupportedByStreamingReadingAPIError(quoted: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1018",
      messageParameters = Map("quoted" -> quoted))
  }

  def starNotAllowedWhenGroupByOrdinalPositionUsedError(): Throwable = {
    new AnalysisException(
      errorClass = "STAR_GROUP_BY_POS",
      messageParameters = Map.empty)
  }

  def invalidStarUsageError(prettyName: String, stars: Seq[Star]): Throwable = {
    val regExpr = stars.collect{ case UnresolvedRegex(pattern, _, _) => s"'$pattern'" }
    val resExprMsg = Option(regExpr.distinct).filter(_.nonEmpty).map {
      case Seq(p) => s"regular expression $p"
      case patterns => s"regular expressions ${patterns.mkString(", ")}"
    }
    val starMsg = if (stars.length - regExpr.length > 0) {
      Some("'*'")
    } else {
      None
    }
    val elem = Seq(starMsg, resExprMsg).flatten.mkString(" and ")
    new AnalysisException(
      errorClass = "INVALID_USAGE_OF_STAR_OR_REGEX",
      messageParameters = Map("elem" -> elem, "prettyName" -> prettyName))
  }

  def singleTableStarInCountNotAllowedError(targetString: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1021",
      messageParameters = Map("targetString" -> targetString))
  }

  def orderByPositionRangeError(index: Int, size: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "ORDER_BY_POS_OUT_OF_RANGE",
      messageParameters = Map(
        "index" -> index.toString,
        "size" -> size.toString),
      origin = t.origin)
  }

  def groupByPositionRefersToAggregateFunctionError(
      index: Int,
      expr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "GROUP_BY_POS_AGGREGATE",
      messageParameters = Map(
        "index" -> index.toString,
        "aggExpr" -> expr.sql))
  }

  def groupByPositionRangeError(index: Int, size: Int): Throwable = {
    new AnalysisException(
      errorClass = "GROUP_BY_POS_OUT_OF_RANGE",
      messageParameters = Map(
        "index" -> index.toString,
        "size" -> size.toString))
  }

  def generatorNotExpectedError(name: FunctionIdentifier, classCanonicalName: String): Throwable = {
    new AnalysisException(errorClass = "UNSUPPORTED_GENERATOR.NOT_GENERATOR",
      messageParameters = Map(
        "functionName" -> toSQLId(name.toString),
        "classCanonicalName" -> classCanonicalName))
  }

  def functionWithUnsupportedSyntaxError(prettyName: String, syntax: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_SQL_SYNTAX.FUNCTION_WITH_UNSUPPORTED_SYNTAX",
      messageParameters = Map("prettyName" -> toSQLId(prettyName), "syntax" -> toSQLStmt(syntax)))
  }

  def subqueryExpressionInLambdaOrHigherOrderFunctionNotAllowedError(): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.HIGHER_ORDER_FUNCTION",
      messageParameters = Map.empty)
  }

  def nonDeterministicFilterInAggregateError(filterExpr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC",
      messageParameters = Map("filterExpr" -> toSQLExpr(filterExpr)))
  }

  def nonBooleanFilterInAggregateError(filterExpr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_AGGREGATE_FILTER.NOT_BOOLEAN",
      messageParameters = Map("filterExpr" -> toSQLExpr(filterExpr)))
  }

  def aggregateInAggregateFilterError(
      filterExpr: Expression,
      aggExpr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_AGGREGATE_FILTER.CONTAINS_AGGREGATE",
      messageParameters = Map(
        "filterExpr" -> toSQLExpr(filterExpr),
        "aggExpr" -> toSQLExpr(aggExpr)))
  }

  def windowFunctionInAggregateFilterError(
      filterExpr: Expression,
      windowExpr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_AGGREGATE_FILTER.CONTAINS_WINDOW_FUNCTION",
      messageParameters = Map(
        "filterExpr" -> toSQLExpr(filterExpr),
        "windowExpr" -> toSQLExpr(windowExpr)))
  }

  def distinctInverseDistributionFunctionUnsupportedError(funcName: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_INVERSE_DISTRIBUTION_FUNCTION.DISTINCT_UNSUPPORTED",
      messageParameters = Map("funcName" -> toSQLId(funcName)))
  }

  def inverseDistributionFunctionMissingWithinGroupError(funcName: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_INVERSE_DISTRIBUTION_FUNCTION.WITHIN_GROUP_MISSING",
      messageParameters = Map("funcName" -> toSQLId(funcName)))
  }

  def wrongNumOrderingsForInverseDistributionFunctionError(
      funcName: String,
      validOrderingsNumber: Int,
      actualOrderingsNumber: Int): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_INVERSE_DISTRIBUTION_FUNCTION.WRONG_NUM_ORDERINGS",
      messageParameters = Map(
        "funcName" -> toSQLId(funcName),
        "expectedNum" -> validOrderingsNumber.toString,
        "actualNum" -> actualOrderingsNumber.toString))
  }

  def aliasNumberNotMatchColumnNumberError(
      columnSize: Int, outputSize: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "ASSIGNMENT_ARITY_MISMATCH",
      messageParameters = Map(
        "numExpr" -> columnSize.toString,
        "numTarget" -> outputSize.toString),
      origin = t.origin)
  }

  def aliasesNumberNotMatchUDTFOutputError(
      aliasesSize: Int, aliasesNames: String): Throwable = {
    new AnalysisException(
      errorClass = "UDTF_ALIAS_NUMBER_MISMATCH",
      messageParameters = Map(
        "aliasesSize" -> aliasesSize.toString,
        "aliasesNames" -> aliasesNames))
  }

  def invalidSortOrderInUDTFOrderingColumnFromAnalyzeMethodHasAlias(
      aliasName: String): Throwable = {
    new AnalysisException(
      errorClass = "UDTF_INVALID_ALIAS_IN_REQUESTED_ORDERING_STRING_FROM_ANALYZE_METHOD",
      messageParameters = Map("aliasName" -> aliasName))
  }

  def invalidUDTFSelectExpressionFromAnalyzeMethodNeedsAlias(expression: String): Throwable = {
    new AnalysisException(
      errorClass = "UDTF_INVALID_REQUESTED_SELECTED_EXPRESSION_FROM_ANALYZE_METHOD_REQUIRES_ALIAS",
      messageParameters = Map("expression" -> expression))
  }

  def windowAggregateFunctionWithFilterNotSupportedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1030",
      messageParameters = Map.empty)
  }

  def windowFunctionInsideAggregateFunctionNotAllowedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1031",
      messageParameters = Map.empty)
  }

  def expressionWithoutWindowExpressionError(expr: NamedExpression): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1032",
      messageParameters = Map("expr" -> expr.toString))
  }

  def expressionWithMultiWindowExpressionsError(
      expr: NamedExpression, distinctWindowSpec: Seq[WindowSpecDefinition]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1033",
      messageParameters = Map(
        "expr" -> expr.toString,
        "distinctWindowSpec" -> distinctWindowSpec.toString()))
  }

  def windowFunctionNotAllowedError(clauseName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1034",
      messageParameters = Map("clauseName" -> clauseName))
  }

  def cannotSpecifyWindowFrameError(prettyName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1035",
      messageParameters = Map("prettyName" -> prettyName))
  }

  def windowFrameNotMatchRequiredFrameError(
      f: SpecifiedWindowFrame, required: WindowFrame): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1036",
      messageParameters = Map(
        "wf" -> f.toString,
        "required" -> required.toString))
  }

  def windowFunctionWithWindowFrameNotOrderedError(wf: WindowFunction): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1037",
      messageParameters = Map("wf" -> wf.toString))
  }

  def multiTimeWindowExpressionsNotSupportedError(t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1039",
      messageParameters = Map.empty,
      origin = t.origin)
  }

  def sessionWindowGapDurationDataTypeError(dt: DataType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1040",
      messageParameters = Map("dt" -> dt.toString))
  }

  def unresolvedVariableError(name: Seq[String], searchPath: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "UNRESOLVED_VARIABLE",
      messageParameters = Map(
        "variableName" -> toSQLId(name),
        "searchPath" -> toSQLId(searchPath)))
  }

  def unresolvedVariableError(
      name: Seq[String],
      searchPath: Seq[String],
      origin: Origin): Throwable = {
    new AnalysisException(
      errorClass = "UNRESOLVED_VARIABLE",
      messageParameters = Map(
        "variableName" -> toSQLId(name),
        "searchPath" -> toSQLId(searchPath)),
      origin = origin)
  }

  def failedToLoadRoutineError(nameParts: Seq[String], e: Exception): Throwable = {
    new AnalysisException(
      errorClass = "FAILED_TO_LOAD_ROUTINE",
      messageParameters = Map("routineName" -> toSQLId(nameParts)),
      cause = Some(e))
  }

  def unresolvedRoutineError(name: FunctionIdentifier, searchPath: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "UNRESOLVED_ROUTINE",
      messageParameters = Map(
        "routineName" -> toSQLId(name.funcName),
        "searchPath" -> searchPath.map(toSQLId).mkString("[", ", ", "]")))
  }

  def unresolvedRoutineError(
      nameParts: Seq[String],
      searchPath: Seq[String],
      context: Origin): Throwable = {
    new AnalysisException(
      errorClass = "UNRESOLVED_ROUTINE",
      messageParameters = Map(
        "routineName" -> toSQLId(nameParts),
        "searchPath" -> searchPath.map(toSQLId).mkString("[", ", ", "]")
      ),
      origin = context)
  }

  def wrongNumArgsError(
      name: String,
      validParametersCount: Seq[Any],
      actualNumber: Int,
      legacyNum: String = "",
      legacyConfKey: String = "",
      legacyConfValue: String = ""): Throwable = {
    val expectedNumberOfParameters = if (validParametersCount.isEmpty) {
      "0"
    } else if (validParametersCount.length == 1) {
      validParametersCount.head.toString
    } else {
      validParametersCount.mkString("[", ", ", "]")
    }
    if (legacyNum.isEmpty) {
      new AnalysisException(
        errorClass = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
        messageParameters = Map(
          "functionName" -> toSQLId(name),
          "expectedNum" -> expectedNumberOfParameters,
          "actualNum" -> actualNumber.toString,
          "docroot" -> SPARK_DOC_ROOT))
    } else {
      new AnalysisException(
        errorClass = "WRONG_NUM_ARGS.WITH_SUGGESTION",
        messageParameters = Map(
          "functionName" -> toSQLId(name),
          "expectedNum" -> expectedNumberOfParameters,
          "actualNum" -> actualNumber.toString,
          "legacyNum" -> legacyNum,
          "legacyConfKey" -> legacyConfKey,
          "legacyConfValue" -> legacyConfValue)
      )
    }
  }

  def alterV2TableSetLocationWithPartitionNotSupportedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1045",
      messageParameters = Map.empty)
  }

  def joinStrategyHintParameterNotSupportedError(unsupported: Any): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1046",
      messageParameters = Map(
        "unsupported" -> unsupported.toString,
        "class" -> unsupported.getClass.toString))
  }

  def invalidHintParameterError(
      hintName: String, invalidParams: Seq[Any]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1047",
      messageParameters = Map(
        "hintName" -> hintName,
        "invalidParams" -> invalidParams.mkString(", ")))
  }

  def invalidCoalesceHintParameterError(hintName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1048",
      messageParameters = Map("hintName" -> hintName))
  }

  def starExpandDataTypeNotSupportedError(attributes: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1050",
      messageParameters = Map("attributes" -> attributes.toString()))
  }

  def cannotResolveStarExpandGivenInputColumnsError(
      targetString: String, columns: String): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_RESOLVE_STAR_EXPAND",
      messageParameters = Map(
        "targetString" -> toSQLId(targetString),
        "columns" -> columns))
  }

  def addColumnWithV1TableCannotSpecifyNotNullError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1052",
      messageParameters = Map.empty)
  }

  def unsupportedTableOperationError(
      catalog: CatalogPlugin,
      ident: Identifier,
      operation: String): Throwable = {
    unsupportedTableOperationError(
      (catalog.name +: ident.namespace :+ ident.name).toImmutableArraySeq, operation)
  }

  def unsupportedTableOperationError(
      ident: TableIdentifier,
      operation: String): Throwable = {
    unsupportedTableOperationError(
      Seq(ident.catalog.get, ident.database.get, ident.table), operation)
  }

  private def unsupportedTableOperationError(
      qualifiedTableName: Seq[String],
      operation: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      messageParameters = Map(
        "tableName" -> toSQLId(qualifiedTableName),
        "operation" -> operation))
  }

  private def unsupportedTableOperationError(
      tableName: String,
      operation: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "operation" -> operation))
  }

  def unsupportedBatchReadError(table: Table): Throwable = {
    unsupportedTableOperationError(table.name(), "batch scan")
  }

  def unsupportedStreamingScanError(table: Table): Throwable = {
    unsupportedTableOperationError(table.name(), "either micro-batch or continuous scan")
  }

  def unsupportedAppendInBatchModeError(name: String): Throwable = {
    unsupportedTableOperationError(name, "append in batch mode")
  }

  def unsupportedDynamicOverwriteInBatchModeError(table: Table): Throwable = {
    unsupportedTableOperationError(table.name(), "dynamic overwrite in batch mode")
  }

  def unsupportedTruncateInBatchModeError(table: Table): Throwable = {
    unsupportedTableOperationError(table.name(), "truncate in batch mode")
  }

  def unsupportedOverwriteByFilterInBatchModeError(name: String): Throwable = {
    unsupportedTableOperationError(name, "overwrite by filter in batch mode")
  }

  def catalogOperationNotSupported(catalog: CatalogPlugin, operation: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.CATALOG_OPERATION",
      messageParameters = Map(
        "catalogName" -> toSQLId(Seq(catalog.name())),
        "operation" -> operation))
  }

  def wrongCommandForObjectTypeError(
      operation: String,
      requiredType: String,
      objectName: String,
      foundType: String,
      alternative: String): Throwable = {
    new AnalysisException(
      errorClass = "WRONG_COMMAND_FOR_OBJECT_TYPE",
      messageParameters = Map(
        "operation" -> operation,
        "requiredType" -> requiredType,
        "objectName" -> objectName,
        "foundType" -> foundType,
        "alternative" -> alternative
      )
    )
  }

  def showColumnsWithConflictNamespacesError(
      namespaceA: Seq[String],
      namespaceB: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "SHOW_COLUMNS_WITH_CONFLICT_NAMESPACE",
      messageParameters = Map(
        "namespaceA" -> toSQLId(namespaceA),
        "namespaceB" -> toSQLId(namespaceB)))
  }

  def cannotCreateTableWithBothProviderAndSerdeError(
      provider: Option[String], maybeSerdeInfo: Option[SerdeInfo]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1058",
      messageParameters = Map(
        "provider" -> provider.toString,
        "serDeInfo" -> maybeSerdeInfo.get.describe))
  }

  def invalidFileFormatForStoredAsError(serdeInfo: SerdeInfo): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1059",
      messageParameters = Map("serdeInfo" -> serdeInfo.storedAs.get))
  }

  def commandNotSupportNestedColumnError(command: String, quoted: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1060",
      messageParameters = Map(
        "command" -> command,
        "column" -> quoted))
  }

  def renameTempViewToExistingViewError(newName: String): Throwable = {
    new TableAlreadyExistsException(newName)
  }

  def cannotDropNonemptyDatabaseError(db: String): Throwable = {
    new AnalysisException(errorClass = "SCHEMA_NOT_EMPTY",
      Map("schemaName" -> toSQLId(db)))
  }

  def cannotDropNonemptyNamespaceError(namespace: Seq[String]): Throwable = {
    new AnalysisException(errorClass = "SCHEMA_NOT_EMPTY",
      Map("schemaName" -> namespace.map(part => quoteIdentifier(part)).mkString(".")))
  }

  def invalidNameForTableOrDatabaseError(name: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_SCHEMA_OR_RELATION_NAME",
      messageParameters = Map("name" -> toSQLId(name)))
  }

  def cannotCreateDatabaseWithSameNameAsPreservedDatabaseError(database: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1066",
      messageParameters = Map("database" -> database))
  }

  def cannotDropDefaultDatabaseError(nameParts: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.DROP_DATABASE",
      messageParameters = Map("database" -> toSQLId(nameParts)))
  }

  def cannotUsePreservedDatabaseAsCurrentDatabaseError(database: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1068",
      messageParameters = Map("database" -> database))
  }

  def createExternalTableWithoutLocationError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1069",
      messageParameters = Map.empty)
  }

  def dropNonExistentColumnsNotSupportedError(
      nonExistentColumnNames: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1071",
      messageParameters = Map(
        "nonExistentColumnNames" -> nonExistentColumnNames.mkString("[", ",", "]")))
  }

  def cannotRetrieveTableOrViewNotInSameDatabaseError(
      qualifiedTableNames: Seq[QualifiedTableName]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1072",
      messageParameters = Map("qualifiedTableNames" -> qualifiedTableNames.toString()))
  }

  def renameTableSourceAndDestinationMismatchError(db: String, newDb: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1073",
      messageParameters = Map("db" -> db, "newDb" -> newDb))
  }

  def cannotRenameTempViewWithDatabaseSpecifiedError(
      oldName: TableIdentifier, newName: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1074",
      messageParameters = Map(
        "oldName" -> oldName.toString,
        "newName" -> newName.toString,
        "db" -> newName.database.get))
  }

  def cannotRenameTempViewToExistingTableError(newName: TableIdentifier): Throwable = {
    new TableAlreadyExistsException(newName.nameParts)
  }

  def invalidPartitionSpecError(details: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1076",
      messageParameters = Map("details" -> details))
  }

  def functionAlreadyExistsError(func: FunctionIdentifier): Throwable = {
    new FunctionAlreadyExistsException(func.nameParts)
  }

  def cannotLoadClassWhenRegisteringFunctionError(
      className: String, func: FunctionIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_LOAD_FUNCTION_CLASS",
      messageParameters = Map(
        "className" -> className,
        "functionName" -> toSQLId(func.toString)))
  }

  def resourceTypeNotSupportedError(resourceType: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1079",
      messageParameters = Map("resourceType" -> resourceType))
  }

  def tableNotSpecifyDatabaseError(identifier: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1080",
      messageParameters = Map("identifier" -> identifier.toString))
  }

  def tableNotSpecifyLocationUriError(identifier: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1081",
      messageParameters = Map("identifier" -> identifier.toString))
  }

  def partitionNotSpecifyLocationUriError(specString: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1082",
      messageParameters = Map("specString" -> specString))
  }

  def invalidBucketNumberError(bucketingMaxBuckets: Int, numBuckets: Int): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1083",
      messageParameters = Map(
        "bucketingMaxBuckets" -> bucketingMaxBuckets.toString,
        "numBuckets" -> numBuckets.toString))
  }

  def corruptedTableNameContextInCatalogError(numParts: Int, index: Int): Throwable = {
    new AnalysisException(
      errorClass = "INTERNAL_ERROR_METADATA_CATALOG.TABLE_NAME_CONTEXT",
      messageParameters = Map(
        "numParts" -> numParts.toString,
        "index" -> index.toString))
  }

  def corruptedViewSQLConfigsInCatalogError(e: Exception): Throwable = {
    new AnalysisException(
      errorClass = "INTERNAL_ERROR_METADATA_CATALOG.SQL_CONFIG",
      messageParameters = Map.empty,
      cause = Some(e))
  }

  def corruptedViewQueryOutputColumnsInCatalogError(numCols: String, index: Int): Throwable = {
    new AnalysisException(
      errorClass = "INTERNAL_ERROR_METADATA_CATALOG.VIEW_QUERY_COLUMN_ARITY",
      messageParameters = Map(
        "numCols" -> numCols,
        "index" -> index.toString))
  }

  def corruptedViewReferredTempViewInCatalogError(e: Exception): Throwable = {
    new AnalysisException(
      errorClass = "INTERNAL_ERROR_METADATA_CATALOG.TEMP_VIEW_REFERENCE",
      messageParameters = Map.empty,
      cause = Some(e))
  }

  def corruptedViewReferredTempFunctionsInCatalogError(e: Exception): Throwable = {
    new AnalysisException(
      errorClass = "INTERNAL_ERROR_METADATA_CATALOG.TEMP_FUNCTION_REFERENCE",
      messageParameters = Map.empty,
      cause = Some(e))
  }

  def columnStatisticsDeserializationNotSupportedError(
      name: String, dataType: DataType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1089",
      messageParameters = Map("name" -> name, "dataType" -> dataType.toString))
  }

  def columnStatisticsSerializationNotSupportedError(
      colName: String, dataType: DataType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1090",
      messageParameters = Map("colName" -> colName, "dataType" -> dataType.toString))
  }

  def insufficientTablePropertyError(key: String): Throwable = {
    new AnalysisException(
      errorClass = "INSUFFICIENT_TABLE_PROPERTY.MISSING_KEY",
      messageParameters = Map("key" -> toSQLConf(key)))
  }

  def insufficientTablePropertyPartError(
      key: String, totalAmountOfParts: String): Throwable = {
    new AnalysisException(
      errorClass = "INSUFFICIENT_TABLE_PROPERTY.MISSING_KEY_PART",
      messageParameters = Map(
        "key" -> toSQLConf(key),
        "totalAmountOfParts" -> totalAmountOfParts))
  }

  def unexpectedSchemaTypeError(exp: Expression): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_SCHEMA.NON_STRING_LITERAL",
      messageParameters = Map("inputSchema" -> toSQLExpr(exp)))
  }

  def schemaIsNotStructTypeError(exp: Expression, dataType: DataType): Throwable = {
    schemaIsNotStructTypeError(toSQLExpr(exp), dataType)
  }

  def schemaIsNotStructTypeError(inputSchema: String, dataType: DataType): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_SCHEMA.NON_STRUCT_TYPE",
      messageParameters = Map(
        "inputSchema" -> inputSchema,
        "dataType" -> toSQLType(dataType)
      ))
  }

  def keyValueInMapNotStringError(m: CreateMap): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_OPTIONS.NON_STRING_TYPE",
      messageParameters = Map("mapType" -> toSQLType(m.dataType)))
  }

  def nonMapFunctionNotAllowedError(): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_OPTIONS.NON_MAP_FUNCTION",
      messageParameters = Map.empty)
  }

  def invalidFieldTypeForCorruptRecordError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1097",
      messageParameters = Map.empty)
  }

  def dataTypeUnsupportedByClassError(x: DataType, className: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1098",
      messageParameters = Map("x" -> x.toString, "className" -> className))
  }

  def parseModeUnsupportedError(funcName: String, mode: ParseMode): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1099",
      messageParameters = Map(
        "funcName" -> funcName,
        "mode" -> mode.name,
        "permissiveMode" -> PermissiveMode.name,
        "failFastMode" -> FailFastMode.name))
  }

  def nonFoldableArgumentError(
      funcName: String,
      paramName: String,
      paramType: DataType): Throwable = {
    new AnalysisException(
      errorClass = "NON_FOLDABLE_ARGUMENT",
      messageParameters = Map(
        "funcName" -> toSQLId(funcName),
        "paramName" -> toSQLId(paramName),
        "paramType" -> toSQLType(paramType)))
  }

  def literalTypeUnsupportedForSourceTypeError(field: String, source: Expression): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_EXTRACT_FIELD",
      messageParameters = Map(
        "field" -> toSQLId(field),
        "expr" -> toSQLExpr(source)))
  }

  def arrayComponentTypeUnsupportedError(clz: Class[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1103",
      messageParameters = Map("clz" -> clz.toString))
  }

  def secondArgumentNotDoubleLiteralError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1104",
      messageParameters = Map.empty)
  }

  def dataTypeUnsupportedByExtractValueError(
      dataType: DataType, extraction: Expression, child: Expression): Throwable = {
    dataType match {
      case StructType(_) =>
        new AnalysisException(
          errorClass = "INVALID_EXTRACT_FIELD_TYPE",
          messageParameters = Map("extraction" -> toSQLExpr(extraction)))
      case other =>
        new AnalysisException(
          errorClass = "INVALID_EXTRACT_BASE_FIELD_TYPE",
          messageParameters = Map(
            "base" -> toSQLExpr(child),
            "other" -> toSQLType(other)))
    }
  }

  def noHandlerForUDAFError(name: String): Throwable = {
    new InvalidUDFClassException(
      errorClass = "NO_HANDLER_FOR_UDAF",
      messageParameters = Map("functionName" -> name))
  }

  def batchWriteCapabilityError(
      table: Table, v2WriteClassName: String, v1WriteClassName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1107",
      messageParameters = Map(
        "table" -> table.name,
        "batchWrite" -> TableCapability.V1_BATCH_WRITE.toString,
        "v2WriteClassName" -> v2WriteClassName,
        "v1WriteClassName" -> v1WriteClassName))
  }

  def unsupportedDeleteByConditionWithSubqueryError(condition: Expression): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1108",
      messageParameters = Map("condition" -> condition.toString))
  }

  def cannotTranslateExpressionToSourceFilterError(f: Expression): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1109",
      messageParameters = Map("f" -> f.toString))
  }

  def cannotDeleteTableWhereFiltersError(table: Table, filters: Array[Predicate]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1110",
      messageParameters = Map(
        "table" -> table.name,
        "filters" -> filters.mkString("[", ", ", "]")))
  }

  def describeDoesNotSupportPartitionForV2TablesError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1111",
      messageParameters = Map.empty)
  }

  def cannotReplaceMissingTableError(
      tableIdentifier: Identifier): Throwable = {
    new CannotReplaceMissingTableException(tableIdentifier)
  }

  def cannotReplaceMissingTableError(
      tableIdentifier: Identifier, cause: Option[Throwable]): Throwable = {
    new CannotReplaceMissingTableException(tableIdentifier, cause)
  }

  def streamingSourcesDoNotSupportCommonExecutionModeError(
      microBatchSources: Seq[String],
      continuousSources: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1114",
      messageParameters = Map(
        "microBatchSources" -> microBatchSources.mkString(", "),
        "continuousSources" -> continuousSources.mkString(", ")))
  }

  def noSuchTableError(ident: Identifier): NoSuchTableException = {
    new NoSuchTableException(ident.asMultipartIdentifier)
  }

  def noSuchTableError(nameParts: Seq[String]): Throwable = {
    new NoSuchTableException(nameParts)
  }

  def noSuchNamespaceError(namespace: Array[String]): Throwable = {
    new NoSuchNamespaceException(namespace)
  }

  def tableAlreadyExistsError(ident: Identifier): Throwable = {
    new TableAlreadyExistsException(ident.asMultipartIdentifier)
  }

  def requiresSinglePartNamespaceError(namespace: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "REQUIRES_SINGLE_PART_NAMESPACE",
      messageParameters = Map(
        "sessionCatalog" -> CatalogManager.SESSION_CATALOG_NAME,
        "namespace" -> toSQLId(namespace)))
  }

  def namespaceAlreadyExistsError(namespace: Array[String]): Throwable = {
    new NamespaceAlreadyExistsException(namespace)
  }

  private def notSupportedInJDBCCatalog(cmd: String): Throwable = {
    new AnalysisException(
      errorClass = "NOT_SUPPORTED_IN_JDBC_CATALOG.COMMAND",
      messageParameters = Map("cmd" -> toSQLStmt(cmd)))
  }

  private def notSupportedInJDBCCatalog(cmd: String, property: String): Throwable = {
    new AnalysisException(
      errorClass = "NOT_SUPPORTED_IN_JDBC_CATALOG.COMMAND_WITH_PROPERTY",
      messageParameters = Map(
        "cmd" -> toSQLStmt(cmd),
        "property" -> toSQLConf(property)))
  }

  def cannotCreateJDBCTableUsingProviderError(): Throwable = {
    notSupportedInJDBCCatalog("CREATE TABLE ... USING ...")
  }

  def cannotCreateJDBCTableUsingLocationError(): Throwable = {
    notSupportedInJDBCCatalog("CREATE TABLE ... LOCATION ...")
  }

  def cannotCreateJDBCNamespaceUsingProviderError(): Throwable = {
    notSupportedInJDBCCatalog("CREATE NAMESPACE ... LOCATION ...")
  }

  def cannotCreateJDBCNamespaceWithPropertyError(property: String): Throwable = {
    notSupportedInJDBCCatalog(s"CREATE NAMESPACE", property)
  }

  def cannotSetJDBCNamespaceWithPropertyError(property: String): Throwable = {
    notSupportedInJDBCCatalog("SET NAMESPACE", property)
  }

  def cannotUnsetJDBCNamespaceWithPropertyError(property: String): Throwable = {
    notSupportedInJDBCCatalog("UNSET NAMESPACE", property)
  }

  def unsupportedJDBCNamespaceChangeInCatalogError(changes: Seq[NamespaceChange]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1120",
      messageParameters = Map("changes" -> changes.toString()))
  }

  private def tableDoesNotSupportError(cmd: String, table: Table): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1121",
      messageParameters = Map(
        "cmd" -> cmd,
        "table" -> table.name))
  }

  def tableDoesNotSupportReadsError(table: Table): Throwable = {
    tableDoesNotSupportError("reads", table)
  }

  def tableDoesNotSupportWritesError(table: Table): Throwable = {
    tableDoesNotSupportError("writes", table)
  }

  def tableDoesNotSupportDeletesError(table: Table): Throwable = {
    tableDoesNotSupportError("deletes", table)
  }

  def tableDoesNotSupportTruncatesError(table: Table): Throwable = {
    tableDoesNotSupportError("truncates", table)
  }

  def tableDoesNotSupportPartitionManagementError(table: Table): Throwable = {
    tableDoesNotSupportError("partition management", table)
  }

  def tableDoesNotSupportAtomicPartitionManagementError(table: Table): Throwable = {
    tableDoesNotSupportError("atomic partition management", table)
  }

  def tableIsNotRowLevelOperationTableError(table: Table): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1122",
      messageParameters = Map("table" -> table.name()))
  }

  def cannotRenameTableWithAlterViewError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1123",
      messageParameters = Map.empty)
  }

  private def notSupportedForV2TablesError(cmd: String): Throwable = {
    new AnalysisException(
      errorClass = "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE",
      messageParameters = Map("cmd" -> toSQLStmt(cmd)))
  }

  def analyzeTableNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("ANALYZE TABLE")
  }

  def alterTableRecoverPartitionsNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("ALTER TABLE ... RECOVER PARTITIONS")
  }

  def alterTableSerDePropertiesNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]")
  }

  def loadDataNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("LOAD DATA")
  }

  def showCreateTableAsSerdeNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("SHOW CREATE TABLE AS SERDE")
  }

  def repairTableNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("MSCK REPAIR TABLE")
  }

  def databaseFromV1SessionCatalogNotSpecifiedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1125",
      messageParameters = Map.empty)
  }

  def nestedDatabaseUnsupportedByV1SessionCatalogError(catalog: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1126",
      messageParameters = Map("catalog" -> catalog))
  }

  def invalidRepartitionExpressionsError(sortOrders: Seq[Any]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1127",
      messageParameters = Map("sortOrders" -> sortOrders.toString()))
  }

  def partitionColumnNotSpecifiedError(format: String, partitionColumn: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1128",
      messageParameters = Map(
        "format" -> format,
        "partitionColumn" -> partitionColumn))
  }

  def dataSchemaNotSpecifiedError(format: String): Throwable = {
    new AnalysisException(
      errorClass = "UNABLE_TO_INFER_SCHEMA",
      messageParameters = Map("format" -> format))
  }

  def dataPathNotExistError(path: String): Throwable = {
    new AnalysisException(
      errorClass = "PATH_NOT_FOUND",
      messageParameters = Map("path" -> path))
  }

  def dataSourceOutputModeUnsupportedError(
      className: String, outputMode: OutputMode): AnalysisException = {
    new AnalysisException(
      errorClass = "STREAMING_OUTPUT_MODE.UNSUPPORTED_DATASOURCE",
      messageParameters = Map(
        "className" -> className,
        "outputMode" -> outputMode.toString.toLowerCase(Locale.ROOT)))
  }

  def unsupportedOutputModeForStreamingOperationError(
      outputMode: OutputMode, operation: String): AnalysisException = {
    new AnalysisException(
      errorClass = "STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION",
      messageParameters = Map(
        "outputMode" -> outputMode.toString().toLowerCase(Locale.ROOT),
        "operation" -> operation))
  }

  def jdbcGeneratedQuerySyntaxError(url: String, query: String): Throwable = {
    new AnalysisException(
      errorClass = "FAILED_JDBC.SYNTAX_ERROR",
      messageParameters = Map(
        "query" -> query,
        "url" -> url
      )
    )
  }

  def jdbcGeneratedQueryGetSchemaError(url: String, query: String): Throwable = {
    new AnalysisException(
      errorClass = "FAILED_JDBC.GET_SCHEMA",
      messageParameters = Map(
        "query" -> query,
        "url" -> url
      )
    )
  }

  def schemaNotSpecifiedForSchemaRelationProviderError(className: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1132",
      messageParameters = Map("className" -> className))
  }

  def userSpecifiedSchemaMismatchActualSchemaError(
      schema: StructType, actualSchema: StructType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1133",
      messageParameters = Map(
        "schema" -> schema.toDDL,
        "actualSchema" -> actualSchema.toDDL))
  }

  def dataSchemaNotSpecifiedError(format: String, fileCatalog: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1134",
      messageParameters = Map(
        "format" -> format,
        "fileCatalog" -> fileCatalog))
  }

  def invalidDataSourceError(className: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1135",
      messageParameters = Map("className" -> className))
  }

  def cannotResolveAttributeError(name: String, outputStr: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1137",
      messageParameters = Map("name" -> name, "outputStr" -> outputStr))
  }

  def orcNotUsedWithHiveEnabledError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1138",
      messageParameters = Map.empty)
  }

  def failedToFindAvroDataSourceError(provider: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1139",
      messageParameters = Map("provider" -> provider))
  }

  def failedToFindKafkaDataSourceError(provider: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1140",
      messageParameters = Map("provider" -> provider))
  }

  def findMultipleDataSourceError(provider: String, sourceNames: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1141",
      messageParameters = Map(
        "provider" -> provider,
        "sourceNames" -> sourceNames.mkString(", ")))
  }

  def writeEmptySchemasUnsupportedByDataSourceError(format: String): Throwable = {
    new AnalysisException(
      errorClass = "EMPTY_SCHEMA_NOT_SUPPORTED_FOR_DATASOURCE",
      messageParameters = Map("format" -> format))
  }

  def insertMismatchedColumnNumberError(
      targetAttributes: Seq[Attribute],
      sourceAttributes: Seq[Attribute],
      staticPartitionsSize: Int): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1143",
      messageParameters = Map(
        "targetSize" -> targetAttributes.size.toString,
        "actualSize" -> (sourceAttributes.size + staticPartitionsSize).toString,
        "staticPartitionsSize" -> staticPartitionsSize.toString))
  }

  def insertMismatchedPartitionNumberError(
      targetPartitionSchema: StructType,
      providedPartitionsSize: Int): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1144",
      messageParameters = Map(
        "targetSize" -> targetPartitionSchema.fields.length.toString,
        "providedPartitionsSize" -> providedPartitionsSize.toString))
  }

  def invalidPartitionColumnError(
      partKey: String, targetPartitionSchema: StructType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1145",
      messageParameters = Map(
        "partKey" -> partKey,
        "partitionColumns" -> targetPartitionSchema.fields.map(_.name).mkString("[", ",", "]")))
  }

  def multiplePartitionColumnValuesSpecifiedError(
      field: StructField, potentialSpecs: Map[String, String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1146",
      messageParameters = Map(
        "partColumn" -> field.name,
        "values" -> potentialSpecs.mkString("[", ", ", "]")))
  }

  def invalidOrderingForConstantValuePartitionColumnError(
      targetPartitionSchema: StructType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1147",
      messageParameters = Map(
        "partColumns" -> targetPartitionSchema.fields.map(_.name).mkString("[", ",", "]")))
  }

  def cannotWriteDataToRelationsWithMultiplePathsError(paths: Seq[Path]): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_INSERT.MULTI_PATH",
      messageParameters = Map(
        "paths" -> paths.mkString("[", ",", "]")))
  }

  def failedToRebuildExpressionError(filter: Filter): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1149",
      messageParameters = Map("filter" -> filter.toString))
  }

  def dataTypeUnsupportedByDataSourceError(format: String, column: StructField): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
      messageParameters = Map(
        "columnName" -> toSQLId(column.name),
        "columnType" -> toSQLType(column.dataType),
        "format" -> format))
  }

  def failToResolveDataSourceForTableError(table: CatalogTable, key: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1151",
      messageParameters = Map(
        "table" -> table.identifier.toString,
        "key" -> key,
        "config" -> SQLConf.LEGACY_EXTRA_OPTIONS_BEHAVIOR.key))
  }

  def outputPathAlreadyExistsError(outputPath: Path): Throwable = {
    new AnalysisException(
      errorClass = "PATH_ALREADY_EXISTS",
      messageParameters = Map("outputPath" -> outputPath.toString))
  }

  def invalidPartitionColumnDataTypeError(field: StructField): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_PARTITION_COLUMN_DATA_TYPE",
      messageParameters = Map("type" -> toSQLType(field.dataType)))
  }

  def cannotUseAllColumnsForPartitionColumnsError(): Throwable = {
    new AnalysisException(
      errorClass = "ALL_PARTITION_COLUMNS_NOT_ALLOWED",
      messageParameters = Map.empty)
  }

  def partitionColumnNotFoundInSchemaError(col: String, schemaCatalog: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1155",
      messageParameters = Map("col" -> col, "schemaCatalog" -> schemaCatalog))
  }

  def columnNotFoundInSchemaError(
      col: StructField, tableSchema: Option[StructType]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1156",
      messageParameters = Map(
        "colName" -> col.name,
        "tableSchema" -> tableSchema.toString))
  }

  def saveDataIntoViewNotAllowedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1158",
      messageParameters = Map.empty)
  }

  def mismatchedTableFormatError(
      tableName: String, existingProvider: Class[_], specifiedProvider: Class[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1159",
      messageParameters = Map(
        "tableName" -> tableName,
        "existingProvider" -> existingProvider.getSimpleName,
        "specifiedProvider" -> specifiedProvider.getSimpleName))
  }

  def mismatchedTableLocationError(
      identifier: TableIdentifier,
      existingTable: CatalogTable,
      tableDesc: CatalogTable): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1160",
      messageParameters = Map(
        "identifier" -> identifier.quotedString,
        "existingTableLoc" -> existingTable.location.toString,
        "tableDescLoc" -> tableDesc.location.toString))
  }

  def mismatchedTableColumnNumberError(
      tableName: String,
      existingTable: CatalogTable,
      query: LogicalPlan): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1161",
      messageParameters = Map(
        "tableName" -> tableName,
        "existingTableSchema" -> existingTable.schema.catalogString,
        "querySchema" -> query.schema.catalogString))
  }

  def cannotResolveColumnGivenInputColumnsError(col: String, inputColumns: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1162",
      messageParameters = Map(
        "col" -> col,
        "inputColumns" -> inputColumns))
  }

  def mismatchedTablePartitionColumnError(
      tableName: String,
      specifiedPartCols: Seq[String],
      existingPartCols: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1163",
      messageParameters = Map(
        "tableName" -> tableName,
        "specifiedPartCols" -> specifiedPartCols.mkString(", "),
        "existingPartCols" -> existingPartCols))
  }

  def mismatchedTableBucketingError(
      tableName: String,
      specifiedBucketString: String,
      existingBucketString: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1164",
      messageParameters = Map(
        "tableName" -> tableName,
        "specifiedBucketString" -> specifiedBucketString,
        "existingBucketString" -> existingBucketString))
  }

  def mismatchedTableClusteringError(
      tableName: String,
      specifiedClusteringString: String,
      existingClusteringString: String): Throwable = {
    new AnalysisException(
      errorClass = "CLUSTERING_COLUMNS_MISMATCH",
      messageParameters = Map(
        "tableName" -> tableName,
        "specifiedClusteringString" -> specifiedClusteringString,
        "existingClusteringString" -> existingClusteringString))
  }

  def specifyPartitionNotAllowedWhenTableSchemaNotDefinedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1165",
      messageParameters = Map.empty)
  }

  def bucketingColumnCannotBePartOfPartitionColumnsError(
      bucketCol: String, normalizedPartCols: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1166",
      messageParameters = Map(
        "bucketCol" -> bucketCol,
        "normalizedPartCols" -> normalizedPartCols.mkString(", ")))
  }

  def bucketSortingColumnCannotBePartOfPartitionColumnsError(
    sortCol: String, normalizedPartCols: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1167",
      messageParameters = Map(
        "sortCol" -> sortCol,
        "normalizedPartCols" -> normalizedPartCols.mkString(", ")))
  }

  def invalidBucketColumnDataTypeError(dataType: DataType): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_BUCKET_COLUMN_DATA_TYPE",
      messageParameters = Map("type" -> toSQLType(dataType)))
  }

  def requestedPartitionsMismatchTablePartitionsError(
      tableName: String,
      normalizedPartSpec: Map[String, Option[String]],
      partColNames: StructType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1169",
      messageParameters = Map(
        "tableName" -> tableName,
        "normalizedPartSpec" -> normalizedPartSpec.keys.mkString(","),
        "partColNames" -> partColNames.map(_.name).mkString(",")))
  }

  def ddlWithoutHiveSupportEnabledError(cmd: String): Throwable = {
    new AnalysisException(
      errorClass = "NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT",
      messageParameters = Map("cmd" -> cmd))
  }

  def createTableColumnTypesOptionColumnNotFoundInSchemaError(
      col: String, schema: StructType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1171",
      messageParameters = Map("col" -> col, "schema" -> schema.catalogString))
  }

  def invalidVariantMissingFieldError(field: String): Throwable = {
    new AnalysisException(errorClass = "INVALID_VARIANT_FROM_PARQUET.MISSING_FIELD",
      messageParameters = Map("field" -> field))
  }

  def invalidVariantNullableOrNotBinaryFieldError(field: String): Throwable = {
    new AnalysisException(errorClass = "INVALID_VARIANT_FROM_PARQUET.NULLABLE_OR_NOT_BINARY_FIELD",
      messageParameters = Map("field" -> field))
  }

  def invalidVariantWrongNumFieldsError(): Throwable = {
    new AnalysisException(errorClass = "INVALID_VARIANT_FROM_PARQUET.WRONG_NUM_FIELDS",
      messageParameters = Map.empty)
  }

  def parquetTypeUnsupportedYetError(parquetType: String): Throwable = {
    new AnalysisException(
      errorClass = "PARQUET_TYPE_NOT_SUPPORTED",
      messageParameters = Map("parquetType" -> parquetType))
  }

  def illegalParquetTypeError(parquetType: String): Throwable = {
    new AnalysisException(
      errorClass = "PARQUET_TYPE_ILLEGAL",
      messageParameters = Map("parquetType" -> parquetType))
  }

  def unrecognizedParquetTypeError(field: String): Throwable = {
    new AnalysisException(
      errorClass = "PARQUET_TYPE_NOT_RECOGNIZED",
      messageParameters = Map("field" -> field))
  }

  def cannotConvertDataTypeToParquetTypeError(field: StructField): Throwable = {
    new AnalysisException(
      errorClass = "INTERNAL_ERROR",
      messageParameters = Map("message" ->
        s"Cannot convert Spark data type ${toSQLType(field.dataType)} to any Parquet type."))
  }

  def incompatibleViewSchemaChangeError(
      viewName: String,
      colName: String,
      expectedNum: Int,
      actualCols: Seq[Attribute],
      viewDDL: Option[String]): Throwable = {
    viewDDL.map { v =>
      new AnalysisException(
        errorClass = "INCOMPATIBLE_VIEW_SCHEMA_CHANGE",
        messageParameters = Map(
          "viewName" -> viewName,
          "colName" -> colName,
          "expectedNum" -> expectedNum.toString,
          "actualCols" -> actualCols.map(_.name).mkString("[", ",", "]"),
          "suggestion" -> v))
    }.getOrElse {
      new AnalysisException(
        errorClass = "INCOMPATIBLE_VIEW_SCHEMA_CHANGE",
        messageParameters = Map(
          "viewName" -> viewName,
          "colName" -> colName,
          "expectedNum" -> expectedNum.toString,
          "actualCols" -> actualCols.map(_.name).mkString("[", ",", "]"),
          "suggestion" -> "CREATE OR REPLACE TEMPORARY VIEW"))
    }
  }

  def numberOfPartitionsNotAllowedWithUnspecifiedDistributionError(): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_WRITE_DISTRIBUTION.PARTITION_NUM_WITH_UNSPECIFIED_DISTRIBUTION",
      messageParameters = Map.empty)
  }

  def partitionSizeNotAllowedWithUnspecifiedDistributionError(): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_WRITE_DISTRIBUTION.PARTITION_SIZE_WITH_UNSPECIFIED_DISTRIBUTION",
      messageParameters = Map.empty)
  }

  def numberAndSizeOfPartitionsNotAllowedTogether(): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_WRITE_DISTRIBUTION.PARTITION_NUM_AND_SIZE",
      messageParameters = Map.empty)
  }

  def unexpectedInputDataTypeError(
      functionName: String,
      paramIndex: Int,
      dataType: DataType,
      expression: Expression): Throwable = {
    new AnalysisException(
      errorClass = "UNEXPECTED_INPUT_TYPE",
      messageParameters = Map(
        "paramIndex" -> ordinalNumber(paramIndex - 1),
        "functionName" -> toSQLId(functionName),
        "requiredType" -> toSQLType(dataType),
        "inputSql" -> toSQLExpr(expression),
        "inputType" -> toSQLType(expression.dataType)))
  }

  def unexpectedNullError(exprName: String, expression: Expression): Throwable = {
    new AnalysisException(
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_NULL",
      messageParameters = Map(
        "exprName" -> toSQLId(exprName),
        "sqlExpr" -> toSQLExpr(expression)
      ))
  }

  def streamJoinStreamWithoutEqualityPredicateUnsupportedError(plan: LogicalPlan): Throwable = {
    new ExtendedAnalysisException(
      new AnalysisException(errorClass = "_LEGACY_ERROR_TEMP_1181", messageParameters = Map.empty),
      plan = plan)
  }

  def invalidPandasUDFPlacementError(
      groupAggPandasUDFNames: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_PANDAS_UDF_PLACEMENT",
      messageParameters = Map(
        "functionList" -> groupAggPandasUDFNames.map(toSQLId).mkString(", ")))
  }

  def ambiguousAttributesInSelfJoinError(
      ambiguousAttrs: Seq[AttributeReference]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1182",
      messageParameters = Map(
        "ambiguousAttrs" -> ambiguousAttrs.mkString(", "),
        "config" -> SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key))
  }

  def ambiguousColumnOrFieldError(
      name: Seq[String], numMatches: Int, context: Origin): Throwable = {
    DataTypeErrors.ambiguousColumnOrFieldError(name, numMatches, context)
  }

  def ambiguousReferenceError(name: String, ambiguousReferences: Seq[Attribute]): Throwable = {
    new AnalysisException(
      errorClass = "AMBIGUOUS_REFERENCE",
      messageParameters = Map(
        "name" -> toSQLId(name),
        "referenceNames" ->
          ambiguousReferences.map(ar => toSQLId(ar.qualifiedName)).sorted.mkString("[", ", ", "]")))
  }

  def cannotUseIntervalTypeInTableSchemaError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1183",
      messageParameters = Map.empty)
  }

  def missingCatalogAbilityError(plugin: CatalogPlugin, ability: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1184",
      messageParameters = Map(
        "plugin" -> plugin.name,
        "ability" -> ability))
  }

  def tableValuedFunctionTooManyTableArgumentsError(num: Int): Throwable = {
    new AnalysisException(
      errorClass = "TABLE_VALUED_FUNCTION_TOO_MANY_TABLE_ARGUMENTS",
      messageParameters = Map("num" -> num.toString)
    )
  }

  def tableValuedFunctionFailedToAnalyseInPythonError(msg: String): Throwable = {
    new AnalysisException(
      errorClass = "TABLE_VALUED_FUNCTION_FAILED_TO_ANALYZE_IN_PYTHON",
      messageParameters = Map("msg" -> msg)
    )
  }

  def pythonDataSourceError(action: String, tpe: String, msg: String): Throwable = {
    new AnalysisException(
      errorClass = "PYTHON_DATA_SOURCE_ERROR",
      messageParameters = Map("action" -> action, "type" -> tpe, "msg" -> msg)
    )
  }

  def identifierTooManyNamePartsError(originalIdentifier: String): Throwable = {
    new AnalysisException(
      errorClass = "IDENTIFIER_TOO_MANY_NAME_PARTS",
      messageParameters = Map("identifier" -> toSQLId(originalIdentifier)))
  }

  def identifierTooManyNamePartsError(names: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "IDENTIFIER_TOO_MANY_NAME_PARTS",
      messageParameters = Map("identifier" -> toSQLId(names)))
  }

  def emptyMultipartIdentifierError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1186",
      messageParameters = Map.empty)
  }

  def cannotOperateOnHiveDataSourceFilesError(operation: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1187",
      messageParameters = Map("operation" -> operation))
  }

  def setPathOptionAndCallWithPathParameterError(method: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1188",
      messageParameters = Map(
        "method" -> method,
        "config" -> SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key))
  }

  def userSpecifiedSchemaUnsupportedError(operation: String): Throwable = {
    DataTypeErrors.userSpecifiedSchemaUnsupportedError(operation)
  }

  def tempViewNotSupportStreamingWriteError(viewName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1190",
      messageParameters = Map("viewName" -> viewName))
  }

  def streamingIntoViewNotSupportedError(viewName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1191",
      messageParameters = Map("viewName" -> viewName))
  }

  def inputSourceDiffersFromDataSourceProviderError(
      source: String, tableName: String, table: CatalogTable): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1192",
      messageParameters = Map(
        "source" -> source,
        "tableName" -> tableName,
        "provider" -> table.provider.get))
  }

  def tableNotSupportStreamingWriteError(tableName: String, t: Table): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1193",
      messageParameters = Map("tableName" -> tableName, "t" -> t.toString))
  }

  def queryNameNotSpecifiedForMemorySinkError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1194",
      messageParameters = Map.empty)
  }

  def sourceNotSupportedWithContinuousTriggerError(source: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1195",
      messageParameters = Map("source" -> source))
  }

  def columnNotFoundInExistingColumnsError(
      columnType: String, columnName: String, validColumnNames: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1196",
      messageParameters = Map(
        "columnType" -> columnType,
        "columnName" -> columnName,
        "validColumnNames" -> validColumnNames.mkString(", ")))
  }

  def mixedRefsInAggFunc(funcStr: String, origin: Origin): Throwable = {
    new AnalysisException(
      errorClass =
        "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.AGGREGATE_FUNCTION_MIXED_OUTER_LOCAL_REFERENCES",
      origin = origin,
      messageParameters = Map("function" -> funcStr))
  }

  def subqueryReturnMoreThanOneColumn(number: Int, origin: Origin): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_SUBQUERY_EXPRESSION." +
        "SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN",
      origin = origin,
      messageParameters = Map("number" -> number.toString))
  }

  def unsupportedCorrelatedReferenceDataTypeError(
      expr: Expression,
      dataType: DataType,
      origin: Origin): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
        "UNSUPPORTED_CORRELATED_REFERENCE_DATA_TYPE",
      origin = origin,
      messageParameters = Map("expr" -> expr.sql, "dataType" -> dataType.typeName))
  }

  def unsupportedCorrelatedSubqueryInJoinConditionError(
      unsupportedSubqueryExpressions: Seq[Expression]): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
        "UNSUPPORTED_CORRELATED_EXPRESSION_IN_JOIN_CONDITION",
      messageParameters = Map("subqueryExpression" ->
        unsupportedSubqueryExpressions.map(_.sql).mkString(", ")))
  }

  def functionCannotProcessInputError(
      unbound: UnboundFunction,
      arguments: Seq[Expression],
      unsupported: UnsupportedOperationException): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1198",
      messageParameters = Map(
        "unbound" -> unbound.name,
        "arguments" -> arguments.map(_.dataType.simpleString).mkString(", "),
        "unsupported" -> unsupported.getMessage),
      cause = Some(unsupported))
  }

  def v2FunctionInvalidInputTypeLengthError(
      bound: BoundFunction,
      args: Seq[Expression]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1199",
      messageParameters = Map(
        "bound" -> bound.name(),
        "argsLen" -> args.length.toString,
        "inputTypesLen" -> bound.inputTypes().length.toString))
  }

  def cannotResolveColumnNameAmongAttributesError(
      colName: String, fieldNames: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1201",
      messageParameters = Map(
        "colName" -> colName,
        "fieldNames" -> fieldNames))
  }

  def cannotWriteTooManyColumnsToTableError(
      tableName: String,
      expected: Seq[String],
      queryOutput: Seq[Attribute]): Throwable = {
    new AnalysisException(
      errorClass = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "tableColumns" -> expected.map(c => toSQLId(c)).mkString(", "),
        "dataColumns" -> queryOutput.map(c => toSQLId(c.name)).mkString(", ")))
  }

  def cannotWriteNotEnoughColumnsToTableError(
      tableName: String,
      expected: Seq[String],
      queryOutput: Seq[Attribute]): Throwable = {
    new AnalysisException(
      errorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "tableColumns" -> expected.map(c => toSQLId(c)).mkString(", "),
        "dataColumns" -> queryOutput.map(c => toSQLId(c.name)).mkString(", ")))
  }

  def incompatibleDataToTableCannotFindDataError(
      tableName: String, colName: String): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "colName" -> toSQLId(colName)
      )
    )
  }

  def incompatibleDataToTableAmbiguousColumnNameError(
      tableName: String, colName: String): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPATIBLE_DATA_FOR_TABLE.AMBIGUOUS_COLUMN_NAME",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "colName" -> toSQLId(colName)
      )
    )
  }

  def incompatibleDataToTableExtraColumnsError(
      tableName: String, extraColumns: String): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "extraColumns" -> extraColumns
      )
    )
  }

  def incompatibleDataToTableExtraStructFieldsError(
      tableName: String, colName: String, extraFields: String): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "colName" -> toSQLId(colName),
        "extraFields" -> extraFields
      )
    )
  }

  def incompatibleDataToTableNullableColumnError(
      tableName: String, colName: String): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPATIBLE_DATA_FOR_TABLE.NULLABLE_COLUMN",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "colName" -> toSQLId(colName)
      )
    )
  }

  def incompatibleDataToTableNullableArrayElementsError(
      tableName: String, colName: String): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPATIBLE_DATA_FOR_TABLE.NULLABLE_ARRAY_ELEMENTS",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "colName" -> toSQLId(colName)
      )
    )
  }

  def incompatibleDataToTableNullableMapValuesError(
      tableName: String, colName: String): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPATIBLE_DATA_FOR_TABLE.NULLABLE_MAP_VALUES",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "colName" -> toSQLId(colName)
      )
    )
  }

  def incompatibleDataToTableCannotSafelyCastError(
      tableName: String, colName: String, srcType: String, targetType: String): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "colName" -> toSQLId(colName),
        "srcType" -> toSQLType(srcType),
        "targetType" -> toSQLType(targetType)
      )
    )
  }

  def incompatibleDataToTableStructMissingFieldsError(
      tableName: String, colName: String, missingFields: String): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPATIBLE_DATA_FOR_TABLE.STRUCT_MISSING_FIELDS",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "colName" -> toSQLId(colName),
        "missingFields" -> missingFields
      )
    )
  }

  def incompatibleDataToTableUnexpectedColumnNameError(
     tableName: String,
     colName: String,
     order: Int,
     expected: String,
     found: String): Throwable = {
    new AnalysisException(
      errorClass = "INCOMPATIBLE_DATA_FOR_TABLE.UNEXPECTED_COLUMN_NAME",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "colName" -> toSQLId(colName),
        "order" -> order.toString,
        "expected" -> toSQLId(expected),
        "found" -> toSQLId(found)
      )
    )
  }

  def invalidRowLevelOperationAssignments(
      assignments: Seq[Assignment],
      errors: Seq[String]): Throwable = {

    new AnalysisException(
      errorClass = "DATATYPE_MISMATCH.INVALID_ROW_LEVEL_OPERATION_ASSIGNMENTS",
      messageParameters = Map(
        "sqlExpr" -> assignments.map(toSQLExpr).mkString(", "),
        "errors" -> errors.mkString("\n- ", "\n- ", "")))
  }

  def invalidEscapeChar(sqlExpr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_ESCAPE_CHAR",
      messageParameters = Map("sqlExpr" -> toSQLExpr(sqlExpr)))
  }

  def secondArgumentOfFunctionIsNotIntegerError(
      function: String, e: NumberFormatException): Throwable = {
    // The second argument of {function} function needs to be an integer
    new AnalysisException(
      errorClass = "SECOND_FUNCTION_ARGUMENT_NOT_INTEGER",
      messageParameters = Map("functionName" -> function),
      cause = Some(e))
  }

  def nonPartitionPruningPredicatesNotExpectedError(
      nonPartitionPruningPredicates: Seq[Expression]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1205",
      messageParameters = Map(
        "nonPartitionPruningPredicates" -> nonPartitionPruningPredicates.toString()))
  }

  def columnNotDefinedInTableError(
      colType: String, colName: String, tableName: String, tableCols: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "COLUMN_NOT_DEFINED_IN_TABLE",
      messageParameters = Map(
        "colType" -> colType,
        "colName" -> toSQLId(colName),
        "tableName" -> toSQLId(tableName),
        "tableCols" -> tableCols.map(toSQLId).mkString(", ")))
  }

  def invalidLiteralForWindowDurationError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1207",
      messageParameters = Map.empty)
  }

  def noSuchStructFieldInGivenFieldsError(
      fieldName: String, fields: Array[StructField]): Throwable = {
    new AnalysisException(
      errorClass = "FIELD_NOT_FOUND",
      messageParameters = Map(
        "fieldName" -> toSQLId(fieldName),
        "fields" -> fields.map(f => toSQLId(f.name)).mkString(", ")))
  }

  def ambiguousReferenceToFieldsError(field: String, numberOfAppearance: Int): Throwable = {
    new AnalysisException(
      errorClass = "AMBIGUOUS_REFERENCE_TO_FIELDS",
      messageParameters = Map("field" -> toSQLId(field), "count" -> numberOfAppearance.toString))
  }

  def secondArgumentInFunctionIsNotBooleanLiteralError(funcName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1210",
      messageParameters = Map("funcName" -> funcName))
  }

  def joinConditionMissingOrTrivialError(
      join: Join, left: LogicalPlan, right: LogicalPlan): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1211",
      messageParameters = Map(
        "joinType" -> join.joinType.sql,
        "leftPlan" -> left.treeString(false).trim,
        "rightPlan" -> right.treeString(false).trim))
  }

  def usePythonUDFInJoinConditionUnsupportedError(joinType: JoinType): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE",
      messageParameters = Map("joinType" -> toSQLStmt(joinType.sql)))
  }

  def conflictingAttributesInJoinConditionError(
      conflictingAttrs: AttributeSet, outerPlan: LogicalPlan, subplan: LogicalPlan): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1212",
      messageParameters = Map(
        "conflictingAttrs" -> conflictingAttrs.mkString(","),
        "outerPlan" -> outerPlan.toString,
        "subplan" -> subplan.toString))
  }

  def emptyWindowExpressionError(expr: Window): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1213",
      messageParameters = Map("expr" -> expr.toString))
  }

  def foundDifferentWindowFunctionTypeError(windowExpressions: Seq[NamedExpression]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1214",
      messageParameters = Map("windowExpressions" -> windowExpressions.toString()))
  }

  def escapeCharacterInTheMiddleError(pattern: String, char: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_FORMAT.ESC_IN_THE_MIDDLE",
      messageParameters = Map(
        "format" -> toSQLValue(pattern, StringType),
        "char" -> toSQLValue(char, StringType)))
  }

  def escapeCharacterAtTheEndError(pattern: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_FORMAT.ESC_AT_THE_END",
      messageParameters = Map("format" -> toSQLValue(pattern, StringType)))
  }

  def tableIdentifierExistsError(tableIdentifier: TableIdentifier): Throwable = {
    new TableAlreadyExistsException(tableIdentifier.nameParts)
  }

  def tableIdentifierNotConvertedToHadoopFsRelationError(
      tableIdentifier: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1218",
      messageParameters = Map("tableIdentifier" -> tableIdentifier.toString))
  }

  def alterDatabaseLocationUnsupportedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1219",
      messageParameters = Map.empty)
  }

  def hiveTableTypeUnsupportedError(tableName: String, tableType: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.HIVE_TABLE_TYPE",
      messageParameters = Map(
        "tableName" -> toSQLId(tableName),
        "tableType" -> tableType))
  }

  def unknownHiveResourceTypeError(resourceType: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1222",
      messageParameters = Map("resourceType" -> resourceType))
  }

  def configRemovedInVersionError(
      configName: String,
      version: String,
      comment: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1226",
      messageParameters = Map(
        "configName" -> configName,
        "version" -> version,
        "comment" -> comment))
  }

  def invalidPartitionColumnKeyInTableError(key: String, tblName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1231",
      messageParameters = Map(
        "key" -> key,
        "tblName" -> toSQLId(tblName)))
  }

  def invalidPartitionSpecError(
      specKeys: String,
      partitionColumnNames: Seq[String],
      tableName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1232",
      messageParameters = Map(
        "specKeys" -> specKeys,
        "partitionColumnNames" -> partitionColumnNames.mkString(", "),
        "tableName" -> toSQLId(tableName)))
  }

  def columnAlreadyExistsError(columnName: String): Throwable = {
    new AnalysisException(
      errorClass = "COLUMN_ALREADY_EXISTS",
      messageParameters = Map("columnName" -> toSQLId(columnName)))
  }

  def noSuchTableError(db: String, table: String): Throwable = {
    new NoSuchTableException(db = db, table = table)
  }

  def tempViewNotCachedForAnalyzingColumnsError(tableIdent: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.ANALYZE_UNCACHED_TEMP_VIEW",
      messageParameters = Map("viewName" -> toSQLId(tableIdent.toString)))
  }

  def columnTypeNotSupportStatisticsCollectionError(
      name: String,
      tableIdent: TableIdentifier,
      dataType: DataType): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.ANALYZE_UNSUPPORTED_COLUMN_TYPE",
      messageParameters = Map(
        "columnType" -> toSQLType(dataType),
        "columnName" -> toSQLId(name),
        "tableName" -> toSQLId(tableIdent.toString)))
  }

  def analyzeTableNotSupportedOnViewsError(): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.ANALYZE_VIEW",
      messageParameters = Map.empty)
  }

  def unexpectedPartitionColumnPrefixError(
      table: String,
      database: String,
      schemaColumns: String,
      specColumns: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1237",
      messageParameters = Map(
        "table" -> table,
        "database" -> database,
        "schemaColumns" -> schemaColumns,
        "specColumns" -> specColumns))
  }

  def noSuchPartitionError(
      db: String,
      table: String,
      partition: TablePartitionSpec): Throwable = {
    new NoSuchPartitionException(db, table, partition)
  }

  def notExistPartitionError(
      table: Identifier,
      partitionIdent: InternalRow,
      partitionSchema: StructType): Throwable = {
    new NoSuchPartitionException(table.toString, partitionIdent, partitionSchema)
  }

  def analyzingColumnStatisticsNotSupportedForColumnTypeError(
      name: String,
      dataType: DataType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1239",
      messageParameters = Map(
        "name" -> name,
        "dataType" -> dataType.toString))
  }

  def tableAlreadyExistsError(table: String): Throwable = {
    new TableAlreadyExistsException(table)
  }

  def createTableAsSelectWithNonEmptyDirectoryError(tablePath: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1241",
      messageParameters = Map(
        "tablePath" -> tablePath,
        "config" -> SQLConf.ALLOW_NON_EMPTY_LOCATION_IN_CTAS.key))
  }

  def alterTableChangeColumnNotSupportedForColumnTypeError(
      tableName: String,
      originColumn: StructField,
      newColumn: StructField,
      origin: Origin): Throwable = {
    new AnalysisException(
      errorClass = "NOT_SUPPORTED_CHANGE_COLUMN",
      messageParameters = Map(
        "table" -> tableName,
        "originName" -> toSQLId(originColumn.name),
        "originType" -> toSQLType(originColumn.dataType),
        "newName" -> toSQLId(newColumn.name),
        "newType"-> toSQLType(newColumn.dataType)),
      origin = origin
    )
  }

  def cannotAlterPartitionColumn(
      tableName: String,
      columnName: String): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_ALTER_PARTITION_COLUMN",
      messageParameters =
        Map("tableName" -> toSQLId(tableName), "columnName" -> toSQLId(columnName))
    )
  }

  def cannotAlterCollationBucketColumn(tableName: String, columnName: String): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_ALTER_COLLATION_BUCKET_COLUMN",
      messageParameters =
        Map("tableName" -> toSQLId(tableName), "columnName" -> toSQLId(columnName))
    )
  }

  def cannotFindColumnError(name: String, fieldNames: Array[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1246",
      messageParameters = Map(
        "name" -> name,
        "fieldNames" -> fieldNames.mkString("[`", "`, `", "`]")))
  }

  def alterTableSetSerdeForSpecificPartitionNotSupportedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1247",
      messageParameters = Map.empty)
  }

  def alterTableSetSerdeNotSupportedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1248",
      messageParameters = Map.empty)
  }

  def cmdOnlyWorksOnPartitionedTablesError(
      operation: String,
      tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      errorClass = "NOT_A_PARTITIONED_TABLE",
      messageParameters = Map(
        "operation" -> toSQLStmt(operation),
        "tableIdentWithDB" -> tableIdentWithDB))
  }

  def cmdOnlyWorksOnTableWithLocationError(cmd: String, tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_2446",
      messageParameters = Map(
        "cmd" -> cmd,
        "tableIdentWithDB" -> tableIdentWithDB))
  }

  def actionNotAllowedOnTableWithFilesourcePartitionManagementDisabledError(
      action: String,
      tableName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1250",
      messageParameters = Map(
        "action" -> action,
        "tableName" -> tableName))
  }

  def actionNotAllowedOnTableSincePartitionMetadataNotStoredError(
     action: String,
     tableName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1251",
      messageParameters = Map(
        "action" -> action,
        "tableName" -> tableName))
  }

  def cannotAlterViewWithAlterTableError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1252",
      messageParameters = Map.empty)
  }

  def cannotAlterTableWithAlterViewError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1253",
      messageParameters = Map.empty)
  }

  def cannotOverwritePathBeingReadFromError(path: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_OVERWRITE.PATH",
      messageParameters = Map("path" -> path))
  }

  def cannotOverwriteTableThatIsBeingReadFromError(tableIdent: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_OVERWRITE.TABLE",
      messageParameters = Map("table" -> toSQLId(tableIdent.nameParts)))
  }

  def cannotDropBuiltinFuncError(functionName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1255",
      messageParameters = Map("functionName" -> functionName))
  }

  def cannotRefreshBuiltInFuncError(functionName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1256",
      messageParameters = Map("functionName" -> functionName))
  }

  def cannotRefreshTempFuncError(functionName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1257",
      messageParameters = Map("functionName" -> functionName))
  }

  def noSuchFunctionError(identifier: FunctionIdentifier): Throwable = {
    new NoSuchFunctionException(identifier.database.get, identifier.funcName)
  }

  def alterAddColNotSupportViewError(table: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1259",
      messageParameters = Map("table" -> table.toString))
  }

  def alterAddColNotSupportDatasourceTableError(
      tableType: Any,
      table: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1260",
      messageParameters = Map(
        "tableType" -> tableType.toString,
        "table" -> table.toString))
  }

  def loadDataNotSupportedForDatasourceTablesError(tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1261",
      messageParameters = Map("tableIdentWithDB" -> tableIdentWithDB))
  }

  def loadDataWithoutPartitionSpecProvidedError(tableIdentWithDB: String): Throwable = {
     new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1262",
      messageParameters = Map("tableIdentWithDB" -> tableIdentWithDB))
  }

  def loadDataPartitionSizeNotMatchNumPartitionColumnsError(
      tableIdentWithDB: String,
      partitionSize: Int,
      targetTableSize: Int): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1263",
      messageParameters = Map(
        "partitionSize" -> partitionSize.toString,
        "targetTableSize" -> targetTableSize.toString,
        "tableIdentWithDB" -> tableIdentWithDB))
  }

  def loadDataTargetTableNotPartitionedButPartitionSpecWasProvidedError(
      tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1264",
      messageParameters = Map("tableIdentWithDB" -> tableIdentWithDB))
  }

  def loadDataInputPathNotExistError(path: String): Throwable = {
    new AnalysisException(
      errorClass = "LOAD_DATA_PATH_NOT_EXISTS",
      messageParameters = Map("path" -> path))
  }

  def truncateTableOnExternalTablesError(tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1266",
      messageParameters = Map("tableIdentWithDB" -> tableIdentWithDB))
  }

  def truncateTablePartitionNotSupportedForNotPartitionedTablesError(
      tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1267",
      messageParameters = Map("tableIdentWithDB" -> tableIdentWithDB))
  }

  def failToTruncateTableWhenRemovingDataError(
      tableIdentWithDB: String,
      path: Path,
      e: Throwable): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1268",
      messageParameters = Map(
        "tableIdentWithDB" -> tableIdentWithDB,
        "path" -> path.toString),
      cause = Some(e))
  }

  def descPartitionNotAllowedOnTempView(table: String): Throwable = {
    new AnalysisException(
      errorClass = "FORBIDDEN_OPERATION",
      messageParameters = Map(
        "statement" -> toSQLStmt("DESC PARTITION"),
        "objectType" -> "TEMPORARY VIEW",
        "objectName" -> toSQLId(table)))
  }

  def descPartitionNotAllowedOnView(table: String): Throwable = {
    new AnalysisException(
      errorClass = "FORBIDDEN_OPERATION",
      messageParameters = Map(
        "statement" -> toSQLStmt("DESC PARTITION"),
        "objectType" -> "VIEW",
        "objectName" -> toSQLId(table)))
  }

  def showPartitionNotAllowedOnTableNotPartitionedError(tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_PARTITION_OPERATION.PARTITION_SCHEMA_IS_EMPTY",
      messageParameters = Map("name" -> toSQLId(tableIdentWithDB)))
  }

  def showCreateTableNotSupportedOnTempView(table: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_SHOW_CREATE_TABLE.ON_TEMPORARY_VIEW",
      messageParameters = Map("tableName" -> toSQLId(table)))
  }

  def showCreateTableNotSupportTransactionalHiveTableError(table: CatalogTable): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_SHOW_CREATE_TABLE.ON_TRANSACTIONAL_HIVE_TABLE",
      messageParameters = Map("tableName" -> toSQLId(table.identifier.nameParts)))
  }

  def showCreateTableFailToExecuteUnsupportedConfError(
      table: TableIdentifier,
      configs: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_SERDE_CONFIGURATION",
      messageParameters = Map(
        "tableName" -> toSQLId(table.nameParts),
        "configs" -> configs))
  }

  def showCreateTableAsSerdeNotAllowedOnSparkDataSourceTableError(
      table: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_SHOW_CREATE_TABLE.ON_DATA_SOURCE_TABLE_WITH_AS_SERDE",
      messageParameters = Map("tableName" -> toSQLId(table.nameParts)))
  }

  def showCreateTableOrViewFailToExecuteUnsupportedFeatureError(
      table: CatalogTable,
      unsupportedFeatures: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_SHOW_CREATE_TABLE.WITH_UNSUPPORTED_FEATURE",
      messageParameters = Map(
        "tableName" -> toSQLId(table.identifier.nameParts),
        "unsupportedFeatures" -> unsupportedFeatures.map(" - " + _).mkString("\n")))
  }

  def logicalPlanForViewNotAnalyzedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1276",
      messageParameters = Map.empty)
  }

  def cannotCreateViewTooManyColumnsError(
      viewIdent: TableIdentifier,
      expected: Seq[String],
      query: LogicalPlan): Throwable = {
    new AnalysisException(
      errorClass = "CREATE_VIEW_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
      messageParameters = Map(
        "viewName" -> toSQLId(viewIdent.nameParts),
        "viewColumns" -> expected.map(c => toSQLId(c)).mkString(", "),
        "dataColumns" -> query.output.map(c => toSQLId(c.name)).mkString(", ")))
  }

  def cannotCreateViewNotEnoughColumnsError(
      viewIdent: TableIdentifier,
      expected: Seq[String],
      query: LogicalPlan): Throwable = {
    new AnalysisException(
      errorClass = "CREATE_VIEW_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
      messageParameters = Map(
        "viewName" -> toSQLId(viewIdent.nameParts),
        "viewColumns" -> expected.map(c => toSQLId(c)).mkString(", "),
        "dataColumns" -> query.output.map(c => toSQLId(c.name)).mkString(", ")))
  }

  def cannotAlterTempViewWithSchemaBindingError() : Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.TEMPORARY_VIEW_WITH_SCHEMA_BINDING_MODE",
      messageParameters = Map.empty)
  }

  def unsupportedCreateOrReplaceViewOnTableError(
      name: TableIdentifier, replace: Boolean): Throwable = {
    if (replace) {
      new AnalysisException(
        errorClass = "EXPECT_VIEW_NOT_TABLE.NO_ALTERNATIVE",
        messageParameters = Map(
          "tableName" -> toSQLId(name.nameParts),
          "operation" -> "CREATE OR REPLACE VIEW"
        )
      )
    } else {
      new AnalysisException(
        errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
        messageParameters = Map(
          "relationName" -> toSQLId(name.nameParts)
        )
      )
    }
  }

  def viewAlreadyExistsError(name: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName" -> name.toString))
  }

  def createPersistedViewFromDatasetAPINotAllowedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1280",
      messageParameters = Map.empty)
  }

  def recursiveViewDetectedError(
      viewIdent: TableIdentifier,
      newPath: Seq[TableIdentifier]): Throwable = {
    new AnalysisException(
      errorClass = "RECURSIVE_VIEW",
      messageParameters = Map(
        "viewIdent" -> toSQLId(viewIdent.nameParts),
        "newPath" -> newPath.map(p => toSQLId(p.nameParts)).mkString(" -> ")))
  }

  def notAllowedToCreatePermanentViewWithoutAssigningAliasForExpressionError(
      name: TableIdentifier,
      attr: Attribute): Throwable = {
    new AnalysisException(
      errorClass = "CREATE_PERMANENT_VIEW_WITHOUT_ALIAS",
      messageParameters = Map(
        "name" -> toSQLId(name.nameParts),
        "attr" -> toSQLExpr(attr)))
  }

  def notAllowedToCreatePermanentViewByReferencingTempViewError(
      name: TableIdentifier,
      nameParts: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_TEMP_OBJ_REFERENCE",
      messageParameters = Map(
        "obj" -> "VIEW",
        "objName" -> toSQLId(name.nameParts),
        "tempObj" -> "VIEW",
        "tempObjName" -> toSQLId(nameParts)))
  }

  def notAllowedToCreatePermanentViewByReferencingTempFuncError(
      name: TableIdentifier,
      funcName: String): Throwable = {
     new AnalysisException(
      errorClass = "INVALID_TEMP_OBJ_REFERENCE",
      messageParameters = Map(
        "obj" -> "VIEW",
        "objName" -> toSQLId(name.nameParts),
        "tempObj" -> "FUNCTION",
        "tempObjName" -> toSQLId(funcName)))
  }

  def notAllowedToCreatePermanentViewByReferencingTempVarError(
      name: TableIdentifier,
      varName: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_TEMP_OBJ_REFERENCE",
      messageParameters = Map(
        "obj" -> "VIEW",
        "objName" -> toSQLId(name.nameParts),
        "tempObj" -> "VARIABLE",
        "tempObjName" -> toSQLId(varName)))
  }

  def queryFromRawFilesIncludeCorruptRecordColumnError(): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN",
      messageParameters = Map.empty)
  }

  def userDefinedPartitionNotFoundInJDBCRelationError(
      columnName: String, schema: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1286",
      messageParameters = Map(
        "columnName" -> columnName,
        "schema" -> schema))
  }

  def invalidPartitionColumnTypeError(column: StructField): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1287",
      messageParameters = Map(
        "numericType" -> NumericType.simpleString,
        "dateType" -> DateType.catalogString,
        "timestampType" -> TimestampType.catalogString,
        "dataType" -> column.dataType.catalogString))
  }

  def tableOrViewAlreadyExistsError(name: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1288",
      messageParameters = Map("name" -> name))
  }

  def invalidColumnNameAsPathError(datasource: String, columnName: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_COLUMN_NAME_AS_PATH",
      messageParameters = Map(
        "datasource" -> datasource,
        "columnName" -> toSQLId(columnName)
      )
    )
  }

  def textDataSourceWithMultiColumnsError(schema: StructType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1290",
      messageParameters = Map("schemaSize" -> schema.size.toString))
  }

  def cannotFindPartitionColumnInPartitionSchemaError(
      readField: StructField, partitionSchema: StructType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1291",
      messageParameters = Map(
        "readField" -> readField.name,
        "partitionSchema" -> partitionSchema.toString()))
  }

  def cannotSpecifyDatabaseForTempViewError(tableIdent: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1292",
      messageParameters = Map("tableIdent" -> tableIdent.toString))
  }

  def cannotCreateTempViewUsingHiveDataSourceError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1293",
      messageParameters = Map.empty)
  }

  def invalidTimestampProvidedForStrategyError(
      strategy: String, timeString: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1294",
      messageParameters = Map(
        "strategy" -> strategy,
        "timeString" -> timeString))
  }

  def hostOptionNotSetError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1295",
      messageParameters = Map.empty)
  }

  def portOptionNotSetError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1296",
      messageParameters = Map.empty)
  }

  def invalidIncludeTimestampValueError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1297",
      messageParameters = Map.empty)
  }

  def checkpointLocationNotSpecifiedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1298",
      messageParameters = Map("config" -> SQLConf.CHECKPOINT_LOCATION.key))
  }

  def recoverQueryFromCheckpointUnsupportedError(checkpointPath: Path): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1299",
      messageParameters = Map("checkpointPath" -> checkpointPath.toString))
  }

  def cannotFindColumnInRelationOutputError(
      colName: String, relation: LogicalPlan): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1300",
      messageParameters = Map(
        "colName" -> colName,
        "actualColumns" -> relation.output.map(_.name).mkString(", ")))
  }

  def tableOrViewNotFound(ident: Seq[String]): Throwable = {
    new NoSuchTableException(ident)
  }

  def unsupportedTableChangeInJDBCCatalogError(change: TableChange): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1305",
      messageParameters = Map("change" -> change.toString))
  }

  def pathOptionNotSetCorrectlyWhenReadingError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1306",
      messageParameters = Map(
        "config" -> SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key))
  }

  def pathOptionNotSetCorrectlyWhenWritingError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1307",
      messageParameters = Map(
        "config" -> SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key))
  }

  def invalidSingleVariantColumn(): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_SINGLE_VARIANT_COLUMN",
      messageParameters = Map.empty)
  }

  def writeWithSaveModeUnsupportedBySourceError(source: String, createMode: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_DATA_SOURCE_SAVE_MODE",
      messageParameters = Map(
        "source" -> source,
        "createMode" -> toDSOption(createMode)))
  }

  def partitionByDoesNotAllowedWhenUsingInsertIntoError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1309",
      messageParameters = Map.empty)
  }

  def cannotFindCatalogToHandleIdentifierError(quote: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1310",
      messageParameters = Map("quote" -> quote))
  }

  def tableAlreadyExistsError(tableIdent: TableIdentifier): Throwable = {
    new TableAlreadyExistsException(tableIdent.nameParts)
  }

  def invalidPartitionTransformationError(expr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1316",
      messageParameters = Map("expr" -> expr.sql))
  }

  def unresolvedColumnError(colName: String, fields: Array[String]): AnalysisException = {
    new AnalysisException(
      errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      messageParameters = Map(
        "objectName" -> toSQLId(colName),
        "proposal" -> fields.map(toSQLId).mkString(", ")
      )
    )
  }

  def unresolvedColumnError(
      fieldName: Seq[String], fields: Array[String], context: Origin): AnalysisException = {
    new AnalysisException(
      errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      messageParameters = Map(
        "objectName" -> toSQLId(fieldName),
        "proposal" -> fields.map(toSQLId).mkString(", ")),
      origin = context)
  }

  def cannotParseIntervalError(delayThreshold: String, e: Throwable): Throwable = {
    val threshold = if (delayThreshold == null) "" else delayThreshold
    new AnalysisException(
      errorClass = "CANNOT_PARSE_INTERVAL",
      messageParameters = Map("intervalString" -> toSQLValue(threshold, StringType)),
      cause = Some(e))
  }

  def invalidJoinTypeInJoinWithError(joinType: JoinType): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_JOIN_TYPE_FOR_JOINWITH",
      messageParameters = Map("joinType" -> joinType.sql))
  }

  def cannotPassTypedColumnInUntypedSelectError(typedCol: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1320",
      messageParameters = Map("typedCol" -> typedCol))
  }

  def invalidViewNameError(viewName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1321",
      messageParameters = Map("viewName" -> viewName))
  }

  def invalidBucketsNumberError(numBuckets: String, e: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1322",
      messageParameters = Map("numBuckets" -> numBuckets, "e" -> e))
  }

  def aggregationFunctionAppliedOnNonNumericColumnError(colName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1323",
      messageParameters = Map("colName" -> colName))
  }

  def aggregationFunctionAppliedOnNonNumericColumnError(
      pivotColumn: String, maxValues: Int): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1324",
      messageParameters = Map(
        "pivotColumn" -> pivotColumn,
        "maxValues" -> maxValues.toString,
        "config" -> SQLConf.DATAFRAME_PIVOT_MAX_VALUES.key))
  }

  def cannotModifyValueOfStaticConfigError(key: String): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_MODIFY_CONFIG",
      messageParameters = Map("key" -> toSQLConf(key), "docroot" -> SPARK_DOC_ROOT)
    )
  }

  def cannotModifyValueOfSparkConfigError(key: String, docroot: String): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_MODIFY_CONFIG",
      messageParameters = Map("key" -> toSQLConf(key), "docroot" -> docroot))
  }

  def commandExecutionInRunnerUnsupportedError(runner: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1327",
      messageParameters = Map("runner" -> runner))
  }

  def udfClassDoesNotImplementAnyUDFInterfaceError(className: String): Throwable = {
    new AnalysisException(
      errorClass = "NO_UDF_INTERFACE",
      messageParameters = Map("className" -> className))
  }

  def udfClassImplementMultiUDFInterfacesError(className: String): Throwable = {
    new AnalysisException(
      errorClass = "MULTI_UDF_INTERFACE_ERROR",
      messageParameters = Map("className" -> className))
  }

  def udfClassWithTooManyTypeArgumentsError(n: Int): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.TOO_MANY_TYPE_ARGUMENTS_FOR_UDF_CLASS",
      messageParameters = Map("num" -> s"$n"))
  }

  def classWithoutPublicNonArgumentConstructorError(className: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1328",
      messageParameters = Map("className" -> className))
  }

  def cannotLoadClassNotOnClassPathError(className: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1329",
      messageParameters = Map("className" -> className))
  }

  def classDoesNotImplementUserDefinedAggregateFunctionError(className: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1330",
      messageParameters = Map("className" -> className))
  }

  def invalidFieldName(fieldName: Seq[String], path: Seq[String], context: Origin): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_FIELD_NAME",
      messageParameters = Map(
        "fieldName" -> toSQLId(fieldName),
        "path" -> toSQLId(path)),
      origin = context)
  }

  def invalidJsonSchema(schema: DataType): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_JSON_SCHEMA_MAP_TYPE",
      messageParameters = Map("jsonSchema" -> toSQLType(schema)))
  }

  def invalidXmlSchema(schema: DataType): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_XML_SCHEMA_MAP_TYPE",
      messageParameters = Map("xmlSchema" -> toSQLType(schema)))
  }

  def tableIndexNotSupportedError(errorMessage: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1332",
      messageParameters = Map("errorMessage" -> errorMessage))
  }

  def invalidTimeTravelSpecError(): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_TIME_TRAVEL_SPEC",
      messageParameters = Map.empty)
  }

  def invalidTimestampExprForTimeTravel(errorClass: String, expr: Expression): Throwable = {
    new AnalysisException(
      errorClass = errorClass,
      messageParameters = Map("expr" -> toSQLExpr(expr)))
  }

  def timeTravelUnsupportedError(relationId: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.TIME_TRAVEL",
      messageParameters = Map("relationId" -> relationId))
  }

  def writeDistributionAndOrderingNotSupportedInContinuousExecution(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1338",
      messageParameters = Map.empty)
  }

  // Return a more descriptive error message if the user tries to nest a DEFAULT column reference
  // inside some other expression (such as DEFAULT + 1) in an INSERT INTO command's VALUES list;
  // this is not allowed.
  def defaultReferencesNotAllowedInComplexExpressionsInInsertValuesList(): Throwable = {
    new AnalysisException(
      errorClass = "DEFAULT_PLACEMENT_INVALID",
      messageParameters = Map.empty)
  }

  // Return a descriptive error message in the presence of INSERT INTO commands with explicit
  // DEFAULT column references and explicit column lists, since this is not implemented yet.
  def defaultReferencesNotAllowedInComplexExpressionsInUpdateSetClause(): Throwable = {
    new AnalysisException(
      errorClass = "DEFAULT_PLACEMENT_INVALID",
      messageParameters = Map.empty)
  }

  def defaultReferencesNotAllowedInComplexExpressionsInMergeInsertsOrUpdates(): Throwable = {
    new AnalysisException(
      errorClass = "DEFAULT_PLACEMENT_INVALID",
      messageParameters = Map.empty)
  }

  def nonDeterministicMergeCondition(condName: String, cond: Expression): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_MERGE_CONDITION.NON_DETERMINISTIC",
      messageParameters = Map(
        "condName" -> condName,
        "cond" -> toSQLExpr(cond)))
  }

  def subqueryNotAllowedInMergeCondition(condName: String, cond: Expression): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_MERGE_CONDITION.SUBQUERY",
      messageParameters = Map(
        "condName" -> condName,
        "cond" -> toSQLExpr(cond)))
  }

  def aggregationNotAllowedInMergeCondition(condName: String, cond: Expression): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_MERGE_CONDITION.AGGREGATE",
      messageParameters = Map(
        "condName" -> condName,
        "cond" -> toSQLExpr(cond)))
  }

  def defaultReferencesNotAllowedInDataSource(
      statementType: String, dataSource: String): Throwable = {
    new AnalysisException(
      errorClass = "DEFAULT_UNSUPPORTED",
      messageParameters = Map(
        "statementType" -> toSQLStmt(statementType),
        "dataSource" -> dataSource))
  }

  def addNewDefaultColumnToExistingTableNotAllowed(
      statementType: String, dataSource: String): Throwable = {
    new AnalysisException(
      errorClass = "ADD_DEFAULT_UNSUPPORTED",
      messageParameters = Map(
        "statementType" -> toSQLStmt(statementType),
        "dataSource" -> dataSource))
  }

  def defaultValuesDataTypeError(
      statement: String,
      colName: String,
      defaultValue: String,
      expectedType: DataType,
      actualType: DataType): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
      messageParameters = Map(
        "statement" -> toSQLStmt(statement),
        "colName" -> toSQLId(colName),
        "defaultValue" -> defaultValue,
        "actualType" -> toSQLType(actualType),
        "expectedType" -> toSQLType(expectedType)))
  }

  def defaultValuesUnresolvedExprError(
      statement: String,
      colName: String,
      defaultValue: String,
      cause: Throwable): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION",
      messageParameters = Map(
        "statement" -> toSQLStmt(statement),
        "colName" -> toSQLId(colName),
        "defaultValue" -> defaultValue
      ),
      cause = Option(cause))
  }

  def defaultValuesMayNotContainSubQueryExpressions(
      statement: String,
      colName: String,
      defaultValue: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
      messageParameters = Map(
        "statement" -> toSQLStmt(statement),
        "colName" -> toSQLId(colName),
        "defaultValue" -> defaultValue))
  }

  def defaultValueNotConstantError(
      statement: String,
      colName: String,
      defaultValue: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_DEFAULT_VALUE.NOT_CONSTANT",
      messageParameters = Map(
        "statement" -> toSQLStmt(statement),
        "colName" -> toSQLId(colName),
        "defaultValue" -> defaultValue
      ))
  }

  def nullableColumnOrFieldError(name: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "NULLABLE_COLUMN_OR_FIELD",
      messageParameters = Map("name" -> toSQLId(name)))
  }

  def notNullConstraintViolationArrayElementError(path: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "NOT_NULL_CONSTRAINT_VIOLATION.ARRAY_ELEMENT",
      messageParameters = Map("columnPath" -> toSQLId(path)))
  }

  def notNullConstraintViolationMapValueError(path: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "NOT_NULL_CONSTRAINT_VIOLATION.MAP_VALUE",
      messageParameters = Map("columnPath" -> toSQLId(path)))
  }

  def invalidColumnOrFieldDataTypeError(
      name: Seq[String],
      dt: DataType,
      expected: DataType): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_COLUMN_OR_FIELD_DATA_TYPE",
      messageParameters = Map(
        "name" -> toSQLId(name),
        "type" -> toSQLType(dt),
        "expectedType" -> toSQLType(expected)))
  }

  def columnNotInGroupByClauseError(expression: Expression): Throwable = {
    new AnalysisException(
      errorClass = "MISSING_AGGREGATION",
      messageParameters = Map(
        "expression" -> toSQLExpr(expression),
        "expressionAnyValue" -> toSQLExpr(new AnyValue(expression)))
    )
  }

  def implicitCollationMismatchError(): Throwable = {
    new AnalysisException(
      errorClass = "COLLATION_MISMATCH.IMPLICIT",
      messageParameters = Map.empty
    )
  }

  def explicitCollationMismatchError(explicitTypes: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "COLLATION_MISMATCH.EXPLICIT",
      messageParameters = Map(
        "explicitTypes" -> explicitTypes.map(toSQLId).mkString(", ")
      )
    )
  }

  def indeterminateCollationError(): Throwable = {
    new AnalysisException(
      errorClass = "INDETERMINATE_COLLATION",
      messageParameters = Map.empty
    )
  }

  def cannotConvertProtobufTypeToSqlTypeError(
      protobufColumn: String,
      sqlColumn: Seq[String],
      protobufType: String,
      sqlType: DataType): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_CONVERT_PROTOBUF_FIELD_TYPE_TO_SQL_TYPE",
      messageParameters = Map(
        "protobufColumn" -> protobufColumn,
        "sqlColumn" -> toSQLId(sqlColumn),
        "protobufType" -> protobufType,
        "sqlType" -> toSQLType(sqlType)))
  }

  def cannotConvertCatalystTypeToProtobufTypeError(
      sqlColumn: Seq[String],
      protobufColumn: String,
      sqlType: DataType,
      protobufType: String): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_CONVERT_SQL_TYPE_TO_PROTOBUF_FIELD_TYPE",
      messageParameters = Map(
        "sqlColumn" -> toSQLId(sqlColumn),
        "protobufColumn" -> protobufColumn,
        "sqlType" -> toSQLType(sqlType),
        "protobufType" -> protobufType))
  }

  def cannotConvertProtobufTypeToCatalystTypeError(
      protobufType: String,
      sqlType: DataType,
      cause: Throwable): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_CONVERT_PROTOBUF_MESSAGE_TYPE_TO_SQL_TYPE",
      messageParameters = Map(
        "protobufType" -> protobufType,
        "toType" -> toSQLType(sqlType)),
      cause = Option(cause))
  }

  def cannotConvertSqlTypeToProtobufError(
      protobufType: String,
      sqlType: DataType,
      cause: Throwable): Throwable = {
    new AnalysisException(
      errorClass = "UNABLE_TO_CONVERT_TO_PROTOBUF_MESSAGE_TYPE",
      messageParameters = Map(
        "protobufType" -> protobufType,
        "toType" -> toSQLType(sqlType)),
      cause = Option(cause))
  }

  def protobufTypeUnsupportedYetError(protobufType: String): Throwable = {
    new AnalysisException(
      errorClass = "PROTOBUF_TYPE_NOT_SUPPORT",
      messageParameters = Map("protobufType" -> protobufType))
  }

  def unknownProtobufMessageTypeError(
      descriptorName: String,
      containingType: String): Throwable = {
    new AnalysisException(
      errorClass = "UNKNOWN_PROTOBUF_MESSAGE_TYPE",
      messageParameters = Map(
        "descriptorName" -> descriptorName,
        "containingType" -> containingType))
  }

  def cannotFindCatalystTypeInProtobufSchemaError(catalystFieldPath: String): Throwable = {
    new AnalysisException(
      errorClass = "NO_SQL_TYPE_IN_PROTOBUF_SCHEMA",
      messageParameters = Map("catalystFieldPath" -> catalystFieldPath))
  }

  def cannotFindProtobufFieldInCatalystError(field: String): Throwable = {
    new AnalysisException(
      errorClass = "PROTOBUF_FIELD_MISSING_IN_SQL_SCHEMA",
      messageParameters = Map("field" -> field))
  }

  def protobufFieldMatchError(field: String,
      protobufSchema: String,
      matchSize: String,
      matches: String): Throwable = {
    new AnalysisException(
      errorClass = "PROTOBUF_FIELD_MISSING",
      messageParameters = Map(
        "field" -> field,
        "protobufSchema" -> protobufSchema,
        "matchSize" -> matchSize,
        "matches" -> matches))
  }

  def unableToLocateProtobufMessageError(messageName: String): Throwable = {
    new AnalysisException(
      errorClass = "PROTOBUF_MESSAGE_NOT_FOUND",
      messageParameters = Map("messageName" -> messageName))
  }

  def foundRecursionInProtobufSchema(fieldDescriptor: String): Throwable = {
    new AnalysisException(
      errorClass = "RECURSIVE_PROTOBUF_SCHEMA",
      messageParameters = Map("fieldDescriptor" -> fieldDescriptor))
  }

  def protobufFieldTypeMismatchError(field: String): Throwable = {
    new AnalysisException(
      errorClass = "PROTOBUF_FIELD_TYPE_MISMATCH",
      messageParameters = Map("field" -> field))
  }

  def protobufClassLoadError(
      protobufClassName: String,
      explanation: String,
      cause: Throwable = null): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_LOAD_PROTOBUF_CLASS",
      messageParameters = Map(
        "protobufClassName" -> protobufClassName,
        "explanation" -> explanation
      ),
      cause = Option(cause))
  }

  def protobufDescriptorDependencyError(dependencyName: String): Throwable = {
    new AnalysisException(
      errorClass = "PROTOBUF_DEPENDENCY_NOT_FOUND",
      messageParameters = Map("dependencyName" -> dependencyName))
  }

  def invalidByteStringFormatError(unsupported: Any): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_BYTE_STRING",
      messageParameters = Map(
        "unsupported" -> unsupported.toString,
        "class" -> unsupported.getClass.toString))
  }

  def unsupportedUDFOuptutType(expr: Expression, dt: DataType): Throwable = {
    new AnalysisException(
      errorClass = "DATATYPE_MISMATCH.UNSUPPORTED_UDF_OUTPUT_TYPE",
      messageParameters = Map("sqlExpr" -> toSQLExpr(expr), "dataType" -> dt.sql))
  }

  def funcBuildError(funcName: String, cause: Exception): Throwable = {
    cause.getCause match {
      case st: SparkThrowable with Throwable => st
      case other =>
        new AnalysisException(
          errorClass = "FAILED_FUNCTION_CALL",
          messageParameters = Map("funcName" -> toSQLId(funcName)),
          cause = Option(other))
    }
  }

  def ambiguousRelationAliasNameInNestedCTEError(name: String): Throwable = {
    new AnalysisException(
      errorClass = "AMBIGUOUS_ALIAS_IN_NESTED_CTE",
      messageParameters = Map(
        "name" -> toSQLId(name),
        "config" -> toSQLConf(LEGACY_CTE_PRECEDENCE_POLICY.key),
        "docroot" -> SPARK_DOC_ROOT))
  }

  def ambiguousLateralColumnAliasError(name: String, numOfMatches: Int): Throwable = {
    new AnalysisException(
      errorClass = "AMBIGUOUS_LATERAL_COLUMN_ALIAS",
      messageParameters = Map(
        "name" -> toSQLId(name),
        "n" -> numOfMatches.toString
      )
    )
  }
  def ambiguousLateralColumnAliasError(nameParts: Seq[String], numOfMatches: Int): Throwable = {
    new AnalysisException(
      errorClass = "AMBIGUOUS_LATERAL_COLUMN_ALIAS",
      messageParameters = Map(
        "name" -> toSQLId(nameParts),
        "n" -> numOfMatches.toString
      )
    )
  }

  def lateralColumnAliasInAggFuncUnsupportedError(
      lcaNameParts: Seq[String], aggExpr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_AGGREGATE_FUNC",
      messageParameters = Map(
        "lca" -> toSQLId(lcaNameParts),
        "aggFunc" -> toSQLExpr(aggExpr)
      )
    )
  }

  def lateralColumnAliasInWindowUnsupportedError(
      lcaNameParts: Seq[String], windowExpr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_WINDOW",
      messageParameters = Map(
        "lca" -> toSQLId(lcaNameParts),
        "windowExpr" -> toSQLExpr(windowExpr)
      )
    )
  }

  def lateralColumnAliasInAggWithWindowAndHavingUnsupportedError(
      lcaNameParts: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_AGGREGATE_WITH_WINDOW_AND_HAVING",
      messageParameters = Map(
        "lca" -> toSQLId(lcaNameParts)
      )
    )
  }

  def dataTypeOperationUnsupportedError(): Throwable = {
    SparkException.internalError(
      "The operation `dataType` is not supported.")
  }

  def nullableRowIdError(nullableRowIdAttrs: Seq[AttributeReference]): Throwable = {
    new AnalysisException(
      errorClass = "NULLABLE_ROW_ID_ATTRIBUTES",
      messageParameters = Map("nullableRowIdAttrs" -> nullableRowIdAttrs.mkString(", ")))
  }

  def cannotRenameTableAcrossSchemaError(): Throwable = {
    new SparkUnsupportedOperationException(
      errorClass = "CANNOT_RENAME_ACROSS_SCHEMA", messageParameters = Map("type" -> "table")
    )
  }

  def avroIncompatibleReadError(
      avroPath: String,
      sqlPath: String,
      avroType: String,
      sqlType: String): Throwable = {
    new AnalysisException(
      errorClass = "AVRO_INCOMPATIBLE_READ_TYPE",
      messageParameters = Map(
        "avroPath" -> avroPath,
        "sqlPath" -> sqlPath,
        "avroType" -> avroType,
        "sqlType" -> toSQLType(sqlType)
      )
    )
  }

  def optionMustBeLiteralString(key: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_SQL_SYNTAX.OPTION_IS_INVALID",
      messageParameters = Map(
        "key" -> key,
        "supported" -> "literal strings"))
  }

  def optionMustBeConstant(key: String, cause: Option[Throwable] = None): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_SQL_SYNTAX.OPTION_IS_INVALID",
      messageParameters = Map(
        "key" -> key,
        "supported" -> "constant expressions"),
      cause = cause)
  }

  def tableValuedFunctionRequiredMetadataIncompatibleWithCall(
      functionName: String,
      requestedMetadata: String,
      invalidFunctionCallProperty: String): Throwable = {
    new AnalysisException(
      errorClass = "TABLE_VALUED_FUNCTION_REQUIRED_METADATA_INCOMPATIBLE_WITH_CALL",
      messageParameters = Map(
        "functionName" -> functionName,
        "requestedMetadata" -> requestedMetadata,
        "invalidFunctionCallProperty" -> invalidFunctionCallProperty))
  }

  def tableValuedFunctionRequiredMetadataInvalid(
      functionName: String,
      reason: String): Throwable = {
    new AnalysisException(
      errorClass = "TABLE_VALUED_FUNCTION_REQUIRED_METADATA_INVALID",
      messageParameters = Map(
        "functionName" -> functionName,
        "reason" -> reason))
  }

  def dataSourceAlreadyExists(name: String): Throwable = {
    new AnalysisException(
      errorClass = "DATA_SOURCE_ALREADY_EXISTS",
      messageParameters = Map("provider" -> name))
  }

  def dataSourceDoesNotExist(name: String): Throwable = {
    new AnalysisException(
      errorClass = "DATA_SOURCE_NOT_EXIST",
      messageParameters = Map("provider" -> name))
  }

  def externalDataSourceException(cause: Throwable): Throwable = {
    new AnalysisException(
      errorClass = "DATA_SOURCE_EXTERNAL_ERROR",
      messageParameters = Map(),
      cause = Some(cause)
    )
  }

  def foundMultipleDataSources(provider: String): Throwable = {
    new AnalysisException(
      errorClass = "FOUND_MULTIPLE_DATA_SOURCES",
      messageParameters = Map("provider" -> provider))
  }

  def foundMultipleXMLDataSourceError(provider: String,
      sourceNames: Seq[String],
      externalSource: String): Throwable = {
    new AnalysisException(
      errorClass = "MULTIPLE_XML_DATA_SOURCE",
      messageParameters = Map("provider" -> provider,
        "sourceNames" -> sourceNames.mkString(", "),
        "externalSource" -> externalSource)
    )
  }

  def xmlRowTagRequiredError(optionName: String): Throwable = {
    new AnalysisException(
      errorClass = "XML_ROW_TAG_MISSING",
      messageParameters = Map("rowTag" -> toSQLId(optionName))
    )
  }

  def invalidUDFClassError(invalidClass: String): Throwable = {
    new InvalidUDFClassException(
      errorClass = "_LEGACY_ERROR_TEMP_2450",
      messageParameters = Map("invalidClass" -> invalidClass))
  }

  def unsupportedParameterExpression(expr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_EXPR_FOR_PARAMETER",
      messageParameters = Map(
        "invalidExprSql" -> toSQLExpr(expr)),
      origin = expr.origin)
  }

  def invalidQueryAllParametersMustBeNamed(expr: Seq[Expression]): Throwable = {
    new AnalysisException(
      errorClass = "ALL_PARAMETERS_MUST_BE_NAMED",
      messageParameters = Map("exprs" -> expr.map(e => toSQLExpr(e)).mkString(", ")))
  }

  def invalidQueryMixedQueryParameters(): Throwable = {
    throw new AnalysisException(
      errorClass = "INVALID_QUERY_MIXED_QUERY_PARAMETERS",
      messageParameters = Map.empty)
  }

  def invalidExecuteImmediateVariableType(dataType: DataType): Throwable = {
    throw new AnalysisException(
      errorClass = "INVALID_VARIABLE_TYPE_FOR_QUERY_EXECUTE_IMMEDIATE",
      messageParameters = Map("varType" -> toSQLType(dataType)))
  }

  def nullSQLStringExecuteImmediate(varName: String): Throwable = {
    throw new AnalysisException(
      errorClass = "NULL_QUERY_STRING_EXECUTE_IMMEDIATE",
      messageParameters = Map("varName" -> toSQLId(varName)))
  }

  def invalidStatementForExecuteInto(queryString: String): Throwable = {
    throw new AnalysisException(
      errorClass = "INVALID_STATEMENT_FOR_EXECUTE_INTO",
      messageParameters = Map("sqlString" -> toSQLStmt(queryString)))
  }

  def nestedExecuteImmediate(queryString: String): Throwable = {
    throw new AnalysisException(
      errorClass = "NESTED_EXECUTE_IMMEDIATE",
      messageParameters = Map("sqlString" -> toSQLStmt(queryString)))
  }

  def dataSourceTableSchemaMismatchError(
      dsSchema: StructType, expectedSchema: StructType): Throwable = {
    new AnalysisException(
      errorClass = "DATA_SOURCE_TABLE_SCHEMA_MISMATCH",
      messageParameters = Map(
        "dsSchema" -> toSQLType(dsSchema),
        "expectedSchema" -> toSQLType(expectedSchema)))
  }

  def cannotResolveDataFrameColumn(e: Expression): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_RESOLVE_DATAFRAME_COLUMN",
      messageParameters = Map("name" -> toSQLExpr(e)),
      origin = e.origin
    )
  }

  def ambiguousColumnReferences(e: Expression): Throwable = {
    new AnalysisException(
      errorClass = "AMBIGUOUS_COLUMN_REFERENCE",
      messageParameters = Map("name" -> toSQLExpr(e)),
      origin = e.origin
    )
  }

  private def callDeprecatedMethodError(oldMethod: String, newMethod: String): Throwable = {
    SparkException.internalError(s"The method `$oldMethod` is deprecated, " +
      s"please use `$newMethod` instead.")
  }

  def createTableDeprecatedError(): Throwable = {
    callDeprecatedMethodError("createTable(..., StructType, ...)",
      "createTable(..., Array[Column], ...)")
  }

  def mustOverrideOneMethodError(methodName: String): RuntimeException = {
    val msg = s"You must override one `$methodName`. It's preferred to not override the " +
      "deprecated one."
    new SparkRuntimeException(
      "INTERNAL_ERROR",
      Map("message" -> msg))
  }

  def cannotAssignEventTimeColumn(): Throwable = {
    new AnalysisException(
      errorClass = "CANNOT_ASSIGN_EVENT_TIME_COLUMN_WITHOUT_WATERMARK",
      messageParameters = Map()
    )
  }

  def avroNotLoadedSqlFunctionsUnusable(functionName: String): Throwable = {
    new AnalysisException(
      errorClass = "AVRO_NOT_LOADED_SQL_FUNCTIONS_UNUSABLE",
      messageParameters = Map("functionName" -> functionName)
    )
  }

  def avroOptionsException(optionName: String, message: String): Throwable = {
    new AnalysisException(
      errorClass = "STDS_INVALID_OPTION_VALUE.WITH_MESSAGE",
      messageParameters = Map("optionName" -> optionName, "message" -> message)
    )
  }

  def protobufNotLoadedSqlFunctionsUnusable(functionName: String): Throwable = {
    new AnalysisException(
      errorClass = "PROTOBUF_NOT_LOADED_SQL_FUNCTIONS_UNUSABLE",
      messageParameters = Map("functionName" -> functionName)
    )
  }

  def pipeOperatorSelectContainsAggregateFunction(expr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "PIPE_OPERATOR_SELECT_CONTAINS_AGGREGATE_FUNCTION",
      messageParameters = Map(
        "expr" -> expr.toString),
      origin = expr.origin)
  }

  def inlineTableContainsScalarSubquery(inlineTable: LogicalPlan): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES",
      messageParameters = Map.empty,
      origin = inlineTable.origin
    )
  }

  def ordinalOutOfBoundsError(
      ordinal: Int,
      attributes: Seq[Attribute]): Throwable = {
    new AnalysisException(
      errorClass = "COLUMN_ORDINAL_OUT_OF_BOUNDS",
      messageParameters = Map(
        "ordinal" -> ordinal.toString,
        "attributesLength" -> attributes.length.toString,
        "attributes" -> attributes.map(attr => toSQLId(attr.name)).mkString(", ")
      )
    )
  }
}
