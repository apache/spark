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

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkThrowableHelper
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NamespaceAlreadyExistsException, NoSuchFunctionException, NoSuchNamespaceException, NoSuchPartitionException, NoSuchTableException, ResolvedTable, Star, TableAlreadyExistsException, UnresolvedRegex}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, InvalidUDFClassException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, CreateMap, Expression, GroupingID, NamedExpression, SpecifiedWindowFrame, WindowFrame, WindowFunction, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, Join, LogicalPlan, SerdeInfo, Window}
import org.apache.spark.sql.catalyst.trees.{Origin, TreeNode}
import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED, LEGACY_CTE_PRECEDENCE_POLICY}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
 * Object for grouping error messages from exceptions thrown during query compilation.
 * As commands are executed eagerly, this also includes errors thrown during the execution of
 * commands, which users can see immediately.
 */
private[sql] object QueryCompilationErrors extends QueryErrorsBase {

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
      errorClass = "INVALID_PARAMETER_VALUE",
      messageParameters = Map(
        "parameter" -> "strfmt",
        "functionName" -> toSQLId("format_string"),
        "expected" -> "expects %1$, %2$ and so on, but got %0$."))
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

  def unpivotRequiresValueColumns(): Throwable = {
    new AnalysisException(
      errorClass = "UNPIVOT_REQUIRES_VALUE_COLUMNS",
      messageParameters = Map.empty)
  }

  def unpivotValDataTypeMismatchError(values: Seq[NamedExpression]): Throwable = {
    val dataTypes = values
      .groupBy(_.dataType)
      .mapValues(values => values.map(value => toSQLId(value.toString)).sorted)
      .mapValues(values => if (values.length > 3) values.take(3) :+ "..." else values)
      .toList.sortBy(_._1.sql)
      .map { case (dataType, values) => s"${toSQLType(dataType)} (${values.mkString(", ")})" }

    new AnalysisException(
      errorClass = "UNPIVOT_VALUE_DATA_TYPE_MISMATCH",
      messageParameters = Map("types" -> dataTypes.mkString(", ")))
  }

  def unsupportedIfNotExistsError(tableName: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "INSERT_PARTITION_SPEC_IF_NOT_EXISTS",
      messageParameters = Map("tableName" -> toSQLId(tableName)))
  }

  def nonPartitionColError(partitionName: String): Throwable = {
    new AnalysisException(
      errorClass = "NON_PARTITION_COLUMN",
      messageParameters = Map("columnName" -> toSQLId(partitionName)))
  }

  def missingStaticPartitionColumn(staticName: String): Throwable = {
    new AnalysisException(
      errorClass = "MISSING_STATIC_PARTITION_COLUMN",
      messageParameters = Map("columnName" -> staticName))
  }

  def nestedGeneratorError(trimmedNestedGenerator: Expression): Throwable = {
    new AnalysisException(errorClass = "UNSUPPORTED_GENERATOR",
      errorSubClass = "NESTED_IN_EXPRESSIONS",
      messageParameters = Map("expression" -> toSQLExpr(trimmedNestedGenerator)))
  }

  def moreThanOneGeneratorError(generators: Seq[Expression], clause: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_GENERATOR",
      errorSubClass = "MULTI_GENERATOR",
      messageParameters = Map(
        "clause" -> clause,
        "num" -> generators.size.toString,
        "generators" -> generators.map(toSQLExpr).mkString(", ")))
  }

  def generatorOutsideSelectError(plan: LogicalPlan): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_GENERATOR",
      errorSubClass = "OUTSIDE_SELECT",
      messageParameters = Map("plan" -> plan.simpleString(SQLConf.get.maxToStringFields)))
  }

  def legacyStoreAssignmentPolicyError(): Throwable = {
    val configKey = SQLConf.STORE_ASSIGNMENT_POLICY.key
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1000",
      messageParameters = Map("configKey" -> configKey))
  }

  def unresolvedUsingColForJoinError(
      colName: String, plan: LogicalPlan, side: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1001",
      messageParameters = Map(
        "colName" -> colName,
        "side" -> side,
        "plan" -> plan.output.map(_.name).mkString(", ")))
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
    new AnalysisException(
      errorClass = errorClass,
      errorSubClass = if (candidates.isEmpty) "WITHOUT_SUGGESTION" else "WITH_SUGGESTION",
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
    new AnalysisException(
      errorClass = "UNRESOLVED_COLUMN",
      errorSubClass = if (proposal.isEmpty) "WITHOUT_SUGGESTION" else "WITH_SUGGESTION",
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
    new AnalysisException(
      errorClass = "UNRESOLVED_FIELD",
      errorSubClass = if (proposal.isEmpty) "WITHOUT_SUGGESTION" else "WITH_SUGGESTION",
      messageParameters = commonParams ++ proposalParam)
  }

  def dataTypeMismatchForDeserializerError(
      dataType: DataType, desiredType: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_DESERIALIZER",
      errorSubClass = "DATA_TYPE_MISMATCH",
      messageParameters = Map(
        "desiredType" -> toSQLType(desiredType),
        "dataType" -> toSQLType(dataType)))
  }

  def fieldNumberMismatchForDeserializerError(
      schema: StructType, maxOrdinal: Int): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_DESERIALIZER",
      errorSubClass = "FIELD_NUMBER_MISMATCH",
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
      after: TableChange.After, parentName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1003",
      messageParameters = Map("after" -> after.toString, "parentName" -> parentName))
  }

  def windowSpecificationNotDefinedError(windowName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1004",
      messageParameters = Map("windowName" -> windowName))
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
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "PANDAS_UDAF_IN_PIVOT",
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
      identifier: TableIdentifier, maxNestedViewDepth: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1009",
      messageParameters = Map(
        "identifier" -> identifier.toString,
        "maxNestedViewDepth" -> maxNestedViewDepth.toString,
        "config" -> SQLConf.MAX_NESTED_VIEW_DEPTH.key),
      origin = t.origin)
  }

  def insertIntoViewNotAllowedError(identifier: TableIdentifier, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1010",
      messageParameters = Map("identifier" -> identifier.toString),
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
      isTemp: Boolean,
      cmd: String,
      mismatchHint: Option[String],
      t: TreeNode[_]): Throwable = {
    val viewStr = if (isTemp) "temp view" else "view"
    val hintStr = mismatchHint.map(" " + _).getOrElse("")
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1013",
      messageParameters = Map(
        "nameParts" -> nameParts.quoted,
        "viewStr" -> viewStr,
        "cmd" -> cmd,
        "hintStr" -> hintStr),
      origin = t.origin)
  }

  def expectViewNotTempViewError(
      nameParts: Seq[String],
      cmd: String,
      t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1014",
      messageParameters = Map(
        "nameParts" -> nameParts.quoted,
        "cmd" -> cmd),
      origin = t.origin)
  }

  def expectViewNotTableError(
      v: ResolvedTable, cmd: String, mismatchHint: Option[String], t: TreeNode[_]): Throwable = {
    val hintStr = mismatchHint.map(" " + _).getOrElse("")
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1015",
      messageParameters = Map(
        "identifier" -> v.identifier.quoted,
        "cmd" -> cmd,
        "hintStr" -> hintStr),
      origin = t.origin)
  }

  def expectTableOrPermanentViewNotTempViewError(
      nameParts: Seq[String], cmd: String, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1016",
      messageParameters = Map(
        "nameParts" -> nameParts.quoted,
        "cmd" -> cmd),
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
      errorClass = "_LEGACY_ERROR_TEMP_1019",
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
      errorClass = "_LEGACY_ERROR_TEMP_1020",
      messageParameters = Map("elem" -> elem, "prettyName" -> prettyName))
  }

  def singleTableStarInCountNotAllowedError(targetString: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1021",
      messageParameters = Map("targetString" -> targetString))
  }

  def orderByPositionRangeError(index: Int, size: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1022",
      messageParameters = Map(
        "index" -> index.toString,
        "size" -> size.toString),
      origin = t.origin)
  }

  def groupByPositionRefersToAggregateFunctionError(
      index: Int,
      expr: Expression): Throwable = {
    new AnalysisException(
      errorClass = "GROUP_BY_POS_REFERS_AGG_EXPR",
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
    new AnalysisException(errorClass = "UNSUPPORTED_GENERATOR",
      errorSubClass = "NOT_GENERATOR",
      messageParameters = Map(
        "functionName" -> toSQLId(name.toString),
        "classCanonicalName" -> classCanonicalName))
  }

  def functionWithUnsupportedSyntaxError(prettyName: String, syntax: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1023",
      messageParameters = Map("prettyName" -> prettyName, "syntax" -> syntax))
  }

  def nonDeterministicFilterInAggregateError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1024",
      messageParameters = Map.empty)
  }

  def nonBooleanFilterInAggregateError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1025",
      messageParameters = Map.empty)
  }

  def aggregateInAggregateFilterError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1026",
      messageParameters = Map.empty)
  }

  def windowFunctionInAggregateFilterError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1027",
      messageParameters = Map.empty)
  }

  def aliasNumberNotMatchColumnNumberError(
      columnSize: Int, outputSize: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1028",
      messageParameters = Map(
        "columnSize" -> columnSize.toString,
        "outputSize" -> outputSize.toString),
      origin = t.origin)
  }

  def aliasesNumberNotMatchUDTFOutputError(
      aliasesSize: Int, aliasesNames: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1029",
      messageParameters = Map(
        "aliasesSize" -> aliasesSize.toString,
        "aliasesNames" -> aliasesNames))
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

  def writeTableWithMismatchedColumnsError(
      columnSize: Int, outputSize: Int, t: TreeNode[_]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1038",
      messageParameters = Map(
        "columnSize" -> columnSize.toString,
        "outputSize" -> outputSize.toString),
      origin = t.origin)
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

  def functionUndefinedError(name: FunctionIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1041",
      messageParameters = Map("name" -> name.toString))
  }

  def invalidFunctionArgumentsError(
      name: String, expectedInfo: String, actualNumber: Int): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1042",
      messageParameters = Map(
        "name" -> name,
        "expectedInfo" -> expectedInfo,
        "actualNumber" -> actualNumber.toString))
  }

  def invalidFunctionArgumentNumberError(
      validParametersCount: Seq[Int], name: String, actualNumber: Int): Throwable = {
    if (validParametersCount.length == 0) {
      new AnalysisException(
        errorClass = "_LEGACY_ERROR_TEMP_1043",
        messageParameters = Map("name" -> name))
    } else {
      val expectedNumberOfParameters = if (validParametersCount.length == 1) {
        validParametersCount.head.toString
      } else {
        validParametersCount.init.mkString("one of ", ", ", " and ") +
          validParametersCount.last
      }
      invalidFunctionArgumentsError(name, expectedNumberOfParameters, actualNumber)
    }
  }

  def functionAcceptsOnlyOneArgumentError(name: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1044",
      messageParameters = Map("name" -> name))
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

  def attributeNameSyntaxError(name: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1049",
      messageParameters = Map("name" -> name))
  }

  def starExpandDataTypeNotSupportedError(attributes: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1050",
      messageParameters = Map("attributes" -> attributes.toString()))
  }

  def cannotResolveStarExpandGivenInputColumnsError(
      targetString: String, columns: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1051",
      messageParameters = Map(
        "targetString" -> targetString,
        "columns" -> columns))
  }

  def addColumnWithV1TableCannotSpecifyNotNullError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1052",
      messageParameters = Map.empty)
  }

  def operationOnlySupportedWithV2TableError(
      nameParts: Seq[String],
      operation: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "TABLE_OPERATION",
      messageParameters = Map(
        "tableName" -> toSQLId(nameParts),
        "operation" -> operation))
  }

  def catalogOperationNotSupported(catalog: CatalogPlugin, operation: String): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "CATALOG_OPERATION",
      messageParameters = Map(
        "catalogName" -> toSQLId(Seq(catalog.name())),
        "operation" -> operation))
  }

  def alterColumnWithV1TableCannotSpecifyNotNullError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1053",
      messageParameters = Map.empty)
  }

  def alterColumnCannotFindColumnInV1TableError(colName: String, v1Table: V1Table): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1054",
      messageParameters = Map(
        "colName" -> colName,
        "fieldNames" -> v1Table.schema.fieldNames.mkString(", ")))
  }

  def invalidDatabaseNameError(quoted: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1055",
      messageParameters = Map("database" -> quoted))
  }

  def cannotDropViewWithDropTableError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1056",
      messageParameters = Map.empty)
  }

  def showColumnsWithConflictDatabasesError(
      db: Seq[String], v1TableName: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1057",
      messageParameters = Map(
        "dbA" -> db.head,
        "dbB" -> v1TableName.database.get))
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

  def columnDoesNotExistError(colName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1061",
      messageParameters = Map("colName" -> colName))
  }

  def renameTempViewToExistingViewError(oldName: String, newName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1062",
      messageParameters = Map("oldName" -> oldName, "newName" -> newName))
  }

  def cannotDropNonemptyDatabaseError(db: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1063",
      messageParameters = Map("db" -> db))
  }

  def cannotDropNonemptyNamespaceError(namespace: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1064",
      messageParameters = Map("namespace" -> namespace.quoted))
  }

  def invalidNameForTableOrDatabaseError(name: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1065",
      messageParameters = Map("name" -> name))
  }

  def cannotCreateDatabaseWithSameNameAsPreservedDatabaseError(database: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1066",
      messageParameters = Map("database" -> database))
  }

  def cannotDropDefaultDatabaseError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1067",
      messageParameters = Map.empty)
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

  def cannotOperateManagedTableWithExistingLocationError(
      methodName: String, tableIdentifier: TableIdentifier, tableLocation: Path): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1070",
      messageParameters = Map(
        "methodName" -> methodName,
        "tableIdentifier" -> tableIdentifier.toString,
        "tableLocation" -> tableLocation.toString))
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

  def cannotRenameTempViewToExistingTableError(
      oldName: TableIdentifier, newName: TableIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1075",
      messageParameters = Map(
        "oldName" -> oldName.toString,
        "newName" -> newName.toString))
  }

  def invalidPartitionSpecError(details: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1076",
      messageParameters = Map("details" -> details))
  }

  def functionAlreadyExistsError(func: FunctionIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1077",
      messageParameters = Map("func" -> func.toString))
  }

  def cannotLoadClassWhenRegisteringFunctionError(
      className: String, func: FunctionIdentifier): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1078",
      messageParameters = Map(
        "className" -> className,
        "func" -> func.toString))
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
      errorClass = "_LEGACY_ERROR_TEMP_1084",
      messageParameters = Map(
        "numParts" -> numParts.toString,
        "index" -> index.toString))
  }

  def corruptedViewSQLConfigsInCatalogError(e: Exception): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1085",
      messageParameters = Map.empty,
      cause = Some(e))
  }

  def corruptedViewQueryOutputColumnsInCatalogError(numCols: String, index: Int): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1086",
      messageParameters = Map(
        "numCols" -> numCols,
        "index" -> index.toString))
  }

  def corruptedViewReferredTempViewInCatalogError(e: Exception): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1087",
      messageParameters = Map.empty,
      cause = Some(e))
  }

  def corruptedViewReferredTempFunctionsInCatalogError(e: Exception): Throwable = {
        new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1088",
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

  def cannotReadCorruptedTablePropertyError(key: String, details: String = ""): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1091",
      messageParameters = Map("key" -> key, "details" -> details))
  }

  def invalidSchemaStringError(exp: Expression): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1092",
      messageParameters = Map("expr" -> exp.sql))
  }

  def schemaNotFoldableError(exp: Expression): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1093",
      messageParameters = Map("expr" -> exp.sql))
  }

  def schemaIsNotStructTypeError(dataType: DataType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1094",
      messageParameters = Map("dataType" -> dataType.toString))
  }

  def keyValueInMapNotStringError(m: CreateMap): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1095",
      messageParameters = Map("map" -> m.dataType.catalogString))
  }

  def nonMapFunctionNotAllowedError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1096",
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

  def requireLiteralParameter(
      funcName: String, argName: String, requiredType: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1100",
      messageParameters = Map(
        "argName" -> argName,
        "funcName" -> funcName,
        "requiredType" -> requiredType))
  }

  def invalidStringLiteralParameter(
      funcName: String,
      argName: String,
      invalidValue: String,
      allowedValues: Option[String] = None): Throwable = {
    val endingMsg = allowedValues.map(" " + _).getOrElse("")
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1101",
      messageParameters = Map(
        "argName" -> argName,
        "funcName" -> funcName,
        "invalidValue" -> invalidValue,
        "endingMsg" -> endingMsg))
  }

  def literalTypeUnsupportedForSourceTypeError(field: String, source: Expression): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1102",
      messageParameters = Map(
        "field" -> field,
        "srcDataType" -> source.dataType.catalogString))
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
          errorClass = "_LEGACY_ERROR_TEMP_1105",
          messageParameters = Map("extraction" -> extraction.toString))
      case other =>
        new AnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_1106",
          messageParameters = Map(
            "child" -> child.toString,
            "other" -> other.catalogString))
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

  def unsupportedTableOperationError(table: Table, cmd: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1113",
      messageParameters = Map(
        "table" -> table.name,
        "cmd" -> cmd))
  }

  def unsupportedBatchReadError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "batch scan")
  }

  def unsupportedMicroBatchOrContinuousScanError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "either micro-batch or continuous scan")
  }

  def unsupportedAppendInBatchModeError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "append in batch mode")
  }

  def unsupportedDynamicOverwriteInBatchModeError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "dynamic overwrite in batch mode")
  }

  def unsupportedTruncateInBatchModeError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "truncate in batch mode")
  }

  def unsupportedOverwriteByFilterInBatchModeError(table: Table): Throwable = {
    unsupportedTableOperationError(table, "overwrite by filter in batch mode")
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

  def noSuchTableError(ident: Identifier): Throwable = {
    new NoSuchTableException(ident)
  }

  def noSuchTableError(nameParts: Seq[String]): Throwable = {
    new NoSuchTableException(nameParts)
  }

  def noSuchNamespaceError(namespace: Array[String]): Throwable = {
    new NoSuchNamespaceException(namespace)
  }

  def tableAlreadyExistsError(ident: Identifier): Throwable = {
    new TableAlreadyExistsException(ident)
  }

  def requiresSinglePartNamespaceError(ns: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1117",
      messageParameters = Map(
        "sessionCatalog" -> CatalogManager.SESSION_CATALOG_NAME,
        "ns" -> ns.mkString("[", ", ", "]")))
  }

  def namespaceAlreadyExistsError(namespace: Array[String]): Throwable = {
    new NamespaceAlreadyExistsException(namespace)
  }

  private def notSupportedInJDBCCatalog(cmd: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1119",
      messageParameters = Map("cmd" -> cmd))
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

  def cannotCreateJDBCNamespaceWithPropertyError(k: String): Throwable = {
    notSupportedInJDBCCatalog(s"CREATE NAMESPACE with property $k")
  }

  def cannotSetJDBCNamespaceWithPropertyError(k: String): Throwable = {
    notSupportedInJDBCCatalog(s"SET NAMESPACE with property $k")
  }

  def cannotUnsetJDBCNamespaceWithPropertyError(k: String): Throwable = {
    notSupportedInJDBCCatalog(s"Remove NAMESPACE property $k")
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
      errorClass = "_LEGACY_ERROR_TEMP_1124",
      messageParameters = Map("cmd" -> cmd))
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

  def showColumnsNotSupportedForV2TablesError(): Throwable = {
    notSupportedForV2TablesError("SHOW COLUMNS")
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
      errorClass = "_LEGACY_ERROR_TEMP_1129",
      messageParameters = Map("format" -> format))
  }

  def dataPathNotExistError(path: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1130",
      messageParameters = Map("path" -> path))
  }

  def dataSourceOutputModeUnsupportedError(
      className: String, outputMode: OutputMode): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1131",
      messageParameters = Map(
        "className" -> className,
        "outputMode" -> outputMode.toString))
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

  def cannotSaveIntervalIntoExternalStorageError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1136",
      messageParameters = Map.empty)
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

  def writeEmptySchemasUnsupportedByDataSourceError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1142",
      messageParameters = Map.empty)
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
        "targetSize" -> targetPartitionSchema.fields.size.toString,
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

  def cannotWriteDataToRelationsWithMultiplePathsError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1148",
      messageParameters = Map.empty)
  }

  def failedToRebuildExpressionError(filter: Filter): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1149",
      messageParameters = Map("filter" -> filter.toString))
  }

  def dataTypeUnsupportedByDataSourceError(format: String, field: StructField): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1150",
      messageParameters = Map(
        "field" -> field.name,
        "fieldType" -> field.dataType.catalogString,
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
      errorClass = "_LEGACY_ERROR_TEMP_1152",
      messageParameters = Map("outputPath" -> outputPath.toString))
  }

  def cannotUseDataTypeForPartitionColumnError(field: StructField): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1153",
      messageParameters = Map("field" -> field.dataType.toString))
  }

  def cannotUseAllColumnsForPartitionColumnsError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1154",
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

  def unsupportedDataSourceTypeForDirectQueryOnFilesError(className: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1157",
      messageParameters = Map("className" -> className))
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

  def mismatchedInsertedDataColumnNumberError(
      tableName: String, insert: InsertIntoStatement, staticPartCols: Set[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1168",
      messageParameters = Map(
        "tableName" -> tableName,
        "targetColumns" -> insert.table.output.size.toString,
        "insertedColumns" -> (insert.query.output.length + staticPartCols.size).toString,
        "staticPartCols" -> staticPartCols.size.toString))
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
        "partColNames" -> partColNames.mkString(",")))
  }

  def ddlWithoutHiveSupportEnabledError(detail: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1170",
      messageParameters = Map("detail" -> detail))
  }

  def createTableColumnTypesOptionColumnNotFoundInSchemaError(
      col: String, schema: StructType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1171",
      messageParameters = Map("col" -> col, "schema" -> schema.catalogString))
  }

  def parquetTypeUnsupportedYetError(parquetType: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1172",
      messageParameters = Map("parquetType" -> parquetType))
  }

  def illegalParquetTypeError(parquetType: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1173",
      messageParameters = Map("parquetType" -> parquetType))
  }

  def unrecognizedParquetTypeError(field: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1174",
      messageParameters = Map("field" -> field))
  }

  def cannotConvertDataTypeToParquetTypeError(field: StructField): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1175",
      messageParameters = Map("dataType" -> field.dataType.catalogString))
  }

  def incompatibleViewSchemaChange(
      viewName: String,
      colName: String,
      expectedNum: Int,
      actualCols: Seq[Attribute],
      viewDDL: Option[String]): Throwable = {
    viewDDL.map { v =>
      new AnalysisException(
        errorClass = "_LEGACY_ERROR_TEMP_1176",
        messageParameters = Map(
          "viewName" -> viewName,
          "colName" -> colName,
          "expectedNum" -> expectedNum.toString,
          "actualCols" -> actualCols.map(_.name).mkString("[", ",", "]"),
          "viewDDL" -> v))
    }.getOrElse {
      new AnalysisException(
        errorClass = "_LEGACY_ERROR_TEMP_1177",
        messageParameters = Map(
          "viewName" -> viewName,
          "colName" -> colName,
          "expectedNum" -> expectedNum.toString,
          "actualCols" -> actualCols.map(_.name).mkString("[", ",", "]")))
    }
  }

  def numberOfPartitionsNotAllowedWithUnspecifiedDistributionError(): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1178",
      messageParameters = Map.empty)
  }

  def cannotApplyTableValuedFunctionError(
      name: String, arguments: String, usage: String, details: String = ""): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1179",
      messageParameters = Map(
        "name" -> name,
        "usage" -> usage,
        "arguments" -> arguments,
        "details" -> details))
  }

  def incompatibleRangeInputDataTypeError(
      expression: Expression, dataType: DataType): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1180",
      messageParameters = Map(
        "expectedDataType" -> dataType.typeName,
        "foundDataType" -> expression.dataType.typeName))
  }

  def streamJoinStreamWithoutEqualityPredicateUnsupportedError(plan: LogicalPlan): Throwable = {
    val errorClass = "_LEGACY_ERROR_TEMP_1181"
    new AnalysisException(
      SparkThrowableHelper.getMessage(errorClass, null, Map.empty[String, String]),
      errorClass = Some(errorClass),
      messageParameters = Map.empty,
      plan = Some(plan))
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
    new AnalysisException(
      errorClass = "AMBIGUOUS_COLUMN_OR_FIELD",
      messageParameters = Map(
        "name" -> toSQLId(name),
        "n" -> numMatches.toString),
      origin = context)
  }

  def ambiguousColumnOrFieldError(
      name: Seq[String], numMatches: Int): Throwable = {
    new AnalysisException(
      errorClass = "AMBIGUOUS_COLUMN_OR_FIELD",
      messageParameters = Map(
        "name" -> toSQLId(name),
        "n" -> numMatches.toString))
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

  def identifierHavingMoreThanTwoNamePartsError(
      quoted: String, identifier: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1185",
      messageParameters = Map(
        "quoted" -> quoted,
        "identifier" -> identifier))
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
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1189",
      messageParameters = Map("operation" -> operation))
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

  def operationNotSupportPartitioningError(operation: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1197",
      messageParameters = Map("operation" -> operation))
  }

  def mixedRefsInAggFunc(funcStr: String, origin: Origin): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY",
      errorSubClass = "AGGREGATE_FUNCTION_MIXED_OUTER_LOCAL_REFERENCES",
      origin = origin,
      messageParameters = Map("function" -> funcStr))
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

  def ambiguousRelationAliasNameInNestedCTEError(name: String): Throwable = {
    new AnalysisException(s"Name $name is ambiguous in nested CTE. " +
      s"Please set ${LEGACY_CTE_PRECEDENCE_POLICY.key} to CORRECTED so that name " +
      "defined in inner CTE takes precedence. If set it to LEGACY, outer CTE " +
      "definitions will take precedence. See more details in SPARK-28228.")
  }

  def commandUnsupportedInV2TableError(name: String): Throwable = {
    new AnalysisException(s"$name is not supported for v2 tables.")
  }

  def cannotResolveColumnNameAmongAttributesError(
      colName: String, fieldNames: String): Throwable = {
    new AnalysisException(s"""Cannot resolve column name "$colName" among ($fieldNames)""")
  }

  def cannotWriteTooManyColumnsToTableError(
      tableName: String, expected: Seq[Attribute], query: LogicalPlan): Throwable = {
    new AnalysisException(
      s"""
         |Cannot write to '$tableName', too many data columns:
         |Table columns: ${expected.map(c => s"'${c.name}'").mkString(", ")}
         |Data columns: ${query.output.map(c => s"'${c.name}'").mkString(", ")}
       """.stripMargin)
  }

  def cannotWriteNotEnoughColumnsToTableError(
      tableName: String, expected: Seq[Attribute], query: LogicalPlan): Throwable = {
    new AnalysisException(
      s"""Cannot write to '$tableName', not enough data columns:
         |Table columns: ${expected.map(c => s"'${c.name}'").mkString(", ")}
         |Data columns: ${query.output.map(c => s"'${c.name}'").mkString(", ")}"""
        .stripMargin)
  }

  def cannotWriteIncompatibleDataToTableError(tableName: String, errors: Seq[String]): Throwable = {
    new AnalysisException(
      s"Cannot write incompatible data to table '$tableName':\n- ${errors.mkString("\n- ")}")
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
      s"Expected only partition pruning predicates: $nonPartitionPruningPredicates")
  }

  def columnNotDefinedInTableError(
      colType: String, colName: String, tableName: String, tableCols: Seq[String]): Throwable = {
    new AnalysisException(s"$colType column $colName is not defined in table $tableName, " +
      s"defined table columns are: ${tableCols.mkString(", ")}")
  }

  def invalidLiteralForWindowDurationError(): Throwable = {
    new AnalysisException("The duration and time inputs to window must be " +
      "an integer, long or string literal.")
  }

  def noSuchStructFieldInGivenFieldsError(
      fieldName: String, fields: Array[StructField]): Throwable = {
    new AnalysisException(
      s"No such struct field $fieldName in ${fields.map(_.name).mkString(", ")}")
  }

  def ambiguousReferenceToFieldsError(fields: String): Throwable = {
    new AnalysisException(s"Ambiguous reference to fields $fields")
  }

  def secondArgumentInFunctionIsNotBooleanLiteralError(funcName: String): Throwable = {
    new AnalysisException(s"The second argument in $funcName should be a boolean literal.")
  }

  def joinConditionMissingOrTrivialError(
      join: Join, left: LogicalPlan, right: LogicalPlan): Throwable = {
    new AnalysisException(
      s"""Detected implicit cartesian product for ${join.joinType.sql} join between logical plans
         |${left.treeString(false).trim}
         |and
         |${right.treeString(false).trim}
         |Join condition is missing or trivial.
         |Either: use the CROSS JOIN syntax to allow cartesian products between these
         |relations, or: enable implicit cartesian products by setting the configuration
         |variable spark.sql.crossJoin.enabled=true"""
        .stripMargin)
  }

  def usePythonUDFInJoinConditionUnsupportedError(joinType: JoinType): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "PYTHON_UDF_IN_ON_CLAUSE",
      messageParameters = Map("joinType" -> toSQLStmt(joinType.sql)))
  }

  def conflictingAttributesInJoinConditionError(
      conflictingAttrs: AttributeSet, outerPlan: LogicalPlan, subplan: LogicalPlan): Throwable = {
    new AnalysisException("Found conflicting attributes " +
      s"${conflictingAttrs.mkString(",")} in the condition joining outer plan:\n  " +
      s"$outerPlan\nand subplan:\n  $subplan")
  }

  def emptyWindowExpressionError(expr: Window): Throwable = {
    new AnalysisException(s"Window expression is empty in $expr")
  }

  def foundDifferentWindowFunctionTypeError(windowExpressions: Seq[NamedExpression]): Throwable = {
    new AnalysisException(
      s"Found different window function type in $windowExpressions")
  }

  def charOrVarcharTypeAsStringUnsupportedError(): Throwable = {
    new AnalysisException("char/varchar type can only be used in the table schema. " +
      s"You can set ${SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key} to true, so that Spark" +
      s" treat them as string type as same as Spark 3.0 and earlier")
  }

  def invalidPatternError(pattern: String, message: String): Throwable = {
    new AnalysisException(
      s"the pattern '$pattern' is invalid, $message")
  }

  def tableIdentifierExistsError(tableIdentifier: TableIdentifier): Throwable = {
    new AnalysisException(s"$tableIdentifier already exists.")
  }

  def tableIdentifierNotConvertedToHadoopFsRelationError(
      tableIdentifier: TableIdentifier): Throwable = {
    new AnalysisException(s"$tableIdentifier should be converted to HadoopFsRelation.")
  }

  def alterDatabaseLocationUnsupportedError(): Throwable = {
    new AnalysisException("Hive metastore does not support altering database location")
  }

  def hiveTableTypeUnsupportedError(tableType: String): Throwable = {
    new AnalysisException(s"Hive $tableType is not supported.")
  }

  def hiveCreatePermanentFunctionsUnsupportedError(): Throwable = {
    new AnalysisException("Hive 0.12 doesn't support creating permanent functions. " +
      "Please use Hive 0.13 or higher.")
  }

  def unknownHiveResourceTypeError(resourceType: String): Throwable = {
    new AnalysisException(s"Unknown resource type: $resourceType")
  }

  def invalidDayTimeField(field: Byte): Throwable = {
    val supportedIds = DayTimeIntervalType.dayTimeFields
      .map(i => s"$i (${DayTimeIntervalType.fieldToString(i)})")
    new AnalysisException(s"Invalid field id '$field' in day-time interval. " +
      s"Supported interval fields: ${supportedIds.mkString(", ")}.")
  }

  def invalidDayTimeIntervalType(startFieldName: String, endFieldName: String): Throwable = {
    new AnalysisException(s"'interval $startFieldName to $endFieldName' is invalid.")
  }

  def invalidYearMonthField(field: Byte): Throwable = {
    val supportedIds = YearMonthIntervalType.yearMonthFields
      .map(i => s"$i (${YearMonthIntervalType.fieldToString(i)})")
    new AnalysisException(s"Invalid field id '$field' in year-month interval. " +
      s"Supported interval fields: ${supportedIds.mkString(", ")}.")
  }

  def configRemovedInVersionError(
      configName: String,
      version: String,
      comment: String): Throwable = {
    new AnalysisException(
      s"The SQL config '$configName' was removed in the version $version. $comment")
  }

  def failedFallbackParsingError(msg: String, e1: Throwable, e2: Throwable): Throwable = {
    new AnalysisException(s"$msg${e1.getMessage}\nFailed fallback parsing: ${e2.getMessage}",
      cause = Some(e1.getCause))
  }

  def decimalCannotGreaterThanPrecisionError(scale: Int, precision: Int): Throwable = {
    new AnalysisException(s"Decimal scale ($scale) cannot be greater than precision ($precision).")
  }

  def decimalOnlySupportPrecisionUptoError(decimalType: String, precision: Int): Throwable = {
    new AnalysisException(s"$decimalType can only support precision up to $precision")
  }

  def negativeScaleNotAllowedError(scale: Int): Throwable = {
    new AnalysisException(
      s"""|Negative scale is not allowed: $scale.
         |You can use ${LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key}=true
         |to enable legacy mode to allow it.""".stripMargin.replaceAll("\n", " "))
  }

  def invalidPartitionColumnKeyInTableError(key: String, tblName: String): Throwable = {
    new AnalysisException(s"$key is not a valid partition column in table $tblName.")
  }

  def invalidPartitionSpecError(
      specKeys: String,
      partitionColumnNames: Seq[String],
      tableName: String): Throwable = {
    new AnalysisException(
      s"""|Partition spec is invalid. The spec ($specKeys) must match
        |the partition spec (${partitionColumnNames.mkString(", ")}) defined in
        |table '$tableName'""".stripMargin.replaceAll("\n", " "))
  }

  def foundDuplicateColumnError(colType: String, duplicateCol: Seq[String]): Throwable = {
    new AnalysisException(
      s"Found duplicate column(s) $colType: ${duplicateCol.sorted.mkString(", ")}")
  }

  def noSuchTableError(db: String, table: String): Throwable = {
    new NoSuchTableException(db = db, table = table)
  }

  def tempViewNotCachedForAnalyzingColumnsError(tableIdent: TableIdentifier): Throwable = {
    new AnalysisException(s"Temporary view $tableIdent is not cached for analyzing columns.")
  }

  def columnTypeNotSupportStatisticsCollectionError(
      name: String,
      tableIdent: TableIdentifier,
      dataType: DataType): Throwable = {
    new AnalysisException(s"Column $name in table $tableIdent is of type $dataType, " +
      "and Spark does not support statistics collection on this column type.")
  }

  def analyzeTableNotSupportedOnViewsError(): Throwable = {
    new AnalysisException("ANALYZE TABLE is not supported on views.")
  }

  def unexpectedPartitionColumnPrefixError(
      table: String,
      database: String,
      schemaColumns: String,
      specColumns: String): Throwable = {
    new AnalysisException(
      s"""
         |The list of partition columns with values
         |in partition specification for table '${table}'
         |in database '${database}' is not a prefix of the list of
         |partition columns defined in the table schema.
         |Expected a prefix of [${schemaColumns}], but got [${specColumns}].
       """.stripMargin.replaceAll("\n", " "))
  }

  def noSuchPartitionError(
      db: String,
      table: String,
      partition: TablePartitionSpec): Throwable = {
    new NoSuchPartitionException(db, table, partition)
  }

  def analyzingColumnStatisticsNotSupportedForColumnTypeError(
      name: String,
      dataType: DataType): Throwable = {
    new AnalysisException("Analyzing column statistics is not supported for column " +
      s"$name of data type: $dataType.")
  }

  def tableAlreadyExistsError(table: String, guide: String = ""): Throwable = {
    new AnalysisException(s"Table $table already exists." + guide)
  }

  def createTableAsSelectWithNonEmptyDirectoryError(tablePath: String): Throwable = {
    new AnalysisException(
      s"CREATE-TABLE-AS-SELECT cannot create table with location to a non-empty directory " +
        s"${tablePath} . To allow overwriting the existing non-empty directory, " +
        s"set '${SQLConf.ALLOW_NON_EMPTY_LOCATION_IN_CTAS.key}' to true.")
  }

  def tableOrViewNotFoundError(table: String): Throwable = {
    new AnalysisException(s"Table or view not found: $table")
  }

  def noSuchFunctionError(
      rawName: Seq[String],
      t: TreeNode[_],
      fullName: Option[Seq[String]] = None): Throwable = {
    if (rawName.length == 1 && fullName.isDefined) {
      new AnalysisException(s"Undefined function: ${rawName.head}. " +
        "This function is neither a built-in/temporary function, nor a persistent " +
        s"function that is qualified as ${fullName.get.quoted}.",
        t.origin.line, t.origin.startPosition)
    } else {
      new AnalysisException(s"Undefined function: ${rawName.quoted}",
        t.origin.line, t.origin.startPosition)
    }
  }

  def unsetNonExistentPropertyError(property: String, table: TableIdentifier): Throwable = {
    new AnalysisException(s"Attempted to unset non-existent property '$property' in table '$table'")
  }

  def alterTableChangeColumnNotSupportedForColumnTypeError(
      originColumn: StructField,
      newColumn: StructField): Throwable = {
    new AnalysisException("ALTER TABLE CHANGE COLUMN is not supported for changing column " +
      s"'${originColumn.name}' with type '${originColumn.dataType}' to " +
      s"'${newColumn.name}' with type '${newColumn.dataType}'")
  }

  def cannotFindColumnError(name: String, fieldNames: Array[String]): Throwable = {
    new AnalysisException(s"Can't find column `$name` given table data columns " +
      s"${fieldNames.mkString("[`", "`, `", "`]")}")
  }

  def alterTableSetSerdeForSpecificPartitionNotSupportedError(): Throwable = {
    new AnalysisException("Operation not allowed: ALTER TABLE SET " +
      "[SERDE | SERDEPROPERTIES] for a specific partition is not supported " +
      "for tables created with the datasource API")
  }

  def alterTableSetSerdeNotSupportedError(): Throwable = {
    new AnalysisException("Operation not allowed: ALTER TABLE SET SERDE is " +
      "not supported for tables created with the datasource API")
  }

  def cmdOnlyWorksOnPartitionedTablesError(cmd: String, tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      s"Operation not allowed: $cmd only works on partitioned tables: $tableIdentWithDB")
  }

  def cmdOnlyWorksOnTableWithLocationError(cmd: String, tableIdentWithDB: String): Throwable = {
    new AnalysisException(s"Operation not allowed: $cmd only works on table with " +
      s"location provided: $tableIdentWithDB")
  }

  def actionNotAllowedOnTableWithFilesourcePartitionManagementDisabledError(
      action: String,
      tableName: String): Throwable = {
    new AnalysisException(
      s"$action is not allowed on $tableName since filesource partition management is " +
        "disabled (spark.sql.hive.manageFilesourcePartitions = false).")
  }

  def actionNotAllowedOnTableSincePartitionMetadataNotStoredError(
     action: String,
     tableName: String): Throwable = {
    new AnalysisException(
      s"$action is not allowed on $tableName since its partition metadata is not stored in " +
        "the Hive metastore. To import this information into the metastore, run " +
        s"`msck repair table $tableName`")
  }

  def cannotAlterViewWithAlterTableError(): Throwable = {
    new AnalysisException(
      "Cannot alter a view with ALTER TABLE. Please use ALTER VIEW instead")
  }

  def cannotAlterTableWithAlterViewError(): Throwable = {
    new AnalysisException(
      "Cannot alter a table with ALTER VIEW. Please use ALTER TABLE instead")
  }

  def cannotOverwritePathBeingReadFromError(): Throwable = {
    new AnalysisException("Cannot overwrite a path that is also being read from.")
  }

  def cannotDropBuiltinFuncError(functionName: String): Throwable = {
    new AnalysisException(s"Cannot drop built-in function '$functionName'")
  }

  def cannotRefreshBuiltInFuncError(functionName: String): Throwable = {
    new AnalysisException(s"Cannot refresh built-in function $functionName")
  }

  def cannotRefreshTempFuncError(functionName: String): Throwable = {
    new AnalysisException(s"Cannot refresh temporary function $functionName")
  }

  def noSuchFunctionError(identifier: FunctionIdentifier): Throwable = {
    new NoSuchFunctionException(identifier.database.get, identifier.funcName)
  }

  def alterAddColNotSupportViewError(table: TableIdentifier): Throwable = {
    new AnalysisException(
      s"""
         |ALTER ADD COLUMNS does not support views.
         |You must drop and re-create the views for adding the new columns. Views: $table
       """.stripMargin)
  }

  def alterAddColNotSupportDatasourceTableError(
      tableType: Any,
      table: TableIdentifier): Throwable = {
    new AnalysisException(
      s"""
         |ALTER ADD COLUMNS does not support datasource table with type $tableType.
         |You must drop and re-create the table for adding the new columns. Tables: $table
       """.stripMargin)
  }

  def loadDataNotSupportedForDatasourceTablesError(tableIdentWithDB: String): Throwable = {
    new AnalysisException(s"LOAD DATA is not supported for datasource tables: $tableIdentWithDB")
  }

  def loadDataWithoutPartitionSpecProvidedError(tableIdentWithDB: String): Throwable = {
    new AnalysisException(s"LOAD DATA target table $tableIdentWithDB is partitioned, " +
      s"but no partition spec is provided")
  }

  def loadDataPartitionSizeNotMatchNumPartitionColumnsError(
      tableIdentWithDB: String,
      partitionSize: Int,
      targetTableSize: Int): Throwable = {
    new AnalysisException(
      s"""
         |LOAD DATA target table $tableIdentWithDB is partitioned,
         |but number of columns in provided partition spec ($partitionSize)
         |do not match number of partitioned columns in table ($targetTableSize)
       """.stripMargin.replaceAll("\n", " "))
  }

  def loadDataTargetTableNotPartitionedButPartitionSpecWasProvidedError(
      tableIdentWithDB: String): Throwable = {
    new AnalysisException(s"LOAD DATA target table $tableIdentWithDB is not " +
      s"partitioned, but a partition spec was provided.")
  }

  def loadDataInputPathNotExistError(path: String): Throwable = {
    new AnalysisException(s"LOAD DATA input path does not exist: $path")
  }

  def truncateTableOnExternalTablesError(tableIdentWithDB: String): Throwable = {
    new AnalysisException(
      s"Operation not allowed: TRUNCATE TABLE on external tables: $tableIdentWithDB")
  }

  def truncateTablePartitionNotSupportedForNotPartitionedTablesError(
      tableIdentWithDB: String): Throwable = {
    new AnalysisException(s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported" +
      s" for tables that are not partitioned: $tableIdentWithDB")
  }

  def failToTruncateTableWhenRemovingDataError(
      tableIdentWithDB: String,
      path: Path,
      e: Throwable): Throwable = {
    new AnalysisException(s"Failed to truncate table $tableIdentWithDB when " +
        s"removing data of the path: $path because of ${e.toString}", cause = Some(e))
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
      s"SHOW PARTITIONS is not allowed on a table that is not partitioned: $tableIdentWithDB")
  }

  def showCreateTableNotSupportedOnTempView(table: String): Throwable = {
    new AnalysisException(s"SHOW CREATE TABLE is not supported on a temporary view: $table")
  }

  def showCreateTableFailToExecuteUnsupportedFeatureError(table: CatalogTable): Throwable = {
    new AnalysisException("Failed to execute SHOW CREATE TABLE against table " +
      s"${table.identifier}, which is created by Hive and uses the " +
      s"following unsupported feature(s)\n" +
      table.unsupportedFeatures.map(" - " + _).mkString("\n") + ". " +
      s"Please use `SHOW CREATE TABLE ${table.identifier} AS SERDE` to show Hive DDL instead.")
  }

  def showCreateTableNotSupportTransactionalHiveTableError(table: CatalogTable): Throwable = {
    new AnalysisException("SHOW CREATE TABLE doesn't support transactional Hive table. " +
      s"Please use `SHOW CREATE TABLE ${table.identifier} AS SERDE` " +
      "to show Hive DDL instead.")
  }

  def showCreateTableFailToExecuteUnsupportedConfError(
      table: TableIdentifier,
      builder: mutable.StringBuilder): Throwable = {
    new AnalysisException("Failed to execute SHOW CREATE TABLE against table " +
        s"${table.identifier}, which is created by Hive and uses the " +
        "following unsupported serde configuration\n" +
        builder.toString() + "\n" +
        s"Please use `SHOW CREATE TABLE ${table.identifier} AS SERDE` to show Hive DDL instead."
    )
  }

  def showCreateTableAsSerdeNotAllowedOnSparkDataSourceTableError(
      table: TableIdentifier): Throwable = {
    new AnalysisException(
      s"$table is a Spark data source table. Use `SHOW CREATE TABLE` without `AS SERDE` instead.")
  }

  def showCreateTableOrViewFailToExecuteUnsupportedFeatureError(
      table: CatalogTable,
      features: Seq[String]): Throwable = {
    new AnalysisException(
      s"Failed to execute SHOW CREATE TABLE against table/view ${table.identifier}, " +
        "which is created by Hive and uses the following unsupported feature(s)\n" +
        features.map(" - " + _).mkString("\n"))
  }

  def logicalPlanForViewNotAnalyzedError(): Throwable = {
    new AnalysisException("The logical plan that represents the view is not analyzed.")
  }

  def createViewNumColumnsMismatchUserSpecifiedColumnLengthError(
      analyzedPlanLength: Int,
      userSpecifiedColumnsLength: Int): Throwable = {
    new AnalysisException(s"The number of columns produced by the SELECT clause " +
      s"(num: `$analyzedPlanLength`) does not match the number of column names " +
      s"specified by CREATE VIEW (num: `$userSpecifiedColumnsLength`).")
  }

  def tableIsNotViewError(name: TableIdentifier): Throwable = {
    new AnalysisException(s"$name is not a view")
  }

  def viewAlreadyExistsError(name: TableIdentifier): Throwable = {
    new AnalysisException(
      s"View $name already exists. If you want to update the view definition, " +
        "please use ALTER VIEW AS or CREATE OR REPLACE VIEW AS")
  }

  def createPersistedViewFromDatasetAPINotAllowedError(): Throwable = {
    new AnalysisException("It is not allowed to create a persisted view from the Dataset API")
  }

  def recursiveViewDetectedError(
      viewIdent: TableIdentifier,
      newPath: Seq[TableIdentifier]): Throwable = {
    new AnalysisException(s"Recursive view $viewIdent detected " +
      s"(cycle: ${newPath.mkString(" -> ")})")
  }

  def notAllowedToCreatePermanentViewWithoutAssigningAliasForExpressionError(
      name: TableIdentifier,
      attrName: String): Throwable = {
    new AnalysisException(s"Not allowed to create a permanent view $name without " +
      s"explicitly assigning an alias for expression $attrName")
  }

  def notAllowedToCreatePermanentViewByReferencingTempViewError(
      name: TableIdentifier,
      nameParts: String): Throwable = {
    new AnalysisException(s"Not allowed to create a permanent view $name by " +
      s"referencing a temporary view $nameParts. " +
      "Please create a temp view instead by CREATE TEMP VIEW")
  }

  def notAllowedToCreatePermanentViewByReferencingTempFuncError(
      name: TableIdentifier,
      funcName: String): Throwable = {
    new AnalysisException(s"Not allowed to create a permanent view $name by " +
      s"referencing a temporary function `$funcName`")
  }

  def queryFromRawFilesIncludeCorruptRecordColumnError(): Throwable = {
    new AnalysisException(
      """
        |Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
        |referenced columns only include the internal corrupt record column
        |(named _corrupt_record by default). For example:
        |spark.read.schema(schema).csv(file).filter($"_corrupt_record".isNotNull).count()
        |and spark.read.schema(schema).csv(file).select("_corrupt_record").show().
        |Instead, you can cache or save the parsed results and then send the same query.
        |For example, val df = spark.read.schema(schema).csv(file).cache() and then
        |df.filter($"_corrupt_record".isNotNull).count().
      """.stripMargin)
  }

  def userDefinedPartitionNotFoundInJDBCRelationError(
      columnName: String, schema: String): Throwable = {
    new AnalysisException(s"User-defined partition column $columnName not " +
      s"found in the JDBC relation: $schema")
  }

  def invalidPartitionColumnTypeError(column: StructField): Throwable = {
    new AnalysisException(
      s"""
         |Partition column type should be ${NumericType.simpleString},
         |${DateType.catalogString}, or ${TimestampType.catalogString}, but
         |${column.dataType.catalogString} found.
       """.stripMargin.replaceAll("\n", " "))
  }

  def tableOrViewAlreadyExistsError(name: String): Throwable = {
    new AnalysisException(
      s"Table or view '$name' already exists. SaveMode: ErrorIfExists.")
  }

  def columnNameContainsInvalidCharactersError(name: String): Throwable = {
    new AnalysisException(
      s"""
         |Column name "$name" contains invalid character(s).
         |Please use alias to rename it.
       """.stripMargin.replaceAll("\n", " "))
  }

  def textDataSourceWithMultiColumnsError(schema: StructType): Throwable = {
    new AnalysisException(
      s"Text data source supports only a single column, and you have ${schema.size} columns.")
  }

  def cannotFindPartitionColumnInPartitionSchemaError(
      readField: StructField, partitionSchema: StructType): Throwable = {
    new AnalysisException(s"Can't find required partition column ${readField.name} " +
      s"in partition schema $partitionSchema")
  }

  def cannotSpecifyDatabaseForTempViewError(tableIdent: TableIdentifier): Throwable = {
    new AnalysisException(
      s"Temporary view '$tableIdent' should not have specified a database")
  }

  def cannotCreateTempViewUsingHiveDataSourceError(): Throwable = {
    new AnalysisException("Hive data source can only be used with tables, " +
      "you can't use it with CREATE TEMP VIEW USING")
  }

  def invalidTimestampProvidedForStrategyError(
      strategy: String, timeString: String): Throwable = {
    new AnalysisException(
      s"The timestamp provided for the '$strategy' option is invalid. The expected format " +
        s"is 'YYYY-MM-DDTHH:mm:ss', but the provided timestamp: $timeString")
  }

  def hostOptionNotSetError(): Throwable = {
    new AnalysisException("Set a host to read from with option(\"host\", ...).")
  }

  def portOptionNotSetError(): Throwable = {
    new AnalysisException("Set a port to read from with option(\"port\", ...).")
  }

  def invalidIncludeTimestampValueError(): Throwable = {
    new AnalysisException("includeTimestamp must be set to either \"true\" or \"false\"")
  }

  def checkpointLocationNotSpecifiedError(): Throwable = {
    new AnalysisException(
      s"""
         |checkpointLocation must be specified either
         |through option("checkpointLocation", ...) or
         |SparkSession.conf.set("${SQLConf.CHECKPOINT_LOCATION.key}", ...)
       """.stripMargin.replaceAll("\n", " "))
  }

  def recoverQueryFromCheckpointUnsupportedError(checkpointPath: Path): Throwable = {
    new AnalysisException("This query does not support recovering from checkpoint location. " +
      s"Delete $checkpointPath to start over.")
  }

  def cannotFindColumnInRelationOutputError(
      colName: String, relation: LogicalPlan): Throwable = {
    new AnalysisException(s"Unable to find the column `$colName` " +
      s"given [${relation.output.map(_.name).mkString(", ")}]")
  }

  def invalidBoundaryStartError(start: Long): Throwable = {
    new AnalysisException(s"Boundary start is not a valid integer: $start")
  }

  def invalidBoundaryEndError(end: Long): Throwable = {
    new AnalysisException(s"Boundary end is not a valid integer: $end")
  }

  def tableOrViewNotFound(ident: Seq[String]): Throwable = {
    new AnalysisException(s"Table or view '${ident.quoted}' not found")
  }

  def unexpectedTypeOfRelationError(relation: LogicalPlan, tableName: String): Throwable = {
    new AnalysisException(
      s"Unexpected type ${relation.getClass.getCanonicalName} of the relation $tableName")
  }

  def unsupportedTableChangeInJDBCCatalogError(change: TableChange): Throwable = {
    new AnalysisException(s"Unsupported TableChange $change in JDBC catalog.")
  }

  def pathOptionNotSetCorrectlyWhenReadingError(): Throwable = {
    new AnalysisException(
      s"""
         |There is a 'path' or 'paths' option set and load() is called
         |with path parameters. Either remove the path option if it's the same as the path
         |parameter, or add it to the load() parameter if you do want to read multiple paths.
         |To ignore this check, set '${SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key}' to 'true'.
       """.stripMargin.replaceAll("\n", " "))
  }

  def pathOptionNotSetCorrectlyWhenWritingError(): Throwable = {
    new AnalysisException(
      s"""
         |There is a 'path' option set and save() is called with a path
         |parameter. Either remove the path option, or call save() without the parameter.
         |To ignore this check, set '${SQLConf.LEGACY_PATH_OPTION_BEHAVIOR.key}' to 'true'.
       """.stripMargin.replaceAll("\n", " "))
  }

  def writeWithSaveModeUnsupportedBySourceError(source: String, createMode: String): Throwable = {
    new AnalysisException(s"TableProvider implementation $source cannot be " +
      s"written with $createMode mode, please use Append or Overwrite modes instead.")
  }

  def partitionByDoesNotAllowedWhenUsingInsertIntoError(): Throwable = {
    new AnalysisException(
      """
        |insertInto() can't be used together with partitionBy().
        |Partition columns have already been defined for the table.
        |It is not necessary to use partitionBy().
      """.stripMargin.replaceAll("\n", " "))
  }

  def cannotFindCatalogToHandleIdentifierError(quote: String): Throwable = {
    new AnalysisException(s"Couldn't find a catalog to handle the identifier $quote.")
  }

  def sortByNotUsedWithBucketByError(): Throwable = {
    new AnalysisException("sortBy must be used together with bucketBy")
  }

  def bucketByUnsupportedByOperationError(operation: String): Throwable = {
    new AnalysisException(s"'$operation' does not support bucketBy right now")
  }

  def bucketByAndSortByUnsupportedByOperationError(operation: String): Throwable = {
    new AnalysisException(s"'$operation' does not support bucketBy and sortBy right now")
  }

  def tableAlreadyExistsError(tableIdent: TableIdentifier): Throwable = {
    new AnalysisException(s"Table $tableIdent already exists.")
  }

  def cannotOverwriteTableThatIsBeingReadFromError(tableName: String): Throwable = {
    new AnalysisException(s"Cannot overwrite table $tableName that is also being read from")
  }

  def invalidPartitionTransformationError(expr: Expression): Throwable = {
    new AnalysisException(s"Invalid partition transformation: ${expr.sql}")
  }

  def cannotResolveColumnNameAmongFieldsError(
      colName: String, fieldsStr: String, extraMsg: String): AnalysisException = {
    new AnalysisException(
      s"""Cannot resolve column name "$colName" among (${fieldsStr})${extraMsg}""")
  }

  def cannotParseIntervalError(delayThreshold: String, e: Throwable): Throwable = {
    new AnalysisException(s"Unable to parse '$delayThreshold'", cause = Some(e))
  }

  def invalidJoinTypeInJoinWithError(joinType: JoinType): Throwable = {
    new AnalysisException(s"Invalid join type in joinWith: ${joinType.sql}")
  }

  def cannotPassTypedColumnInUntypedSelectError(typedCol: String): Throwable = {
    new AnalysisException(s"Typed column $typedCol that needs input type and schema " +
      "cannot be passed in untyped `select` API. Use the typed `Dataset.select` API instead.")
  }

  def invalidViewNameError(viewName: String): Throwable = {
    new AnalysisException(s"Invalid view name: $viewName")
  }

  def invalidBucketsNumberError(numBuckets: String, e: String): Throwable = {
    new AnalysisException(s"Invalid number of buckets: bucket($numBuckets, $e)")
  }

  def usingUntypedScalaUDFError(): Throwable = {
    new AnalysisException(
      errorClass = "UNTYPED_SCALA_UDF",
      messageParameters = Map.empty)
  }

  def aggregationFunctionAppliedOnNonNumericColumnError(colName: String): Throwable = {
    new AnalysisException(s""""$colName" is not a numeric column. """ +
      "Aggregation function can only be applied on a numeric column.")
  }

  def aggregationFunctionAppliedOnNonNumericColumnError(
      pivotColumn: String, maxValues: Int): Throwable = {
    new AnalysisException(
      s"""
         |The pivot column $pivotColumn has more than $maxValues distinct values,
         |this could indicate an error.
         |If this was intended, set ${SQLConf.DATAFRAME_PIVOT_MAX_VALUES.key}
         |to at least the number of distinct values of the pivot column.
       """.stripMargin.replaceAll("\n", " "))
  }

  def cannotModifyValueOfStaticConfigError(key: String): Throwable = {
    new AnalysisException(s"Cannot modify the value of a static config: $key")
  }

  def cannotModifyValueOfSparkConfigError(key: String): Throwable = {
    new AnalysisException(
      s"""
         |Cannot modify the value of a Spark config: $key.
         |See also 'https://spark.apache.org/docs/latest/sql-migration-guide.html#ddl-statements'
       """.stripMargin.replaceAll("\n", " "))
  }

  def commandExecutionInRunnerUnsupportedError(runner: String): Throwable = {
    new AnalysisException(s"Command execution is not supported in runner $runner")
  }

  def udfClassDoesNotImplementAnyUDFInterfaceError(className: String): Throwable = {
    new AnalysisException(
      errorClass = "NO_UDF_INTERFACE_ERROR",
      messageParameters = Map("className" -> className))
  }

  def udfClassImplementMultiUDFInterfacesError(className: String): Throwable = {
    new AnalysisException(
      errorClass = "MULTI_UDF_INTERFACE_ERROR",
      messageParameters = Map("className" -> className))
  }

  def udfClassWithTooManyTypeArgumentsError(n: Int): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_FEATURE",
      errorSubClass = "TOO_MANY_TYPE_ARGUMENTS_FOR_UDF_CLASS",
      messageParameters = Map("num" -> s"$n"))
  }

  def classWithoutPublicNonArgumentConstructorError(className: String): Throwable = {
    new AnalysisException(s"Can not instantiate class $className, please make sure" +
      " it has public non argument constructor")
  }

  def cannotLoadClassNotOnClassPathError(className: String): Throwable = {
    new AnalysisException(s"Can not load class $className, please make sure it is on the classpath")
  }

  def classDoesNotImplementUserDefinedAggregateFunctionError(className: String): Throwable = {
    new AnalysisException(
      s"class $className doesn't implement interface UserDefinedAggregateFunction")
  }

  def missingFieldError(
      fieldName: Seq[String], table: ResolvedTable, context: Origin): Throwable = {
    throw new AnalysisException(
      s"Missing field ${fieldName.quoted} in table ${table.name} with schema:\n" +
        table.schema.treeString,
      context.line,
      context.startPosition)
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

  def tableIndexNotSupportedError(errorMessage: String): Throwable = {
    new AnalysisException(errorMessage)
  }

  def invalidViewText(viewText: String, tableName: String): Throwable = {
    new AnalysisException(
      s"Invalid view text: $viewText. The view $tableName may have been tampered with")
  }

  def invalidTimeTravelSpecError(): Throwable = {
    new AnalysisException(
      "Cannot specify both version and timestamp when time travelling the table.")
  }

  def invalidTimestampExprForTimeTravel(expr: Expression): Throwable = {
    new AnalysisException(s"${expr.sql} is not a valid timestamp expression for time travel.")
  }

  def timeTravelUnsupportedError(target: String): Throwable = {
    new AnalysisException(s"Cannot time travel $target.")
  }

  def tableNotSupportTimeTravelError(tableName: Identifier): UnsupportedOperationException = {
    new UnsupportedOperationException(s"Table $tableName does not support time travel.")
  }

  def writeDistributionAndOrderingNotSupportedInContinuousExecution(): Throwable = {
    new AnalysisException(
      "Sinks cannot request distribution and ordering in continuous execution mode")
  }

  // Return a more descriptive error message if the user tries to nest a DEFAULT column reference
  // inside some other expression (such as DEFAULT + 1) in an INSERT INTO command's VALUES list;
  // this is not allowed.
  def defaultReferencesNotAllowedInComplexExpressionsInInsertValuesList(): Throwable = {
    new AnalysisException(
      "Failed to execute INSERT INTO command because the VALUES list contains a DEFAULT column " +
        "reference as part of another expression; this is not allowed")
  }

  // Return a descriptive error message in the presence of INSERT INTO commands with explicit
  // DEFAULT column references and explicit column lists, since this is not implemented yet.
  def defaultReferencesNotAllowedInComplexExpressionsInUpdateSetClause(): Throwable = {
    new AnalysisException(
      "Failed to execute UPDATE command because the SET list contains a DEFAULT column reference " +
        "as part of another expression; this is not allowed")
  }

  // Return a more descriptive error message if the user tries to use a DEFAULT column reference
  // inside an UPDATE command's WHERE clause; this is not allowed.
  def defaultReferencesNotAllowedInUpdateWhereClause(): Throwable = {
    new AnalysisException(
      "Failed to execute UPDATE command because the WHERE clause contains a DEFAULT column " +
        "reference; this is not allowed")
  }

  // Return a more descriptive error message if the user tries to use a DEFAULT column reference
  // inside an UPDATE command's WHERE clause; this is not allowed.
  def defaultReferencesNotAllowedInMergeCondition(): Throwable = {
    new AnalysisException(
      "Failed to execute MERGE command because the WHERE clause contains a DEFAULT column " +
        "reference; this is not allowed")
  }

  def defaultReferencesNotAllowedInComplexExpressionsInMergeInsertsOrUpdates(): Throwable = {
    new AnalysisException(
      "Failed to execute MERGE INTO command because one of its INSERT or UPDATE assignments " +
        "contains a DEFAULT column reference as part of another expression; this is not allowed")
  }

  def failedToParseExistenceDefaultAsLiteral(fieldName: String, defaultValue: String): Throwable = {
    throw new AnalysisException(
      s"Invalid DEFAULT value for column $fieldName: $defaultValue fails to parse as a valid " +
        "literal value")
  }

  def defaultReferencesNotAllowedInDataSource(
      statementType: String, dataSource: String): Throwable = {
    new AnalysisException(
      s"Failed to execute $statementType command because DEFAULT values are not supported for " +
        "target data source with table provider: \"" + dataSource + "\"")
  }

  def addNewDefaultColumnToExistingTableNotAllowed(
      statementType: String, dataSource: String): Throwable = {
    new AnalysisException(
      s"Failed to execute $statementType command because DEFAULT values are not supported when " +
        "adding new columns to previously existing target data source with table " +
        "provider: \"" + dataSource + "\"")
  }

  def defaultValuesMayNotContainSubQueryExpressions(): Throwable = {
    new AnalysisException(
      "Failed to execute command because subquery expressions are not allowed in DEFAULT values")
  }

  def nullableColumnOrFieldError(name: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "NULLABLE_COLUMN_OR_FIELD",
      messageParameters = Map("name" -> toSQLId(name)))
  }

  def nullableArrayOrMapElementError(path: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "NULLABLE_ARRAY_OR_MAP_ELEMENT",
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
      errorClass = "COLUMN_NOT_IN_GROUP_BY_CLAUSE",
      messageParameters = Map("expression" -> toSQLExpr(expression))
    )
  }
}
