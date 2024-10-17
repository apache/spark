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

package org.apache.spark.sql.catalyst.expressions

import java.text.{DecimalFormat, DecimalFormatSymbols, ParsePosition}
import java.util.Locale

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, CharVarcharUtils}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.internal.types.{AbstractMapType, StringTypeWithCollation}
import org.apache.spark.sql.types.{DataType, MapType, StringType, StructType, VariantType}
import org.apache.spark.unsafe.types.UTF8String

object ExprUtils extends EvalHelper with QueryErrorsBase {

  def evalTypeExpr(exp: Expression): DataType = {
    if (exp.foldable) {
      prepareForEval(exp).eval() match {
        case s: UTF8String if s != null =>
          val dataType = DataType.parseTypeWithFallback(
            s.toString,
            DataType.fromDDL,
            DataType.fromJson)
          CharVarcharUtils.failIfHasCharVarchar(dataType)
        case _ => throw QueryCompilationErrors.unexpectedSchemaTypeError(exp)

      }
    } else {
      throw QueryCompilationErrors.unexpectedSchemaTypeError(exp)
    }
  }

  def evalSchemaExpr(exp: Expression): StructType = {
    val dataType = evalTypeExpr(exp)
    if (!dataType.isInstanceOf[StructType]) {
      throw QueryCompilationErrors.schemaIsNotStructTypeError(exp, dataType)
    }
    dataType.asInstanceOf[StructType]
  }

  def convertToMapData(exp: Expression): Map[String, String] = exp match {
    case m: CreateMap
      if AbstractMapType(StringTypeWithCollation, StringTypeWithCollation)
        .acceptsType(m.dataType) =>
      val arrayMap = m.eval().asInstanceOf[ArrayBasedMapData]
      ArrayBasedMapData.toScalaMap(arrayMap).map { case (key, value) =>
        key.toString -> value.toString
      }
    case m: CreateMap =>
      throw QueryCompilationErrors.keyValueInMapNotStringError(m)
    case _ =>
      throw QueryCompilationErrors.nonMapFunctionNotAllowedError()
  }

  /**
   * A convenient function for schema validation in datasources supporting
   * `columnNameOfCorruptRecord` as an option.
   */
  def verifyColumnNameOfCorruptRecord(
      schema: StructType,
      columnNameOfCorruptRecord: String): Unit = {
    schema.getFieldIndex(columnNameOfCorruptRecord).foreach { corruptFieldIndex =>
      val f = schema(corruptFieldIndex)
      if (!f.dataType.isInstanceOf[StringType] || !f.nullable) {
        throw QueryCompilationErrors.invalidFieldTypeForCorruptRecordError(
          columnNameOfCorruptRecord, f.dataType)
      }
    }
  }

  def getDecimalParser(locale: Locale): String => java.math.BigDecimal = {
    if (locale == Locale.US) { // Special handling the default locale for backward compatibility
      (s: String) => new java.math.BigDecimal(s.replaceAll(",", ""))
    } else {
      val decimalFormat = new DecimalFormat("", new DecimalFormatSymbols(locale))
      decimalFormat.setParseBigDecimal(true)
      (s: String) => {
        val pos = new ParsePosition(0)
        val result = decimalFormat.parse(s, pos).asInstanceOf[java.math.BigDecimal]
        if (pos.getIndex() != s.length() || pos.getErrorIndex() != -1) {
          throw QueryExecutionErrors.cannotParseDecimalError()
        } else {
          result
        }
      }
    }
  }

  /**
   * Check if the schema is valid for Json
   * @param schema The schema to check.
   * @return
   *  `TypeCheckSuccess` if the schema is valid
   *  `DataTypeMismatch` with an error error if the schema is not valid
   */
  def checkJsonSchema(schema: DataType): TypeCheckResult = {
    val isInvalid = schema.existsRecursively {
      case MapType(keyType, _, _) if !keyType.isInstanceOf[StringType] => true
      case _ => false
    }
    if (isInvalid) {
      DataTypeMismatch(
        errorSubClass = "INVALID_JSON_MAP_KEY_TYPE",
        messageParameters = Map("schema" -> toSQLType(schema)))
    } else {
      TypeCheckSuccess
    }
  }

  /**
   * Check if the schema is valid for XML
   *
   * @param schema The schema to check.
   * @return
   * `TypeCheckSuccess` if the schema is valid
   * `DataTypeMismatch` with an error error if the schema is not valid
   */
  def checkXmlSchema(schema: DataType): TypeCheckResult = {
    val isInvalid = schema.existsRecursively {
      // XML field names must be StringType
      case MapType(keyType, _, _) if !keyType.isInstanceOf[StringType] => true
      case _ => false
    }
    if (isInvalid) {
      DataTypeMismatch(
        errorSubClass = "INVALID_XML_MAP_KEY_TYPE",
        messageParameters = Map("schema" -> toSQLType(schema)))
    } else {
      TypeCheckSuccess
    }
  }

  def assertValidAggregation(a: Aggregate): Unit = {
    def checkValidAggregateExpression(expr: Expression): Unit = expr match {
      case expr: AggregateExpression =>
        val aggFunction = expr.aggregateFunction
        aggFunction.children.foreach { child =>
          child.foreach {
            case expr: AggregateExpression =>
              expr.failAnalysis(
                errorClass = "NESTED_AGGREGATE_FUNCTION",
                messageParameters = Map.empty)
            case other => // OK
          }

          if (!child.deterministic) {
            child.failAnalysis(
              errorClass = "AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION",
              messageParameters = Map("sqlExpr" -> toSQLExpr(expr)))
          }
        }
      case _: Attribute if a.groupingExpressions.isEmpty =>
        a.failAnalysis(
          errorClass = "MISSING_GROUP_BY",
          messageParameters = Map.empty)
      case e: Attribute if !a.groupingExpressions.exists(_.semanticEquals(e)) =>
        throw QueryCompilationErrors.columnNotInGroupByClauseError(e)
      case s: ScalarSubquery
        if s.children.nonEmpty && !a.groupingExpressions.exists(_.semanticEquals(s)) =>
        s.failAnalysis(
          errorClass = "SCALAR_SUBQUERY_IS_IN_GROUP_BY_OR_AGGREGATE_FUNCTION",
          messageParameters = Map("sqlExpr" -> toSQLExpr(s)))
      case e if a.groupingExpressions.exists(_.semanticEquals(e)) => // OK
      // There should be no Window in Aggregate - this case will fail later check anyway.
      // Perform this check for special case of lateral column alias, when the window
      // expression is not eligible to propagate to upper plan because it is not valid,
      // containing non-group-by or non-aggregate-expressions.
      case WindowExpression(function, spec) =>
        function.children.foreach(checkValidAggregateExpression)
        checkValidAggregateExpression(spec)
      case e => e.children.foreach(checkValidAggregateExpression)
    }

    def checkValidGroupingExprs(expr: Expression): Unit = {
      if (expr.exists(_.isInstanceOf[AggregateExpression])) {
        expr.failAnalysis(
          errorClass = "GROUP_BY_AGGREGATE",
          messageParameters = Map("sqlExpr" -> expr.sql))
      }

      // Check if the data type of expr is orderable.
      if (expr.dataType.existsRecursively(_.isInstanceOf[VariantType])) {
        expr.failAnalysis(
          errorClass = "GROUP_EXPRESSION_TYPE_IS_NOT_ORDERABLE",
          messageParameters = Map(
            "sqlExpr" -> toSQLExpr(expr),
            "dataType" -> toSQLType(expr.dataType)))
      }

      if (!expr.deterministic) {
        // This is just a sanity check, our analysis rule PullOutNondeterministic should
        // already pull out those nondeterministic expressions and evaluate them in
        // a Project node.
        throw SparkException.internalError(
          msg = s"Non-deterministic expression '${toSQLExpr(expr)}' should not appear in " +
            "grouping expression.",
          context = expr.origin.getQueryContext,
          summary = expr.origin.context.summary)
      }
    }

    a.groupingExpressions.foreach(checkValidGroupingExprs)
    a.aggregateExpressions.foreach(checkValidAggregateExpression)
  }
}
