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

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.ReplaceExpressions
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, CharVarcharUtils}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.{AbstractMapType, StringTypeWithCollation}
import org.apache.spark.sql.types.{DataType, FileType, MapType, StringType, StructType, VariantType}
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
          // The DDL parser rejects FILE when the type is disabled, but `fromJson` does not, as
          // it also restores persisted schemas. Reject FILE in a JSON schema argument here.
          if (!SQLConf.get.isFileTypeEnabled &&
            dataType.existsRecursively(_.isInstanceOf[FileType])) {
            throw QueryCompilationErrors.unsupportedFileTypeError()
          }
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

  def convertToMapData(exp: Expression, functionName: String): Map[String, String] = exp match {
    case m: CreateMap
      if AbstractMapType(
        StringTypeWithCollation(supportsTrimCollation = true),
        StringTypeWithCollation(supportsTrimCollation = true))
        .acceptsType(m.dataType) =>
      val preparedMap = ReplaceExpressions.replace(m)
      val unsafeToEvaluate = preparedMap.references.nonEmpty ||
        !preparedMap.deterministic || preparedMap.exists(_.isInstanceOf[Unevaluable])
      val allowNonFoldable =
        SQLConf.get.getConf(SQLConf.LEGACY_ALLOW_NON_FOLDABLE_OPTIONS)
      if (unsafeToEvaluate || (!preparedMap.foldable && !allowNonFoldable)) {
        throw QueryCompilationErrors.nonFoldableArgumentError(
          functionName, "options", m.dataType)
      }
      val arrayMap = preparedMap.eval().asInstanceOf[ArrayBasedMapData]
      ArrayBasedMapData.toScalaMap(arrayMap).map { case (key, value) =>
        if (key == null) {
          throw QueryExecutionErrors.nullAsMapKeyNotAllowedError()
        }
        key.toString -> (if (value == null) "null" else value.toString)
      }
    case m: CreateMap =>
      throw QueryCompilationErrors.keyValueInMapNotStringError(m)
    case _ =>
      throw QueryCompilationErrors.nonMapFunctionNotAllowedError()
  }

  /**
   * Returns the value, possibly null, of a foldable expression, or None if the expression is not
   * foldable or fails to evaluate. `ConstantFolding` keeps a foldable expression that fails to
   * evaluate in a conditional branch, which no row may reach. An expression that uses the value
   * of such a child once, e.g. in generated code, should evaluate the child for each row instead,
   * so that the error is raised only when a row evaluates it, as in `eval`.
   */
  def tryEvalFoldable(exp: Expression): Option[Any] = if (exp.foldable) {
    try {
      Some(exp.eval())
    } catch {
      case NonFatal(_) => None
    }
  } else {
    None
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

  def assertValidAggregation(
      a: Aggregate,
      semanticEquality: (Expression, Expression) => Boolean =
        (groupingExpression, checkedExpression) =>
          groupingExpression.semanticEquals(checkedExpression)): Unit = {
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
      case e: Attribute if !a.groupingExpressions.exists(semanticEquality(_, e)) =>
        throw QueryCompilationErrors.columnNotInGroupByClauseError(e)
      case s: ScalarSubquery
        if s.children.nonEmpty && !a.groupingExpressions.exists(semanticEquality(_, s)) =>
        s.failAnalysis(
          errorClass = "SCALAR_SUBQUERY_IS_IN_GROUP_BY_OR_AGGREGATE_FUNCTION",
          messageParameters = Map("sqlExpr" -> toSQLExpr(s)))
      case e if a.groupingExpressions.exists(semanticEquality(_, e)) => // OK
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

      // VARIANT and FILE have no defined equality or ordering, so they cannot be grouping keys.
      // Other types that are not orderable, such as MAP, are handled elsewhere.
      if (expr.dataType.existsRecursively(
        t => t.isInstanceOf[VariantType] || t.isInstanceOf[FileType])) {
        expr.failAnalysis(
          errorClass = "GROUP_EXPRESSION_TYPE_IS_NOT_ORDERABLE",
          messageParameters = Map(
            "sqlExpr" -> toSQLExpr(expr),
            "dataType" -> toSQLType(expr.dataType)))
      }
    }

    a.groupingExpressions.foreach(checkValidGroupingExprs)
    a.aggregateExpressions.foreach(checkValidAggregateExpression)
  }

  /**
   * Returns true if `e` is safe to evaluate unconditionally, i.e. on rows where the
   * original plan would not have evaluated it: evaluating it must not raise an error and
   * must not change the query result. This is the check to use when relocating an
   * expression out of a short-circuited position, e.g. moving the base out of the taken
   * branch of an If/CaseWhen, or hoisting a streamed-side join conjunct above the probe
   * so it also runs for streamed rows that have no match.
   *
   * Only a tree of these qualifies:
   *   - at the leaves, attribute references, literals and the collation of a Collate, which
   *     evaluate to a value they hold or read. They are accepted here, and declare nothing;
   *   - above them, the expressions that declare themselves total with [[Expression.isTotal]].
   *     The declaration is on each expression, next to how it evaluates, and says why it holds:
   *     the expressions that are always total mix in [[NoThrow]], e.g. And, a comparison, IsNull,
   *     Coalesce, GetStructField, Size, StartsWith, Substring and Year, and [[Cast]]
   *     (`Cast.isTotalCast`), Add, Subtract, Multiply, Divide, IntegralDivide, Remainder, Pmod,
   *     UnaryMinus and Abs are total for some input types and eval modes only. `ExprUtilsSuite`
   *     checks the classes of both kinds against a list.
   * Some expressions that look alike are not total: GetArrayItem and ElementAt raise on an invalid
   * ordinal in ANSI mode, Like and RLike on an invalid pattern, and CreateMap on a NULL or a
   * duplicate key.
   * Being total is more than not raising: `InferFiltersFromConstraints` and `ConstantPropagation`
   * replace an attribute with a value equal to it under the operators this check accepts
   * (`isTotalOperator`), so e.g. a cast of a value with a FLOAT or DOUBLE in it to a type with a
   * STRING or a VARIANT in it is left out, although it can not fail: it tells -0.0 from 0.0,
   * which are equal.
   * Anything else (other functions, UDFs, nested IF/CASE WHEN, ...) conservatively returns false.
   * On top of that, the expression must be deterministic and must not contain subqueries.
   *
   * Note: this deliberately does not rely on [[Expression.throwable]], which is opt-in
   * metadata that most expressions do not override. A throwing ScalaUDF with
   * non-throwing children, for example, reports non-throwable.
   */
  def canEvaluateUnconditionally(e: Expression): Boolean =
    e.deterministic && !e.containsPattern(PLAN_EXPRESSION) &&
      canEvaluateUnconditionallyInternal(e)

  private def canEvaluateUnconditionallyInternal(e: Expression): Boolean = e match {
    // The collation of a Collate evaluates to its name; it only sets the type.
    case _: AttributeReference | _: Literal | _: ResolvedCollation => true
    case _ if isTotalOperator(e) => e.children.forall(canEvaluateUnconditionallyInternal)
    case _ => false
  }

  /**
   * Returns true if `e` itself cannot raise an error, whatever the values of its children of
   * their current types. Each expression declares it itself, see [[Expression.isTotal]]: one that
   * is always total mixes in [[NoThrow]], and one that is total only for some input types or in
   * some eval modes, such as [[Cast]] and the arithmetic expressions, overrides the method.
   * `ConstantPropagation` replaces a child with a value equal to it under such an expression, so
   * it must also give equal results for equal children, e.g. a cast of a DOUBLE to a STRING is
   * left out, as it tells -0.0 from 0.0, and so is a cast of a UDT. Strings of a non-binary
   * collation are guarded by the callers instead.
   */
  def isTotalOperator(e: Expression): Boolean = e.isTotal
}
