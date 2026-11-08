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
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DecimalType, DoubleType, FileType, FloatType, FractionalType, MapType, NumericType, StringHelper, StringType, StructType, TimestampNTZType, TimestampType, UserDefinedType, VariantType}
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

      // Check if the data type of expr is orderable.
      if (expr.dataType.existsRecursively(_.isInstanceOf[VariantType])) {
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
   * Only an allowlist of total, deterministic expressions qualifies:
   *   - leaves: attribute references, literals and the collation of a Collate;
   *   - total accessors: GetStructField, GetArrayStructFields and GetMapValue never throw.
   *     Note that GetArrayItem/ElementAt are NOT included: they throw on invalid ordinals
   *     when ANSI mode is on;
   *   - the expressions marked [[NoThrow]], such as CreateArray and CreateNamedStruct, which
   *     only build a value from their children, as `ConstantFolding` assumes too;
   *   - logic/predicates: And, Or, Not, comparisons, IsNull, IsNotNull, IsNaN, NullIf,
   *     Coalesce, In and InSet are total boolean functions;
   *   - Size, which returns -1 or NULL for a NULL input rather than raising;
   *   - StartsWith, EndsWith and Contains, which are total for every collation, unlike Like
   *     and RLike, which raise on an invalid pattern;
   *   - Length and OctetLength, which only count the characters or bytes of a string or binary,
   *     and with which `LikeSimplification` guards a StartsWith and an EndsWith;
   *   - Collate, which only changes the collation in the type of a string (`COLLATE`);
   *   - Substring, which returns an empty string for a position out of range;
   *   - the fields of a date or a timestamp, such as Year and Hour (GetDateField and
   *     GetTimeField), which only read the date or time of a valid value;
   *   - in every mode: a cast that widens a numeric type to another one (`Cast.canUpCast`),
   *     such as the INT to BIGINT cast that type coercion adds to a comparison, or a cast of a
   *     DECIMAL(18, 0), whose values all fit, to BIGINT; a cast of a DECIMAL to a FLOAT or a
   *     DOUBLE, which rounds; a cast of a timestamp to a date; and a cast of a number other than
   *     a FLOAT or DOUBLE, a boolean or a string to a STRING of any collation, not to a CHAR or
   *     VARCHAR, which checks the length;
   *   - outside ANSI mode, the casts that return NULL, wrap or clamp for a value they can not
   *     convert: in LEGACY mode, the ones `Cast.isNoThrowLegacyCast` accepts, between numbers,
   *     booleans and strings, except to a CHAR or VARCHAR, and from a string to a date or a
   *     timestamp; in TRY mode (`try_cast`), every cast, which either catches the errors of the
   *     cast or, from a string to a fractional or datetime type, runs the LEGACY cast;
   *   - outside ANSI mode, numeric arithmetic, which returns NULL on a division by zero or a
   *     decimal overflow, and wraps an integral overflow in LEGACY mode and returns NULL for it
   *     in TRY mode: in LEGACY mode, Add, Subtract, Multiply, Divide, IntegralDivide, Remainder
   *     and Pmod, UnaryMinus and Abs; in TRY mode, which only catches the errors of Add,
   *     Subtract and Multiply, and in which Divide and Remainder behave as in LEGACY mode, those
   *     five. Arithmetic over intervals is left out, which over a DAY TO SECOND or YEAR TO MONTH
   *     interval can overflow in every mode;
   *   - in every mode, also in ANSI mode, Add, Subtract and Multiply over FLOAT and DOUBLE, which
   *     return an infinity or NaN rather than raising, and UnaryMinus and Abs over FLOAT, DOUBLE
   *     and DECIMAL, which only check for an overflow over the integral types.
   * In every mode, a cast of a value with a FLOAT or DOUBLE in it to a type with a STRING or a
   * VARIANT in it is left out, although such a cast to a STRING or a VARIANT can not fail: it
   * tells -0.0 from 0.0, which are equal, and `InferFiltersFromConstraints` and
   * `ConstantPropagation` replace an attribute with a value equal to it under the operators this
   * check accepts (`isTotalOperator`). For the same reason, so is every cast from or to a type
   * with a UDT in it, which renders its value with its own `stringifyValue`, or a Python UDT as
   * its `sqlType`. Outside TRY mode, and in TRY mode from a string, the casts from or to a decimal
   * of a negative scale (`spark.sql.legacy.allowNegativeScaleOfDecimal`) other than the widenings
   * between decimals and the casts to a FLOAT or DOUBLE are left out, as `Decimal.toLong` raises
   * for one, and in every mode so is the binary arithmetic over one, as `Decimal.%` raises for a
   * very negative one. UnaryMinus and Abs only negate one and are kept.
   * Anything else (other arithmetic, other casts, other functions, UDFs, nested IF/CASE
   * WHEN, ...) conservatively returns false. On top of the allowlist, the expression must be
   * deterministic and must not contain subqueries.
   *
   * Note: this deliberately does not rely on [[Expression.throwable]], which is opt-in
   * metadata that most expressions do not override. A throwing ScalaUDF with
   * non-throwing children, for example, reports non-throwable.
   */
  def canEvaluateUnconditionally(e: Expression): Boolean =
    e.deterministic && !e.containsPattern(PLAN_EXPRESSION) &&
      canEvaluateUnconditionallyInternal(e)

  // Only the numeric pairs are taken from `Cast.canUpCast`, which also covers e.g. DATE to
  // TIMESTAMP, which can overflow, and casts between complex types. Among the numeric ones, it
  // only takes a decimal to an integral type whose values all fit.
  private def isNumericUpCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (_: NumericType, _: NumericType) => Cast.canUpCast(from, to)
    case _ => false
  }

  // Whether a cast can not fail. A cast of a date to a timestamp is left out also in LEGACY mode,
  // as converting a date far from the epoch to microseconds overflows there.
  private def cannotFailCast(c: Cast): Boolean = (c.child.dataType, c.dataType) match {
    // Left out whether or not it can fail, as it may tell -0.0 from 0.0: see
    // `canEvaluateUnconditionally`.
    case (from, to) if tellsSignedZeros(from, to) => false
    // A UDT renders its value with its own `stringifyValue`, or a Python UDT as its `sqlType`,
    // which can tell apart values that are equal, and the checks here do not look inside it. The
    // target is checked too, defensively, although only a UDT or a NULL can be cast to one.
    case (from, to) if hasUserDefinedType(from) || hasUserDefinedType(to) => false
    // A numeric up-cast can not fail. It goes before the negative scales, as a widening between
    // decimals never calls `Decimal.toLong`, also from or to a negative scale; a change to a
    // negative scale only checks that `spark.sql.legacy.allowNegativeScaleOfDecimal` is on.
    case (from, to) if isNumericUpCast(from, to) => true
    // `Decimal.toDouble` and `Decimal.toFloat` round through a `BigDecimal`, also for a negative
    // scale, and do not check the range in ANSI mode.
    case (_: DecimalType, FloatType | DoubleType) => true
    // TRY mode catches the errors of the cast, but not from a string, which runs the LEGACY cast.
    case (from, to) if hasNegativeScale(from) || hasNegativeScale(to) =>
      c.evalMode == EvalMode.TRY && !from.isInstanceOf[StringType]
    case (TimestampType | TimestampNTZType, DateType) => true
    // A CHAR or VARCHAR target checks the length of the string. A string to a STRING of another
    // collation, as `CollationTypeCoercion` adds for an explicit `COLLATE`, keeps the value.
    case (_: NumericType | BooleanType | _: StringType, s: StringType)
        if StringHelper.isPlainString(s) => true
    case _ if c.evalMode == EvalMode.TRY => true
    // `Cast.isNoThrowLegacyCast` covers the numbers, booleans and strings. A string to a date or
    // a timestamp does not raise in LEGACY mode either: the parsers catch the errors and return
    // None, which becomes NULL.
    case (_: StringType, DateType | TimestampType | TimestampNTZType)
        if c.evalMode == EvalMode.LEGACY => true
    case (from, to) if c.evalMode == EvalMode.LEGACY => Cast.isNoThrowLegacyCast(from, to)
    case _ => false
  }

  // Whether the source type has a FLOAT or DOUBLE and the target type a STRING or VARIANT in it,
  // so that the cast may keep the sign of a zero, e.g. -0.0 as '-0.0'. This is conservative: it
  // does not match the fields of the types.
  private def tellsSignedZeros(from: DataType, to: DataType): Boolean =
    from.existsRecursively(t => t == FloatType || t == DoubleType) &&
      to.existsRecursively(t => t.isInstanceOf[StringType] || t == VariantType)

  // `existsRecursively` does not look inside a UDT, so neither does this.
  private def hasUserDefinedType(dt: DataType): Boolean =
    dt.existsRecursively(_.isInstanceOf[UserDefinedType[_]])

  private def hasNegativeScale(dt: DataType): Boolean = dt.existsRecursively {
    case d: DecimalType => d.scale < 0
    case _ => false
  }

  // Whether numeric arithmetic can not fail, in its own eval mode.
  private def cannotFailArithmetic(b: BinaryArithmetic): Boolean = {
    b.children.forall(isNumericOfNonNegativeScale) && (b match {
      // Over FLOAT and DOUBLE, they do not check for an overflow, also in ANSI mode. Divide,
      // Remainder and Pmod raise on a zero divisor in ANSI mode, also over them.
      case _: Add | _: Subtract | _: Multiply if b.children.forall(isFloatOrDouble) => true
      case _ if b.evalMode == EvalMode.ANSI => false
      case _: Add | _: Subtract | _: Multiply | _: Divide | _: Remainder => true
      // `DivModLike` and Pmod override `eval` without the catch of TRY mode, and IntegralDivide
      // and Pmod check a zero divisor there as in ANSI mode.
      case _: IntegralDivide | _: Pmod => b.evalMode == EvalMode.LEGACY
      case _ => false
    })
  }

  private def isNumericOfNonNegativeScale(e: Expression): Boolean =
    e.dataType.isInstanceOf[NumericType] && !hasNegativeScale(e.dataType)

  // Not DECIMAL: Add, Subtract and Multiply over it check for an overflow in ANSI mode.
  private def isFloatOrDouble(e: Expression): Boolean = e.dataType match {
    case FloatType | DoubleType => true
    case _ => false
  }

  // Whether UnaryMinus or Abs can not fail: over numbers, they check for an overflow only over the
  // integral types, and only when `failOnError` is set. Over a DAY TO SECOND or YEAR TO MONTH
  // interval, they can overflow in every mode, and intervals are left out.
  private def cannotFailUnaryArithmetic(child: Expression, failOnError: Boolean): Boolean =
    child.dataType match {
      case _: FractionalType => true
      case _: NumericType => !failOnError
      case _ => false
    }

  private def canEvaluateUnconditionallyInternal(e: Expression): Boolean = e match {
    // The collation of a Collate evaluates to its name; it only sets the type.
    case _: AttributeReference | _: Literal | _: ResolvedCollation => true
    case _ if isTotalOperator(e) => e.children.forall(canEvaluateUnconditionallyInternal)
    case _ => false
  }

  /**
   * Returns true if `e` itself cannot raise an error, whatever the values of its children of
   * their current types. See [[canEvaluateUnconditionally]] for the allowlist.
   * `ConstantPropagation` replaces a child with a value equal to it under such an expression, so
   * it must also give equal results for equal children, e.g. a cast of a DOUBLE to a STRING is
   * left out, as it tells -0.0 from 0.0, and so is a cast of a UDT. Strings of a non-binary
   * collation are guarded by the callers instead.
   */
  def isTotalOperator(e: Expression): Boolean = e match {
    case _: GetStructField | _: GetArrayStructFields | _: GetMapValue | _: NoThrow | _: Collate =>
      true
    case _: And | _: Or | _: Not | _: BinaryComparison | _: IsNull | _: IsNotNull |
         _: IsNaN | _: NullIf | _: Coalesce | _: In | _: InSet | _: Size | _: StartsWith |
         _: EndsWith | _: Contains | _: Length | _: OctetLength | _: Substring | _: GetDateField |
         _: GetTimeField => true
    case c: Cast => cannotFailCast(c)
    case b: BinaryArithmetic => cannotFailArithmetic(b)
    case u: UnaryMinus => cannotFailUnaryArithmetic(u.child, u.failOnError)
    case a: Abs => cannotFailUnaryArithmetic(a.child, a.failOnError)
    case _ => false
  }
}
