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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{AliasHelper, Attribute, AttributeMap, AttributeReference, AttributeSet, Cast, EmptyRow, Expression, ExprId, GapFillGenerator, Literal, OuterReference}
import org.apache.spark.sql.catalyst.optimizer.{ComputeCurrentTime, ReplaceExpressions, SpecialDatetimeValues}
import org.apache.spark.sql.catalyst.plans.logical.{GapFillColumn, GapFillMethod, Generate, LogicalPlan, Project, UnresolvedGapFill}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateTimeUtils, MetadataColumnHelper, TypeUtils}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_DAY
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils

/**
 * GAP FILL validation and plan building for the [[ResolveGapFill]] rule. It is kept apart so
 * that the single-pass analyzer can use it later. Checks that need the whole analyzed query,
 * such as a correlated or streaming input or a metadata column used after GAP FILL, are in
 * CheckAnalysis.
 */
object GapFillResolution extends SQLConfHelper with AliasHelper {

  // The time from 0001-01-01 to 9999-12-31.
  private val MAX_BIN_WIDTH_MICROS = 3652058L * MICROS_PER_DAY

  /**
   * Resolves the expressions of a GAP FILL with `resolve`, for the ResolveReferences rule. A
   * PARTITION BY, ORDER BY or FILL column must name a column of the input. A column of an outer
   * query resolves too, so that [[resolve]] can name it in its error. A name that `resolve` turns
   * into a value that reads no column, such as a session variable or a function such as
   * current_user, is put back as it was. So it gets UNRESOLVED_COLUMN, as any other name that the
   * input does not have.
   *
   * `resolve` puts an alias on a name that is a whole BIN WIDTH, ALIGN TO, range bound or MAX GAP
   * value, such as a session variable or current_date. An alias is never foldable, so it is
   * removed here.
   */
  def resolveExpressions(
      gapFill: UnresolvedGapFill,
      resolve: Expression => Expression): UnresolvedGapFill = {
    val resolved = gapFill.mapExpressions(resolve)
    // This does not wait for the value to be resolved, because current_date is resolved only
    // once its time zone is set, and then its name is gone. A name that is not resolved yet is an
    // Attribute too, so it stays as it is.
    def columnOrName(name: Expression, column: Expression): Expression = {
      val readsColumn = column.exists {
        case _: Attribute | _: OuterReference => true
        case _ => false
      }
      if (readsColumn) column else name
    }
    resolved.copy(
      partitionColumns = gapFill.partitionColumns.zip(resolved.partitionColumns).map {
        case (name, column) => columnOrName(name, column)
      },
      timeColumn = columnOrName(gapFill.timeColumn, resolved.timeColumn),
      binWidthExpr = trimAliases(resolved.binWidthExpr),
      originExpr = resolved.originExpr.map(trimAliases),
      rangeStartExpr = resolved.rangeStartExpr.map(trimAliases),
      rangeEndExpr = resolved.rangeEndExpr.map(trimAliases),
      maxGapExpr = resolved.maxGapExpr.map(trimAliases),
      fillColumns = gapFill.fillColumns.zip(resolved.fillColumns).map { case (name, fill) =>
        fill.withNewChildren(columnOrName(name.column, fill.column) +: fill.children.tail)
          .asInstanceOf[GapFillColumn]
      })
  }

  /**
   * Checks a GAP FILL whose child and clause expressions are resolved, and builds a [[Project]]
   * over a [[Generate]] with a [[GapFillGenerator]]. Only the partition columns pass through the
   * Generate. Every other input column becomes a new nullable column with the name and the table
   * name of the input column, and the Project puts the columns back in input order.
   *
   * BIN WIDTH, ALIGN TO, the range bounds, MAX GAP and each CONSTANT are evaluated here, and the
   * generator holds them as literals. A value for which [[evaluatedByOptimizer]] holds, such as
   * current_date() or CAST('today' AS DATE), gets only its type checks here. The optimizer
   * evaluates and checks it with [[evalCurrentTimeValues]] after ComputeCurrentTime and
   * SpecialDatetimeValues, so that it sees the same time as the rest of the query, also when an
   * analyzed plan is used again.
   *
   * When several errors apply, it raises the first one in this order. Column errors, each kind
   * for every column in the order PARTITION BY, ORDER BY, FILL. The time column type. The
   * PARTITION BY and FILL column rules. BIN WIDTH, ALIGN TO, range_start, range_end, MAX GAP and
   * each CONSTANT. The range. LINEAR. The unsupported cases. The value errors of a value that the
   * optimizer evaluates come after every analysis error, as the optimizer raises them.
   */
  def resolve(gapFill: UnresolvedGapFill): LogicalPlan = {
    val child = gapFill.child
    val columns = topLevelInputColumns(
      gapFill.partitionColumns.map("PARTITION BY" -> _) ++
        Seq("ORDER BY" -> gapFill.timeColumn) ++
        gapFill.fillColumns.map("FILL" -> _.column),
      child.outputSet)
    val partitionColumns = columns.take(gapFill.partitionColumns.length)
    val timeColumn = columns(partitionColumns.length)
    val fillColumns = columns.drop(partitionColumns.length + 1)
    val timeType = timeColumn.dataType

    timeType match {
      case TimestampType | TimestampNTZType | DateType | _: AnyTimestampNanoType =>
      case _ =>
        throw QueryCompilationErrors.gapFillTimeTypeMismatchError(timeColumn.name, timeType)
    }

    partitionColumns.find(_.exprId == timeColumn.exprId).foreach { column =>
      throw QueryCompilationErrors.gapFillInvalidPartitionColumnError(column.name)
    }
    val partitionSet = AttributeSet(partitionColumns)
    fillColumns.find(c => c.exprId == timeColumn.exprId || partitionSet.contains(c)).foreach {
      column => throw QueryCompilationErrors.gapFillInvalidFillColumnError(column.name)
    }
    val seen = mutable.HashSet.empty[ExprId]
    fillColumns.foreach { column =>
      if (!seen.add(column.exprId)) {
        throw QueryCompilationErrors.gapFillDuplicateFillColumnError(column.name)
      }
    }

    val binWidth = intervalValue("BIN WIDTH", gapFill.binWidthExpr)(
      binWidthLiteral(_, gapFill.binWidthExpr, timeType))
    val origin = gapFill.originExpr.map(timeValue("ALIGN TO", _, timeType))
    val rangeStart = gapFill.rangeStartExpr.map(timeValue("range_start", _, timeType))
    val rangeEnd = gapFill.rangeEndExpr.map(timeValue("range_end", _, timeType))
    val maxGap = gapFill.maxGapExpr.map { expr =>
      intervalValue("MAX GAP", expr)(intervalLiteral("MAX GAP", _, expr, Long.MaxValue))
    }
    val fills = fillColumns.zip(gapFill.fillColumns.map(_.method)).map {
      case (column, GapFillMethod.Constant(value)) =>
        column -> GapFillMethod.Constant(constantValue(value, column))
      case other => other
    }

    checkRange(rangeStart, rangeEnd, timeType)

    fills.foreach {
      case (column, GapFillMethod.Linear) if !column.dataType.isInstanceOf[NumericType] =>
        throw QueryCompilationErrors.gapFillLinearTypeMismatchError(column.name, column.dataType)
      case _ =>
    }

    partitionColumns.find(c => !isSupportedPartitionKeyType(c.dataType)).foreach { column =>
      throw QueryCompilationErrors.gapFillPartitionKeyTypeNotSupportedError(
        column.name, column.dataType)
    }
    if (timeType.isInstanceOf[AnyTimestampNanoType]) {
      throw QueryCompilationErrors.gapFillTimestampNanosNotSupportedError(
        timeColumn.name, timeType)
    }

    val valueColumns = child.output.filterNot { column =>
      column.exprId == timeColumn.exprId || partitionSet.contains(column)
    }.distinct
    val fillMethods = fills.map { case (column, method) => column.exprId -> method }.toMap
    val binStart = gapFill.binStart.map { c =>
      AttributeReference(c.alias.getOrElse("bin_start"), timeType)()
    }
    val isGenerated = gapFill.isGenerated.map { c =>
      AttributeReference(c.alias.getOrElse("is_generated"), BooleanType, nullable = false)()
    }
    // The generator holds most clause values as literals, so the analyzed plan no longer shows a
    // session variable that a value reads. It is recorded as ResolveIdentifierClause records the
    // variables of an IDENTIFIER clause. So a temporary view stores it, to resolve it when its
    // text is analyzed again, and a persisted view rejects it.
    AnalysisContext.get.referredTempVariableNamesUnderIdentifier ++=
      gapFill.expressions.flatMap(IdentifierResolution.collectTemporaryVariablesInExpressionTree)
    val generator = GapFillGenerator(
      partitionColumns = partitionColumns,
      timeColumn = timeColumn,
      valueColumns = valueColumns,
      fillMethods = valueColumns.map(c => fillMethods.getOrElse(c.exprId, GapFillMethod.Null)),
      binWidth = binWidth,
      alignTo = origin.getOrElse(defaultOrigin(timeType)),
      rangeStart = rangeStart,
      rangeEnd = rangeEnd,
      maxGap = maxGap,
      outputBinStart = binStart.isDefined,
      outputIsGenerated = isGenerated.isDefined,
      timeZoneId = if (timeType == TimestampType) Some(conf.sessionLocalTimeZone) else None)
    // The clause can spell the time column in another letter case. Its new column is built from
    // the input column, as the value columns are, so that it keeps the input name. Each new
    // column takes its type from the generator, which gives a CONSTANT column the type of its
    // value.
    val inputTimeColumn = child.output.find(_.exprId == timeColumn.exprId).get
    val newColumns = (inputTimeColumn +: valueColumns).zip(generator.elementSchema).map {
      case (column, field) =>
        column.newInstance().withNullability(true).withDataType(field.dataType)
    }
    val generate = Generate(
      generator = generator,
      unrequiredChildIndex = child.output.zipWithIndex.collect {
        case (column, index) if !partitionSet.contains(column) => index
      },
      outer = false,
      qualifier = None,
      generatorOutput = newColumns ++ binStart ++ isGenerated,
      child = child)
    val newColumnFor = AttributeMap((timeColumn +: valueColumns).zip(newColumns))
    val project = Project(
      child.output.map(c => newColumnFor.getOrElse(c, c)) ++ binStart ++ isGenerated, generate)
    // The analyzer copies tags only into a new node without tags, so the tags of GAP FILL, such
    // as the plan id of a Spark Connect DataFrame, are copied here before the tag below.
    project.copyTagsFrom(gapFill)
    // A hidden column such as the right key of a USING join cannot be used above GAP FILL. A
    // metadata column still resolves, so that CheckAnalysis can name it in its error.
    project.setTagValue(
      Project.hiddenOutputTag, child.metadataOutput.filterNot(_.qualifiedAccessOnly))
    project
  }

  /**
   * Evaluates the clause values that [[resolve]] kept as expressions because the optimizer
   * evaluates them, with the value checks that [[resolve]] makes on the other values, in the same
   * order. It runs after ComputeCurrentTime and SpecialDatetimeValues, so such a value now has
   * the time of the whole query, and its errors show that time.
   */
  def evalCurrentTimeValues(generator: GapFillGenerator): GapFillGenerator = {
    val timeType = generator.timeColumn.dataType
    // A literal is evaluated already, by the analyzer, ComputeCurrentTime or
    // SpecialDatetimeValues.
    def eval(value: Expression)(toLiteral: Expression => Literal): Expression = value match {
      case literal: Literal => literal
      case _ => toLiteral(value)
    }
    val binWidth = eval(generator.binWidth)(binWidthLiteral(_, generator.binWidth, timeType))
    val alignTo = eval(generator.alignTo)(nonNullLiteral("ALIGN TO", _, generator.alignTo))
    val rangeStart = generator.rangeStart.map(e => eval(e)(nonNullLiteral("range_start", _, e)))
    val rangeEnd = generator.rangeEnd.map(e => eval(e)(nonNullLiteral("range_end", _, e)))
    val maxGap =
      generator.maxGap.map(e => eval(e)(intervalLiteral("MAX GAP", _, e, Long.MaxValue)))
    // A CONSTANT has the type of its column already, and its errors are raised as they are.
    val fillMethods = generator.fillMethods.map {
      case GapFillMethod.Constant(value) =>
        GapFillMethod.Constant(eval(value)(v => Literal(v.eval(EmptyRow), v.dataType)))
      case method => method
    }
    checkRange(rangeStart, rangeEnd, timeType)
    generator.copy(
      binWidth = binWidth,
      alignTo = alignTo,
      rangeStart = rangeStart,
      rangeEnd = rangeEnd,
      maxGap = maxGap,
      fillMethods = fillMethods)
  }

  /**
   * Returns the clause columns as input columns. It raises every column of one error kind before
   * the next kind, as the fixed-point analyzer reports an unresolved column only at the end.
   * A column of the outer query is not in the input, also when the clause names a nested field
   * of it, so it gets the error that names the outer column.
   */
  private def topLevelInputColumns(
      columns: Seq[(String, Expression)],
      input: AttributeSet): Seq[Attribute] = {
    columns.foreach {
      case (clause, column: Attribute) if !input.contains(column) =>
        throw QueryCompilationErrors.gapFillColumnsNotInInputError(clause, column)
      case (clause, column) =>
        column.collectFirst { case OuterReference(outer) => outer }.foreach { outer =>
          throw QueryCompilationErrors.gapFillColumnsNotInInputError(clause, outer.toAttribute)
        }
    }
    columns.map {
      case (_, column: Attribute) => column
      case (_, other) => throw QueryCompilationErrors.gapFillRequiresTopLevelColumnError(
        trimAliases(other))
    }
  }

  private def constantValue(value: Expression, column: Attribute): Expression = {
    val replaced = replacedClauseValue("CONSTANT", value)
    // For a CHAR or VARCHAR column the value is cast to STRING, also with first-class CHAR and
    // VARCHAR types, and then padded and checked below.
    val targetType = CharVarcharUtils.replaceCharVarcharWithStringAlways(column.dataType)
    // Strings that differ only by collation are cast as is, like the arguments of a function.
    // Other types go through the implicit cast, so it rejects a day-time interval of another type
    // instead of truncating it.
    val cast = if (SchemaUtils.replaceCollatedStringWithString(value.dataType) ==
        SchemaUtils.replaceCollatedStringWithString(targetType)) {
      Some(Cast(replaced, targetType))
    } else {
      val typeCoercion = if (conf.ansiEnabled) AnsiTypeCoercion else TypeCoercion
      typeCoercion.implicitCast(replaced, targetType)
    }
    cast.map { c =>
      // A CHAR or VARCHAR column pads and checks the value as an INSERT does. A table column keeps
      // its CHAR or VARCHAR type in its metadata. With first-class types, a CAST column keeps it
      // only in its data type.
      val checked = if (CharVarcharUtils.shouldApplyWriteSideLengthCheck(conf)) {
        CharVarcharUtils.stringLengthCheck(c,
          CharVarcharUtils.getRawType(column.metadata).getOrElse(column.dataType))
      } else {
        c
      }
      // The implicit cast does not look at the nullability inside a type, so the value type can
      // allow a NULL inside an ARRAY, MAP or STRUCT where the column type allows none. Then the
      // value gets the column type with NULL allowed at every level, and so does its new column.
      val dataType =
        if (DataType.equalsIgnoreNameAndCompatibleNullability(c.dataType, targetType)) {
          column.dataType
        } else {
          column.dataType.asNullable
        }
      // The cast is checked too, as CONSTANT 'today' on a DATE column reads the current date.
      if (evaluatedByOptimizer(checked)) {
        // The optimizer turns the value into a literal of the value's own type, so the value gets
        // the type of its new column here.
        ResolveTimeZone.resolveTimeZones(Cast(checked, dataType))
      } else {
        Literal(ResolveTimeZone.resolveTimeZones(checked).eval(EmptyRow), dataType)
      }
    }.getOrElse {
      throw QueryCompilationErrors.gapFillConstantTypeMismatchError(
        value.dataType, column.name, column.dataType)
    }
  }

  private def intervalValue(clause: String, expr: Expression)(
      toLiteral: Expression => Literal): Expression =
    clauseValue(clause, expr, _.isInstanceOf[DayTimeIntervalType],
      QueryCompilationErrors.gapFillInvalidIntervalTypeError(clause, expr))(toLiteral)

  private def timeValue(clause: String, expr: Expression, timeType: DataType): Expression =
    clauseValue(clause, expr, _ == timeType,
      QueryCompilationErrors.gapFillArgumentTypeMismatchError(clause, expr.dataType, timeType))(
      nonNullLiteral(clause, _, expr))

  /**
   * Returns a BIN WIDTH, ALIGN TO, range bound or MAX GAP value after its type check, as the
   * literal that `toLiteral` makes, or with its replacements applied when the optimizer
   * evaluates it.
   */
  private def clauseValue(
      clause: String,
      expr: Expression,
      isValidType: DataType => Boolean,
      typeError: => Throwable)(toLiteral: Expression => Literal): Expression = {
    val replaced = replacedClauseValue(clause, expr)
    // A NULL literal has no type, so it gets the NULL error and not the type error.
    if (expr.dataType != NullType && !isValidType(expr.dataType)) {
      throw typeError
    }
    if (evaluatedByOptimizer(replaced)) replaced else toLiteral(replaced)
  }

  /** Evaluates a clause value. `shown` is the value that its errors show. */
  private def nonNullLiteral(clause: String, value: Expression, shown: Expression): Literal = {
    val result = value.eval(EmptyRow)
    if (result == null) {
      throw QueryCompilationErrors.unexpectedNullError(clause, shown)
    }
    Literal(result, value.dataType)
  }

  private def intervalLiteral(
      clause: String,
      value: Expression,
      shown: Expression,
      maxMicros: Long): Literal = {
    val literal = nonNullLiteral(clause, value, shown)
    val micros = literal.value.asInstanceOf[Long]
    if (micros <= 0 || micros > maxMicros) {
      throw QueryCompilationErrors.gapFillValueOutOfRangeError(clause, shown, micros, maxMicros)
    }
    literal
  }

  private def binWidthLiteral(value: Expression, shown: Expression, timeType: DataType): Literal = {
    val literal = intervalLiteral("BIN WIDTH", value, shown, MAX_BIN_WIDTH_MICROS)
    val micros = literal.value.asInstanceOf[Long]
    if (timeType == DateType && micros % MICROS_PER_DAY != 0) {
      throw QueryCompilationErrors.gapFillInvalidDateBinWidthError(micros, shown.dataType)
    }
    literal
  }

  /** Checks the range once both of its bounds are literals. */
  private def checkRange(
      rangeStart: Option[Expression],
      rangeEnd: Option[Expression],
      timeType: DataType): Unit = (rangeStart, rangeEnd) match {
    case (Some(Literal(start, _)), Some(Literal(end, _)))
        if TypeUtils.getInterpretedOrdering(timeType).gt(start, end) =>
      throw QueryCompilationErrors.gapFillInvalidRangeError(start, end, timeType)
    case _ =>
  }

  /**
   * Checks that a clause value can be folded once its current time functions are set, and
   * returns it with its replacements applied. Some expressions, such as a timestamp minus an
   * interval, are foldable only after their replacement.
   */
  private def replacedClauseValue(clause: String, expr: Expression): Expression = {
    val replaced = ReplaceExpressions.replace(expr)
    if (!ComputeCurrentTime.applyForExpression(replaced).foldable) {
      throw QueryCompilationErrors.nonFoldableClauseInputError(clause, expr)
    }
    replaced
  }

  /**
   * Whether a clause value is evaluated only by the optimizer, after ComputeCurrentTime and
   * SpecialDatetimeValues, because one of them changes it. Then it gets the same values as the
   * rest of the query. ComputeCurrentTime changes a value that reads the current date or time,
   * such as current_date() or CAST(TIME '00:05:00' AS TIMESTAMP_NTZ). SpecialDatetimeValues
   * changes a cast of a special datetime string, such as CAST('today' AS DATE), which a cast
   * cannot evaluate by itself.
   */
  private def evaluatedByOptimizer(value: Expression): Boolean = {
    // A cast of a string to a DATE resolves without a time zone, but its special value needs one.
    val withTimeZone = ResolveTimeZone.resolveTimeZones(value)
    !ComputeCurrentTime.applyForExpression(withTimeZone).fastEquals(withTimeZone) ||
      !SpecialDatetimeValues.applyForExpression(withTimeZone).fastEquals(withTimeZone)
  }

  // A filter on partition columns is pushed below GAP FILL, so equal keys must have equal bytes.
  // A FILE has no equality at all, so GROUP BY and DISTRIBUTE BY reject it too.
  private def isSupportedPartitionKeyType(dataType: DataType): Boolean = dataType match {
    case FloatType | DoubleType | CalendarIntervalType | VariantType => false
    case _: MapType | _: GeometryType | _: GeographyType | _: FileType => false
    case s: StringType => s.supportsBinaryEquality
    case s: StructType => s.fields.forall(f => isSupportedPartitionKeyType(f.dataType))
    case a: ArrayType => isSupportedPartitionKeyType(a.elementType)
    case u: UserDefinedType[_] => isSupportedPartitionKeyType(u.sqlType)
    case _ => true
  }

  private def defaultOrigin(timeType: DataType): Literal = timeType match {
    case TimestampType => Literal(
      DateTimeUtils.daysToMicros(0, DateTimeUtils.getZoneId(conf.sessionLocalTimeZone)),
      TimestampType)
    case DateType => Literal(0, DateType)
    case _ => Literal(0L, timeType)
  }
}
