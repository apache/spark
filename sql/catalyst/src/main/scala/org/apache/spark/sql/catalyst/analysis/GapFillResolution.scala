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

import java.time.Instant

import scala.collection.mutable

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, Cast, EmptyRow, Expression, ExprId, GapFillGenerator, Literal, OuterReference}
import org.apache.spark.sql.catalyst.optimizer.{ComputeCurrentTime, ReplaceExpressions}
import org.apache.spark.sql.catalyst.plans.logical.{GapFillMethod, Generate, LogicalPlan, Project, UnresolvedGapFill}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateTimeUtils, MetadataColumnHelper, TypeUtils}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_DAY
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._

/**
 * GAP FILL validation and plan building for the [[ResolveGapFill]] rule. It is kept apart so
 * that the single-pass analyzer can use it later. Checks that need the whole analyzed query,
 * such as a correlated or streaming input or a metadata column used after GAP FILL, are in
 * CheckAnalysis.
 */
object GapFillResolution extends SQLConfHelper {

  // The time from 0001-01-01 to 9999-12-31.
  private val MAX_BIN_WIDTH_MICROS = 3652058L * MICROS_PER_DAY

  /**
   * Checks a GAP FILL whose child and clause expressions are resolved, and builds a [[Project]]
   * over a [[Generate]] with a [[GapFillGenerator]]. Only the partition columns pass through the
   * Generate. Every other input column becomes a new nullable column with the table name of the
   * input column, and the Project puts the columns back in input order.
   *
   * BIN WIDTH, ALIGN TO, the range bounds, MAX GAP and each CONSTANT are evaluated here, and the
   * generator holds their values. Current time functions in these values are evaluated during
   * analysis, so they can differ from the same functions elsewhere in the query.
   *
   * When several errors apply, it raises the first one in this order. Column errors, each kind
   * for every column in the order PARTITION BY, ORDER BY, FILL. The time column type. The
   * PARTITION BY and FILL column rules. BIN WIDTH, ALIGN TO, range_start, range_end, MAX GAP and
   * each CONSTANT. The range. LINEAR. The unsupported cases.
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

    // Every current time function of the clause sees the same time.
    val now = Instant.now()
    val binWidth = intervalValue("BIN WIDTH", gapFill.binWidthExpr, MAX_BIN_WIDTH_MICROS, now)
    if (timeType == DateType && binWidth % MICROS_PER_DAY != 0) {
      throw QueryCompilationErrors.gapFillInvalidDateBinWidthError(
        binWidth, gapFill.binWidthExpr.dataType)
    }
    val origin = gapFill.originExpr.map(timeValue("ALIGN TO", _, timeType, now))
    val rangeStart = gapFill.rangeStartExpr.map(timeValue("range_start", _, timeType, now))
    val rangeEnd = gapFill.rangeEndExpr.map(timeValue("range_end", _, timeType, now))
    val maxGap = gapFill.maxGapExpr.map(intervalValue("MAX GAP", _, Long.MaxValue, now))
    val fills = fillColumns.zip(gapFill.fillColumns.map(_.method)).map {
      case (column, GapFillMethod.Constant(value)) =>
        column -> GapFillMethod.Constant(constantValue(value, column, now))
      case other => other
    }

    for (start <- rangeStart; end <- rangeEnd) {
      if (TypeUtils.getInterpretedOrdering(timeType).gt(start, end)) {
        throw QueryCompilationErrors.gapFillInvalidRangeError(start, end, timeType)
      }
    }

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
    val newColumns = (timeColumn +: valueColumns).map(_.newInstance().withNullability(true))
    val binStart = gapFill.binStart.map { c =>
      AttributeReference(c.alias.getOrElse("bin_start"), timeType)()
    }
    val isGenerated = gapFill.isGenerated.map { c =>
      AttributeReference(c.alias.getOrElse("is_generated"), BooleanType, nullable = false)()
    }
    val generator = GapFillGenerator(
      partitionColumns = partitionColumns,
      timeColumn = timeColumn,
      valueColumns = valueColumns,
      fillMethods = valueColumns.map(c => fillMethods.getOrElse(c.exprId, GapFillMethod.Null)),
      binWidthMicros = binWidth,
      alignTo = origin.map(toLong).getOrElse(defaultOrigin(timeType)),
      rangeStart = rangeStart.map(toLong),
      rangeEnd = rangeEnd.map(toLong),
      maxGapMicros = maxGap,
      outputBinStart = binStart.isDefined,
      outputIsGenerated = isGenerated.isDefined,
      timeZoneId = if (timeType == TimestampType) Some(conf.sessionLocalTimeZone) else None)
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
   * Returns the clause columns as input columns. It raises every column of one error kind before
   * the next kind, as the fixed-point analyzer reports an unresolved column only at the end.
   */
  private def topLevelInputColumns(
      columns: Seq[(String, Expression)],
      input: AttributeSet): Seq[Attribute] = {
    columns.foreach {
      case (clause, column: Attribute) if !input.contains(column) =>
        throw QueryCompilationErrors.gapFillColumnsNotInInputError(clause, column)
      case (clause, OuterReference(column)) =>
        throw QueryCompilationErrors.gapFillColumnsNotInInputError(clause, column.toAttribute)
      case _ =>
    }
    columns.map {
      case (_, column: Attribute) => column
      case (_, Alias(nested, _)) =>
        throw QueryCompilationErrors.gapFillRequiresTopLevelColumnError(nested)
      case (_, other) =>
        throw QueryCompilationErrors.gapFillRequiresTopLevelColumnError(other)
    }
  }

  private def constantValue(value: Expression, column: Attribute, now: Instant): Literal = {
    val replaced = ComputeCurrentTime.applyForExpression(ReplaceExpressions.replace(value), now)
    if (!replaced.foldable) {
      throw QueryCompilationErrors.gapFillNonFoldableInputError("CONSTANT", value)
    }
    // For a CHAR or VARCHAR column the value is cast to STRING, also with first-class CHAR and
    // VARCHAR types, and then padded and checked below.
    val targetType = CharVarcharUtils.replaceCharVarcharWithStringAlways(column.dataType)
    // Strings that differ only by collation are cast as is, like the arguments of a function.
    val cast = if (DataType.equalsIgnoreCompatibleCollation(value.dataType, targetType)) {
      Some(Cast(replaced, targetType))
    } else {
      val typeCoercion = if (conf.ansiEnabled) AnsiTypeCoercion else TypeCoercion
      typeCoercion.implicitCast(replaced, targetType)
    }
    cast.map { c =>
      // A CHAR or VARCHAR column pads and checks the value as an INSERT does.
      val checked = if (CharVarcharUtils.shouldApplyWriteSideLengthCheck(conf)) {
        CharVarcharUtils.stringLengthCheck(c, column)
      } else {
        c
      }
      Literal(ResolveTimeZone.resolveTimeZones(checked).eval(EmptyRow), column.dataType)
    }.getOrElse {
      throw QueryCompilationErrors.gapFillConstantTypeMismatchError(
        value.dataType, column.name, column.dataType)
    }
  }

  private def intervalValue(
      clause: String,
      expr: Expression,
      maxMicros: Long,
      now: Instant): Long = {
    val micros = constant(clause, expr, _.isInstanceOf[DayTimeIntervalType],
      QueryCompilationErrors.gapFillInvalidIntervalTypeError(clause, expr), now).asInstanceOf[Long]
    if (micros <= 0 || micros > maxMicros) {
      throw QueryCompilationErrors.gapFillValueOutOfRangeError(clause, expr, micros, maxMicros)
    }
    micros
  }

  private def timeValue(clause: String, expr: Expression, timeType: DataType, now: Instant): Any =
    constant(clause, expr, _ == timeType,
      QueryCompilationErrors.gapFillArgumentTypeMismatchError(clause, expr.dataType, timeType), now)

  // A NULL literal has no type, so it gets the NULL error and not the type error. Some
  // expressions, such as a timestamp minus an interval, are foldable only after their replacement.
  private def constant(
      clause: String,
      expr: Expression,
      isValidType: DataType => Boolean,
      typeError: => Throwable,
      now: Instant): Any = {
    val replaced = ComputeCurrentTime.applyForExpression(ReplaceExpressions.replace(expr), now)
    if (!replaced.foldable) {
      throw QueryCompilationErrors.gapFillNonFoldableInputError(clause, expr)
    }
    if (expr.dataType != NullType && !isValidType(expr.dataType)) {
      throw typeError
    }
    val value = replaced.eval(EmptyRow)
    if (value == null) {
      throw QueryCompilationErrors.unexpectedNullError(clause, expr)
    }
    value
  }

  // A filter on partition columns is pushed below GAP FILL, so equal keys must have equal bytes.
  private def isSupportedPartitionKeyType(dataType: DataType): Boolean = dataType match {
    case FloatType | DoubleType | CalendarIntervalType | VariantType => false
    case _: MapType | _: GeometryType | _: GeographyType => false
    case s: StringType => s.supportsBinaryEquality
    case s: StructType => s.fields.forall(f => isSupportedPartitionKeyType(f.dataType))
    case a: ArrayType => isSupportedPartitionKeyType(a.elementType)
    case u: UserDefinedType[_] => isSupportedPartitionKeyType(u.sqlType)
    case _ => true
  }

  private def defaultOrigin(timeType: DataType): Long = timeType match {
    case TimestampType =>
      DateTimeUtils.daysToMicros(0, DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
    case _ => 0L
  }

  private def toLong(value: Any): Long = value match {
    case days: Int => days
    case micros: Long => micros
  }
}
