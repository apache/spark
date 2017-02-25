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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import java.sql.{Date, Timestamp}

import scala.collection.immutable.{HashSet, Map}
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

case class FilterEstimation(plan: Filter, catalystConf: CatalystConf) extends Logging {

  /**
   * We use a mutable colStats because we need to update the corresponding ColumnStat
   * for a column after we apply a predicate condition.  For example, column c has
   * [min, max] value as [0, 100].  In a range condition such as (c > 40 AND c <= 50),
   * we need to set the column's [min, max] value to [40, 100] after we evaluate the
   * first condition c > 40.  We need to set the column's [min, max] value to [40, 50]
   * after we evaluate the second condition c <= 50.
   */
  private var mutableColStats: mutable.Map[ExprId, ColumnStat] = mutable.Map.empty

  /**
   * Returns an option of Statistics for a Filter logical plan node.
   * For a given compound expression condition, this method computes filter selectivity
   * (or the percentage of rows meeting the filter condition), which
   * is used to compute row count, size in bytes, and the updated statistics after a given
   * predicated is applied.
   *
   * @return Option[Statistics] When there is no statistics collected, it returns None.
   */
  def estimate: Option[Statistics] = {
    // We first copy child node's statistics and then modify it based on filter selectivity.
    val stats: Statistics = plan.child.stats(catalystConf)
    if (stats.rowCount.isEmpty) return None

    // save a mutable copy of colStats so that we can later change it recursively
    mutableColStats = mutable.Map(stats.attributeStats.map(kv => (kv._1.exprId, kv._2)).toSeq: _*)

    // estimate selectivity of this filter predicate
    val filterSelectivity: Double = calculateFilterSelectivity(plan.condition) match {
      case Some(percent) => percent
      // for not-supported condition, set filter selectivity to a conservative estimate 100%
      case None => 1.0
    }

    // attributeStats has mapping Attribute-to-ColumnStat.
    // mutableColStats has mapping ExprId-to-ColumnStat.
    // We use an ExprId-to-Attribute map to facilitate the mapping Attribute-to-ColumnStat
    val expridToAttrMap: Map[ExprId, Attribute] =
      stats.attributeStats.map(kv => (kv._1.exprId, kv._1))
    // copy mutableColStats contents to an immutable AttributeMap.
    val mutableAttributeStats: mutable.Map[Attribute, ColumnStat] =
      mutableColStats.map(kv => expridToAttrMap(kv._1) -> kv._2)
    val newColStats = AttributeMap(mutableAttributeStats.toSeq)

    val filteredRowCount: BigInt =
      EstimationUtils.ceil(BigDecimal(stats.rowCount.get) * filterSelectivity)
    val filteredSizeInBytes =
      EstimationUtils.getOutputSize(plan.output, filteredRowCount, newColStats)

    Some(stats.copy(sizeInBytes = filteredSizeInBytes, rowCount = Some(filteredRowCount),
      attributeStats = newColStats))
  }

  /**
   * Returns a percentage of rows meeting a compound condition in Filter node.
   * A compound condition is decomposed into multiple single conditions linked with AND, OR, NOT.
   * For logical AND conditions, we need to update stats after a condition estimation
   * so that the stats will be more accurate for subsequent estimation.  This is needed for
   * range condition such as (c > 40 AND c <= 50)
   * For logical OR conditions, we do not update stats after a condition estimation.
   *
   * @param condition the compound logical expression
   * @param update a boolean flag to specify if we need to update ColumnStat of a column
   *               for subsequent conditions
   * @return a double value to show the percentage of rows meeting a given condition.
   *         It returns None if the condition is not supported.
   */
  def calculateFilterSelectivity(condition: Expression, update: Boolean = true): Option[Double] = {

    condition match {
      case And(cond1, cond2) =>
        (calculateFilterSelectivity(cond1, update), calculateFilterSelectivity(cond2, update))
        match {
          case (Some(p1), Some(p2)) => Some(p1 * p2)
          case (Some(p1), None) => Some(p1)
          case (None, Some(p2)) => Some(p2)
          case (None, None) => None
        }

      case Or(cond1, cond2) =>
        // For ease of debugging, we compute percent1 and percent2 in 2 statements.
        val percent1 = calculateFilterSelectivity(cond1, update = false)
        val percent2 = calculateFilterSelectivity(cond2, update = false)
        (percent1, percent2) match {
          case (Some(p1), Some(p2)) => Some(math.min(1.0, p1 + p2 - (p1 * p2)))
          case (Some(p1), None) => Some(1.0)
          case (None, Some(p2)) => Some(1.0)
          case (None, None) => None
        }

      case Not(cond) => calculateFilterSelectivity(cond, update = false) match {
        case Some(percent) => Some(1.0 - percent)
        // for not-supported condition, set filter selectivity to a conservative estimate 100%
        case None => None
      }

      case _ =>
        calculateSingleCondition(condition, update)
    }
  }

  /**
   * Returns a percentage of rows meeting a single condition in Filter node.
   * Currently we only support binary predicates where one side is a column,
   * and the other is a literal.
   *
   * @param condition a single logical expression
   * @param update a boolean flag to specify if we need to update ColumnStat of a column
   *               for subsequent conditions
   * @return Option[Double] value to show the percentage of rows meeting a given condition.
   *         It returns None if the condition is not supported.
   */
  def calculateSingleCondition(condition: Expression, update: Boolean): Option[Double] = {
    condition match {
      // For evaluateBinary method, we assume the literal on the right side of an operator.
      // So we will change the order if not.

      // EqualTo does not care about the order
      case op @ EqualTo(ar: AttributeReference, l: Literal) =>
        evaluateBinary(op, ar, l, update)
      case op @ EqualTo(l: Literal, ar: AttributeReference) =>
        evaluateBinary(op, ar, l, update)

      case op @ LessThan(ar: AttributeReference, l: Literal) =>
        evaluateBinary(op, ar, l, update)
      case op @ LessThan(l: Literal, ar: AttributeReference) =>
        evaluateBinary(GreaterThan(ar, l), ar, l, update)

      case op @ LessThanOrEqual(ar: AttributeReference, l: Literal) =>
        evaluateBinary(op, ar, l, update)
      case op @ LessThanOrEqual(l: Literal, ar: AttributeReference) =>
        evaluateBinary(GreaterThanOrEqual(ar, l), ar, l, update)

      case op @ GreaterThan(ar: AttributeReference, l: Literal) =>
        evaluateBinary(op, ar, l, update)
      case op @ GreaterThan(l: Literal, ar: AttributeReference) =>
        evaluateBinary(LessThan(ar, l), ar, l, update)

      case op @ GreaterThanOrEqual(ar: AttributeReference, l: Literal) =>
        evaluateBinary(op, ar, l, update)
      case op @ GreaterThanOrEqual(l: Literal, ar: AttributeReference) =>
        evaluateBinary(LessThanOrEqual(ar, l), ar, l, update)

      case In(ar: AttributeReference, expList)
        if expList.forall(e => e.isInstanceOf[Literal]) =>
        // Expression [In (value, seq[Literal])] will be replaced with optimized version
        // [InSet (value, HashSet[Literal])] in Optimizer, but only for list.size > 10.
        // Here we convert In into InSet anyway, because they share the same processing logic.
        val hSet = expList.map(e => e.eval())
        evaluateInSet(ar, HashSet() ++ hSet, update)

      case InSet(ar: AttributeReference, set) =>
        evaluateInSet(ar, set, update)

      case IsNull(ar: AttributeReference) =>
        evaluateIsNull(ar, isNull = true, update)

      case IsNotNull(ar: AttributeReference) =>
        evaluateIsNull(ar, isNull = false, update)

      case _ =>
        // TODO: it's difficult to support string operators without advanced statistics.
        // Hence, these string operators Like(_, _) | Contains(_, _) | StartsWith(_, _)
        // | EndsWith(_, _) are not supported yet
        logDebug("[CBO] Unsupported filter condition: " + condition)
        None
    }
  }

  /**
   * Returns a percentage of rows meeting "IS NULL" or "IS NOT NULL" condition.
   *
   * @param attrRef an AttributeReference (or a column)
   * @param isNull set to true for "IS NULL" condition.  set to false for "IS NOT NULL" condition
   * @param update a boolean flag to specify if we need to update ColumnStat of a given column
   *               for subsequent conditions
   * @return an optional double value to show the percentage of rows meeting a given condition
   *         It returns None if no statistics collected for a given column.
   */
  def evaluateIsNull(
      attrRef: AttributeReference,
      isNull: Boolean,
      update: Boolean)
    : Option[Double] = {
    if (!mutableColStats.contains(attrRef.exprId)) {
      logDebug("[CBO] No statistics for " + attrRef)
      return None
    }
    val aColStat = mutableColStats(attrRef.exprId)
    val rowCountValue = plan.child.stats(catalystConf).rowCount.get
    val nullPercent: BigDecimal =
      if (rowCountValue == 0) 0.0
      else BigDecimal(aColStat.nullCount) / BigDecimal(rowCountValue)

    if (update) {
      val newStats =
        if (isNull) aColStat.copy(distinctCount = 0, min = None, max = None)
        else aColStat.copy(nullCount = 0)

      mutableColStats += (attrRef.exprId -> newStats)
    }

    val percent =
      if (isNull) {
        nullPercent.toDouble
      }
      else {
        /** ISNOTNULL(column) */
        1.0 - nullPercent.toDouble
      }

    Some(percent)
  }

  /**
   * Returns a percentage of rows meeting a binary comparison expression.
   *
   * @param op a binary comparison operator uch as =, <, <=, >, >=
   * @param attrRef an AttributeReference (or a column)
   * @param literal a literal value (or constant)
   * @param update a boolean flag to specify if we need to update ColumnStat of a given column
   *               for subsequent conditions
   * @return an optional double value to show the percentage of rows meeting a given condition
    *         It returns None if no statistics exists for a given column or wrong value.
   */
  def evaluateBinary(
      op: BinaryComparison,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean)
    : Option[Double] = {
    if (!mutableColStats.contains(attrRef.exprId)) {
      logDebug("[CBO] No statistics for " + attrRef)
      return None
    }

    op match {
      case EqualTo(l, r) => evaluateEqualTo(attrRef, literal, update)
      case _ =>
        attrRef.dataType match {
          case _: NumericType | DateType | TimestampType =>
            evaluateBinaryForNumeric(op, attrRef, literal, update)
          case StringType | BinaryType =>
            // TODO: It is difficult to support other binary comparisons for String/Binary
            // type without min/max and advanced statistics like histogram.
            logDebug("[CBO] No range comparison statistics for String/Binary type " + attrRef)
            None
        }
    }
  }

  /**
   * For a SQL data type, its internal data type may be different from its external type.
   * For DateType, its internal type is Int, and its external data type is Java Date type.
   * The min/max values in ColumnStat are saved in their corresponding external type.
   *
   * @param attrDataType the column data type
   * @param litValue the literal value
   * @return a BigDecimal value
   */
  def convertBoundValue(attrDataType: DataType, litValue: Any): Option[Any] = {
    attrDataType match {
      case DateType =>
        Some(DateTimeUtils.toJavaDate(litValue.toString.toInt))
      case TimestampType =>
        Some(DateTimeUtils.toJavaTimestamp(litValue.toString.toLong))
      case StringType | BinaryType =>
        None
      case _ =>
        Some(litValue)
    }
  }

  /**
   * Returns a percentage of rows meeting an equality (=) expression.
   * This method evaluates the equality predicate for all data types.
   *
   * @param attrRef an AttributeReference (or a column)
   * @param literal a literal value (or constant)
   * @param update a boolean flag to specify if we need to update ColumnStat of a given column
   *               for subsequent conditions
   * @return an optional double value to show the percentage of rows meeting a given condition
   */
  def evaluateEqualTo(
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean)
    : Option[Double] = {

    val aColStat = mutableColStats(attrRef.exprId)
    val ndv = aColStat.distinctCount

    // decide if the value is in [min, max] of the column.
    // We currently don't store min/max for binary/string type.
    // Hence, we assume it is in boundary for binary/string type.
    val statsRange = Range(aColStat.min, aColStat.max, attrRef.dataType)
    val inBoundary: Boolean = Range.rangeContainsLiteral(statsRange, literal)

    if (inBoundary) {

      if (update) {
        // We update ColumnStat structure after apply this equality predicate.
        // Set distinctCount to 1.  Set nullCount to 0.
        // Need to save new min/max using the external type value of the literal
        val newValue = convertBoundValue(attrRef.dataType, literal.value)
        val newStats = aColStat.copy(distinctCount = 1, min = newValue,
          max = newValue, nullCount = 0)
        mutableColStats += (attrRef.exprId -> newStats)
      }

      Some(1.0 / ndv.toDouble)
    } else {
      Some(0.0)
    }

  }

  /**
   * Returns a percentage of rows meeting "IN" operator expression.
   * This method evaluates the equality predicate for all data types.
   *
   * @param attrRef an AttributeReference (or a column)
   * @param hSet a set of literal values
   * @param update a boolean flag to specify if we need to update ColumnStat of a given column
   *               for subsequent conditions
   * @return an optional double value to show the percentage of rows meeting a given condition
   *         It returns None if no statistics exists for a given column.
   */

  def evaluateInSet(
      attrRef: AttributeReference,
      hSet: Set[Any],
      update: Boolean)
    : Option[Double] = {
    if (!mutableColStats.contains(attrRef.exprId)) {
      logDebug("[CBO] No statistics for " + attrRef)
      return None
    }

    val aColStat = mutableColStats(attrRef.exprId)
    val ndv = aColStat.distinctCount
    val aType = attrRef.dataType
    var newNdv: Long = 0

    // use [min, max] to filter the original hSet
    aType match {
      case _: NumericType | DateType | TimestampType =>
        val statsRange =
          Range(aColStat.min, aColStat.max, aType).asInstanceOf[NumericRange]

        // To facilitate finding the min and max values in hSet, we map hSet values to BigDecimal.
        // Using hSetBigdec, we can find the min and max values quickly in the ordered hSetBigdec.
        val hSetBigdec = hSet.map(e => BigDecimal(e.toString))
        val validQuerySet = hSetBigdec.filter(e => e >= statsRange.min && e <= statsRange.max)
        // We use hSetBigdecToAnyMap to help us find the original hSet value.
        val hSetBigdecToAnyMap: Map[BigDecimal, Any] =
          hSet.map(e => BigDecimal(e.toString) -> e).toMap

        if (validQuerySet.isEmpty) {
          return Some(0.0)
        }

        // Need to save new min/max using the external type value of the literal
        val newMax = convertBoundValue(attrRef.dataType, hSetBigdecToAnyMap(validQuerySet.max))
        val newMin = convertBoundValue(attrRef.dataType, hSetBigdecToAnyMap(validQuerySet.min))

        // newNdv should not be greater than the old ndv.  For example, column has only 2 values
        // 1 and 6. The predicate column IN (1, 2, 3, 4, 5). validQuerySet.size is 5.
        newNdv = math.min(validQuerySet.size.toLong, ndv.longValue())
        if (update) {
          val newStats = aColStat.copy(distinctCount = newNdv, min = newMin,
                max = newMax, nullCount = 0)
          mutableColStats += (attrRef.exprId -> newStats)
        }

      // We assume the whole set since there is no min/max information for String/Binary type
      case StringType | BinaryType =>
        newNdv = math.min(hSet.size.toLong, ndv.longValue())
        if (update) {
          val newStats = aColStat.copy(distinctCount = newNdv, nullCount = 0)
          mutableColStats += (attrRef.exprId -> newStats)
        }
    }

    // return the filter selectivity.  Without advanced statistics such as histograms,
    // we have to assume uniform distribution.
    Some(math.min(1.0, newNdv.toDouble / ndv.toDouble))
  }

  /**
   * Returns a percentage of rows meeting a binary comparison expression.
   * This method evaluate expression for Numeric columns only.
   *
   * @param op a binary comparison operator uch as =, <, <=, >, >=
   * @param attrRef an AttributeReference (or a column)
   * @param literal a literal value (or constant)
   * @param update a boolean flag to specify if we need to update ColumnStat of a given column
   *               for subsequent conditions
   * @return an optional double value to show the percentage of rows meeting a given condition
   */
  def evaluateBinaryForNumeric(
      op: BinaryComparison,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean)
    : Option[Double] = {

    var percent = 1.0
    val aColStat = mutableColStats(attrRef.exprId)
    val ndv = aColStat.distinctCount
    val statsRange =
      Range(aColStat.min, aColStat.max, attrRef.dataType).asInstanceOf[NumericRange]

    // determine the overlapping degree between predicate range and column's range
    val literalValueBD = BigDecimal(literal.value.toString)
    val (noOverlap: Boolean, completeOverlap: Boolean) = op match {
      case _: LessThan =>
        (literalValueBD <= statsRange.min, literalValueBD > statsRange.max)
      case _: LessThanOrEqual =>
        (literalValueBD < statsRange.min, literalValueBD >= statsRange.max)
      case _: GreaterThan =>
        (literalValueBD >= statsRange.max, literalValueBD < statsRange.min)
      case _: GreaterThanOrEqual =>
        (literalValueBD > statsRange.max, literalValueBD <= statsRange.min)
    }

    if (noOverlap) {
      percent = 0.0
    } else if (completeOverlap) {
      percent = 1.0
    } else {
      // this is partial overlap case
      var newMax = aColStat.max
      var newMin = aColStat.min
      var newNdv = ndv
      val literalToDouble = literalValueBD.toDouble
      val maxToDouble = BigDecimal(statsRange.max).toDouble
      val minToDouble = BigDecimal(statsRange.min).toDouble

      // Without advanced statistics like histogram, we assume uniform data distribution.
      // We just prorate the adjusted range over the initial range to compute filter selectivity.
      // For ease of computation, we convert all relevant numeric values to Double.
      percent = op match {
        case _: LessThan =>
          (literalToDouble - minToDouble) / (maxToDouble - minToDouble)
        case _: LessThanOrEqual =>
          if (literalValueBD == BigDecimal(statsRange.min)) 1.0 / ndv.toDouble
          else (literalToDouble - minToDouble) / (maxToDouble - minToDouble)
        case _: GreaterThan =>
          (maxToDouble - literalToDouble) / (maxToDouble - minToDouble)
        case _: GreaterThanOrEqual =>
          if (literalValueBD == BigDecimal(statsRange.max)) 1.0 / ndv.toDouble
          else (maxToDouble - literalToDouble) / (maxToDouble - minToDouble)
      }

      // Need to save new min/max using the external type value of the literal
      val newValue = convertBoundValue(attrRef.dataType, literal.value)

      if (update) {
        op match {
          case _: GreaterThan => newMin = newValue
          case _: GreaterThanOrEqual => newMin = newValue
          case _: LessThan => newMax = newValue
          case _: LessThanOrEqual => newMax = newValue
        }

        newNdv = math.max(math.round(ndv.toDouble * percent), 1)
        val newStats = aColStat.copy(distinctCount = newNdv, min = newMin,
          max = newMax, nullCount = 0)

        mutableColStats += (attrRef.exprId -> newStats)
      }
    }

    Some(percent)
  }

}
