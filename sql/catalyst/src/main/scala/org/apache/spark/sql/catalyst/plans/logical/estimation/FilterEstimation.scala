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

package org.apache.spark.sql.catalyst.plans.logical.estimation

import scala.collection.immutable.{HashSet, Map}
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


object FilterEstimation extends Logging {

  /**
   * We use a mutable colStats because we need to update the corresponding ColumnStat
   * for a column after we apply a predicate condition.
   */
  private var mutableColStats: mutable.Map[String, ColumnStat] = mutable.Map.empty

  def estimate(plan: Filter): Option[Statistics] = {
    val stats: Statistics = plan.child.statistics
    if (stats.rowCount.isEmpty) return None

    /** save a mutable copy of colStats so that we can later change it recursively */
    mutableColStats = mutable.HashMap(stats.colStats.toSeq: _*)

    /** estimate selectivity for this filter */
    val percent: Double = calculateConditions(plan, plan.condition)

    /** copy mutableColStats contents to an immutable map */
    val newColStats = mutableColStats.toMap

    val filteredRowCountValue: BigInt =
      EstimationUtils.ceil(BigDecimal(stats.rowCount.get) * percent)
    val avgRowSize = BigDecimal(EstimationUtils.getRowSize(plan.output, newColStats))
    val filteredSizeInBytes: BigInt =
      EstimationUtils.ceil(BigDecimal(filteredRowCountValue) * avgRowSize)

    Some(stats.copy(sizeInBytes = filteredSizeInBytes, rowCount = Some(filteredRowCountValue),
      colStats = newColStats))
  }

  def calculateConditions(
      plan: Filter,
      condition: Expression,
      update: Boolean = true)
    : Double = {
    /**
     * For conditions linked by And, we need to update stats after a condition estimation
     * so that the stats will be more accurate for subsequent estimation.
     * For conditions linked by OR, we do not update stats after a condition estimation.
     */
    condition match {
      case And(cond1, cond2) =>
        calculateConditions(plan, cond1, update) * calculateConditions(plan, cond2, update)

      case Or(cond1, cond2) =>
        val p1 = calculateConditions(plan, cond1, update = false)
        val p2 = calculateConditions(plan, cond2, update = false)
        math.min(1.0, p1 + p2 - (p1 * p2))

      case Not(cond) => calculateSingleCondition(plan, cond, isNot = true, update = false)
      case _ => calculateSingleCondition(plan, condition, isNot = false, update)
    }
  }

  def calculateSingleCondition(
      plan: Filter,
      condition: Expression,
      isNot: Boolean,
      update: Boolean)
    : Double = {
    var notSupported: Boolean = false
    val planStat = plan.child.statistics
    val percent: Double = condition match {
      /**
       * Currently we only support binary predicates where one side is a column,
       * and the other is a literal.
       * Note that: all binary predicate computing methods assume the literal is at the right side,
       * so we will change the predicate order if not.
       */
      case op@LessThan(ExtractAttr(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op@LessThan(l: Literal, ExtractAttr(ar)) =>
        evaluateBinary(GreaterThan(ar, l), planStat, ar, l, update)

      case op@LessThanOrEqual(ExtractAttr(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op@LessThanOrEqual(l: Literal, ExtractAttr(ar)) =>
        evaluateBinary(GreaterThanOrEqual(ar, l), planStat, ar, l, update)

      case op@GreaterThan(ExtractAttr(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op@GreaterThan(l: Literal, ExtractAttr(ar)) =>
        evaluateBinary(LessThan(ar, l), planStat, ar, l, update)

      case op@GreaterThanOrEqual(ExtractAttr(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op@GreaterThanOrEqual(l: Literal, ExtractAttr(ar)) =>
        evaluateBinary(LessThanOrEqual(ar, l), planStat, ar, l, update)

      /** EqualTo does not care about the order */
      case op@EqualTo(ExtractAttr(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op@EqualTo(l: Literal, ExtractAttr(ar)) =>
        evaluateBinary(op, planStat, ar, l, update)

      case In(ExtractAttr(ar), expList) if !expList.exists(!_.isInstanceOf[Literal]) =>
        /**
         * Expression [In (value, seq[Literal])] will be replaced with optimized version
         * [InSet (value, HashSet[Literal])] in Optimizer, but only for list.size > 10.
         * Here we convert In into InSet anyway, because they share the same processing logic.
         */
        val hSet = expList.map(e => e.eval())
        evaluateInSet(planStat, ar, HashSet() ++ hSet, update)

      case InSet(ExtractAttr(ar), set) =>
        evaluateInSet(planStat, ar, set, update)

      /**
       * It's difficult to estimate IsNull after outer joins.  Hence,
       * we support IsNull and IsNotNull only when the child is a leaf node (table).
       */
      case IsNull(ExtractAttr(ar)) =>
        if (plan.child.isInstanceOf[LeafNode ]) {
          evaluateIsNull(planStat, ar, true, update)
        }
        else 1.0

      case IsNotNull(ExtractAttr(ar)) =>
        if (plan.child.isInstanceOf[LeafNode ]) {
          evaluateIsNull(planStat, ar, false, update)
        }
        else 1.0

      case _ =>
        /**
         * TODO: it's difficult to support string operators without advanced statistics.
         * Hence, these string operators Like(_, _) | Contains(_, _) | StartsWith(_, _)
         * | EndsWith(_, _) are not supported yet
         */
        logDebug("[CBO] Unsupported filter condition: " + condition)
        notSupported = true
        1.0
    }
    if (notSupported) {
      1.0
    } else if (isNot) {
      1.0 - percent
    } else {
      percent
    }
  }

  def evaluateIsNull(
      planStat: Statistics,
      attrRef: AttributeReference,
      isNull: Boolean,
      update: Boolean)
    : Double = {
    if (!planStat.colStats.contains(attrRef.name)) {
      logDebug("[CBO] No statistics for " + attrRef)
      return 1.0
    }
    val aColStat = planStat.colStats(attrRef.name)
    val rowCountValue = planStat.rowCount.get
    val nullPercent: BigDecimal =
      if (rowCountValue == 0) 0.0
      else BigDecimal(aColStat.nullCount)/BigDecimal(rowCountValue)

    if (update) {
      val newStats =
        if (isNull) aColStat.copy(distinctCount = 0, min = None, max = None)
        else aColStat.copy(nullCount = 0)

      mutableColStats += (attrRef.name -> newStats)
    }

    val percent =
      if (isNull) nullPercent.toDouble
      else {
        /** ISNOTNULL(column) */
        1.0 - nullPercent.toDouble
      }

    percent
  }

  /** This method evaluates binary comparison operators such as =, <, <=, >, >= */
  def evaluateBinary(
      op: BinaryComparison,
      planStat: Statistics,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean)
    : Double = {
    if (!planStat.colStats.contains(attrRef.name)) {
      logDebug("[CBO] No statistics for " + attrRef)
      return 1.0
    }

    /** Make sure that the Date/Timestamp literal is a valid one */
    attrRef.dataType match {
      case DateType =>
        val dateLiteral = DateTimeUtils.stringToDate(literal.value.asInstanceOf[UTF8String])
        if (dateLiteral.isEmpty) {
          logDebug("[CBO] Date literal is wrong, No statistics for " + attrRef)
          return 1.0
        }
      case TimestampType =>
        val tsLiteral = DateTimeUtils.stringToTimestamp(literal.value.asInstanceOf[UTF8String])
        if (tsLiteral.isEmpty) {
          logDebug("[CBO] Timestamp literal is wrong, No statistics for " + attrRef)
          return 1.0
        }
      case _ =>
    }

    op match {
      case EqualTo(l, r) => evaluateEqualTo(op, planStat, attrRef, literal, update)
      case _ =>
        attrRef.dataType match {
          case _: NumericType | DateType | TimestampType =>
            evaluateBinaryForNumeric(op, planStat, attrRef, literal, update)
          case StringType | BinaryType =>
            /**
             * TODO: It is difficult to support other binary comparisons for String/Binary
             * type without min/max and advanced statistics like histogram.
             */
            logDebug("[CBO] No statistics for String/Binary type " + attrRef)
            return 1.0
        }
    }
  }

  def numericLiteralToBigDecimal(
      literal: Literal,
      dataType: DataType)
    : BigDecimal = {
    dataType match {
      case _: IntegralType =>
        BigDecimal(literal.value.asInstanceOf[Long])
      case _: FractionalType =>
        BigDecimal(literal.value.asInstanceOf[Double])
      case DateType =>
        val dateLiteral = DateTimeUtils.stringToDate(literal.value.asInstanceOf[UTF8String])
        BigDecimal(dateLiteral.asInstanceOf[BigInt])
      case TimestampType =>
        val tsLiteral = DateTimeUtils.stringToTimestamp(literal.value.asInstanceOf[UTF8String])
        BigDecimal(tsLiteral.asInstanceOf[BigInt])
    }
  }

  /** This method evaluates the equality predicate for all data types. */
  def evaluateEqualTo(
      op: BinaryComparison,
      planStat: Statistics,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean)
    : Double = {

    val aColStat = planStat.colStats(attrRef.name)
    val ndv = aColStat.distinctCount

    /**
     * decide if the value is in [min, max] of the column.
     * We currently don't store min/max for binary/string type.
     * Hence, we assume it is in boundary for binary/string type.
     */
    val inBoundary: Boolean = attrRef.dataType match {
      case _: NumericType | DateType | TimestampType =>
        val statsRange =
          Range(aColStat.min, aColStat.max, attrRef.dataType).asInstanceOf[NumericRange]
        val lit = numericLiteralToBigDecimal(literal, attrRef.dataType)
        (lit >= statsRange.min) && (lit <= statsRange.max)

      case _ => true  /** for String/Binary type */
    }

    val percent: Double =
      if (inBoundary) {

        if (update) {
          /**
           * We update ColumnStat structure after apply this equality predicate.
           * Set distinctCount to 1.  Set nullCount to 0.
           */
          val newStats = attrRef.dataType match {
            case _: NumericType | DateType | TimestampType =>
              val newValue = Some(literal.value)
              aColStat.copy(distinctCount = 1, min = newValue,
                max = newValue, nullCount = 0)
            case _ => aColStat.copy(distinctCount = 1, nullCount = 0)
          }
          mutableColStats += (attrRef.name -> newStats)
        }

        1.0 / ndv.toDouble
      } else {
        0.0
      }

    percent
  }

  def evaluateInSet(
      planStat: Statistics,
      attrRef: AttributeReference,
      hSet: Set[Any],
      update: Boolean)
    : Double = {
    if (!planStat.colStats.contains(attrRef.name)) {
      logDebug("[CBO] No statistics for " + attrRef)
      return 1.0
    }

    val aColStat = planStat.colStats(attrRef.name)
    val ndv = aColStat.distinctCount
    val aType = attrRef.dataType

    // use [min, max] to filter the original hSet
    val validQuerySet = aType match {
      case _: NumericType | DateType | TimestampType =>
        val statsRange =
          Range(aColStat.min, aColStat.max, aType).asInstanceOf[NumericRange]
        hSet.map(e => numericLiteralToBigDecimal(e.asInstanceOf[Literal], aType)).
          filter(e => e >= statsRange.min && e <= statsRange.max)

      /** We assume the whole set since there is no min/max information for String/Binary type */
      case StringType | BinaryType => hSet
    }
    if (validQuerySet.isEmpty) {
      return 0.0
    }

    val newNdv = validQuerySet.size
    val(newMax, newMin) = aType match {
      case _: NumericType | DateType | TimestampType =>
        val tmpSet: Set[Double] = validQuerySet.map(e => e.toString.toDouble)
        (Some(tmpSet.max), Some(tmpSet.min))
      case _ =>
        (None, None)
    }

    if (update) {
      val newStats = attrRef.dataType match {
        case _: NumericType | DateType | TimestampType =>
          aColStat.copy(distinctCount = newNdv, min = newMin,
            max = newMax, nullCount = 0)
        case StringType | BinaryType =>
          aColStat.copy(distinctCount = newNdv, nullCount = 0)
      }
      mutableColStats += (attrRef.name -> newStats)
    }

    /**
     * return the filter selectivity.  Without advanced statistics such as histograms,
     * we have to assume uniform distribution.
     */
    math.min(1.0, validQuerySet.size / ndv.toDouble)
  }

  def evaluateBinaryForNumeric(
      op: BinaryComparison,
      planStat: Statistics,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean)
    : Double = {

    var percent = 1.0
    val aColStat = planStat.colStats(attrRef.name)
    val ndv = aColStat.distinctCount
    val statsRange =
      Range(aColStat.min, aColStat.max, attrRef.dataType).asInstanceOf[NumericRange]

    val literalValueBD = numericLiteralToBigDecimal(literal, attrRef.dataType)

    /** determine the overlapping degree between predicate range and column's range */
    val (noOverlap: Boolean, completeOverlap: Boolean) = op match {
      case LessThan(l, r) =>
        (literalValueBD <= statsRange.min, literalValueBD > statsRange.max)
      case LessThanOrEqual(l, r) =>
        (literalValueBD < statsRange.min, literalValueBD >= statsRange.max)
      case GreaterThan(l, r) =>
        (literalValueBD >= statsRange.max, literalValueBD < statsRange.min)
      case GreaterThanOrEqual(l, r) =>
        (literalValueBD > statsRange.max, literalValueBD <= statsRange.min)
    }

    if (noOverlap) {
      percent = 0.0
    } else if (completeOverlap) {
      percent = 1.0
    } else {
      /** this is partial overlap case */
      var newMax = aColStat.max
      var newMin = aColStat.min
      var newNdv = ndv
      val literalToDouble = literalValueBD.toDouble
      val maxToDouble = BigDecimal(statsRange.max).toDouble
      val minToDouble = BigDecimal(statsRange.min).toDouble

      /**
       * Without advanced statistics like histogram, we assume uniform data distribution.
       * We just prorate the adjusted range over the initial range to compute filter selectivity.
       */
      percent = op match {
        case LessThan(l, r) =>
          (literalToDouble - minToDouble) / (maxToDouble - minToDouble)
        case LessThanOrEqual(l, r) =>
          if (literalValueBD == BigDecimal(statsRange.min)) 1.0 / ndv.toDouble
          else (literalToDouble - minToDouble) / (maxToDouble - minToDouble)
        case GreaterThan(l, r) =>
          (maxToDouble - literalToDouble) / (maxToDouble - minToDouble)
        case GreaterThanOrEqual(l, r) =>
          if (literalValueBD == BigDecimal(statsRange.max)) 1.0 / ndv.toDouble
          else (maxToDouble - literalToDouble) / (maxToDouble - minToDouble)
      }

      if (update) {
        op match {
          case GreaterThan(l, r) => newMin = Some(literal.value)
          case GreaterThanOrEqual(l, r) => newMin = Some(literal.value)
          case LessThan(l, r) => newMax = Some(literal.value)
          case LessThanOrEqual(l, r) => newMax = Some(literal.value)
        }
        newNdv = math.max(math.round(ndv.toDouble * percent), 1)
        val newStats = aColStat.copy(distinctCount = newNdv, min = newMin,
          max = newMax, nullCount = 0)

        mutableColStats += (attrRef.name -> newStats)
      }
    }

    percent
  }

}
