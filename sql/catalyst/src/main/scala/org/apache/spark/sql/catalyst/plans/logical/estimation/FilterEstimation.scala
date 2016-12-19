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

  // We use a mutable colStats because we need to update the corresponding ColumnStat
  // for a column after we apply a predicate condition.
  private val mutableColStats: mutable.Map[String, ColumnStat] = mutable.Map.empty[String, ColumnStat]

  def apply(plan: Filter)
    : Option[Statistics] = {
    val stats: Statistics = plan.child.statistics
    if (stats.rowCount.isEmpty) return None

    copyFromColStats(stats.colStats)
    // estimate selectivity for this filter
    val percent: Double = calculateConditions(stats, plan.condition)
    val filteredSizeInBytes = RoundingToBigInt(BigDecimal(plan.statistics.sizeInBytes) * percent)
    val filteredRowCount = plan.statistics.rowCount.map(
      r => RoundingToBigInt(BigDecimal(r) * percent)
    )

    Some(stats.copy(sizeInBytes = filteredSizeInBytes, rowCount = filteredRowCount,
      colStats = copyToColStats))
  }

  // save local copy of colStats so that we may update it
  def copyFromColStats(colStats: Map[String, ColumnStat]): Unit = {
    for ( (k, v) <- colStats ) {
      mutableColStats += (k -> v)
    }
  }
  // return the updated colStats to the calling method.
  def copyToColStats: Map[String, ColumnStat] = {
    var stats: Map[String, ColumnStat] = Map.empty[String, ColumnStat]
    for ( (k, v) <- mutableColStats ) {
      stats = stats + (k -> v)
    }
    stats
  }

  def calculateConditions(
      planStat: Statistics,
      condition: Expression,
      update: Boolean = true)
    : Double = {
    // For conditions linked by And, we need to update stats after a condition estimation
    // so that the stats will be more accurate for subsequent estimation.
    condition match {
      case And(cond1, cond2) =>
        calculateConditions(planStat, cond1, update) * calculateConditions(planStat, cond2, update)
      case Or(cond1, cond2) =>
        math.min(1.0, calculateConditions(planStat, cond1, update = false) +
          calculateConditions(planStat, cond2, update = false))
      case Not(cond) => calculateSingleCondition(planStat, cond, isNot = true, update = false)
      case _ => calculateSingleCondition(planStat, condition, isNot = false, update)
    }
  }

  def calculateSingleCondition(
      planStat: Statistics,
      condition: Expression,
      isNot: Boolean,
      update: Boolean)
    : Double = {
    var notSupported: Boolean = false
    val percent: Double = condition match {
      // Currently we only support binary predicates where one side is a column,
      // and the other is a literal.
      // Note that: all binary predicate computing methods assume the literal is at the right side,
      // so we will change the predicate order if not.
      case op@LessThan(ExtractAttrRef(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op@LessThan(l: Literal, ExtractAttrRef(ar)) =>
        evaluateBinary(GreaterThan(ar, l), planStat, ar, l, update)

      case op@LessThanOrEqual(ExtractAttrRef(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op@LessThanOrEqual(l: Literal, ExtractAttrRef(ar)) =>
        evaluateBinary(GreaterThanOrEqual(ar, l), planStat, ar, l, update)

      case op@GreaterThan(ExtractAttrRef(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op@GreaterThan(l: Literal, ExtractAttrRef(ar)) =>
        evaluateBinary(LessThan(ar, l), planStat, ar, l, update)

      case op@GreaterThanOrEqual(ExtractAttrRef(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op@GreaterThanOrEqual(l: Literal, ExtractAttrRef(ar)) =>
        evaluateBinary(LessThanOrEqual(ar, l), planStat, ar, l, update)

      // EqualTo does not care about the order
      case op@EqualTo(ExtractAttrRef(ar), l: Literal) =>
        evaluateBinary(op, planStat, ar, l, update)
      case op@EqualTo(l: Literal, ExtractAttrRef(ar)) =>
        evaluateBinary(op, planStat, ar, l, update)

      case In(ExtractAttrRef(ar), expList) if !expList.exists(!_.isInstanceOf[Literal]) =>
        // Expression [In (value, seq[Literal])] will be replaced with optimized version
        // [InSet (value, HashSet[Literal])] in Optimizer, but only for list.size > 10.
        // Here we convert In into InSet anyway, because they share the same processing logic.
        val hSet = expList.map(e => e.eval())
        evaluateInSet(planStat, ar, HashSet() ++ hSet, update)
      case InSet(ExtractAttrRef(ar), set) =>
        evaluateInSet(planStat, ar, set, update)

      case Like(_, _) | Contains(_, _) | StartsWith(_, _) | EndsWith(_, _) =>
        evaluateLike(condition, planStat, update)

      // TODO: it's difficult to estimate IsNull after outer joins
      // case IsNull(ExtractAttrRef(ar)) =>
      // evaluateIsNull(planStat, ar, update)
      case IsNotNull(ExtractAttrRef(ar)) =>
        evaluateIsNotNull(planStat, ar, update)

      // case op @ EqualNullSafe(ExtractAttrRef(ar), l: Literal) =>
      // case op @ EqualNullSafe(l: Literal, ExtractAttrRef(ar)) =>

      case _ =>
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

  def evaluateIsNotNull(
      planStat: Statistics,
      attrRef: AttributeReference,
      update: Boolean)
    : Double = {
    if (!planStat.colStats.contains(attrRef.name)) {
      logInfo("[CBO] No statistics for " + attrRef)
      return 1.0
    }
    val aColStat = planStat.colStats(attrRef.name)
    val percent: BigDecimal = planStat.rowCount match {
      case Some(r) =>
        if (r == 0) 0.asInstanceOf[BigDecimal]
        else BigDecimal(aColStat.nullCount) / BigDecimal(r)
      case None => 0.asInstanceOf[BigDecimal]
    }
    if (update) {
      val newStats = aColStat.copy(nullCount = 0)
      mutableColStats += (attrRef.name -> newStats)
    }
    1.0 - percent.toDouble
  }

  // This method evaluates binary comparison operators such as =, <, <=, >, >=
  def evaluateBinary(
      op: BinaryComparison,
      planStat: Statistics,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean)
    : Double = {
    if (!planStat.colStats.contains(attrRef.name)) {
      logInfo("[CBO] No statistics for " + attrRef)
      return 1.0
    }
    op match {
      case EqualTo(l, r) => evaluateEqualTo(op, planStat, attrRef, literal, update)
      case _ =>
        attrRef.dataType match {
          case StringType => evaluateBinaryForString(op, planStat, attrRef, literal, update)
          case dataType: DataType if (dataType.isInstanceOf[NumericType]
            || dataType.isInstanceOf[DateType]
            || dataType.isInstanceOf[TimestampType]) =>
            evaluateBinaryForNumeric(op, planStat, attrRef, literal, update)
        }
    }
  }

  // This method evaluates the equality predicate for all data types.
  def evaluateEqualTo(
      op: BinaryComparison,
      planStat: Statistics,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean)
    : Double = {

    var percent: Double = 1.0
    val aColStat = planStat.colStats(attrRef.name)
    val ndv = aColStat.distinctCount
    val datumString = attrRef.dataType match {
      case d: DateType if literal.dataType.isInstanceOf[StringType] =>
        val date = DateTimeUtils.stringToDate(literal.value.asInstanceOf[UTF8String])
        if (date.isEmpty) {
          logInfo("[CBO] Date literal is wrong, No statistics for " + attrRef)
          return 1.0
        }
        date.get.toString
      case t: TimestampType if literal.dataType.isInstanceOf[StringType] =>
        val timestamp = DateTimeUtils.stringToTimestamp(literal.value.asInstanceOf[UTF8String])
        if (timestamp.isEmpty) {
          logInfo("[CBO] Timestamp literal is wrong, No statistics for " + attrRef)
          return 1.0
        }
        timestamp.get.toString
      case _ => literal.value.toString
    }

    val minString: String = attrRef.dataType match {
      case dataType: DataType if (dataType.isInstanceOf[NumericType]
        || dataType.isInstanceOf[DateType]
        || dataType.isInstanceOf[TimestampType]) =>
        aColStat.min match {
          case Some(v) => v.toString
          case None => ""
        }
      case _ => ""
    }
    val maxString: String = attrRef.dataType match {
      case dataType: DataType if (dataType.isInstanceOf[NumericType]
        || dataType.isInstanceOf[DateType]
        || dataType.isInstanceOf[TimestampType]) =>
        aColStat.max match {
          case Some(v) => v.toString
          case None => ""
        }
      case _ => ""
    }

    // decide if the value is in [min, max] of the column.
    // We currently don't store min/max for binary/string type.
    // Hence, we assume it is in boundary for binary/string type.
    val inBoundary: Boolean = attrRef.dataType match {
      case dataType: DataType if (dataType.isInstanceOf[NumericType]
        || dataType.isInstanceOf[DateType]
        || dataType.isInstanceOf[TimestampType]) =>
        aColStat.min match {
          case Some(v) =>
            datumString.toDouble >= minString.toDouble && datumString.toDouble <= maxString.toDouble
          case None => true
        }
      case _ => true
    }

    if (inBoundary) {
      percent = 1.0 / ndv.toDouble

      if (update) {
        // We update ColumnStat structure after apply this equality predicate.
        // Set distinctCount to 1.  Set nullCount to 0.
        val oneBigInt: BigInt = 1
        val zeroBigInt: BigInt = 0
        val newStats = attrRef.dataType match {
          case dataType: DataType if (dataType.isInstanceOf[NumericType]
            || dataType.isInstanceOf[DateType]
            || dataType.isInstanceOf[TimestampType]) =>
            val newValue = Some(datumString.toDouble)
            aColStat.copy(distinctCount = oneBigInt, min = newValue,
              max = newValue, nullCount = zeroBigInt)
          case _ => aColStat.copy(distinctCount = oneBigInt, nullCount = zeroBigInt)
        }
        mutableColStats += (attrRef.name -> newStats)
      }

    } else {
      percent = 0.0
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
      logInfo("[CBO] No statistics for " + attrRef)
      return 1.0
    }
    // TODO: will fill in this method later.
    1.0
  }

  def evaluateBinaryForNumeric(
      op: BinaryComparison,
      planStat: Statistics,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean)
    : Double = {
    // TODO: will fill in this method later.
    1.0
  }

  def evaluateBinaryForString(
      op: BinaryComparison,
      planStat: Statistics,
      attrRef: AttributeReference,
      literal: Literal,
      update: Boolean)
    : Double = {
    // TODO: will fill in this method later.
    1.0
  }

  def evaluateLike(
      cond: Expression,
      planStat: Statistics,
      update: Boolean)
    : Double = {
    // TODO: will fill in this method later.
    1.0
  }

}
