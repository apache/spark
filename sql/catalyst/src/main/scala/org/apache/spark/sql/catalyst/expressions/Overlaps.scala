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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.trees.QuaternaryLike
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, TimeType, TimestampNTZType, TimestampType}

/**
 * Implements the ANSI SQL OVERLAPS predicate for datetime periods.
 *
 * Syntax:
 *   (start1, end1) OVERLAPS (start2, end2)
 *
 * Semantics (per ISO/IEC 9075-2):
 * 1. Each period is normalized so start <= end (endpoints swapped if needed).
 * 2. A zero-length period represents a single point in time.
 * 3. Two periods overlap iff they share at least one common point:
 *    normalizedStart1 < normalizedEnd2 AND normalizedStart2 < normalizedEnd1
 *    (For zero-length periods, use <= for the point-containment check.)
 * 4. NULL endpoints follow standard three-valued logic.
 */
case class Overlaps(
    start1: Expression,
    end1: Expression,
    start2: Expression,
    end2: Expression)
  extends Expression with QuaternaryLike[Expression] {

  override def first: Expression = start1
  override def second: Expression = end1
  override def third: Expression = start2
  override def fourth: Expression = end2

  override val nodePatterns: Seq[TreePattern] = Seq(OVERLAPS)

  override def dataType: DataType = BooleanType
  override def nullable: Boolean = start1.nullable || end1.nullable ||
    start2.nullable || end2.nullable

  // All four endpoints must be the same datetime type.
  // The (start, interval) form is supported: the interval is added to the start
  // to produce the endpoint. This is handled transparently during analysis via
  // the analyzer rewriting Overlaps nodes with interval children.
  override def checkInputDataTypes(): TypeCheckResult = {
    val types = Seq(start1.dataType, end1.dataType, start2.dataType, end2.dataType)

    // Check all are the same datetime family (after interval resolution)
    val distinctTypes = types.map(canonicalType).distinct
    if (distinctTypes.length != 1) {
      TypeCheckResult.TypeCheckFailure(
        s"All endpoints in OVERLAPS must be the same datetime type, " +
          s"but got ${types.map(_.sql).mkString(", ")}")
    } else if (!isSupportedType(distinctTypes.head)) {
      TypeCheckResult.TypeCheckFailure(
        s"OVERLAPS requires datetime endpoints (TIME, DATE, TIMESTAMP, or TIMESTAMP_NTZ), " +
          s"but got ${distinctTypes.head.sql}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  private def canonicalType(dt: DataType): DataType = dt match {
    case _: TimeType => TimeType(6) // normalize precision for comparison
    case other => other
  }

  private def isSupportedType(dt: DataType): Boolean = dt match {
    case _: TimeType | DateType | TimestampType | TimestampNTZType => true
    case _ => false
  }

  override def foldable: Boolean = children.forall(_.foldable)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Fall back to interpreted evaluation
    val thisTerm = ctx.addReferenceObj("overlaps", this)
    val inputTerm = ctx.INPUT_ROW
    ev.copy(code =
      code"""
        Object ${ev.value}Obj = $thisTerm.eval($inputTerm);
        boolean ${ev.isNull} = ${ev.value}Obj == null;
        boolean ${ev.value} = ${ev.isNull} ? false : (Boolean) ${ev.value}Obj;
      """)
  }

  override def eval(input: InternalRow): Any = {
    val s1 = start1.eval(input)
    val e1 = end1.eval(input)
    val s2 = start2.eval(input)
    val e2 = end2.eval(input)

    // NULL handling: if any endpoint is null, result is null
    if (s1 == null || e1 == null || s2 == null || e2 == null) {
      return null
    }

    start1.dataType match {
      case DateType =>
        overlapCheck(s1.asInstanceOf[Int], e1.asInstanceOf[Int],
          s2.asInstanceOf[Int], e2.asInstanceOf[Int])
      case _: TimeType | TimestampType | TimestampNTZType =>
        overlapCheck(s1.asInstanceOf[Long], e1.asInstanceOf[Long],
          s2.asInstanceOf[Long], e2.asInstanceOf[Long])
      case _ => null
    }
  }

  private def overlapCheck(s1: Long, e1: Long, s2: Long, e2: Long): Boolean = {
    // Normalize: ensure start <= end
    val (ns1, ne1) = if (s1 <= e1) (s1, e1) else (e1, s1)
    val (ns2, ne2) = if (s2 <= e2) (s2, e2) else (e2, s2)

    val isPoint1 = ns1 == ne1
    val isPoint2 = ns2 == ne2

    if (isPoint1 && isPoint2) {
      ns1 == ns2
    } else if (isPoint1) {
      ns1 >= ns2 && ns1 < ne2
    } else if (isPoint2) {
      ns2 >= ns1 && ns2 < ne1
    } else {
      ns1 < ne2 && ns2 < ne1
    }
  }

  private def overlapCheck(s1: Int, e1: Int, s2: Int, e2: Int): Boolean = {
    val (ns1, ne1) = if (s1 <= e1) (s1, e1) else (e1, s1)
    val (ns2, ne2) = if (s2 <= e2) (s2, e2) else (e2, s2)

    val isPoint1 = ns1 == ne1
    val isPoint2 = ns2 == ne2

    if (isPoint1 && isPoint2) {
      ns1 == ns2
    } else if (isPoint1) {
      ns1 >= ns2 && ns1 < ne2
    } else if (isPoint2) {
      ns2 >= ns1 && ns2 < ne1
    } else {
      ns1 < ne2 && ns2 < ne1
    }
  }

  override def sql: String =
    s"(${start1.sql}, ${end1.sql}) OVERLAPS (${start2.sql}, ${end2.sql})"

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): Overlaps =
    copy(start1 = newFirst, end1 = newSecond, start2 = newThird, end2 = newFourth)
}
