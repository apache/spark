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
package org.apache.spark.sql.hbase.types

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.language.implicitConversions
import scala.math.PartialOrdering
import scala.reflect.runtime.universe.typeTag

class Range[T](val start: Option[T], // None for open ends
               val startInclusive: Boolean,
               val end: Option[T], // None for open ends
               val endInclusive: Boolean,
               val dt: NativeType) extends Serializable {
  require(dt != null && !(start.isDefined && end.isDefined &&
    ((dt.ordering.eq(start.get, end.get) &&
      (!startInclusive || !endInclusive)) ||
      dt.ordering.gt(start.get.asInstanceOf[dt.JvmType], end.get.asInstanceOf[dt.JvmType]))),
    "Inappropriate range parameters")
  @transient lazy val isPoint: Boolean = start.isDefined && end.isDefined &&
    startInclusive && endInclusive && start.get.equals(end.get)
}

/**
 * HBase partition range
 * @param start start position
 * @param startInclusive whether the start position is inclusive or not
 * @param end end position
 * @param endInclusive whether the end position is inclusive or not
 * @param id the partition id
 * @param dt the data type
 * @param pred the associated predicate
 * @tparam T template of the type
 */
class PartitionRange[T](start: Option[T], startInclusive: Boolean,
                        end: Option[T], endInclusive: Boolean,
                        val id: Int, dt: NativeType, var pred: Expression)
  extends Range[T](start, startInclusive, end, endInclusive, dt)

private[hbase] class RangeType[T] extends PartialOrderingDataType {
  override def defaultSize: Int = 4096
  private[sql] type JvmType = Range[T]
  // TODO: can not use ScalaReflectionLock now for its accessibility
  // @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[JvmType] }
  @transient private[sql] lazy val tag = synchronized(typeTag[JvmType])
  
  private[spark] override def asNullable: RangeType[T] = this

  def toPartiallyOrderingDataType(s: Any, dt: NativeType): Any = s match {
    case i: Int => new Range[Int](Some(i), true, Some(i), true, IntegerType)
    case l: Long => new Range[Long](Some(l), true, Some(l), true, LongType)
    case d: Double => new Range[Double](Some(d), true, Some(d), true, DoubleType)
    case f: Float => new Range[Float](Some(f), true, Some(f), true, FloatType)
    case b: Byte => new Range[Byte](Some(b), true, Some(b), true, ByteType)
    case s: Short => new Range[Short](Some(s), true, Some(s), true, ShortType)
    case s: String => new Range[String](Some(s), true, Some(s), true, StringType)
    case b: Boolean => new Range[Boolean](Some(b), true, Some(b), true, BooleanType)
    case t: Timestamp => new Range[Timestamp](Some(t), true, Some(t), true, TimestampType)
    case _ => s
  }

  val partialOrdering = new PartialOrdering[JvmType] {
    // Right now we just support comparisons between a range and a point
    // In the future when more generic range comparisons, these two methods
    // must be functional as expected
    // return -2 if a < b; -1 if a <= b; 0 if a = b; 1 if a >= b; 2 if a > b
    def tryCompare(a: JvmType, b: JvmType): Option[Int] = {
      val aRange = a.asInstanceOf[Range[T]]
      val aStartInclusive = aRange.startInclusive
      val aStart = aRange.start.getOrElse(null).asInstanceOf[aRange.dt.JvmType]
      val aEnd = aRange.end.getOrElse(null).asInstanceOf[aRange.dt.JvmType]
      val aEndInclusive = aRange.endInclusive
      val bRange = b.asInstanceOf[Range[T]]
      val bStart = bRange.start.getOrElse(null).asInstanceOf[aRange.dt.JvmType]
      val bEnd = bRange.end.getOrElse(null).asInstanceOf[aRange.dt.JvmType]
      val bStartInclusive = bRange.startInclusive
      val bEndInclusive = bRange.endInclusive

      // return 1 iff aStart > bEnd
      // return 1 iff aStart = bEnd, aStartInclusive & bEndInclusive are not true at same position
      if ((aStart != null && bEnd != null)
        && (aRange.dt.ordering.gt(aStart, bEnd)
        || (aRange.dt.ordering.equiv(aStart, bEnd) && !(aStartInclusive && bEndInclusive)))) {
        Some(2)
      } // Vice versa
      else if ((bStart != null && aEnd != null)
        && (aRange.dt.ordering.gt(bStart, aEnd)
        || (aRange.dt.ordering.equiv(bStart, aEnd) && !(bStartInclusive && aEndInclusive)))) {
        Some(-2)
      } else if (aStart != null && aEnd != null && bStart != null && bEnd != null &&
        aRange.dt.ordering.equiv(bStart, aEnd)
        && aRange.dt.ordering.equiv(aStart, aEnd)
        && aRange.dt.ordering.equiv(bStart, bEnd)
        && (aStartInclusive && aEndInclusive && bStartInclusive && bEndInclusive)) {
        Some(0)
      } else if (aEnd != null && bStart != null && aRange.dt.ordering.equiv(aEnd, bStart)
        && aEndInclusive && bStartInclusive) {
        Some(-1)
      } else if (aStart != null && bEnd != null && aRange.dt.ordering.equiv(aStart, bEnd)
        && aStartInclusive && bEndInclusive) {
        Some(1)
      } else {
        None
      }
    }

    def lteq(a: JvmType, b: JvmType): Boolean = {
      // [(aStart, aEnd)] and [(bStart, bEnd)]
      // [( and )] mean the possibilities of the inclusive and exclusive condition
      val aRange = a.asInstanceOf[Range[T]]
      val aStartInclusive = aRange.startInclusive
      val aEnd = aRange.end.getOrElse(null)
      val aEndInclusive = aRange.endInclusive
      val bRange = b.asInstanceOf[Range[T]]
      val bStart = bRange.start.getOrElse(null)
      val bStartInclusive = bRange.startInclusive
      val bEndInclusive = bRange.endInclusive

      // Compare two ranges, return true iff the upper bound of the lower range is lteq to
      // the lower bound of the upper range. Because the exclusive boundary could be null, which
      // means the boundary could be infinity, we need to further check this conditions.
      val result =
        (aStartInclusive, aEndInclusive, bStartInclusive, bEndInclusive) match {
          // [(aStart, aEnd] compare to [bStart, bEnd)]
          case (_, true, true, _) =>
            if (aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.JvmType],
              bStart.asInstanceOf[aRange.dt.JvmType])) {
              true
            } else {
              false
            }
          // [(aStart, aEnd] compare to (bStart, bEnd)]
          case (_, true, false, _) =>
            if (bStart != null && aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.JvmType],
              bStart.asInstanceOf[aRange.dt.JvmType])) {
              true
            } else {
              false
            }
          // [(aStart, aEnd) compare to [bStart, bEnd)]
          case (_, false, true, _) =>
            if (aEnd != null && aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.JvmType],
              bStart.asInstanceOf[aRange.dt.JvmType])) {
              true
            } else {
              false
            }
          // [(aStart, aEnd) compare to (bStart, bEnd)]
          case (_, false, false, _) =>
            if (aEnd != null && bStart != null &&
              aRange.dt.ordering.lteq(aEnd.asInstanceOf[aRange.dt.JvmType],
                bStart.asInstanceOf[aRange.dt.JvmType])) {
              true
            } else {
              false
            }
        }

      result
    }
  }
}

object RangeType {

  object StringRangeType extends RangeType[String]

  object IntegerRangeType extends RangeType[Int]

  object LongRangeType extends RangeType[Long]

  object DoubleRangeType extends RangeType[Double]

  object FloatRangeType extends RangeType[Float]

  object ByteRangeType extends RangeType[Byte]

  object ShortRangeType extends RangeType[Short]

  object BooleanRangeType extends RangeType[Boolean]

  object DecimalRangeType extends RangeType[BigDecimal]

  object TimestampRangeType extends RangeType[Timestamp]

  val primitiveToPODataTypeMap: HashMap[NativeType, PartialOrderingDataType] =
    HashMap(
      IntegerType -> IntegerRangeType,
      LongType -> LongRangeType,
      DoubleType -> DoubleRangeType,
      FloatType -> FloatRangeType,
      ByteType -> ByteRangeType,
      ShortType -> ShortRangeType,
      BooleanType -> BooleanRangeType,
      TimestampType -> TimestampRangeType,
      StringType -> StringRangeType
    )
}
