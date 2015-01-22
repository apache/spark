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
package org.apache.spark.sql.hbase

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hbase.catalyst.expressions.PartialPredicateOperations._
import org.apache.spark.sql.hbase.types.Range

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CriticalPointType extends Enumeration {
  type CriticalPointType = Value
  val upInclusive = Value("Up Inclusive: (...)[...)")
  val lowInclusive = Value("Low Inclusive: (...](...)")
  val bothInclusive = Value("Both Inclusive: (...)[](...)")
}

/**
 *
 * @param value value of this critical point
 * @param ctype type of this critical point
 * @param dt the runtime data type of this critical point
 * @tparam T the data type parameter of this critical point
 */
case class CriticalPoint[T](value: T, ctype: CriticalPointType.CriticalPointType, dt: NativeType) {
  override def hashCode() = value.hashCode()
  override def equals(other: Any): Boolean = other match {
    case cp: CriticalPoint[T] => value.equals(cp.value)
    case _ => false
  }

  override def toString = {
    s"CriticalPoint: value=$value, ctype=$ctype., dt=${dt.typeName}"
  }
}

/**
 * Range based on a Critical point
 *
 * @param start the start of the range in native type; None for an open start
 * @param startInclusive inclusive for the start
 * @param end the end of the range in native; None for an open end
 * @param endInclusive inclusive for the end
 * @param dimIndex the dimension index. For
 * @param dt the data type
 * @param pred the associated predicate
 * @tparam T the native data type of the critical point range
 *
 */
private[hbase] class CriticalPointRange[T](start: Option[T], startInclusive: Boolean,
                                           end: Option[T], endInclusive: Boolean, dimIndex: Int,
                                           dt: NativeType, var pred: Expression)
  extends Range[T](start, startInclusive, end, endInclusive, dt) {
  var nextDimCriticalPointRanges: Seq[CriticalPointRange[_]] = Nil

  /**
   * expand on nested critical point ranges of sub-dimensions
   * @param prefix the buffer to build prefix on all leading dimensions
   * @return a list of Multiple dimensional critical point ranges
   */
  private[hbase] def flatten(prefix: ArrayBuffer[(Any, NativeType)])
  : Seq[MDCriticalPointRange[_]] = {
    if (nextDimCriticalPointRanges.isEmpty) {
      // Leaf node
      Seq(new MDCriticalPointRange(prefix.toSeq, this, dt))
    } else {
      prefix += ((start.get, dt))
      require(isPoint, "Internal Logical Error: point range expected")
      nextDimCriticalPointRanges.map(_.flatten(prefix)).reduceLeft(_ ++ _)
    }
  }

  override def toString = {
    val result = new mutable.StringBuilder()
    if (startInclusive) result.append("[") else result.append("(")
    result.append(s"$start, $end")
    if (endInclusive) result.append("]") else result.append(")")
    result.append(s" ${dt.typeName} $pred")
    result.toString()
  }
}

/**
 * Multidimensional critical point range. It uses native data types of the dimensions for comparison
 *
 * @param prefix prefix values and their native types of the leading dimensions;
 *               Nil for the first dimension ranges
 * @param lastRange the range of the last dimension (not necessarily the last dimension of the
 *                  table but just in this invocation!)
 * @param dt the data type of the range of the last dimension
 * @tparam T the type parameter of the range of the last dimension
 */
private[hbase] case class MDCriticalPointRange[T](prefix: Seq[(Any, NativeType)],
                                                  lastRange: CriticalPointRange[T],
                                                  dt: NativeType) {
  /**
   * Compare this range's start/end with the partition's end/start
   * @param startOrEnd TRUE if compare this start with the partition's end;
   *                   FALSE if compare this end with the partition's start
   * @param part the HBase partition to compare with
   * @return -1 if this critical point range's start is smaller than the partition's end
   *         1  if this critical point range's start is bigger than the partition's end
   */
  private[hbase] def compareWithPartition(startOrEnd: Boolean, part: HBasePartition): Int = {
    val (comparePoint, comparePointInclusive, comparePPoint, comparePPointInclusive) =
      if (startOrEnd) {
        (lastRange.start, part.end) match {
          case (None, None) => (null, false, null, false)
          case (None, Some(_)) => (null, false, part.endNative, part.endInclusive)
          case (Some(start), None) => (start, lastRange.startInclusive, null, false)
          case (Some(start), Some(_)) => (start, lastRange.startInclusive,
            part.endNative, part.endInclusive)
        }
      } else {
        (lastRange.end, part.start) match {
          case (None, None) => (null, false, null, false)
          case (None, Some(_)) => (null, false, part.startNative, part.startInclusive)
          case (Some(end), None) => (end, lastRange.endInclusive, null, false)
          case (Some(end), Some(_)) => (end, lastRange.endInclusive,
            part.startNative, part.startInclusive)
        }
      }

    (prefix, comparePoint, comparePPoint) match {
      case (_, _, null) => if (startOrEnd) -1 else 1
      case (Nil, null, _) => if (startOrEnd) -1 else 1
      case _ =>
        val zippedPairs = prefix.zip(comparePPoint)
        var i = 0
        for (zippedPair <- zippedPairs
             if zippedPair._1._2.ordering.equiv(
               zippedPair._1._1.asInstanceOf[zippedPair._1._2.JvmType],
               zippedPair._2.asInstanceOf[zippedPair._1._2.JvmType])) {
          i = i + 1
        }
        if (i < zippedPairs.size) {
          val ((prefixPoint, dt), pPoint) = zippedPairs(i)
          if (dt.ordering.gt(prefixPoint.asInstanceOf[dt.JvmType],
            pPoint.asInstanceOf[dt.JvmType])) {
            1
          }
          else {
            -1
          }
        } else {
        (comparePoint, comparePPoint(i)) match {
          case (null, _) => if (startOrEnd) {
            -1
          } else {
            1
          }
          case (_, pend) =>
            if (dt.ordering.gt(comparePoint.asInstanceOf[dt.JvmType],
              pend.asInstanceOf[dt.JvmType])) {
              1
            }
            else if (dt.ordering.lt(comparePoint.asInstanceOf[dt.JvmType],
              pend.asInstanceOf[dt.JvmType])) {
              -1
            } else {
              if (comparePointInclusive && comparePPointInclusive) {
                0
              } else if ((comparePointInclusive && prefix.size + 1 < comparePPoint.size) ||
                         (comparePPointInclusive && prefix.size + 1 < comparePPoint.size)) {
                // if the inclusive side has smaller dimensionality, there is overlap
                0
              } else if (startOrEnd) {
                1
              }
              else {
                -1
              }
            }
          }
        }
    }
  }

  override def toString = {
    val result = new mutable.StringBuilder()
    for (item <- prefix) {
      result.append(s"(${item._1} ${item._2}}) ")
    }
    result.append(s"${lastRange.toString()} ${dt.typeName}}")
    result.toString()
  }
}

/**
 * find the critical points in the given expression: not really a transformer
 * Must be called before reference binding
 */
object RangeCriticalPoint {
  /**
   * collect all critical points from an expression on a specific dimension key
   * @param expression the expression from where the critical points will be identified
   * @param key the dimension key for which the critical points will be identified
   * @tparam T type parameter of the critical points
   * @return
   */
  private[hbase] def collect[T](expression: Expression, key: AttributeReference)
  : Seq[CriticalPoint[T]] = {
    if (key.references.subsetOf(expression.references)) {
      val pointSet = mutable.Set[CriticalPoint[T]]()
      val dt: NativeType = key.dataType.asInstanceOf[NativeType]
      def checkAndAdd(value: Any, ct: CriticalPointType.CriticalPointType): Unit = {
        val cp = CriticalPoint[T](value.asInstanceOf[T], ct, dt)
        if (!pointSet.add(cp)) {
          val oldCp = pointSet.find(_.value == value).get
          if (oldCp.ctype != ct && oldCp.ctype != CriticalPointType.bothInclusive) {
            pointSet.remove(cp)
            if (ct == CriticalPointType.bothInclusive) {
              pointSet.add(cp)
            } else {
              pointSet.add(CriticalPoint[T](value.asInstanceOf[T],
                CriticalPointType.bothInclusive, dt))
            }
          }
        }
      }
      expression transform {
        case a@EqualTo(AttributeReference(_, _, _, _), Literal(value, _)) =>
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.bothInclusive)
          a
        case a@EqualTo(Literal(value, _), AttributeReference(_, _, _, _)) =>
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.bothInclusive)
          a
        case a@LessThan(AttributeReference(_, _, _, _), Literal(value, _)) =>
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
          a
        case a@LessThan(Literal(value, _), AttributeReference(_, _, _, _)) =>
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
          a
        case a@LessThanOrEqual(AttributeReference(_, _, _, _), Literal(value, _)) =>
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
          a
        case a@LessThanOrEqual(Literal(value, _), AttributeReference(_, _, _, _)) =>
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
          a
        case a@GreaterThanOrEqual(AttributeReference(_, _, _, _), Literal(value, _)) =>
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
          a
        case a@GreaterThanOrEqual(Literal(value, _), AttributeReference(_, _, _, _)) =>
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
          a
        case a@GreaterThan(AttributeReference(_, _, _, _), Literal(value, _)) =>
          if (a.left.equals(key)) checkAndAdd(value, CriticalPointType.lowInclusive)
          a
        case a@GreaterThan(Literal(value, _), AttributeReference(_, _, _, _)) =>
          if (a.right.equals(key)) checkAndAdd(value, CriticalPointType.upInclusive)
          a
      }
      pointSet.toSeq.sortWith((a: CriticalPoint[T], b: CriticalPoint[T])
      => dt.ordering.lt(a.value.asInstanceOf[dt.JvmType], b.value.asInstanceOf[dt.JvmType]))
    } else Nil
  }


  /**
   * create partition ranges on a *sorted* list of critical points
   * @param cps a sorted list of critical points
   * @param dimIndex the dimension index for this set of critical points
   * @param dt the runtime data type of this set of critical points
   * @tparam T the type parameter of this set of critical points
   * @return a list of generated critical point ranges
   */
  private[hbase] def generateCriticalPointRange[T](cps: Seq[CriticalPoint[T]],
                                                   dimIndex: Int, dt: NativeType)
  : Seq[CriticalPointRange[T]] = {
    if (cps.isEmpty) Nil
    else {
      val discreteType = dt.isInstanceOf[IntegralType]
      val result = new ArrayBuffer[CriticalPointRange[T]](cps.size + 1)
      var prev: CriticalPoint[T] = null
      cps.foreach(cp => {
        if (prev == null) {
          cp.ctype match {
            case CriticalPointType.lowInclusive =>
              result += new CriticalPointRange[T](None, false, Some(cp.value), true,
                dimIndex, cp.dt, null)
            case CriticalPointType.upInclusive =>
              result += new CriticalPointRange[T](None, false, Some(cp.value), false,
                dimIndex, cp.dt, null)
            case CriticalPointType.bothInclusive =>
              result +=(new CriticalPointRange[T](None, false, Some(cp.value), false,
                dimIndex, cp.dt, null),
                new CriticalPointRange[T](Some(cp.value), true, Some(cp.value), true,
                  dimIndex, cp.dt, null))
          }
        } else {
          (prev.ctype, cp.ctype) match {
            case (CriticalPointType.lowInclusive, CriticalPointType.lowInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), true,
                dimIndex, cp.dt, null)
            case (CriticalPointType.lowInclusive, CriticalPointType.upInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), false,
                dimIndex, cp.dt, null)
            case (CriticalPointType.lowInclusive, CriticalPointType.bothInclusive) =>
              result +=(new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), false,
                dimIndex, cp.dt, null),
                new CriticalPointRange[T](Some(cp.value), true, Some(cp.value), true,
                  dimIndex, cp.dt, null))
            case (CriticalPointType.upInclusive, CriticalPointType.lowInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), true, Some(cp.value), true,
                dimIndex, cp.dt, null)
            case (CriticalPointType.upInclusive, CriticalPointType.upInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), true, Some(cp.value), false,
                dimIndex, cp.dt, null)
            case (CriticalPointType.upInclusive, CriticalPointType.bothInclusive) =>
              result +=(new CriticalPointRange[T](Some(prev.value), true, Some(cp.value), false,
                dimIndex, cp.dt, null),
                new CriticalPointRange[T](Some(cp.value), true, Some(cp.value), true,
                  dimIndex, cp.dt, null))
            case (CriticalPointType.bothInclusive, CriticalPointType.lowInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), true,
                dimIndex, cp.dt, null)
            case (CriticalPointType.bothInclusive, CriticalPointType.upInclusive) =>
              result += new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), false,
                dimIndex, cp.dt, null)
            case (CriticalPointType.bothInclusive, CriticalPointType.bothInclusive) =>
              result +=(new CriticalPointRange[T](Some(prev.value), false, Some(cp.value), false,
                dimIndex, cp.dt, null),
                new CriticalPointRange[T](Some(cp.value), true, Some(cp.value), true,
                  dimIndex, cp.dt, null))
          }
        }
        prev = cp
      })
      if (prev != null) {
        result += {
          prev.ctype match {
            case CriticalPointType.lowInclusive =>
              new CriticalPointRange[T](Some(prev.value), false,
                None, false, dimIndex, prev.dt, null)
            case CriticalPointType.upInclusive =>
              new CriticalPointRange[T](Some(prev.value), true,
                None, false, dimIndex, prev.dt, null)
            case CriticalPointType.bothInclusive =>
              new CriticalPointRange[T](Some(prev.value), false,
                None, false, dimIndex, prev.dt, null)
          }
        }
      }
      // remove any redundant ranges for integral type
      if (discreteType) {
        result.map ( r => {
            var gotNew = false
            val numeric = dt.asInstanceOf[IntegralType]
              .numeric.asInstanceOf[Integral[T]]

            val (start, startInclusive) = {
              if (r.start.isDefined && !r.startInclusive) {
                gotNew = true
                (Some(numeric.plus(r.start.get, numeric.one)),true)
              } else (r.start, r.startInclusive)
            }

            val (end, endInclusive) = {
              if (r.end.isDefined && !r.endInclusive) {
                gotNew = true
                (Some(numeric.minus(r.end.get, numeric.one)),true)
              } else (r.end, r.endInclusive)
            }

            if (gotNew) {
              if (start.isDefined
                && end.isDefined
                && (start.get == numeric.plus(end.get, numeric.one))) {
                null
              } else new CriticalPointRange[T](start, startInclusive, end, endInclusive,
                dimIndex, r.dt, null)
            } else r
          }
        ).filter(r => r != null)
      } else result
    }
  }

  /**
   * Step 1: generate critical point ranges for a particular dimension
   * @param relation the HBase relation
   * @param pred the predicate expression to work on
   * @return a list of critical point ranges
   */
  private[hbase] def generateCriticalPointRanges(relation: HBaseRelation, pred: Option[Expression])
  : Seq[CriticalPointRange[_]] = {
    if (!pred.isDefined) Nil
    else {
      val predExpr = pred.get
      val predRefs = predExpr.references.toSeq
      val row = new GenericMutableRow(predRefs.size)
      // Step 1
      generateCriticalPointRangesHelper(relation, predExpr, 0, row, predRefs)
    }
  }

  /**
   * The workhorse method to generate critical points
   * @param relation the hbase relation
   * @param predExpr the predicate to work on
   * @param dimIndex the dimension index
   * @param row a row for partial reduction
   * @param predRefs the references in the predicate expression
   * @return a list of critical point ranges
   */
  private[hbase] def generateCriticalPointRangesHelper(relation: HBaseRelation,
                                                       predExpr: Expression,
                                                       dimIndex: Int,
                                                       row: MutableRow,
                                                       predRefs: Seq[Attribute])
  : Seq[CriticalPointRange[_]] = {
    val keyDim = relation.partitionKeys(dimIndex)
    val boundPred = BindReferences.bindReference(predExpr, predRefs)
    val dt: NativeType = keyDim.dataType.asInstanceOf[NativeType]
    // Step 1.1
    val criticalPoints: Seq[CriticalPoint[dt.JvmType]]
    = collect(predExpr, relation.partitionKeys(dimIndex))
    if (criticalPoints.isEmpty) Nil
    else {
      val cpRanges: Seq[CriticalPointRange[dt.JvmType]]
      = generateCriticalPointRange[dt.JvmType](criticalPoints, dimIndex, dt)
      // Step 1.2
      val keyIndex = predRefs.indexWhere(_.exprId == relation.partitionKeys(dimIndex).exprId)
      val qualifiedCPRanges = cpRanges.filter(cpr => {
        row.update(keyIndex, cpr)
        val prRes = boundPred.partialReduce(row, predRefs)
        if (prRes._1 == null) cpr.pred = prRes._2
        prRes._1 == null || prRes._1.asInstanceOf[Boolean]
      })

      // Step 1.3
      if (dimIndex < relation.partitionKeys.size - 1) {
        // For point range, generate CPs for the next dim
        qualifiedCPRanges.foreach(cpr => {
          if (cpr.isPoint && cpr.pred != null) {
            cpr.nextDimCriticalPointRanges = generateCriticalPointRangesHelper(relation,
              cpr.pred, dimIndex + 1, row, predRefs)
          }
        })
      }
      // Update row(keyIndex) to null for future reuse
      row.update(keyIndex, null)

      qualifiedCPRanges
    }
  }

  // Step 3

  /**
   * Search for a tight, either upper or lower, equality bound
   * @param eq the equality point to start search with
   * @param limit the limit for the search, exclusive
   * @param src the source to search for a match
   * @param tgt the list to search on
   * @param threshold linear search threshold
   * @param comp the comparison function
   * @tparam S the source type
   * @tparam T the type of the target elements
   * @return the index of the target element
   */
  private def binarySearchEquality[S, T](eq: Int, limit: Int, src: S, tgt: Seq[T], threshold: Int,
                                         comp: (S, T) => Int): Int = {
    val incr = if (eq > limit) -1 else 1 // search direction
    var mid = limit
    var newLimit = limit
    var cmp = 0
    var prevEq = eq
    while (incr * (newLimit - prevEq) >= 0) {
      if (incr * (newLimit - prevEq) < threshold) {
        // linear search
        mid = prevEq + incr
        while (incr * (newLimit - mid) > 0 && cmp == 0) {
          prevEq = mid
          mid = mid + incr
          cmp = comp(src, tgt(mid))
        }
      } else {
        mid = (prevEq + newLimit) / 2
        cmp = comp(src, tgt(mid))
        if (cmp == 0) prevEq = mid
        else newLimit = mid
      }
    }
    prevEq
  }

  /**
   *
   * @param src the source to base search for
   * @param tgt the list to be searched on
   * @param startIndex the index of the target to start search on
   * @param upperBound TRUE for tight upper bound; FALSE for tight lower bound
   * @param comp a comparison function
   * @tparam S the type of the source
   * @tparam T the type of the target elements
   * @return the index of the result
   */
  private def binarySearchForTightBound[S, T](src: S, tgt: Seq[T],
                                              startIndex: Int,
                                              upperBound: Boolean,
                                              comp: (S, T) => Int,
                                              threshold:Int = 10): Int = {
    var left = startIndex
    var right = tgt.size - 1
    var prevLarger = -1
    var prevSmaller = -1
    var mid = -1
    var cmp: Int = 0
    while (right >= left) {
      if (right - left < threshold) {
        // linear search
        cmp = 0
        if (upperBound) {
          // tight upper bound
          var i = right + 1
          while (i > left && cmp <= 0) {
            i = i - 1
            cmp = comp(src, tgt(i))
          }
          prevLarger = if (i == left && cmp <= 0) i
                       else i + 1
        } else {
          // tight lower bound
          var i = left - 1
          while (i < right && cmp >= 0) {
            i = i + 1
            cmp = comp(src, tgt(i))
          }
          prevSmaller = if (i ==  right && cmp >= 0) i
                        else i - 1
        }
        right = left - 1 // break the outer while loop
      } else {
        // binary search
        mid = left + (right - left) / 2
        cmp = comp(src, tgt(mid))
        if (cmp == 0) {
          if (upperBound) {
            prevLarger = binarySearchEquality(mid, prevSmaller, src, tgt, threshold, comp)
          } else {
            prevSmaller = binarySearchEquality(mid, prevLarger, src, tgt, threshold, comp)
          }
          right = left // break the outer loop
        } else if (cmp < 0) {
          prevLarger = mid
          right = mid - 1
        } else {
          prevSmaller = mid
          left = mid + 1
        }
      }
    }
    if (upperBound) {
      prevLarger
    }
    else {
      prevSmaller
    }
  }

  /**
   * find partitions covered by a critical point range
   * @param cpr: the critical point range
   * @param partitions the partitions to be qualified
   * @param pStartIndex the index of the partition to start the qualification process with
   * @return the start and end index of the qualified partitions, inclusive on both boundaries
   */
  private[hbase] def getQualifiedPartitions[T](cpr: MDCriticalPointRange[T],
                                               partitions: Seq[HBasePartition],
                                               pStartIndex: Int,
                                               threshold:Int = 10): (Int, Int) = {
    val largestStart = binarySearchForTightBound[MDCriticalPointRange[T], HBasePartition](
      cpr, partitions, pStartIndex, upperBound = false,
      (mdpr: MDCriticalPointRange[T], p: HBasePartition) =>
        mdpr.compareWithPartition(startOrEnd = false, p), threshold)
    val smallestEnd = binarySearchForTightBound[MDCriticalPointRange[T], HBasePartition](
      cpr, partitions, pStartIndex, upperBound = true,
      (mdpr: MDCriticalPointRange[T], p: HBasePartition) =>
        mdpr.compareWithPartition(startOrEnd = true, p), threshold)
    if (largestStart == -1 || smallestEnd == -1 || smallestEnd > largestStart) {
      null // no overlapping
    }
    else {
      (smallestEnd, largestStart)
    }
  }

  /**
   * Find critical point ranges covered by a partition
   * @param partition: the partition
   * @param crps the critical point ranges to be qualified
   * @param startIndex the index of the crp to start the qualification process with
   * @return the start and end index of the qualified crps, inclusive on both boundaries
   */
  private[hbase] def getQualifiedCRRanges(partition: HBasePartition,
                                          crps: Seq[MDCriticalPointRange[_]],
                                          startIndex: Int,
                                          threshold:Int = 10): Int = {
    val largestStart = binarySearchForTightBound[HBasePartition, MDCriticalPointRange[_]](
      partition, crps, startIndex, upperBound = false,
      (p: HBasePartition, mdpr: MDCriticalPointRange[_]) =>
        -mdpr.compareWithPartition(startOrEnd = true, p), threshold)
//    val smallestEnd = binarySearchForTightBound[HBasePartition, MDCriticalPointRange[_]](
//      partition, crps, startIndex, upperBound = true,
//      (p: HBasePartition, mdpr: MDCriticalPointRange[_]) =>
//        -mdpr.compareWithPartition(startOrEnd = false, p), threshold)
    largestStart
  }

  private[hbase] def prunePartitions(cprs: Seq[MDCriticalPointRange[_]],
                                     pred: Option[Expression],
                                     partitions: Seq[HBasePartition],
                                     dimSize: Int,
                                     threshold:Int = 10): Seq[HBasePartition] = {
    // no need to prune as hbase partitions size is 1. Generally for single hbase partition there
    // will not be any lowerBound and upperBound key.
    if (cprs.isEmpty || partitions.length == 1) {
      partitions.map(p => new HBasePartition(p.idx, p.mappedIndex, p.start, p.end, p.server, pred))
    } else {
      var cprStartIndex = 0
      var pStartIndex = 0
      var pIndex = 0
      var done = false
      var result = Seq[HBasePartition]()
      while (cprStartIndex < cprs.size && pStartIndex < partitions.size && !done) {
        val cpr = cprs(cprStartIndex)
        val qualifiedPartitionIndexes =
          getQualifiedPartitions(cpr, partitions, pStartIndex)
        if (qualifiedPartitionIndexes != null) {
          val (pstart, pend) = qualifiedPartitionIndexes
          var p = partitions(pstart)

          for (i <- pstart to pend) {
            p = partitions(i)
            result = result :+ new HBasePartition(pIndex, p.idx,
              p.start, p.end, p.server, pred)
            pIndex += 1
          }

          pStartIndex = pend + 1
          // Step 3.2
          // skip any critical point ranges that possibly are covered by
          // the last of just-qualified partitions
          val qualifiedCPRIndexes = getQualifiedCRRanges(
            partitions(pend), cprs, cprStartIndex, threshold)
          if (qualifiedCPRIndexes == -1) done = true
          else cprStartIndex = if (qualifiedCPRIndexes == cprStartIndex) {
             qualifiedCPRIndexes + 1
          } else qualifiedCPRIndexes
        } else {
          done = true
        }
      }
      result
    }
  }

  /*
   * Given a HBase relation, generate a sequence of pruned partitions and their
   * associated filter predicates that are further subject to slave-side refinement
   * The algorithm goes like this:
   * 1. For each dimension key (starting from the primary key dimension)
   *    1.1 collect the critical points and their sorted ranges
   *    1.2 For each range, partial reduce to qualify and generate associated filter predicates
   *    1.3 For each "point range", repeat Step 1 for the next key dimension
   * 2. For each critical point based range,
   *    potentially expand the original top-level critical point ranges into multidimensional
   *    critical point ranges incorporating
   *    lower level nested critical point ranges for next key dimension(s)
   * 3. For each converted critical point based range, map them to partitions to partitions
   *    3.1 start the binary search from the last mapped partition
   *    3.2 For last mapped partition, find the the last critical point range covered
   *        by this last mapped partition and use that as the next start point of the critical
   *        point range to find next set of mapped partitions. This ping-pong manner
   *        of searching will continue until either list is exhausted.
   */
  private[hbase] def generatePrunedPartitions(relation: HBaseRelation, pred: Option[Expression])
  : Seq[HBasePartition] = {
    if (pred.isEmpty) relation.partitions
    else {
      // Step 1
      val cprs: Seq[CriticalPointRange[_]] = generateCriticalPointRanges(relation, pred)

      // Step 2
      val expandedCPRs: Seq[MDCriticalPointRange[_]] =
        cprs.flatMap(_.flatten(new ArrayBuffer[(Any, NativeType)](relation.dimSize)))

      // Step 3
      prunePartitions(expandedCPRs, pred, relation.partitions, relation.partitionKeys.size)
    }
  }
}
