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

package org.apache.spark.sql.execution.joins

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import scala.collection.mutable.ArrayBuffer

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildSide}
import org.apache.spark.sql.catalyst.planning.{IntervalOverlapJoin, PartialRangeJoin, PointInRangeJoin, RangeJoinKind}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, IntegerType, LongType, ShortType, TimestampNTZType, TimestampType}
import org.apache.spark.util.{KnownSizeEstimation, SizeEstimator, Utils}

/**
 * Broadcast payload of a range join. An index only names candidate rows.
 * [[BroadcastRangeJoinExec]] evaluates the original join condition to filter
 * candidates. Keys that map to one order-preserving long are stored raw and
 * compared unboxed ([[LongIntervalIndex]], [[LongPointIndex]]); every other
 * supported type is compared as boxed values ([[IntervalIndex]],
 * [[PointIndex]]). [[RangeRelationBroadcastMode]] picks the family from the
 * build keys' data types.
 *
 *  - The interval indexes answer "which ranges overlap this window?"
 *  - The point indexes answer "which points lie on this side of a bound?"
 */
private[execution] sealed trait RangeRelation extends KnownSizeEstimation with Serializable {
  def keyType: DataType
}

/**
 * Which [[RangeRelation]] a broadcast materializes.
 *
 * A point index has no direction, so one broadcast of a column serves both
 * `<` and `>`. The join records which side holds the lower bound and scans
 * accordingly.
 */
private[execution] sealed abstract class RangeIndexKind {
  def matches(rangeJoinKind: RangeJoinKind): Boolean = RangeIndexKind(rangeJoinKind) == this
}

private[execution] case object IntervalIndexKind extends RangeIndexKind

private[execution] case object PointIndexKind extends RangeIndexKind

private[execution] object RangeIndexKind {
  def apply(rangeJoinKind: RangeJoinKind): RangeIndexKind = rangeJoinKind match {
    case PointInRangeJoin | IntervalOverlapJoin => IntervalIndexKind
    case _: PartialRangeJoin => PointIndexKind
  }
}

/**
 * The boxed-key index family's shared ordering cache, derived from `keyType`
 * on first comparison and kept out of the serialized form, so each executor
 * re-derives its own instance instead of receiving one per broadcast. The
 * long index family compares raw keys and has no ordering to cache.
 */
private[joins] sealed trait BoxedKeyOrdering extends RangeRelation {
  @transient
  @volatile private[this] var _ordering: Ordering[Any] = _

  private[joins] def ordering: Ordering[Any] = {
    var ord = _ordering
    if (ord == null) {
      ord = PhysicalDataType.ordering(keyType)
      _ordering = ord
    }
    ord
  }
}

/**
 * The long index family's shared key conversion, derived from `keyType` on
 * first probe and kept out of the serialized form, so each executor derives
 * its own converter after deserialization. The interpreted probe path calls
 * [[toLong]] once per streamed key; generated probe code keeps its own
 * conversion, emitted at plan time.
 */
private[joins] sealed trait LongKeyConversion extends RangeRelation {
  @transient
  @volatile private[this] var _toLong: Any => Long = _

  /**
   * One evaluated streamed key to its order-preserving long. Within a predicate, the
   * analyzer's comparison type coercion gives both sides one type; across the two
   * predicates an interval-overlap join pairs, `ExtractRangeJoinKeys`' own same-type
   * guards keep the streamed key on the build side's type, so the streamed key type
   * is this index's `keyType`.
   */
  private[joins] def toLong(key: Any): Long = {
    var f = _toLong
    if (f == null) {
      f = RangeIndex.requiredLongKey(keyType).value
      _toLong = f
    }
    f(key)
  }
}

private[joins] object RangeIndex {

  /**
   * Capacity for a build buffer from the driver's row-count hint, capped at 2^20 for any
   * larger hint (well under a hint that large) and equal to the hint itself below that.
   * `BroadcastExchangeExec` only checks the row count against its row-count
   * limit (up to half a billion for this mode) before calling `transform`, and only
   * checks the built relation's byte size afterward, so sizing a buffer directly off an
   * inflated hint can allocate hundreds of megabytes before a single row is read - ahead
   * of the size check that is supposed to catch an over-large broadcast. Starting small
   * and letting the buffer grow from there bounds only the initial allocation; the final
   * size still tracks the actual row count, which is what the post-build size check sees.
   */
  def initialCapacity(sizeHint: Option[Long]): Int =
    sizeHint.map(n => math.min(n, 1L << 20).toInt).getOrElse(64)

  /**
   * One key type's bridge to an order-preserving long: converting an evaluated
   * key value (from either the build or the streamed side), and turning
   * generated Java for a typed key into generated Java for a long. The two
   * agree, so a build key and a probe key of the same type become the same
   * long.
   */
  final case class LongKey(value: Any => Long, code: String => String)

  /**
   * An order-preserving long for a key type that has one, or None when the type
   * must stay on the generic boxed index: variable-length keys, floating point,
   * whose SQL equality for -0.0 and NaN has no bit-preserving order, and
   * decimals wider than `Decimal.MAX_LONG_DIGITS`, whose unscaled value does
   * not fit a long. This is the only place a key type is specialized; the long
   * indexes and the generated probe code both consult it.
   */
  def longKey(dt: DataType): Option[LongKey] = {
    def key(value: Any => Long): LongKey = LongKey(value, v => s"((long) $v)")
    dt match {
      case _: BooleanType =>
        Some(LongKey(v => if (v.asInstanceOf[Boolean]) 1L else 0L, v => s"($v ? 1L : 0L)"))
      case _: ByteType =>
        Some(key(v => v.asInstanceOf[Byte].toLong))
      case _: ShortType =>
        Some(key(v => v.asInstanceOf[Short].toLong))
      case _: IntegerType | _: DateType =>
        Some(key(v => v.asInstanceOf[Int].toLong))
      case _: LongType | _: TimestampType | _: TimestampNTZType =>
        Some(key(v => v.asInstanceOf[Long]))
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        Some(LongKey(v => v.asInstanceOf[Decimal].toUnscaledLong, v => s"$v.toUnscaledLong()"))
      case _ => None
    }
  }

  /**
   * Every key maps to one order-preserving long. [[RangeRelationBroadcastMode]] picks
   * the index family by this test and [[BroadcastRangeJoinExec]] picks its probe
   * code by the same one, so the two cannot drift apart. Within a predicate, the
   * analyzer's comparison type coercion gives both sides one type; across predicates,
   * `ExtractRangeJoinKeys`' own same-type guards keep build and streamed keys aligned,
   * so testing the build keys also settles the streamed keys.
   */
  def canMapKeysToLong(keys: Seq[Expression]): Boolean =
    keys.forall(k => longKey(k.dataType).isDefined)

  /** longKey for a type the caller has routed to the long family. */
  def requiredLongKey(dt: DataType): LongKey = {
    longKey(dt).getOrElse(
      throw new IllegalStateException(s"$dt has no order-preserving long key."))
  }
}

/**
 * Array-backed candidate scan. The index hands rows to the join in array order,
 * without a closure or a tuple hop: the per-pair overhead then matches a bare
 * loop over a broadcast row array. One instance per probe; never serialized.
 */
private[joins] final class RowSlice(
    rows: Array[InternalRow],
    from: Int,
    until: Int) extends Iterator[InternalRow] {
  private var i = from
  def hasNext: Boolean = i < until
  def next(): InternalRow = {
    val r = rows(i)
    i += 1
    r
  }
}

private[joins] object RowSlice {
  /** A shared empty scan: a null probe bound matches nothing. */
  val empty: RowSlice = new RowSlice(new Array[InternalRow](0), 0, 0)
}

// ===== Boxed-key indexes ====================================================
//
// Keys are the boxed values each build key expression evaluates to, compared
// through `BoxedKeyOrdering`. Floating point, strings, binary, and decimals wider
// than 18 digits stay here: these types have no order-preserving
// single-long form, which is the property that routes a key type to the
// long indexes below (`RangeIndex.longKey`).

/**
 * Augmented interval tree of build-side intervals. Lows and highs sit in
 * parallel arrays sorted by low bound, with a row array in the same order
 * feeding the scan path and `maxHighIdx(mid)` indexing the maximum high
 * bound in the span rooted at mid. Each key and row object is owned once:
 * the tree holds one reference per value plus the single row array. Java
 * serialization keeps these aliases; Kryo's reference resolver does not
 * track primitive wrappers, so a key shared by `lows` and `highs`, such as
 * the one key of a degenerate build, can deserialize as two objects.
 * Subtrees whose max high is below the probe window, or whose low starts
 * after it, are pruned during search, and a window covering every interval
 * skips the walk entirely.
 */
private[joins] class IntervalIndex private (
    private[this] var _keyType: DataType,
    private[this] var lows: Array[Any],
    private[this] var highs: Array[Any],
    private[this] var maxHighIdx: Array[Int],
    private[this] var rows: Array[InternalRow])
  extends BoxedKeyOrdering with Externalizable with KryoSerializable {

  // Needed for Java/Kryo deserialization; see readExternal/read below.
  def this() = this(null, null, null, null, null)

  def keyType: DataType = _keyType

  // All-covering window bounds. `lows` is sorted, so the last entry is the
  // largest low: it bounds every low from above, and coreHigh is the minimum
  // high, which bounds every high from below. A window overlaps every stored
  // interval exactly when it holds probeLow <= coreHigh and probeHigh >= coreLow,
  // whether or not the intervals share a common core.
  // Recomputed in `read` too, since `lows`/`highs` are reassigned there.
  private[this] var coreLow: Any = IntervalIndex.coreLow(lows)
  private[this] var coreHigh: Any = IntervalIndex.coreHigh(highs, ordering)

  /**
   * Measured over the retained arrays, not the index object: SizeEstimator
   * would consult `estimatedSize` for a KnownSizeEstimation and recurse.
   * Its identity tracking charges a key shared by `lows` and `highs`, or
   * aliased across indexes, exactly once.
   */
  override def estimatedSize: Long =
    SizeEstimator.estimate(Array[AnyRef](lows, highs, maxHighIdx, rows))

  def overlapping(low: Any, high: Any): Iterator[InternalRow] = {
    if (lows.isEmpty || low == null || high == null) return Iterator.empty
    val ord = ordering
    val (probeLow, probeHigh) = if (ord.lteq(low, high)) (low, high) else (high, low)
    // A window that covers every interval gets every row the walk would
    // return, without visiting the tree. Returns all rows iff every interval
    // overlaps the probe; the condition still re-checks each candidate.
    if (ord.lteq(probeLow, coreHigh) && ord.gteq(probeHigh, coreLow)) {
      return new RowSlice(rows, 0, rows.length)
    }
    new OverlapIterator(lows, highs, maxHighIdx, rows, ord, probeLow, probeHigh)
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeUTF, out.writeInt, (o: Any) => out.writeObject(o.asInstanceOf[AnyRef]))
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeString, out.writeInt,
      (o: Any) => kryo.writeClassAndObject(out, o.asInstanceOf[AnyRef]))
  }

  private def write(
      writeString: String => Unit,
      writeInt: Int => Unit,
      writeObj: Any => Unit): Unit = {
    writeString(keyType.json)
    val n = lows.length
    writeInt(n)
    var i = 0
    while (i < n) {
      writeObj(lows(i))
      writeObj(highs(i))
      i += 1
    }
    i = 0
    while (i < n) {
      writeInt(maxHighIdx(i))
      i += 1
    }
    i = 0
    while (i < n) {
      writeObj(rows(i))
      i += 1
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(() => in.readUTF(), () => in.readInt(), () => in.readObject())
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(() => in.readString(), () => in.readInt(), () => kryo.readClassAndObject(in))
  }

  private def read(
      readString: () => String,
      readInt: () => Int,
      readObj: () => AnyRef): Unit = {
    _keyType = DataType.fromJson(readString())
    val n = readInt()
    lows = new Array[Any](n)
    highs = new Array[Any](n)
    maxHighIdx = new Array[Int](n)
    rows = new Array[InternalRow](n)
    var i = 0
    while (i < n) {
      lows(i) = readObj()
      highs(i) = readObj()
      i += 1
    }
    i = 0
    while (i < n) {
      maxHighIdx(i) = readInt()
      i += 1
    }
    i = 0
    while (i < n) {
      rows(i) = readObj().asInstanceOf[InternalRow]
      i += 1
    }
    coreLow = IntervalIndex.coreLow(lows)
    coreHigh = IntervalIndex.coreHigh(highs, ordering)
  }
}

private[joins] object IntervalIndex {

  // Shared by the primary constructor and `read`, which recomputes these
  // caches after reassigning `lows`/`highs` on deserialization. `lows`/
  // `highs` are null right after the zero-arg constructor runs, before
  // `readExternal`/`read` populate them.
  private[joins] def coreLow(lows: Array[Any]): Any =
    if (lows == null || lows.isEmpty) null else lows(lows.length - 1)

  // `ordering` is by-name: the zero-arg constructor calls this with `highs ==
  // null`, before `keyType` is set, and forcing `ordering` eagerly would
  // throw trying to derive an ordering for a not-yet-known key type.
  private[joins] def coreHigh(highs: Array[Any], ordering: => Ordering[Any]): Any = {
    if (highs == null || highs.isEmpty) {
      null
    } else {
      val ord = ordering
      var min = highs(0)
      var i = 1
      while (i < highs.length) {
        if (ord.lt(highs(i), min)) min = highs(i)
        i += 1
      }
      min
    }
  }

  def apply(
      keyType: DataType,
      buildKeys: Seq[Expression],
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): IntervalIndex = {
    val buffer = new ArrayBuffer[(Any, Any, InternalRow)](RangeIndex.initialCapacity(sizeHint))
    val ordering = PhysicalDataType.ordering(keyType)

    if (buildKeys.length == 1) {
      // A point build key is one key used as both ends, so it is copied once
      // and serves as both bounds, matching `LongIntervalIndex.apply`.
      val lowExpr = buildKeys.head
      while (rows.hasNext) {
        val r = rows.next()
        val rawLo = lowExpr.eval(r)
        if (rawLo != null) {
          val key = InternalRow.copyValue(rawLo)
          buffer += ((key, key, r.copy()))
        }
      }
    } else {
      val lowExpr = buildKeys.head
      val highExpr = buildKeys(1)
      while (rows.hasNext) {
        val r = rows.next()
        val rawLo = lowExpr.eval(r)
        val rawHi = highExpr.eval(r)
        if (rawLo != null && rawHi != null) {
          val lo = InternalRow.copyValue(rawLo)
          val hi = InternalRow.copyValue(rawHi)
          val (normLo, normHi) = if (ordering.lteq(lo, hi)) (lo, hi) else (hi, lo)
          buffer += ((normLo, normHi, r.copy()))
        }
      }
    }

    sortAndBuild(keyType, ordering, buffer.toArray)
  }

  private def sortAndBuild(
      keyType: DataType,
      ordering: Ordering[Any],
      valid: Array[(Any, Any, InternalRow)]): IntervalIndex = {
    java.util.Arrays.sort(valid, Ordering.by[(Any, Any, InternalRow), Any](_._1)(ordering))
    val length = valid.length
    val lows = new Array[Any](length)
    val highs = new Array[Any](length)
    val rows = new Array[InternalRow](length)
    var i = 0
    while (i < length) {
      lows(i) = valid(i)._1
      highs(i) = valid(i)._2
      rows(i) = valid(i)._3
      i += 1
    }

    // `maxHighIdx(mid)` indexes the maximum high bound in the span rooted at
    // mid. Storing an index instead of aliasing the key keeps each key owned,
    // and serialized, exactly once.
    val maxHighIdx = new Array[Int](length)

    def fillMaxHigh(start: Int, end: Int): Int = {
      if (start >= end) return -1
      val mid = (start + end) >>> 1
      var best = mid
      val left = fillMaxHigh(start, mid)
      if (left >= 0 && ordering.gt(highs(left), highs(best))) best = left
      val right = fillMaxHigh(mid + 1, end)
      if (right >= 0 && ordering.gt(highs(right), highs(best))) best = right
      maxHighIdx(mid) = best
      best
    }

    fillMaxHigh(0, length)
    new IntervalIndex(keyType, lows, highs, maxHighIdx, rows)
  }
}

private[joins] class OverlapIterator(
    lows: Array[Any],
    highs: Array[Any],
    maxHighIdx: Array[Int],
    rows: Array[InternalRow],
    ord: Ordering[Any],
    probeLow: Any,
    probeHigh: Any) extends Iterator[InternalRow] {

  private[this] val depth = 32 - java.lang.Integer.numberOfLeadingZeros(lows.length)
  private[this] var stackStart = new Array[Int](math.max(8, depth * 3))
  private[this] var stackEnd = new Array[Int](math.max(8, depth * 3))
  private[this] var stackTop = 0
  private[this] var nextRow: InternalRow = null

  push(0, lows.length)

  private[joins] def stackCapacity: Int = stackStart.length
  private[joins] def pendingSpans: Int = stackTop

  private def push(s: Int, e: Int): Unit = {
    if (stackTop >= stackStart.length) {
      stackStart = java.util.Arrays.copyOf(stackStart, stackStart.length * 2)
      stackEnd = java.util.Arrays.copyOf(stackEnd, stackEnd.length * 2)
    }
    stackStart(stackTop) = s
    stackEnd(stackTop) = e
    stackTop += 1
  }

  private def advance(): InternalRow = {
    while (stackTop > 0) {
      stackTop -= 1
      val start = stackStart(stackTop)
      val end = stackEnd(stackTop)
      if (start < end) {
        val mid = (start + end) >>> 1
        if (ord.gteq(highs(maxHighIdx(mid)), probeLow)) {
          if (ord.lteq(lows(mid), probeHigh)) {
            push(mid + 1, end)
          }
          push(mid, -1)
          push(start, mid)
        }
      } else if (end == -1) {
        if (ord.gteq(highs(start), probeLow) && ord.lteq(lows(start), probeHigh)) {
          return rows(start)
        }
      }
    }
    null
  }

  override def hasNext: Boolean = {
    if (nextRow != null) {
      true
    } else {
      nextRow = advance()
      nextRow != null
    }
  }

  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException("empty iterator")
    val r = nextRow
    nextRow = null
    r
  }
}

/**
 * Sorted build-side points for single cross-side inequalities (`<`, `<=`, `>`, `>=`).
 * Keys sit beside the rows they name, one owned copy each, in key order.
 */
private[joins] class PointIndex private (
    private[this] var _keyType: DataType,
    private[this] var keys: Array[Any],
    private[this] var rows: Array[InternalRow])
  extends BoxedKeyOrdering with Externalizable with KryoSerializable {

  // Needed for Java/Kryo deserialization; see readExternal/read below.
  def this() = this(null, null, null)

  def keyType: DataType = _keyType

  private[this] def length: Int = keys.length

  override def estimatedSize: Long = SizeEstimator.estimate(Array[AnyRef](keys, rows))

  /** Points whose key is less than or equal to `value`. */
  def upTo(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(0, searchBound(value, skipEquals = true))

  /** Points whose key is greater than or equal to `value`. */
  def from(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(searchBound(value, skipEquals = false), length)

  /**
   * Points whose key is strictly less than `value`. For a strict partial-range
   * predicate the original condition rejects keys equal to the probe value,
   * so the scan can skip them: a build heavy in duplicated keys would only
   * hand the join rows that all fail.
   */
  def upToExclusive(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(0, searchBound(value, skipEquals = false))

  /** Points whose key is strictly greater than `value`. */
  def fromExclusive(value: Any): Iterator[InternalRow] =
    if (value == null) Iterator.empty else slice(searchBound(value, skipEquals = true), length)

  private def searchBound(value: Any, skipEquals: Boolean): Int = {
    val ord = ordering
    var lo = 0
    var hi = length
    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      val c = ord.compare(keys(mid), value)
      if (c < 0 || (skipEquals && c == 0)) lo = mid + 1 else hi = mid
    }
    lo
  }

  private def slice(from: Int, until: Int): Iterator[InternalRow] = new RowSlice(rows, from, until)

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeUTF, out.writeInt, (o: Any) => out.writeObject(o.asInstanceOf[AnyRef]))
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeString, out.writeInt,
      (o: Any) => kryo.writeClassAndObject(out, o.asInstanceOf[AnyRef]))
  }

  private def write(
      writeString: String => Unit,
      writeInt: Int => Unit,
      writeObj: Any => Unit): Unit = {
    writeString(keyType.json)
    val n = keys.length
    writeInt(n)
    var i = 0
    while (i < n) {
      writeObj(keys(i))
      i += 1
    }
    i = 0
    while (i < n) {
      writeObj(rows(i))
      i += 1
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(() => in.readUTF(), () => in.readInt(), () => in.readObject())
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(() => in.readString(), () => in.readInt(), () => kryo.readClassAndObject(in))
  }

  private def read(
      readString: () => String,
      readInt: () => Int,
      readObj: () => AnyRef): Unit = {
    _keyType = DataType.fromJson(readString())
    val n = readInt()
    keys = new Array[Any](n)
    rows = new Array[InternalRow](n)
    var i = 0
    while (i < n) {
      keys(i) = readObj()
      i += 1
    }
    i = 0
    while (i < n) {
      rows(i) = readObj().asInstanceOf[InternalRow]
      i += 1
    }
  }
}

private[joins] object PointIndex {

  def apply(
      keyType: DataType,
      buildKeys: Seq[Expression],
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): PointIndex = {
    val buffer = new ArrayBuffer[(Any, InternalRow)](RangeIndex.initialCapacity(sizeHint))
    val keyExpr = buildKeys.head
    val ordering = PhysicalDataType.ordering(keyType)

    while (rows.hasNext) {
      val r = rows.next()
      val rawKey = keyExpr.eval(r)
      if (rawKey != null) {
        val key = InternalRow.copyValue(rawKey)
        buffer += ((key, r.copy()))
      }
    }

    sortAndBuild(keyType, ordering, buffer.toArray)
  }

  private def sortAndBuild(
      keyType: DataType,
      ordering: Ordering[Any],
      valid: Array[(Any, InternalRow)]): PointIndex = {
    java.util.Arrays.sort(valid, Ordering.by[(Any, InternalRow), Any](_._1)(ordering))
    val length = valid.length
    val keys = new Array[Any](length)
    val rows = new Array[InternalRow](length)
    var i = 0
    while (i < length) {
      keys(i) = valid(i)._1
      rows(i) = valid(i)._2
      i += 1
    }
    new PointIndex(keyType, keys, rows)
  }
}

// ===== Long-specialized indexes for keys that map to one long ==============
//
// Indexes for the key types `RangeIndex.longKey` maps to one order-preserving
// long, stored as raw longs converted at the build boundary. The key comparison
// itself touches no Spark types once built: the tree walk and the binary searches
// compare raw longs, and generated probe code passes long keys without boxing.
// The row payload still does - `rows: Array[InternalRow]` and the serialized
// `_keyType: DataType` are retained alongside the long arrays. Fixed width alone
// is not the routed property: floating point is fixed-width but stays boxed,
// while compact decimals route here as unscaled longs.
// `RangeRelationBroadcastMode` picks between the two families once, from the
// build keys' data types.

/**
 * Augmented interval tree of build-side intervals, on raw keys. Lows and highs sit in
 * parallel arrays sorted by low bound, with a row array in the same order feeding the
 * scan path and `maxHighs(mid)` holding the maximum high bound in the span rooted at
 * mid - the value itself, unlike [[IntervalIndex.maxHighIdx]], which stores an index
 * into `highs` because boxed values there are shared references worth not duplicating.
 * `highs` here is already an unboxed `Array[Long]`, so storing the max value directly
 * costs 4 bytes/row more than an Int index would (a Long vs. an Int), in exchange for
 * comparing it without the extra `highs(idx)` indirection on every node a probe visits.
 */
private[joins] class LongIntervalIndex private (
    private[this] var _keyType: DataType,
    private[this] var lows: Array[Long],
    private[this] var highs: Array[Long],
    private[this] var maxHighs: Array[Long],
    private[this] var rows: Array[InternalRow])
  extends LongKeyConversion with Externalizable with KryoSerializable {

  // Needed for Java/Kryo deserialization; see readExternal/read below.
  def this() = this(null, null, null, null, null)

  def keyType: DataType = _keyType

  // All-covering window bounds on raw keys: `lows` is sorted, so the last entry
  // is the largest low, bounding every low from above, and coreHigh is the
  // minimum high, bounding every high from below. A window overlaps every
  // stored interval exactly when it holds probeLow <= coreHigh and
  // probeHigh >= coreLow, whether or not a common core exists.
  // Recomputed in `read` too, since `lows`/`highs` are reassigned there.
  private[this] var coreLow: Long = LongIntervalIndex.coreLow(lows)
  private[this] var coreHigh: Long = LongIntervalIndex.coreHigh(highs)

  override def estimatedSize: Long =
    SizeEstimator.estimate(Array[AnyRef](lows, highs, maxHighs, rows))

  def overlapping(low: Long, high: Long): Iterator[InternalRow] = {
    if (lows.isEmpty) return Iterator.empty
    var probeLow = low
    var probeHigh = high
    if (probeLow > probeHigh) {
      val t = probeLow
      probeLow = probeHigh
      probeHigh = t
    }
    // A window that covers every interval gets every row the walk would
    // return, without visiting the tree. Returns all rows iff every interval
    // overlaps the probe; the condition still re-checks each candidate.
    if (probeLow <= coreHigh && probeHigh >= coreLow) {
      return new RowSlice(rows, 0, rows.length)
    }
    new LongOverlapIterator(lows, highs, rows, maxHighs, probeLow, probeHigh)
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeUTF, out.writeInt, out.writeLong,
      (o: Any) => out.writeObject(o.asInstanceOf[AnyRef]))
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeString, out.writeInt, out.writeLong,
      (o: Any) => kryo.writeClassAndObject(out, o.asInstanceOf[AnyRef]))
  }

  private def write(
      writeString: String => Unit,
      writeInt: Int => Unit,
      writeLong: Long => Unit,
      writeObj: Any => Unit): Unit = {
    writeString(keyType.json)
    val n = lows.length
    writeInt(n)
    var i = 0
    while (i < n) {
      writeLong(lows(i))
      writeLong(highs(i))
      writeLong(maxHighs(i))
      i += 1
    }
    i = 0
    while (i < n) {
      writeObj(rows(i))
      i += 1
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(() => in.readUTF(), () => in.readInt(), () => in.readLong(), () => in.readObject())
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(() => in.readString(), () => in.readInt(), () => in.readLong(),
      () => kryo.readClassAndObject(in))
  }

  private def read(
      readString: () => String,
      readInt: () => Int,
      readLong: () => Long,
      readObj: () => AnyRef): Unit = {
    _keyType = DataType.fromJson(readString())
    val n = readInt()
    lows = new Array[Long](n)
    highs = new Array[Long](n)
    maxHighs = new Array[Long](n)
    rows = new Array[InternalRow](n)
    var i = 0
    while (i < n) {
      lows(i) = readLong()
      highs(i) = readLong()
      maxHighs(i) = readLong()
      i += 1
    }
    i = 0
    while (i < n) {
      rows(i) = readObj().asInstanceOf[InternalRow]
      i += 1
    }
    coreLow = LongIntervalIndex.coreLow(lows)
    coreHigh = LongIntervalIndex.coreHigh(highs)
  }
}

private[joins] object LongIntervalIndex {

  // Shared by the primary constructor and `read`, which recomputes these
  // caches after reassigning `lows`/`highs` on deserialization. `lows`/
  // `highs` are null right after the zero-arg constructor runs, before
  // `readExternal`/`read` populate them.
  private[joins] def coreLow(lows: Array[Long]): Long =
    if (lows == null || lows.isEmpty) {
      Long.MinValue
    } else {
      lows(lows.length - 1)
    }

  private[joins] def coreHigh(highs: Array[Long]): Long = {
    if (highs == null || highs.isEmpty) {
      Long.MaxValue
    } else {
      var min = Long.MaxValue
      var i = 0
      while (i < highs.length) {
        if (highs(i) < min) min = highs(i)
        i += 1
      }
      min
    }
  }

  def apply(
      keyType: DataType,
      buildKeys: Seq[Expression],
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): LongIntervalIndex = {
    val toLongs: Seq[Any => Long] = buildKeys.map(k => RangeIndex.requiredLongKey(k.dataType).value)
    val valid = new ArrayBuffer[(Long, Long, InternalRow)](RangeIndex.initialCapacity(sizeHint))
    val lowExpr = buildKeys.head
    val highExpr = if (buildKeys.length == 1) null else buildKeys(1)

    while (rows.hasNext) {
      val r = rows.next()
      val rawLo = lowExpr.eval(r)
      val rawHi = if (buildKeys.length == 1) rawLo else highExpr.eval(r)
      if (rawLo != null && rawHi != null) {
        // A point build key is one key used as both ends, so it is read once.
        var lo = toLongs.head(rawLo)
        var hi = if (buildKeys.length == 1) lo else toLongs(1)(rawHi)
        if (lo > hi) {
          val t = lo
          lo = hi
          hi = t
        }
        valid += ((lo, hi, r.copy()))
      }
    }

    val staged = valid.toArray
    java.util.Arrays.sort(staged, Ordering.by[(Long, Long, InternalRow), Long](_._1))
    val length = staged.length
    val lows = new Array[Long](length)
    val highs = new Array[Long](length)
    val rowArr = new Array[InternalRow](length)
    var i = 0
    while (i < length) {
      lows(i) = staged(i)._1
      highs(i) = staged(i)._2
      rowArr(i) = staged(i)._3
      i += 1
    }

    val maxHighs = new Array[Long](length)

    def fillMaxHigh(start: Int, end: Int): Long = {
      if (start >= end) return Long.MinValue
      val mid = (start + end) >>> 1
      var max = highs(mid)
      val left = fillMaxHigh(start, mid)
      val right = fillMaxHigh(mid + 1, end)
      if (left > max) max = left
      if (right > max) max = right
      maxHighs(mid) = max
      max
    }

    fillMaxHigh(0, length)
    new LongIntervalIndex(keyType, lows, highs, maxHighs, rowArr)
  }
}

private[joins] class LongOverlapIterator(
    lows: Array[Long],
    highs: Array[Long],
    rows: Array[InternalRow],
    maxHighs: Array[Long],
    probeLow: Long,
    probeHigh: Long) extends Iterator[InternalRow] {

  private[this] val depth = 32 - java.lang.Integer.numberOfLeadingZeros(lows.length)
  private[this] var stackStart = new Array[Int](math.max(8, depth * 3))
  private[this] var stackEnd = new Array[Int](math.max(8, depth * 3))
  private[this] var stackTop = 0
  private[this] var nextRow: InternalRow = null

  push(0, lows.length)

  private[joins] def stackCapacity: Int = stackStart.length
  private[joins] def pendingSpans: Int = stackTop

  private def push(s: Int, e: Int): Unit = {
    if (stackTop >= stackStart.length) {
      stackStart = java.util.Arrays.copyOf(stackStart, stackStart.length * 2)
      stackEnd = java.util.Arrays.copyOf(stackEnd, stackEnd.length * 2)
    }
    stackStart(stackTop) = s
    stackEnd(stackTop) = e
    stackTop += 1
  }

  private def advance(): InternalRow = {
    while (stackTop > 0) {
      stackTop -= 1
      val start = stackStart(stackTop)
      val end = stackEnd(stackTop)
      if (start < end) {
        val mid = (start + end) >>> 1
        if (maxHighs(mid) >= probeLow) {
          if (lows(mid) <= probeHigh) {
            push(mid + 1, end)
          }
          push(mid, -1)
          push(start, mid)
        }
      } else if (end == -1) {
        if (highs(start) >= probeLow && lows(start) <= probeHigh) {
          return rows(start)
        }
      }
    }
    null
  }

  override def hasNext: Boolean = {
    if (nextRow != null) {
      true
    } else {
      nextRow = advance()
      nextRow != null
    }
  }

  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException("empty iterator")
    val r = nextRow
    nextRow = null
    r
  }
}

private[joins] class LongPointIndex private (
    private[this] var _keyType: DataType,
    private[this] var keys: Array[Long],
    private[this] var rows: Array[InternalRow])
  extends LongKeyConversion with Externalizable with KryoSerializable {

  // Needed for Java/Kryo deserialization; see readExternal/read below.
  def this() = this(null, null, null)

  def keyType: DataType = _keyType

  private[this] def length: Int = keys.length

  override def estimatedSize: Long = SizeEstimator.estimate(Array[AnyRef](keys, rows))

  /** Points whose key is less than or equal to `value`. */
  def upTo(value: Long): Iterator[InternalRow] =
    new RowSlice(rows, 0, searchBound(value, skipEquals = true))

  /** Points whose key is greater than or equal to `value`. */
  def from(value: Long): Iterator[InternalRow] =
    new RowSlice(rows, searchBound(value, skipEquals = false), length)

  /**
   * Points whose key is strictly less than `value`. For a strict partial-range
   * predicate the original condition rejects keys equal to the probe value,
   * so the scan can skip them; see [[PointIndex.upToExclusive]].
   */
  def upToExclusive(value: Long): Iterator[InternalRow] =
    new RowSlice(rows, 0, searchBound(value, skipEquals = false))

  /** Points whose key is strictly greater than `value`. */
  def fromExclusive(value: Long): Iterator[InternalRow] =
    new RowSlice(rows, searchBound(value, skipEquals = true), length)

  // java.util.Arrays.binarySearch returns an arbitrary hit; a bound scan needs
  // the first or last position, so lower and upper bounds are hand-rolled.
  private def searchBound(value: Long, skipEquals: Boolean): Int = {
    var lo = 0
    var hi = length
    while (lo < hi) {
      val mid = (lo + hi) >>> 1
      val c = java.lang.Long.compare(keys(mid), value)
      if (c < 0 || (skipEquals && c == 0)) lo = mid + 1 else hi = mid
    }
    lo
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeUTF, out.writeInt, out.writeLong,
      (o: Any) => out.writeObject(o.asInstanceOf[AnyRef]))
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeString, out.writeInt, out.writeLong,
      (o: Any) => kryo.writeClassAndObject(out, o.asInstanceOf[AnyRef]))
  }

  private def write(
      writeString: String => Unit,
      writeInt: Int => Unit,
      writeLong: Long => Unit,
      writeObj: Any => Unit): Unit = {
    writeString(keyType.json)
    val n = keys.length
    writeInt(n)
    var i = 0
    while (i < n) {
      writeLong(keys(i))
      i += 1
    }
    i = 0
    while (i < n) {
      writeObj(rows(i))
      i += 1
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(() => in.readUTF(), () => in.readInt(), () => in.readLong(), () => in.readObject())
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(() => in.readString(), () => in.readInt(), () => in.readLong(),
      () => kryo.readClassAndObject(in))
  }

  private def read(
      readString: () => String,
      readInt: () => Int,
      readLong: () => Long,
      readObj: () => AnyRef): Unit = {
    _keyType = DataType.fromJson(readString())
    val n = readInt()
    keys = new Array[Long](n)
    rows = new Array[InternalRow](n)
    var i = 0
    while (i < n) {
      keys(i) = readLong()
      i += 1
    }
    i = 0
    while (i < n) {
      rows(i) = readObj().asInstanceOf[InternalRow]
      i += 1
    }
  }
}

private[joins] object LongPointIndex {

  def apply(
      keyType: DataType,
      buildKeys: Seq[Expression],
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): LongPointIndex = {
    require(buildKeys.length == 1, "A long point index expects one build key.")
    val toLong = RangeIndex.requiredLongKey(buildKeys.head.dataType).value
    val valid = new ArrayBuffer[(Long, InternalRow)](RangeIndex.initialCapacity(sizeHint))
    val keyExpr = buildKeys.head

    while (rows.hasNext) {
      val r = rows.next()
      val rawKey = keyExpr.eval(r)
      if (rawKey != null) {
        valid += ((toLong(rawKey), r.copy()))
      }
    }

    val staged = valid.toArray
    java.util.Arrays.sort(staged, Ordering.by[(Long, InternalRow), Long](_._1))
    val length = staged.length
    val keys = new Array[Long](length)
    val rowArr = new Array[InternalRow](length)
    var i = 0
    while (i < length) {
      keys(i) = staged(i)._1
      rowArr(i) = staged(i)._2
      i += 1
    }
    new LongPointIndex(keyType, keys, rowArr)
  }
}

/**
 * Which side of a single-inequality range join's bound the build side holds, and
 * whether the comparison is strict - the four spellings a [[PointIndex]] /
 * [[LongPointIndex]] expose as scan methods. A `match` on this sealed hierarchy is
 * exhaustiveness-checked by the compiler, so both the interpreted probe
 * ([[BroadcastRangeJoinExec]]'s `makeCandidateFn`) and generated probe code have to
 * handle every case; it is the compiler, not a developer re-reading four string
 * literals, that notices when a fifth case gets added here. Generated code still has
 * to splice `methodName` into the Java source it emits, since codegen has no way to
 * reference a Scala method value directly; that string has to be kept in sync with
 * the method it names by hand, and a mismatch only surfaces as a Janino compile
 * failure at query-run time. RangeJoinSuite's codegen point-scan tests are what
 * actually catches that class of typo.
 */
private[joins] sealed abstract class PointScan(val methodName: String)

private[joins] object PointScan {
  case object UpTo extends PointScan("upTo")
  case object UpToExclusive extends PointScan("upToExclusive")
  case object From extends PointScan("from")
  case object FromExclusive extends PointScan("fromExclusive")

  /**
   * The scan a partial-range join's build side probes with. Lower bound build side
   * uses `UpTo` (points <= probe), upper bound uses `From` (points >= probe); strict
   * inequality selects the Exclusive variant. Throws for any other range-join shape;
   * callers only touch this for a [[PointIndexKind]] build.
   */
  def forJoin(rangeJoinKind: RangeJoinKind, buildSide: BuildSide): PointScan = rangeJoinKind match {
    case partial: PartialRangeJoin =>
      val isLower = partial.leftIsLower == (buildSide == BuildLeft)
      (isLower, partial.strict) match {
        case (true, true) => UpToExclusive
        case (true, false) => UpTo
        case (false, true) => FromExclusive
        case (false, false) => From
      }
    case other =>
      throw SparkException.internalError(
        s"Point index is only built for a partial range, got $other.")
  }
}

/**
 * A [[BroadcastMode]] that materializes the build side into a [[RangeRelation]].
 *
 * Symmetric with `HashedRelationBroadcastMode`. `buildKeys` are bound to the
 * build row, so the index does not depend on attribute ids. Which join input is
 * broadcast, and the original join condition, live on [[BroadcastRangeJoinExec]].
 * Under AQE, `LogicalQueryStageStrategy` checks that this mode's bound keys are
 * the keys `ExtractRangeJoinKeys` finds.
 *
 * Bound keys are stored with nullability forced on. AQE can rewrite a stage's
 * nullability, and [[org.apache.spark.sql.catalyst.plans.physical.BroadcastPartitioning]]
 * decides reuse with `==`. Forcing it here keeps that check aligned with the
 * strategy, which compares modes the same way. The index already treats a null
 * key as non-matching.
 */
private[execution] case class RangeRelationBroadcastMode private (
    buildKeys: Seq[Expression],
    indexKind: RangeIndexKind)
  extends BroadcastMode with Serializable {

  override def transform(rows: Array[InternalRow]): RangeRelation =
    transform(rows.iterator, Some(rows.length))

  override def transform(
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): RangeRelation = {
    val longKeys = RangeIndex.canMapKeysToLong(buildKeys)
    indexKind match {
      case IntervalIndexKind =>
        require(buildKeys.nonEmpty && buildKeys.length <= 2,
          "An interval index expects (low, high), or one point used as both ends.")
        if (longKeys) {
          LongIntervalIndex(buildKeys.head.dataType, buildKeys, rows, sizeHint)
        } else {
          IntervalIndex(buildKeys.head.dataType, buildKeys, rows, sizeHint)
        }

      case PointIndexKind =>
        require(buildKeys.length == 1, "A point index expects one build key.")
        if (longKeys) {
          LongPointIndex(buildKeys.head.dataType, buildKeys, rows, sizeHint)
        } else {
          PointIndex(buildKeys.head.dataType, buildKeys, rows, sizeHint)
        }
    }
  }

  override lazy val canonicalized: RangeRelationBroadcastMode =
    RangeRelationBroadcastMode(buildKeys.map(_.canonicalized), indexKind)

  def copy(
      buildKeys: Seq[Expression] = buildKeys,
      indexKind: RangeIndexKind = indexKind): RangeRelationBroadcastMode =
    RangeRelationBroadcastMode(buildKeys, indexKind)
}

private[execution] object RangeRelationBroadcastMode {

  def apply(buildKeys: Seq[Expression], indexKind: RangeIndexKind): RangeRelationBroadcastMode = {
    val normalizedKeys = buildKeys.map { key =>
      key.transform {
        case b: BoundReference => b.copy(nullable = true)
      }
    }
    new RangeRelationBroadcastMode(normalizedKeys, indexKind)
  }
}
