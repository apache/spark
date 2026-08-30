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

package org.apache.spark.sql.execution.python

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.unsafe.types.VariantVal

private[python] object PickledSizeAccumulator {
  // Pickle framing overhead per value (opcode + length prefix), a coarse constant.
  val PER_VALUE_OVERHEAD = 5L

  // Unknown leaf type (timestamp objects, intervals, ...): a small constant, never zero,
  // so the cap degrades to a loose bound instead of going blind.
  val UNKNOWN_VALUE_SIZE = 32L
}

/**
 * Accumulates a best-effort estimate of the pickled size of converted values, fed by the leaf
 * cases of [[EvaluatePython.toJava]] DURING conversion, so estimating costs no second traversal
 * of the fields. The source InternalRow cannot be used for sizing instead: EvalPythonExec feeds
 * this path from a MutableProjection whose target is a GenericInternalRow, so an UnsafeRow-based
 * source estimate never applies. String/binary/decimal/geo/variant payloads dominate pickled size
 * and are sized from their byte lengths at the catalyst leaves (UTF8String.numBytes is the UTF-8
 * byte count pickle writes); residual error (pickle overhead, memoization, unknown leaves) is
 * observable by comparing the pythonEstimatedInputBytes metric against the measured pythonDataSent.
 *
 * Single-threaded: one instance per partition, reset per row via [[getAndReset]].
 */
private[python] final class PickledSizeAccumulator {
  import PickledSizeAccumulator._

  private var _size = 0L

  /** Adds raw bytes (container/framing overhead). */
  def add(n: Long): Unit = _size += n

  /** Adds a leaf value with a known payload size, plus the per-value framing overhead. */
  def addValue(payloadBytes: Long): Unit = _size += payloadBytes + PER_VALUE_OVERHEAD

  /** Adds a pass-through leaf (boxed primitives and anything else toJava leaves untouched). */
  def addLeaf(value: Any): Unit = value match {
    case null => _size += 1L
    case _: java.lang.Boolean => _size += 2L
    case _: java.lang.Byte | _: java.lang.Short => _size += PER_VALUE_OVERHEAD
    case _: java.lang.Number => _size += 4L + PER_VALUE_OVERHEAD // Int/Long/Float/Double box
    case v: VariantVal =>
      // Variant payloads can be arbitrarily large; size by the serialized byte lengths.
      _size += v.getValue.length.toLong + v.getMetadata.length.toLong + PER_VALUE_OVERHEAD
    case _ => _size += UNKNOWN_VALUE_SIZE
  }

  /** Returns the accumulated estimate and resets, closing one per-row accounting cycle. */
  def getAndReset(): Long = {
    val s = _size
    _size = 0L
    s
  }
}

/**
 * Groups converted input objects into batches bounded by both a record count and an estimated
 * byte size (the second tuple element is the per-row estimate of the object's pickled size, see
 * [[PickledSizeAccumulator]], fed by toJava during conversion). A batch always holds at least one
 * row, so a single oversized row still forms a one-row batch.
 *
 * A batch is cut once its accumulated estimate reaches the cap, so it can exceed the cap by
 * the last row. Each cut batch increments `oversizedBatchMetric` once, and
 * `estimatedInputBytesMetric` accumulates the per-row estimates for comparison against the
 * measured pythonDataSent (the estimator-accuracy signal).
 */
private[python] class ByteBoundedAsArrayIterator(
    iter: Iterator[(Any, Long)],
    maxRecordsPerBatch: Int,
    maxBytesPerBatch: Long,
    oversizedBatchMetric: Option[SQLMetric],
    estimatedInputBytesMetric: Option[SQLMetric])
  extends Iterator[Array[Any]] {

  // Parity with the row-count batching path: a non-positive limit would loop forever emitting
  // empty batches. The config already enforces `> 0`; this is defensive.
  require(maxRecordsPerBatch > 0, "max records per batch must be positive")

  // Only finite positive caps may reach this class (BatchEvalPythonExec routes the -1 "no limit"
  // sentinel to the row-count-only batcher, and the config rejects 0). Defensive: a cap of 0
  // would silently degrade every batch to a single row instead of failing loudly.
  require(maxBytesPerBatch > 0, "max bytes per batch must be positive")

  override def hasNext: Boolean = iter.hasNext

  override def next(): Array[Any] = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    val batch = new ArrayBuffer[Any]()
    var accumulatedBytes = 0L
    var cut = false
    while (!cut && iter.hasNext && batch.length < maxRecordsPerBatch) {
      val (obj, sizeBytes) = iter.next()
      // Sum the raw estimate for estimator-accuracy analysis (vs pythonDataSent).
      estimatedInputBytesMetric.foreach(_ += sizeBytes)
      batch += obj
      accumulatedBytes += sizeBytes
      if (accumulatedBytes >= maxBytesPerBatch) {
        cut = true
      }
    }
    // Count each batch cut at the byte limit once.
    if (cut) oversizedBatchMetric.foreach(_ += 1)
    batch.toArray
  }
}
