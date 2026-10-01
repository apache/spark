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
package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.sql.catalyst.expressions.{Attribute, Predicate}
import org.apache.spark.sql.execution.streaming.operators.stateful.WatermarkSupport

/**
 * An iterator over the evictable rows of a [[StateStore]], which removes each row it returns and
 * reports how far it has progressed.
 *
 * Only the rows that are actually evicted are returned, so every row this iterator yields has been
 * removed from the store. The removal happens in `hasNext` rather than `next` (see the note on
 * `pending` below) precisely so that a caller which stops early still leaves the store consistent
 * with what it observed. That lets a caller both emit the evicted rows -- streaming aggregation in
 * append mode outputs a grouping key once the watermark passes it -- and count real removals rather
 * than state rows scanned.
 */
trait EvictionIterator extends Iterator[UnsafeRowPair] {
  /** Number of state rows examined so far, whether or not they were evicted. */
  def numRowsReadDuringEvictionSoFar: Long

  /** Number of state rows removed so far. */
  def numRowsRemovedSoFar: Long
}

object EvictionIterator {

  /**
   * Returns an [[EvictionIterator]] over the rows of `store` whose event time is older than
   * `evictionTimestamp`, removing each row as it is returned.
   *
   * The event time is read from the state store key, using the watermark metadata on
   * `keyExpressions`. If those attributes carry no event time column, or `evictionTimestamp` is
   * empty, nothing can be evicted and the iterator is empty.
   *
   * Note `evictionTimestamp` is not necessarily the current watermark: a caller doing incremental
   * cleanup may pass an earlier timestamp, before which no further input can arrive.
   */
  def apply(
      store: StateStore,
      storeIterator: Iterator[UnsafeRowPair],
      keyExpressions: Seq[Attribute],
      allowMultipleEventTimeColumns: Boolean,
      evictionTimestamp: Option[Long]): EvictionIterator = {

    val evictionPredicate = WatermarkSupport.watermarkExpression(
      WatermarkSupport.findEventTimeColumn(keyExpressions, allowMultipleEventTimeColumns),
      evictionTimestamp).map { expr =>
      Predicate.create(expr, keyExpressions)
    }

    new EvictionIterator {
      private var rowsRead = 0L
      private var rowsRemoved = 0L

      override def numRowsReadDuringEvictionSoFar: Long = rowsRead
      override def numRowsRemovedSoFar: Long = rowsRemoved

      // The row hasNext has advanced to and already removed from the store, held so next() can
      // return it. Removal happens in hasNext (not next()) so that a caller which stops iterating
      // after hasNext -- without the matching next() -- still leaves the store consistent with the
      // rows it was told are evicted; every row this iterator surfaces has already been removed.
      private var pending: Option[UnsafeRowPair] = None

      override def hasNext: Boolean = evictionPredicate match {
        case Some(predicate) =>
          while (pending.isEmpty && storeIterator.hasNext) {
            val rowPair = storeIterator.next()
            rowsRead += 1
            if (predicate.eval(rowPair.key)) {
              store.remove(rowPair.key)
              rowsRemoved += 1
              pending = Some(rowPair)
            }
          }
          pending.isDefined
        case None => false
      }

      override def next(): UnsafeRowPair = {
        if (!hasNext) {
          throw new NoSuchElementException("End of the iterator")
        }
        val rowPair = pending.get
        pending = None
        rowPair
      }
    }
  }
}
