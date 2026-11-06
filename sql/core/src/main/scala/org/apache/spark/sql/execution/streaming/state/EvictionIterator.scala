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
 * A row is removed when `next` returns it; `hasNext` only finds the next evictable row and does
 * not remove it. So a caller which stops early leaves every row it did not consume in the store,
 * and `numRowsRemovedSoFar` equals the number of rows returned. That lets a caller both emit the
 * evicted rows -- streaming aggregation in append mode outputs a grouping key once the watermark
 * passes it -- and count real removals rather than state rows scanned.
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

      // The next evictable row found by hasNext, not yet removed from the store.
      private var pending: Option[UnsafeRowPair] = None

      override def hasNext: Boolean = evictionPredicate match {
        case Some(predicate) =>
          while (pending.isEmpty && storeIterator.hasNext) {
            val rowPair = storeIterator.next()
            rowsRead += 1
            if (predicate.eval(rowPair.key)) {
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
        store.remove(rowPair.key)
        rowsRemoved += 1
        rowPair
      }
    }
  }
}
