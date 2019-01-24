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

package org.apache.spark.sql.execution.streaming.state.join

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, UnsafeRow}
import org.apache.spark.sql.execution.streaming.{StatefulOperatorStateInfo, StreamingSymmetricHashJoinExec}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.JoinSide
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreMetrics}

/**
 * Helper class to manage state required by a single side of [[StreamingSymmetricHashJoinExec]].
 * The interface of this class is basically that of a multi-map:
 * - Get: Returns an iterator of multiple values for given key
 * - Append: Append a new value to the given key
 * - Remove Data by predicate: Drop any state using a predicate condition on keys or values
 *
 * @param joinSide              Defines the join side
 * @param inputValueAttributes  Attributes of the input row which will be stored as value
 * @param joinKeys              Expressions to generate rows that will be used to key the value rows
 * @param stateInfo             Information about how to retrieve the correct version of state
 * @param storeConf             Configuration for the state store.
 * @param hadoopConf            Hadoop configuration for reading state data from storage
 */
trait StreamingJoinStateManager extends Serializable {
  import StreamingJoinStateManager._

  /** Get all the values of a key */
  def get(key: UnsafeRow): Iterator[UnsafeRow]

  /**
   * Get all the matched values for given join condition, with marking matched.
   * This method is designed to mark joined rows properly without exposing internal index of row.
   */
  def getJoinedRows(
      key: UnsafeRow,
      generateJoinedRow: InternalRow => JoinedRow,
      predicate: JoinedRow => Boolean): Iterator[JoinedRow]

  /** Append a new value to the key, with marking matched */
  def append(key: UnsafeRow, value: UnsafeRow, matched: Boolean): Unit

  /**
   * Remove using a predicate on keys.
   *
   * This produces an iterator over the (key, value, matched) tuples satisfying condition(key),
   * where the underlying store is updated as a side-effect of producing next.
   *
   * This implies the iterator must be consumed fully without any other operations on this manager
   * or the underlying store being interleaved.
   */
  def removeByKeyCondition(removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched]

  /**
   * Remove using a predicate on values.
   *
   * At a high level, this produces an iterator over the (key, (value, matched)) pairs such that
   * value satisfies the predicate, where producing an element removes the value from the state
   * store and producing all elements with a given key updates it accordingly.
   *
   * This implies the iterator must be consumed fully without any other operations on this manager
   * or the underlying store being interleaved.
   */
  def removeByValueCondition(removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched]

  /** Commit all the changes to all the state stores */
  def commit(): Unit

  /** Abort any changes to the state stores if needed */
  def abortIfNeeded(): Unit

  /** Get the combined metrics of all the state stores */
  def metrics: StateStoreMetrics
}

object StreamingJoinStateManager {
  val supportedVersions = Seq(1, 2)
  val legacyVersion = 1

  def createStateManager(
      joinSide: JoinSide,
      inputValueAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      stateInfo: Option[StatefulOperatorStateInfo],
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      stateFormatVersion: Int): StreamingJoinStateManager = stateFormatVersion match {
    case 1 => new StreamingJoinStateManagerImplV1(joinSide, inputValueAttributes, joinKeys,
      stateInfo, storeConf, hadoopConf)
    case 2 => new StreamingJoinStateManagerImplV2(joinSide, inputValueAttributes, joinKeys,
      stateInfo, storeConf, hadoopConf)
    case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
  }

  def allStateStoreNames(stateFormatVersion: Int, joinSides: JoinSide*): Seq[String] = {
    val allStateStoreTypes: Seq[StateStoreType] = stateFormatVersion match {
      case 1 => StreamingJoinStateManagerImplV1.allStateStoreTypes
      case 2 => StreamingJoinStateManagerImplV2.allStateStoreTypes
      case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
    }

    for (joinSide <- joinSides; stateStoreType <- allStateStoreTypes) yield {
      getStateStoreName(joinSide, stateStoreType)
    }
  }

  def getStateStoreName(joinSide: JoinSide, storeType: StateStoreType): String = {
    s"$joinSide-$storeType"
  }

  /**
   * Helper class for representing data key to (value, matched).
   * Designed for object reuse.
   */
  case class KeyToValueAndMatched(
      var key: UnsafeRow = null,
      var value: UnsafeRow = null,
      var matched: Option[Boolean] = None) {
    def withNew(newKey: UnsafeRow, newValue: UnsafeRow, newMatched: Option[Boolean]): this.type = {
      this.key = newKey
      this.value = newValue
      this.matched = newMatched
      this
    }
  }

  trait StateStoreType
}
