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

package org.apache.spark.sql.execution.streaming

import scala.reflect.ClassTag

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.StructType

package object state {

  implicit class StateStoreOps[T: ClassTag](dataRDD: RDD[T]) {

    /** Map each partition of an RDD along with data in a [[StateStore]]. */
    def mapPartitionsWithStateStore[U: ClassTag](
        sqlContext: SQLContext,
        stateInfo: StatefulOperatorStateInfo,
        keySchema: StructType,
        valueSchema: StructType,
        indexOrdinal: Option[Int])(
        storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U]): StateStoreRDD[T, U] = {

      mapPartitionsWithStateStore(
        stateInfo,
        keySchema,
        valueSchema,
        indexOrdinal,
        sqlContext.sessionState,
        Some(sqlContext.streams.stateStoreCoordinator))(
        storeUpdateFunction)
    }

    /** Map each partition of an RDD along with data in a [[StateStore]]. */
    private[streaming] def mapPartitionsWithStateStore[U: ClassTag](
        stateInfo: StatefulOperatorStateInfo,
        keySchema: StructType,
        valueSchema: StructType,
        indexOrdinal: Option[Int],
        sessionState: SessionState,
        storeCoordinator: Option[StateStoreCoordinatorRef])(
        storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U]): StateStoreRDD[T, U] = {

      val cleanedF = dataRDD.sparkContext.clean(storeUpdateFunction)
      val wrappedF = (store: StateStore, iter: Iterator[T]) => {
        // Abort the state store in case of error
        TaskContext.get().addTaskCompletionListener(_ => {
          if (!store.hasCommitted) store.abort()
        })
        cleanedF(store, iter)
      }

      new StateStoreRDD(
        dataRDD,
        wrappedF,
        stateInfo.checkpointLocation,
        stateInfo.queryRunId,
        stateInfo.operatorId,
        stateInfo.storeVersion,
        keySchema,
        valueSchema,
        indexOrdinal,
        sessionState,
        storeCoordinator)
    }
  }

  sealed trait StreamingAggregationStateManager extends Serializable {
    def getKey(row: InternalRow): UnsafeRow
    def getStateValueSchema: StructType
    def get(store: StateStore, key: UnsafeRow): UnsafeRow
    def put(store: StateStore, row: UnsafeRow): Unit
    def commit(store: StateStore): Long
    def remove(store: StateStore, key: UnsafeRow): Unit
    def iterator(store: StateStore): Iterator[UnsafeRowPair]
    def keys(store: StateStore): Iterator[UnsafeRow]
    def values(store: StateStore): Iterator[UnsafeRow]
  }

  object StreamingAggregationStateManager extends Logging {
    val supportedVersions = Seq(1, 2)
    val legacyVersion = 1

    def createStateManager(
        keyExpressions: Seq[Attribute],
        inputRowAttributes: Seq[Attribute],
        stateFormatVersion: Int): StreamingAggregationStateManager = {
      stateFormatVersion match {
        case 1 => new StreamingAggregationStateManagerImplV1(keyExpressions, inputRowAttributes)
        case 2 => new StreamingAggregationStateManagerImplV2(keyExpressions, inputRowAttributes)
        case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
      }
    }
  }

  abstract class StreamingAggregationStateManagerBaseImpl(
      protected val keyExpressions: Seq[Attribute],
      protected val inputRowAttributes: Seq[Attribute]) extends StreamingAggregationStateManager {

    @transient protected lazy val keyProjector =
      GenerateUnsafeProjection.generate(keyExpressions, inputRowAttributes)

    def getKey(row: InternalRow): UnsafeRow = keyProjector(row)

    override def commit(store: StateStore): Long = store.commit()

    override def remove(store: StateStore, key: UnsafeRow): Unit = store.remove(key)

    override def keys(store: StateStore): Iterator[UnsafeRow] = {
      // discard and don't convert values to avoid computation
      store.getRange(None, None).map(_.key)
    }
  }

  class StreamingAggregationStateManagerImplV1(
      keyExpressions: Seq[Attribute],
      inputRowAttributes: Seq[Attribute])
    extends StreamingAggregationStateManagerBaseImpl(keyExpressions, inputRowAttributes) {

    override def getStateValueSchema: StructType = inputRowAttributes.toStructType

    override def get(store: StateStore, key: UnsafeRow): UnsafeRow = {
      store.get(key)
    }

    override def put(store: StateStore, row: UnsafeRow): Unit = {
      store.put(getKey(row), row)
    }

    override def iterator(store: StateStore): Iterator[UnsafeRowPair] = {
      store.iterator()
    }

    override def values(store: StateStore): Iterator[UnsafeRow] = {
      store.iterator().map(_.value)
    }
  }

  class StreamingAggregationStateManagerImplV2(
      keyExpressions: Seq[Attribute],
      inputRowAttributes: Seq[Attribute])
    extends StreamingAggregationStateManagerBaseImpl(keyExpressions, inputRowAttributes) {

    private val valueExpressions: Seq[Attribute] = inputRowAttributes.diff(keyExpressions)
    private val keyValueJoinedExpressions: Seq[Attribute] = keyExpressions ++ valueExpressions
    private val needToProjectToRestoreValue: Boolean =
      keyValueJoinedExpressions != inputRowAttributes

    @transient private lazy val valueProjector =
      GenerateUnsafeProjection.generate(valueExpressions, inputRowAttributes)

    @transient private lazy val joiner =
      GenerateUnsafeRowJoiner.create(StructType.fromAttributes(keyExpressions),
        StructType.fromAttributes(valueExpressions))
    @transient private lazy val restoreValueProjector = GenerateUnsafeProjection.generate(
      keyValueJoinedExpressions, inputRowAttributes)

    override def getStateValueSchema: StructType = valueExpressions.toStructType

    override def get(store: StateStore, key: UnsafeRow): UnsafeRow = {
      val savedState = store.get(key)
      if (savedState == null) {
        return savedState
      }

      val joinedRow = joiner.join(key, savedState)
      if (needToProjectToRestoreValue) {
        restoreValueProjector(joinedRow)
      } else {
        joinedRow
      }
    }

    override def put(store: StateStore, row: UnsafeRow): Unit = {
      val key = keyProjector(row)
      val value = valueProjector(row)
      store.put(key, value)
    }

    override def iterator(store: StateStore): Iterator[UnsafeRowPair] = {
      store.iterator().map(rowPair => new UnsafeRowPair(rowPair.key, restoreOriginRow(rowPair)))
    }

    override def values(store: StateStore): Iterator[UnsafeRow] = {
      store.iterator().map(rowPair => restoreOriginRow(rowPair))
    }

    private def restoreOriginRow(rowPair: UnsafeRowPair): UnsafeRow = {
      val joinedRow = joiner.join(rowPair.key, rowPair.value)
      if (needToProjectToRestoreValue) {
        restoreValueProjector(joinedRow)
      } else {
        joinedRow
      }
    }
  }

}
