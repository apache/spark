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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor}
import org.apache.spark.sql.types._
import org.apache.spark.util.CompletionIterator

/**
 * Physical operator for executing `TransformWithState`
 *
 * @param statefulProcessor processor methods called on underlying data
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 * @param groupingAttributes used to group the data
 * @param dataAttributes used to read the data
 * @param outputObjAttr Defines the output object
 * @param batchTimestampMs processing timestamp of the current batch.
 * @param eventTimeWatermarkForLateEvents event time watermark for filtering late events
 * @param eventTimeWatermarkForEviction event time watermark for state eviction
 * @param child the physical plan for the underlying data
 */
case class TransformWithStateExec(
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    statefulProcessor: StatefulProcessor[Any, Any, Any],
    outputMode: OutputMode,
    outputObjAttr: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo],
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    child: SparkPlan)
  extends UnaryExecNode
    with StateStoreWriter
    with WatermarkSupport
    with ObjectProducerExec {

  override def shortName: String = "transformWithStateExec"

  // TODO: fix this for existing state instances
  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = false

  override protected def withNewChildInternal(
    newChild: SparkPlan): TransformWithStateExec = copy(child = newChild)

  override def keyExpressions: Seq[Attribute] = groupingAttributes

  protected val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)

  protected val schemaForValueRow: StructType = new StructType().add("value", BinaryType)

  override def requiredChildDistribution: Seq[Distribution] = {
    StatefulOperatorPartitioning.getCompatibleDistribution(groupingAttributes,
      getStateInfo, conf) ::
      Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(
    groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    metrics
    child.execute().mapPartitionsWithStateStore[InternalRow](
      getStateInfo,
      schemaForKeyRow,
      schemaForValueRow,
      numColsPrefixKey = 0,
      session.sqlContext.sessionState,
      Some(session.sqlContext.streams.stateStoreCoordinator),
      useColumnFamilies = true
    ) {
      case (store: StateStore, singleIterator: Iterator[InternalRow]) =>
        CompletionIterator[InternalRow, Iterator[InternalRow]](null, Iterator.empty)
    }
  }
}
