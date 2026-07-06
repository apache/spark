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
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.{AccumulatorV2, LastAttemptAccumulator, Utils}

class LastAttemptAggregatingAccumulator private[execution](
    bufferSchema: Seq[DataType],
    initialValues: Seq[Expression],
    updateExpressions: Seq[Expression],
    mergeExpressions: Seq[Expression],
    resultExpressions: Seq[Expression],
    imperatives: Array[ImperativeAggregate],
    typedImperatives: Array[TypedImperativeAggregate[_]],
    conf: SQLConf)
  extends AggregatingAccumulator(
    bufferSchema,
    initialValues,
    updateExpressions,
    mergeExpressions,
    resultExpressions,
    imperatives,
    typedImperatives,
    conf)
  with LastAttemptAccumulator[InternalRow, InternalRow, InternalRow] {

  // Snapshot the schema before task serialization drops transient result expressions. On the driver,
  // the last-attempt merge path compares this accumulator with copies returned from tasks; those
  // task-side copies may no longer be able to lazily rebuild AggregatingAccumulator.schema.
  private val outputSchema = schema

  override protected def partialMergeVal: InternalRow = {
    // The aggregate buffer is mutable, so last-attempt tracking keeps a snapshot.
    if (buffer == null) null else buffer.copy()
  }

  override protected def partialMerge(otherVal: InternalRow): Unit = {
    if (otherVal != null) {
      mergeBuffer(otherVal)
    }
  }

  override protected def isMergeable(other: AccumulatorV2[_, _]): Boolean = other match {
    case o: LastAttemptAggregatingAccumulator => o.outputSchema == outputSchema
    case _ => false
  }

  override protected def accumulatorStoresUserData: Boolean = true

  override def copyAndReset(): LastAttemptAggregatingAccumulator =
    new LastAttemptAggregatingAccumulator(
      bufferSchema,
      initialValues,
      updateExpressions,
      mergeExpressions,
      resultExpressions,
      imperatives,
      typedImperatives,
      conf)

  override def add(v: InternalRow): Unit = {
    super.add(v)
    if (isAtDriverSide && !Utils.isInRunningSparkTask) {
      setValueIfOnDriverSide(value.copy())
    }
  }
}
