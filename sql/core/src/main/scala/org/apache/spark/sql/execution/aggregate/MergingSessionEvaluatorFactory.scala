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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, MutableProjection, NamedExpression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.metric.SQLMetric

class MergingSessionEvaluatorFactory(
    groupingExpressions: Seq[NamedExpression],
    sessionExpression: NamedExpression,
    output: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    numOutputRows: SQLMetric) extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] = {
    new MergingSessionEvaluator()
  }

  private class MergingSessionEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    def eval(partitionIndex: Int, inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      // Because the constructor of an aggregation iterator will read at least the first row,
      // we need to get the value of iter.hasNext first.
      val iter = inputs.head
      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator[UnsafeRow]()
      } else {
        val outputIter = new MergingSessionsIterator(
          partitionIndex,
          groupingExpressions,
          sessionExpression,
          output,
          iter,
          aggregateExpressions,
          aggregateAttributes,
          initialInputBufferOffset,
          resultExpressions,
          (expressions, inputSchema) =>
            MutableProjection.create(expressions, inputSchema),
          numOutputRows)
        if (!hasInput && groupingExpressions.isEmpty) {
          // There is no input and there is no grouping expressions.
          // We need to output a single row as the output.
          numOutputRows += 1
          Iterator[UnsafeRow](outputIter.outputForEmptyGroupingKeyWithoutInput())
        } else {
          outputIter
        }
      }
    }
  }
}
