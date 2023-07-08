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

package org.apache.spark.sql.execution.window

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, DenseRank, Expression, Rank, RowNumber, SortOrder}
import org.apache.spark.sql.execution.metric.SQLMetric

class WindowGroupLimitEvaluatorFactory(
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    childOutput: Seq[Attribute],
    numOutputRows: SQLMetric)
  extends PartitionEvaluatorFactory[InternalRow, InternalRow] {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] = {
    rankLikeFunction match {
      case _: RowNumber if partitionSpec.isEmpty =>
        new WindowGroupLimitPartitionEvaluator(
          input => SimpleLimitIterator(input, limit, numOutputRows))
      case _: RowNumber =>
        new WindowGroupLimitPartitionEvaluator(
          input => new GroupedLimitIterator(input, childOutput, partitionSpec,
            (input: Iterator[InternalRow]) => SimpleLimitIterator(input, limit, numOutputRows)))
      case _: Rank if partitionSpec.isEmpty =>
        new WindowGroupLimitPartitionEvaluator(
          input => RankLimitIterator(childOutput, input, orderSpec, limit, numOutputRows))
      case _: Rank =>
        new WindowGroupLimitPartitionEvaluator(
          input => new GroupedLimitIterator(input, childOutput, partitionSpec,
            (input: Iterator[InternalRow]) =>
              RankLimitIterator(childOutput, input, orderSpec, limit, numOutputRows)))
      case _: DenseRank if partitionSpec.isEmpty =>
        new WindowGroupLimitPartitionEvaluator(
          input => DenseRankLimitIterator(childOutput, input, orderSpec, limit, numOutputRows))
      case _: DenseRank =>
        new WindowGroupLimitPartitionEvaluator(
          input => new GroupedLimitIterator(input, childOutput, partitionSpec,
            (input: Iterator[InternalRow]) =>
              DenseRankLimitIterator(childOutput, input, orderSpec, limit, numOutputRows)))
    }

  }

  class WindowGroupLimitPartitionEvaluator(f: Iterator[InternalRow] => Iterator[InternalRow])
    extends PartitionEvaluator[InternalRow, InternalRow] {

    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      f(inputs.head)
    }
  }
}
