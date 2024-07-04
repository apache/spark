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
    val limitFunc = rankLikeFunction match {
      case _: RowNumber =>
        (iter: Iterator[InternalRow]) => SimpleLimitIterator(iter, limit, numOutputRows)
      case _: Rank =>
        (iter: Iterator[InternalRow]) =>
          RankLimitIterator(childOutput, iter, orderSpec, limit, numOutputRows)
      case _: DenseRank =>
        (iter: Iterator[InternalRow]) =>
          DenseRankLimitIterator(childOutput, iter, orderSpec, limit, numOutputRows)
    }

    if (partitionSpec.isEmpty) {
      new WindowGroupLimitPartitionEvaluator(limitFunc)
    } else {
      new WindowGroupLimitPartitionEvaluator(
        input => new GroupedLimitIterator(input, childOutput, partitionSpec, limitFunc))
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
