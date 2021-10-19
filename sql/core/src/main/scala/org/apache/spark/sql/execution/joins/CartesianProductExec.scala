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

package org.apache.spark.sql.execution.joins

import org.apache.spark._
import org.apache.spark.rdd.{CartesianPartition, CartesianRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, Predicate, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.CompletionIterator

/**
 * An optimized CartesianRDD for UnsafeRow, which will cache the rows from second child RDD,
 * will be much faster than building the right partition for every row in left RDD, it also
 * materialize the right RDD (in case of the right RDD is nondeterministic).
 */
class UnsafeCartesianRDD(
    left : RDD[UnsafeRow],
    right : RDD[UnsafeRow],
    inMemoryBufferThreshold: Int,
    spillThreshold: Int)
  extends CartesianRDD[UnsafeRow, UnsafeRow](left.sparkContext, left, right) {

  override def compute(split: Partition, context: TaskContext): Iterator[(UnsafeRow, UnsafeRow)] = {
    val rowArray = new ExternalAppendOnlyUnsafeRowArray(inMemoryBufferThreshold, spillThreshold)

    val partition = split.asInstanceOf[CartesianPartition]
    rdd2.iterator(partition.s2, context).foreach(rowArray.add)

    // Create an iterator from rowArray
    def createIter(): Iterator[UnsafeRow] = rowArray.generateIterator()

    val resultIter =
      for (x <- rdd1.iterator(partition.s1, context);
           y <- createIter()) yield (x, y)
    CompletionIterator[(UnsafeRow, UnsafeRow), Iterator[(UnsafeRow, UnsafeRow)]](
      resultIter, rowArray.clear())
  }
}


case class CartesianProductExec(
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends BaseJoinExec {

  override def joinType: JoinType = Inner
  override def leftKeys: Seq[Expression] = Nil
  override def rightKeys: Seq[Expression] = Nil

  override def output: Seq[Attribute] = left.output ++ right.output

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val leftResults = left.execute().asInstanceOf[RDD[UnsafeRow]]
    val rightResults = right.execute().asInstanceOf[RDD[UnsafeRow]]

    val pair = new UnsafeCartesianRDD(
      leftResults,
      rightResults,
      conf.cartesianProductExecBufferInMemoryThreshold,
      conf.cartesianProductExecBufferSpillThreshold)
    pair.mapPartitionsWithIndexInternal { (index, iter) =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      val filtered = if (condition.isDefined) {
        val boundCondition = Predicate.create(condition.get, left.output ++ right.output)
        boundCondition.initialize(index)
        val joined = new JoinedRow

        iter.filter { r =>
          boundCondition.eval(joined(r._1, r._2))
        }
      } else {
        iter
      }
      filtered.map { r =>
        numOutputRows += 1
        joiner.join(r._1, r._2)
      }
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): CartesianProductExec =
    copy(left = newLeft, right = newRight)
}
