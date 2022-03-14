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
    // md: [[CartesianRDD]]里面是直接迭代两个Iterator，做join操作；但是对于超大的表来说性能不可接受；
    //  所以在这里通过一个数组来缓冲数据；
    val rowArray = new ExternalAppendOnlyUnsafeRowArray(inMemoryBufferThreshold, spillThreshold)

    // md: 对右表构建可复用的空间（在内存和磁盘中）来做迭代；
    //  为什么对右表做buffer，一般join-reorder之后形成left-deep tree，左表数量预估大于右表，所以缓存右表整体代价更低
    val partition = split.asInstanceOf[CartesianPartition]
    // md: 这里的rdd2，有可能在其他task也被计算过，会存在重复计算的
    rdd2.iterator(partition.s2, context).foreach(rowArray.add)

    // Create an iterator from rowArray
    def createIter(): Iterator[UnsafeRow] = rowArray.generateIterator()

    // md: 这里看起来是直接利用scala语言而创建一个迭代器(list或者seq)
    val resultIter =
      for (x <- rdd1.iterator(partition.s1, context);
           y <- createIter()) yield (x, y)
    CompletionIterator[(UnsafeRow, UnsafeRow), Iterator[(UnsafeRow, UnsafeRow)]](
      resultIter, rowArray.clear())
  }
}

// md: cartesianJoin会有shuffle吗？难道是在同一个进程来运行的吗？
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
    // md: 通过map创建一个新的RDD（拼接到整个rdd-tree上去），并包装了对依赖算子（底层）的调用，同时对迭代器数据嵌入当前算子真正的join处理逻辑；
    //  然后，在真正执行时通过对顶层的plan递归调用doExecute，就可以一层层的执行rdd，然后就会真正触发整个计算流（可以画个图）
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
