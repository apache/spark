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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.{Inner, Nearest}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{DataType, IntegerType}

class SortMergeAsOfJoinScannerSuite extends SparkFunSuite with LocalSparkContext {

  test("nearest scan stops at the first eligible row after the left key") {
    sc = new SparkContext("local", "SortMergeAsOfJoinScannerSuite", new SparkConf(false))
    val (matchedKeys, evaluations) = sc.parallelize(Seq(0), 1)
      .mapPartitions { _ =>
        val leftKey = AttributeReference("leftKey", IntegerType)()
        val rightKey = AttributeReference("rightKey", IntegerType)()
        val count = new AtomicInteger()
        val scanner = new SortMergeAsOfJoinScanner(
          leftIter = Iterator(InternalRow(98)),
          rightIter = (0 to 200 by 10).iterator.map(i => InternalRow(i)),
          leftOutput = Seq(leftKey),
          rightOutput = Seq(rightKey),
          leftKeys = Nil,
          rightKeys = Nil,
          leftSortExprs = Seq(leftKey),
          rightSortExprs = Seq(rightKey),
          asOfCondition = CountedEvaluation(Literal.TrueLiteral, count),
          joinType = Inner,
          residualCondition = None,
          numOutputRows = new SQLMetric("sum"),
          spillSize = new SQLMetric("sum"),
          direction = Nearest,
          inMemoryThreshold = 256,
          sizeInBytesSpillThreshold = Long.MaxValue,
          spillThreshold = 256)
        try {
          Iterator.single((scanner.iterator.map(_.getInt(1)).toList, count.get()))
        } finally {
          scanner.close()
        }
      }.collect().head

    // The scan reads the right rows 0 to 100 and stops at 100, the first row after 98.
    assert(matchedKeys == List(100))
    assert(evaluations == 11, s"evaluated the as-of condition $evaluations times")
  }
}

private[joins] case class CountedEvaluation(child: Expression, counter: AtomicInteger)
    extends UnaryExpression with CodegenFallback {
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = child.dataType
  override lazy val resolved: Boolean = child.resolved

  override def eval(input: InternalRow): Any = {
    counter.incrementAndGet()
    child.eval(input)
  }

  override protected def withNewChildInternal(newChild: Expression): CountedEvaluation =
    copy(child = newChild)
}
