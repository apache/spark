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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.exchange.KeySketcher
import org.apache.spark.sql.test.SharedSparkSession


/**
 * Test suite for [[ShuffleExecAccumulator]].
 */
class ShuffleExecAccumulatorSuite
  extends SparkFunSuite
    with SharedSparkSession
    with ExpressionEvalHelper {

  test("KeySketcher") {
    val numPartitions = 10
    val numTasks = 3

    val a = 'a.int
    val b = 'b.long
    val c = 'c.double

    val rowSchema = Seq(a, b, c)
    val keySchema = Seq(a, b)
      .zipWithIndex
      .map { case (attr, i) => BoundReference(i, attr.dataType, attr.nullable) }

    val partitionIdExpr = Pmod(new Murmur3Hash(keySchema), Literal(numPartitions))

    val rows = Seq(
      InternalRow(1, 73L, 3.0),
      InternalRow(2, 36L, -3.0),
      InternalRow(3, -66L, 4.0),
      InternalRow(4, -3L, 6.0)
    )

    val partitionIdSet = rows.map(row => partitionIdExpr.eval(row).asInstanceOf[Int]).toSet

    def pmod(a: Int, n: Int): Int = {
      val r = a % n
      if (r < 0) {(r + n) % n} else r
    }
    def getPartId(hash64: Long): Int = {
      pmod((hash64 >> 32).toInt, numPartitions)
    }


    // Initialization
    val acc = new KeySketcher(rowSchema, partitionIdExpr, numTasks, 128, 1, false)
    assert(acc.mapId == -1)
    assert(acc.isZero)
    assert(acc.numMappers == numTasks)
    assert(acc.numPartitions == numPartitions)


    // Merge another empty acc from driver
    val accEmpty1 = acc.copyAndReset()
    acc.merge(accEmpty1)
    assert(acc.isZero)


    // Merge another empty acc from executor
    val accEmpty2 = acc.copyAndReset()
    accEmpty2.mapId = 1
    acc.merge(accEmpty2)
    assert(acc.isZero)


    // Merge an accumulator
    val acc1 = acc.copyAndReset()
    acc1.onTaskStart()
    acc1.mapId = 1
    rows.foreach(acc1.add)
    acc1.onTaskEnd()
    assert(acc1.count == 4)
    assert(acc1.sampled == 4)

    assert(acc.isZero)
    acc.merge(acc1)
    assert(!acc.isZero)
    assert(acc.count == 4)
    assert(acc.sampled == 4)


    // Never merge an accumulator twice
    acc.merge(acc1)
    assert(acc.count == 4)
    assert(acc.sampled == 4)


    // Check partitionId
    assert(
      acc.sketchMap
        .iterator
        .map(_._1)
        .forall(h64 => partitionIdSet.contains(getPartId(h64)))
    )


    // Hot Key
    val acc2 = acc.copyAndReset()
    acc2.onTaskStart()
    acc2.mapId = 0
    val skewRow = InternalRow(321, 123L, -33.0)
    val skewHash = partitionIdExpr.eval(skewRow).asInstanceOf[Int]
    val rng = new scala.util.Random(45678)
    Iterator.range(0, 100000).map { i =>
      if (rng.nextDouble < 0.1) {
        skewRow.copy()
      } else {
        InternalRow(rng.nextInt, rng.nextLong, rng.nextDouble)
      }
    }.foreach(acc2.add)
    acc2.onTaskEnd()
    assert(acc2.count == 100000)
    assert(acc2.sampled == 128)

    acc.reset()
    acc.mapId = -1
    acc.merge(acc2)
    assert(acc.count == 100000)
    assert(acc.sampled == 128)

    // most frequent hash64
    val (h64, w) = acc.sketchMap.maxBy(_._2)
    assert(getPartId(h64) == skewHash)
    assert(7000 < w && w < 13000)


    // Reset
    acc.reset()
    assert(acc.isZero)
  }
}
