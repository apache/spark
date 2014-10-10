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

package org.apache.spark.rdd

import org.scalatest.FunSuite

import org.apache.spark.{Partition, SharedSparkContext, TaskContext}

class PartitionPruningRDDSuite extends FunSuite with SharedSparkContext {


  test("Pruned Partitions inherit locality prefs correctly") {

    val rdd = new RDD[Int](sc, Nil) {
      override protected def getPartitions = {
        Array[Partition](
          new TestPartition(0, 1),
          new TestPartition(1, 1),
          new TestPartition(2, 1))
      }

      def compute(split: Partition, context: TaskContext) = {
        Iterator()
      }
    }
    val prunedRDD = PartitionPruningRDD.create(rdd, {
      x => if (x == 2) true else false
    })
    assert(prunedRDD.partitions.length == 1)
    val p = prunedRDD.partitions(0)
    assert(p.index == 0)
    assert(p.asInstanceOf[PartitionPruningRDDPartition].parentSplit.index == 2)
  }


  test("Pruned Partitions can be unioned ") {

    val rdd = new RDD[Int](sc, Nil) {
      override protected def getPartitions = {
        Array[Partition](
          new TestPartition(0, 4),
          new TestPartition(1, 5),
          new TestPartition(2, 6))
      }

      def compute(split: Partition, context: TaskContext) = {
        List(split.asInstanceOf[TestPartition].testValue).iterator
      }
    }
    val prunedRDD1 = PartitionPruningRDD.create(rdd, {
      x => if (x == 0) true else false
    })

    val prunedRDD2 = PartitionPruningRDD.create(rdd, {
      x => if (x == 2) true else false
    })

    val merged = prunedRDD1 ++ prunedRDD2
    assert(merged.count() == 2)
    val take = merged.take(2)
    assert(take.apply(0) == 4)
    assert(take.apply(1) == 6)
  }
}

class TestPartition(i: Int, value: Int) extends Partition with Serializable {
  def index = i

  def testValue = this.value

}
