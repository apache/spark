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

package org.apache.spark

import org.scalatest.FunSuite
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{RDD, PartitionPruningRDD}


class PartitionPruningRDDSuite extends FunSuite with SharedSparkContext {

  test("Pruned Partitions inherit locality prefs correctly") {
    class TestPartition(i: Int) extends Partition {
      def index = i
    }
    val rdd = new RDD[Int](sc, Nil) {
      override protected def getPartitions = {
        Array[Partition](
            new TestPartition(1),
            new TestPartition(2), 
            new TestPartition(3))
      }
      def compute(split: Partition, context: TaskContext) = {Iterator()}
    }
    val prunedRDD = PartitionPruningRDD.create(rdd, {x => if (x==2) true else false})
    val p = prunedRDD.partitions(0)
    assert(p.index == 2)
    assert(prunedRDD.partitions.length == 1)
  }
}
