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

import scala.collection.mutable

import org.apache.spark.{LocalSparkContext, SparkContext, SparkFunSuite, TaskContext}

class MapPartitionsWithPreparationRDDSuite extends SparkFunSuite with LocalSparkContext {

  test("prepare called before parent partition is computed") {
    sc = new SparkContext("local", "test")

    // Have the parent partition push a number to the list
    val parent = sc.parallelize(1 to 100, 1).mapPartitions { iter =>
      TestObject.things.append(20)
      iter
    }

    // Push a different number during the prepare phase
    val preparePartition = () => { TestObject.things.append(10) }

    // Push yet another number during the execution phase
    val executePartition = (
        taskContext: TaskContext,
        partitionIndex: Int,
        notUsed: Unit,
        parentIterator: Iterator[Int]) => {
      TestObject.things.append(30)
      TestObject.things.iterator
    }

    // Verify that the numbers are pushed in the order expected
    val rdd = new MapPartitionsWithPreparationRDD[Int, Int, Unit](
      parent, preparePartition, executePartition)
    val result = rdd.collect()
    assert(result === Array(10, 20, 30))

    TestObject.things.clear()
    // Zip two of these RDDs, both should be prepared before the parent is executed
    val rdd2 = new MapPartitionsWithPreparationRDD[Int, Int, Unit](
      parent, preparePartition, executePartition)
    val result2 = rdd.zipPartitions(rdd2)((a, b) => a).collect()
    assert(result2 === Array(10, 10, 20, 30, 20, 30))
  }

}

private object TestObject {
  val things = new mutable.ListBuffer[Int]
}
