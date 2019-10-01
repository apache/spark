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

import org.apache.spark.{SharedSparkContext, SparkFunSuite}

class RDDBarrierSuite extends SparkFunSuite with SharedSparkContext {

  test("create an RDDBarrier") {
    val rdd = sc.parallelize(1 to 10, 4)
    assert(rdd.isBarrier() === false)

    val rdd2 = rdd.barrier().mapPartitions(iter => iter)
    assert(rdd2.isBarrier())
  }

  test("create an RDDBarrier in the middle of a chain of RDDs") {
    val rdd = sc.parallelize(1 to 10, 4).map(x => x * 2)
    val rdd2 = rdd.barrier().mapPartitions(iter => iter).map(x => (x, x + 1))
    assert(rdd2.isBarrier())
  }

  test("RDDBarrier with shuffle") {
    val rdd = sc.parallelize(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions(iter => iter).repartition(2)
    assert(rdd2.isBarrier() === false)
  }
}
