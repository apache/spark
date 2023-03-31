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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.{SharedSparkContext, SparkFunSuite, TaskContext}

class UnionZipRDDSuite extends SparkFunSuite with SharedSparkContext {

  test("UnionZipRDD raises exception if children RDD partition size are not the same") {
    val rdd1 = sc.makeRDD(Seq(1, 2, 3, 4), numSlices = 2)
    val rdd2 = sc.makeRDD(Seq(5, 6, 7, 8), numSlices = 3)
    intercept[IllegalArgumentException] {
      new UnionZipRDD(sc, Seq(rdd1, rdd2))
    }
  }

  test("UnionZipRDD partition data") {
    val rdd1 = sc.makeRDD(Seq(1, 2, 3, 4), numSlices = 2)
    val rdd2 = sc.makeRDD(Seq(5, 6, 7, 8), numSlices = 2)
    val unionZipRdd = new UnionZipRDD(sc, Seq(rdd1, rdd2))
    val tContext = TaskContext.empty()
    val partition1 = unionZipRdd.compute(unionZipRdd.partitions(0), tContext).toList
    val partition2 = unionZipRdd.compute(unionZipRdd.partitions(1), tContext).toList

    assert(unionZipRdd.partitions.length == 2)
    assert(partition1 == List(1, 2, 5, 6))
    assert(partition2 == List(3, 4, 7, 8))
  }
}
