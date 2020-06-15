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

object ZippedPartitionsSuite {
  def procZippedData(i: Iterator[Int], s: Iterator[String], d: Iterator[Double]) : Iterator[Int] = {
    Iterator(i.toArray.size, s.toArray.size, d.toArray.size)
  }
}

class ZippedPartitionsSuite extends SparkFunSuite with SharedSparkContext {
  test("print sizes") {
    val data1 = sc.makeRDD(Seq(1, 2, 3, 4), 2)
    val data2 = sc.makeRDD(Seq("1", "2", "3", "4", "5", "6"), 2)
    val data3 = sc.makeRDD(Seq(1.0, 2.0), 2)

    val zippedRDD = data1.zipPartitions(data2, data3)(ZippedPartitionsSuite.procZippedData)

    val obtainedSizes = zippedRDD.collect()
    val expectedSizes = Array(2, 3, 1, 2, 3, 1)
    assert(obtainedSizes.size == 6)
    assert(obtainedSizes.zip(expectedSizes).forall(x => x._1 == x._2))
  }
}
