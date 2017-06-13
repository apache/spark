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

package org.apache.spark.util

import java.io.File

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.rdd.ZipPartitionsRDDUtils

class ZipPartitionsRDDUtilsSuite extends SparkFunSuite with SharedSparkContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("zipPartitionsWithPreferredLocation 2 rdds") {
    val rdd1 = sc.makeRDD(List((1, List("a")), (2, List("b")), (3, List("c"))))
    val rdd2 = sc.makeRDD(List((1, List("d")), (2, List("e")), (3, List("f"))))

    val res1 = ZipPartitionsRDDUtils.zipPartitionsWithPreferredLocation(rdd1, rdd2) {
      (iter1, iter2) => {
        iter1.zip(iter2).map(t => t._1 + t._2)
      }
    }
    assert(res1.collect() === Array(2, 4, 6))
    assert(res1.partitions.map(split => res1.preferredLocations(split))
      === Array(List("a"), List("b"), List("c")))

    val res2 = ZipPartitionsRDDUtils.zipPartitionsWithPreferredLocation(rdd2, rdd1) {
      (iter1, iter2) => {
        iter1.zip(iter2).map(t => t._1 + t._2)
      }
    }
    assert(res2.collect() === Array(2, 4, 6))
    assert(res2.partitions.map(split => res1.preferredLocations(split))
      === Array(List("d"), List("e"), List("f")))
  }

  test("zipPartitionsWithPreferredLocation 3 rdds") {
    val rdd1 = sc.makeRDD(List((1, List("a")), (2, List("b")), (3, List("c"))))
    val rdd2 = sc.makeRDD(List((1, List("d")), (2, List("e")), (3, List("f"))))
    val rdd3 = sc.makeRDD(List((1, List("g")), (2, List("h")), (3, List("i"))))

    val res1 = ZipPartitionsRDDUtils.zipPartitionsWithPreferredLocation(rdd1, rdd2, rdd3) {
      (iter1, iter2, iter3) => {
        iter1.zip(iter2).zip(iter3).map(t => t._1._1 + t._1._2 + t._2)
      }
    }
    assert(res1.collect() === Array(3, 6, 9))
    assert(res1.partitions.map(split => res1.preferredLocations(split))
      === Array(List("a"), List("b"), List("c")))
  }

  test("zipPartitionsWithPreferredLocation 4 rdds") {
    val rdd1 = sc.makeRDD(List((1, List("a")), (2, List("b")), (3, List("c"))))
    val rdd2 = sc.makeRDD(List((1, List("d")), (2, List("e")), (3, List("f"))))
    val rdd3 = sc.makeRDD(List((1, List("g")), (2, List("h")), (3, List("i"))))
    val rdd4 = sc.makeRDD(List((1, List("j")), (2, List("k")), (3, List("l"))))

    val res1 = ZipPartitionsRDDUtils.zipPartitionsWithPreferredLocation(rdd1, rdd2, rdd3, rdd4) {
      (iter1, iter2, iter3, iter4) => {
        iter1.zip(iter2).zip(iter3.zip(iter4)).map(t => t._1._1 + t._1._2 + t._2._1 + t._2._2)
      }
    }
    assert(res1.collect() === Array(4, 8, 12))
    assert(res1.partitions.map(split => res1.preferredLocations(split))
      === Array(List("a"), List("b"), List("c")))
  }

  test("zipPartitionsWithPreferredLocation 3 rdds when major rdd preferred locations are Nil") {
    val rdd1 = sc.makeRDD(List((1, Nil), (2, Nil), (3, Nil)))
    val rdd2 = sc.makeRDD(List((1, List("d")), (2, List("e")), (3, List("f"))))
    val rdd3 = sc.makeRDD(List((1, List("g")), (2, List("h")), (3, List("i"))))

    val res1 = ZipPartitionsRDDUtils.zipPartitionsWithPreferredLocation(rdd1, rdd2, rdd3) {
      (iter1, iter2, iter3) => {
        iter1.zip(iter2).zip(iter3).map(t => t._1._1 + t._1._2 + t._2)
      }
    }
    assert(res1.collect() === Array(3, 6, 9))

    // This case zipPartitionsWithPreferredLocation will fallback using
    // default zipPartitions preferred locations strategy.
    assert(res1.partitions.map(split => res1.preferredLocations(split))
      === Array(List("d", "g"), List("e", "h"), List("f", "i")))
  }
}
