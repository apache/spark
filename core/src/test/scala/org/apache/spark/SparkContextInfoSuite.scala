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

class SparkContextInfoSuite extends FunSuite with LocalSparkContext {
  test("getPersistentRDDs only returns RDDs that are marked as cached") {
    sc = new SparkContext("local", "test")
    assert(sc.getPersistentRDDs.isEmpty === true)

    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2)
    assert(sc.getPersistentRDDs.isEmpty === true)

    rdd.cache()
    assert(sc.getPersistentRDDs.size === 1)
    assert(sc.getPersistentRDDs.values.head === rdd)
  }

  test("getPersistentRDDs returns an immutable map") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()

    val myRdds = sc.getPersistentRDDs
    assert(myRdds.size === 1)
    assert(myRdds.values.head === rdd1)

    val rdd2 = sc.makeRDD(Array(5, 6, 7, 8), 1).cache()

    // getPersistentRDDs should have 2 RDDs, but myRdds should not change
    assert(sc.getPersistentRDDs.size === 2)
    assert(myRdds.size === 1)
  }

  test("getRDDStorageInfo only reports on RDDs that actually persist data") {
    sc = new SparkContext("local", "test")
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()

    assert(sc.getRDDStorageInfo.size === 0)

    rdd.collect()
    assert(sc.getRDDStorageInfo.size === 1)
  }
}
