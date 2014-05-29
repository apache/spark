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

import org.apache.spark._

class IndexedRDDSuite extends FunSuite with SharedSparkContext {

  def pairs(sc: SparkContext, n: Int) = {
    IndexedRDD(sc.parallelize((0 to n).map(x => (x.toLong, x)), 5))
  }

  test("filter") {
    val n = 100
    val ps = pairs(sc, n)
    val evens = ps.filter(q => ((q._2 % 2) == 0))
    assert(evens.count === (0 to n).filter(_ % 2 == 0).size)
  }

  test("mapValues") {
    val n = 100
    val ps = pairs(sc, n)
    val negatives = ps.mapValues(x => -x).cache() // Allow joining b with a derived RDD of b
    assert(negatives.count === n + 1)
  }

  test("diff") {
    val n = 100
    val ps = pairs(sc, n).cache()
    val flipEvens = ps.mapValues(x => if (x % 2 == 0) -x else x).cache()
    // diff should keep only the changed pairs
    assert(ps.diff(flipEvens).map(_._2).collect().toSet === (2 to n by 2).map(-_).toSet)
    // diff should keep the vertex values from `other`
    assert(flipEvens.diff(ps).map(_._2).collect().toSet === (2 to n by 2).toSet)
  }

  test("leftJoin") {
    val n = 100
    val ps = pairs(sc, n).cache()
    val evens = ps.filter(q => ((q._2 % 2) == 0)).cache()
    // leftJoin with another IndexedRDD
    assert(ps.leftJoin(evens) { (id, a, bOpt) => a - bOpt.getOrElse(0) }.collect.toSet ===
      (0 to n by 2).map(x => (x.toLong, 0)).toSet ++ (1 to n by 2).map(x => (x.toLong, x)).toSet)
    // leftJoin with an RDD
    val evensRDD = evens.map(identity)
    assert(ps.leftJoin(evensRDD) { (id, a, bOpt) => a - bOpt.getOrElse(0) }.collect.toSet ===
      (0 to n by 2).map(x => (x.toLong, 0)).toSet ++ (1 to n by 2).map(x => (x.toLong, x)).toSet)
  }

  test("join") {
    val n = 100
    val ps = pairs(sc, n).cache()
    val evens = ps.filter(q => ((q._2 % 2) == 0)).cache()
    // join with another VertexRDD
    assert(ps.join(evens) { (id, a, b) => a - b }.collect.toSet ===
      (0 to n by 2).map(x => (x.toLong, 0)).toSet ++ (1 to n by 2).map(x => (x.toLong, x)).toSet)
    // join with an RDD
    val evensRDD = evens.map(identity)
    assert(ps.join(evensRDD) { (id, a, b) => a - b }.collect.toSet ===
      (0 to n by 2).map(x => (x.toLong, 0)).toSet ++ (1 to n by 2).map(x => (x.toLong, x)).toSet)
  }

  test("innerJoin") {
    val n = 100
    val ps = pairs(sc, n).cache()
    val evens = ps.filter(q => ((q._2 % 2) == 0)).cache()
    // innerJoin with another IndexedRDD
    assert(ps.innerJoin(evens) { (id, a, b) => a - b }.collect.toSet ===
      (0 to n by 2).map(x => (x.toLong, 0)).toSet)
    // innerJoin with an RDD
    val evensRDD = evens.map(identity)
    assert(ps.innerJoin(evensRDD) { (id, a, b) => a - b }.collect.toSet ===
     (0 to n by 2).map(x => (x.toLong, 0)).toSet)
  }

  test("aggregateUsingIndex") {
    val n = 100
    val ps = pairs(sc, n)
    val messageTargets = (0 to n) ++ (0 to n by 2)
    val messages = sc.parallelize(messageTargets.map(x => (x.toLong, 1)))
    assert(ps.aggregateUsingIndex[Int](messages, _ + _).collect.toSet ===
      (0 to n).map(x => (x.toLong, if (x % 2 == 0) 2 else 1)).toSet)
  }

}
