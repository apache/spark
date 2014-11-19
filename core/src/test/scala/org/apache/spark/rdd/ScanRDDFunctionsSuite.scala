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
import org.scalatest.Matchers

import org.apache.spark.{Logging, SharedSparkContext}
import org.apache.spark.SparkContext._

class ScanRDDFunctionsSuite extends FunSuite with SharedSparkContext with Matchers with Logging {

  test("empty") {
    val e = sc.emptyRDD[Int]

    // scanLeft is stable with any initial value
    assert(e.scanLeft(0)(_ + _).collect === Array(0))
    assert(e.scanLeft(77)(_ + _).collect === Array(77))

    // parallel scan requires an initial value that is
    // the identity element of the given scanning function
    assert(e.scan(0)(_ + _).collect === Array(0))
  }

  test("simple") {
    val rdd = sc.parallelize(Array(1, 2, 3, 4), 2)

    assert(rdd.scanLeft(0)(_ + _).collect === Array(0, 1, 3, 6, 10))
    assert(rdd.scanLeft(1)(_ + _).collect === Array(1, 2, 4, 7, 11))

    assert(rdd.scan(0)(_ + _).collect === Array(0, 1, 3, 6, 10))
  }

  test("multiply") {
    val rdd = sc.parallelize(Array(1, 2, 3, 4), 2)

    assert(rdd.scanLeft(1)(_ * _).collect === Array(1, 1, 2, 6, 24))
    assert(rdd.scanLeft(2)(_ * _).collect === Array(2, 2, 4, 12, 48))
    assert(rdd.scanLeft(0)(_ * _).collect === Array(0, 0, 0, 0, 0))

    assert(rdd.scan(1)(_ * _).collect === Array(1, 1, 2, 6, 24))
  }

  test("concatenate") {
    val rdd = sc.parallelize(Array("a", "b", "c", "d"), 4)

    assert(rdd.scanLeft("")(_ ++ _).collect === Array("", "a", "ab", "abc", "abcd"))
    assert(rdd.scanLeft("x")(_ ++ _).collect === Array("x", "xa", "xab", "xabc", "xabcd"))

    assert(rdd.scan("")(_ ++ _).collect === Array("", "a", "ab", "abc", "abcd"))
  }

  test("cumulative with raw") {
    val rdd = sc.parallelize(1 to 4, 2)
    val f = (x:(Int,Int), y:Int) => (y, y+x._2)
    assert(rdd.scanLeft((0,0))(f).collect === Array((0,0), (1,1), (2,3), (3,6), (4,10)))
  }

  test("empty partitions") {
    val pe = sc.emptyRDD[Int]
    val p1 = sc.parallelize(Array(1, 2), 1)
    val p2 = sc.parallelize(Array(3, 4), 1)

    assert((pe ++ p1 ++ p2).scanLeft(0)(_ + _).collect === Array(0, 1, 3, 6, 10))
    assert((p1 ++ pe ++ p2).scanLeft(0)(_ + _).collect === Array(0, 1, 3, 6, 10))
    assert((p1 ++ p2 ++ pe).scanLeft(0)(_ + _).collect === Array(0, 1, 3, 6, 10))

    assert((pe ++ pe ++ p1 ++ p2).scanLeft(0)(_ + _).collect === Array(0, 1, 3, 6, 10))
    assert((p1 ++ pe ++ pe ++ p2).scanLeft(0)(_ + _).collect === Array(0, 1, 3, 6, 10))
    assert((p1 ++ p2 ++ pe ++ pe).scanLeft(0)(_ + _).collect === Array(0, 1, 3, 6, 10))

    assert((pe).scanLeft(0)(_ + _).collect === Array(0))
    assert((pe ++ pe).scanLeft(0)(_ + _).collect === Array(0))
    assert((pe ++ pe ++ pe).scanLeft(0)(_ + _).collect === Array(0))

    assert((pe ++ p1 ++ p2).scan(0)(_ + _).collect === Array(0, 1, 3, 6, 10))
    assert((p1 ++ pe ++ p2).scan(0)(_ + _).collect === Array(0, 1, 3, 6, 10))
    assert((p1 ++ p2 ++ pe).scan(0)(_ + _).collect === Array(0, 1, 3, 6, 10))

    assert((pe ++ pe ++ p1 ++ p2).scan(0)(_ + _).collect === Array(0, 1, 3, 6, 10))
    assert((p1 ++ pe ++ pe ++ p2).scan(0)(_ + _).collect === Array(0, 1, 3, 6, 10))
    assert((p1 ++ p2 ++ pe ++ pe).scan(0)(_ + _).collect === Array(0, 1, 3, 6, 10))

    assert((pe).scan(0)(_ + _).collect === Array(0))
    assert((pe ++ pe).scan(0)(_ + _).collect === Array(0))
    assert((pe ++ pe ++ pe).scan(0)(_ + _).collect === Array(0))
  }

  test("randomized small") {
    val n = 20
    val rng = new scala.util.Random()

    for (unused <- 1 to 50) {
      val data = Array.fill(n) { rng.nextInt(100) }
      val rdd = sc.parallelize(data, 1 + rng.nextInt(n))
      assert(rdd.scanLeft(0)(_ + _).collect === data.scanLeft(0)(_ + _))
      assert(rdd.scan(0)(_ + _).collect === data.scan(0)(_ + _))
    }
  }

  test("randomized large") {
    val n = 1000
    val pmin = 32
    val pmax = 128
    val rng = new scala.util.Random()

    for (unused <- 1 to 50) {
      val data = Array.fill(n) { rng.nextInt(100) }
      val rdd = sc.parallelize(data, pmin + rng.nextInt(pmax - pmin))
      assert(rdd.scanLeft(0)(_ + _).collect === data.scanLeft(0)(_ + _))
      assert(rdd.scan(0)(_ + _).collect === data.scan(0)(_ + _))
    }
  }
}
