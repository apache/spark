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
import org.apache.spark.SparkContext._
import org.apache.spark.util.Utils

class DropRDDFunctionsSuite extends FunSuite with SharedSparkContext {

  test("drop") {
    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)
    assert(rdd.drop(0).collect() === Array(1, 2, 3, 4, 5, 6))
    assert(rdd.drop(1).collect() === Array(2, 3, 4, 5, 6))
    assert(rdd.drop(2).collect() === Array(3, 4, 5, 6))
    assert(rdd.drop(3).collect() === Array(4, 5, 6))
    assert(rdd.drop(4).collect() === Array(5, 6))
    assert(rdd.drop(5).collect() === Array(6))
    assert(rdd.drop(6).collect() === Array())
    assert(rdd.drop(7).collect() === Array())
  }

  test("dropRight") {
    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)
    assert(rdd.dropRight(0).collect() === Array(1, 2, 3, 4, 5, 6))
    assert(rdd.dropRight(1).collect() === Array(1, 2, 3, 4, 5))
    assert(rdd.dropRight(2).collect() === Array(1, 2, 3, 4))
    assert(rdd.dropRight(3).collect() === Array(1, 2, 3))
    assert(rdd.dropRight(4).collect() === Array(1, 2))
    assert(rdd.dropRight(5).collect() === Array(1))
    assert(rdd.dropRight(6).collect() === Array())
    assert(rdd.dropRight(7).collect() === Array())
  }

  test("dropWhile") {
    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)
    assert(rdd.dropWhile(_ <= 0).collect() === Array(1, 2, 3, 4, 5, 6))
    assert(rdd.dropWhile(_ <= 1).collect() === Array(2, 3, 4, 5, 6))
    assert(rdd.dropWhile(_ <= 2).collect() === Array(3, 4, 5, 6))
    assert(rdd.dropWhile(_ <= 3).collect() === Array(4, 5, 6))
    assert(rdd.dropWhile(_ <= 4).collect() === Array(5, 6))
    assert(rdd.dropWhile(_ <= 5).collect() === Array(6))
    assert(rdd.dropWhile(_ <= 6).collect() === Array())
    assert(rdd.dropWhile(_ <= 7).collect() === Array())
  }

  test("empty input RDD") {
    val rdd = sc.emptyRDD[Int]

    assert(rdd.drop(0).collect() === Array())
    assert(rdd.drop(1).collect() === Array())

    assert(rdd.dropRight(0).collect() === Array())
    assert(rdd.dropRight(1).collect() === Array())

    assert(rdd.dropWhile((x:Int)=>false).collect() === Array())
    assert(rdd.dropWhile((x:Int)=>true).collect() === Array())
  }

  test("filtered and unioned input") {
    val consecutive = sc.makeRDD(Array(0, 1, 2, 3, 4, 5, 6, 7, 8), 3)
    val rdd0 = consecutive.filter((x:Int)=>(x % 3)==0)
    val rdd1 = consecutive.filter((x:Int)=>(x % 3)==1)
    val rdd2 = consecutive.filter((x:Int)=>(x % 3)==2)

    // input RDD:  0, 3, 6, 1, 4, 7, 2, 5, 8
    assert((rdd0 ++ rdd1 ++ rdd2).drop(6).collect() === Array(2, 5, 8))
    assert((rdd0 ++ rdd1 ++ rdd2).dropRight(6).collect() === Array(0, 3, 6))
    assert((rdd0 ++ rdd1 ++ rdd2).dropWhile(_ < 7).collect() === Array(7, 2, 5, 8))
  }
}
