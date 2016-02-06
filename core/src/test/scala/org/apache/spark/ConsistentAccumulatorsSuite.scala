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

import scala.ref.WeakReference

import org.scalatest.Matchers

import org.apache.spark.scheduler._


class ConsistentAccumulatorSuite extends SparkFunSuite with Matchers with LocalSparkContext {

  test("single partition") {
    sc = new SparkContext("local", "test")
    val acc : ConsistentAccumulator[Int] = sc.consistentAccumulator(0)

    val a = sc.parallelize(1 to 20, 1)
    val b = a.map{x => acc += x; x}
    b.cache()
    b.count()
    acc.value should be (210)
  }

  test("map + cache + first + count") {
    sc = new SparkContext("local", "test")
    val acc : ConsistentAccumulator[Int] = sc.consistentAccumulator(0)

    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc += x; x}
    b.cache()
    b.first()
    acc.value should be > (0)
    b.collect()
    acc.value should be (210)
  }

  test ("basic accumulation"){
    sc = new SparkContext("local", "test")
    val acc : ConsistentAccumulator[Int] = sc.consistentAccumulator(0)

    val d = sc.parallelize(1 to 20)
    d.map{x => acc += x}.count()
    acc.value should be (210)

    val longAcc = sc.consistentAccumulator(0L)
    val maxInt = Integer.MAX_VALUE.toLong
    d.map{x => longAcc += maxInt + x; x}.count()
    longAcc.value should be (210L + maxInt * 20)
  }

  test ("basic accumulation flatMap"){
    sc = new SparkContext("local", "test")
    val acc : ConsistentAccumulator[Int] = sc.consistentAccumulator(0)

    val d = sc.parallelize(1 to 20)
    d.map{x => acc += x}.count()
    acc.value should be (210)

    val longAcc = sc.consistentAccumulator(0L)
    val maxInt = Integer.MAX_VALUE.toLong
    val c = d.flatMap{x =>
      longAcc += maxInt + x
      if (x % 2 == 0) {
        Some(x)
      } else {
        None
      }
    }.count()
    longAcc.value should be (210L + maxInt * 20)
    c should be (10)
  }

  test("map + map + count") {
    sc = new SparkContext("local", "test")
    val acc : ConsistentAccumulator[Int] = sc.consistentAccumulator(0)

    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc += x; x}
    val c = b.map{x => acc += x; x}
    c.count()
    acc.value should be (420)
  }

  test("first + count") {
    sc = new SparkContext("local", "test")
    val acc : ConsistentAccumulator[Int] = sc.consistentAccumulator(0)

    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc += x; x}
    b.first()
    b.count()
    acc.value should be (210)
  }

  test("map + count + count + map + count") {
    sc = new SparkContext("local", "test")
    val acc : ConsistentAccumulator[Int] = sc.consistentAccumulator(0)

    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc += x; x}
    b.count()
    acc.value should be (210)
    b.count()
    acc.value should be (210)
    val c = b.map{x => acc += x; x}
    c.count()
    acc.value should be (420)
  }

  test ("map + toLocalIterator + count"){
    sc = new SparkContext("local", "test")
    val acc : ConsistentAccumulator[Int] = sc.consistentAccumulator(0)

    val a = sc.parallelize(1 to 100, 10)
    val b = a.map{x => acc += x; x}
    // This depends on toLocalIterators per-partition fetch behaviour
    b.toLocalIterator.take(2).toList
    acc.value should be > (0)
    b.count()
    acc.value should be (5050)
    b.count()
    acc.value should be (5050)

    val c = b.map{x => acc += x; x}
    c.cache()
    c.toLocalIterator.take(2).toList
    acc.value should be > (5050)
    c.count()
    acc.value should be (10100)
  }

  test ("garbage collection") {
    // Create an accumulator and let it go out of scope to test that it's properly garbage collected
    sc = new SparkContext("local", "test")
    var acc: ConsistentAccumulator[Int] = sc.consistentAccumulator(0)
    val accId = acc.id
    val ref = WeakReference(acc)

    // Ensure the accumulator is present
    assert(ref.get.isDefined)

    // Remove the explicit reference to it and allow weak reference to get garbage collected
    acc = null
    System.gc()
    assert(ref.get.isEmpty)

    Accumulators.remove(accId)
    assert(!Accumulators.originals.get(accId).isDefined)
  }
}
