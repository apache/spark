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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.ref.WeakReference

import org.scalatest.Matchers

import org.apache.spark.scheduler._
import org.apache.spark.util.{AccumulatorContext, AccumulatorMetadata, AccumulatorV2, LongAccumulator}


class DataPropertyAccumulatorSuite extends SparkFunSuite with Matchers with LocalSparkContext {
  test("two partition old and new") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true, "l2")

    val a = sc.parallelize(1 to 20, 2)
    val b = a.map{x => acc.add(x); x}
    b.cache()
    b.count()
    acc.value should be (210)
  }

  test("single partition") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)

    val a = sc.parallelize(1 to 20, 1)
    val b = a.map{x => acc.add(x); x}
    b.cache()
    b.count()
    acc.value should be (210)
  }

  test("adding only the first element per partition should work even if partition is empty") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)
    val a = sc.parallelize(1 to 20, 30)
    val b = a.mapPartitions{itr =>
      acc.add(1)
      itr
    }
    b.count()
    acc.value should be (30)
  }

  test("shuffled (combineByKey)") {
    sc = new SparkContext("local[2]", "test")
    val a = sc.parallelize(1L to 40L, 5)
    val buckets = 4
    val b = a.map{x => ((x % buckets), x)}
    val inputs = List(b, b.repartition(10), b.partitionBy(new HashPartitioner(5))).map(_.cache())
    val mapSideCombines = List(true, false)
    inputs.foreach { input =>
      mapSideCombines.foreach { mapSideCombine =>
        val accs = (1 to 4).map(x => sc.longAccumulator(dataProperty = true)).toList
        val raccs = (1 to 4).map(x => sc.longAccumulator(dataProperty = false)).toList
        val List(acc, acc1, acc2, acc3) = accs
        val List(racc, racc1, racc2, racc3) = raccs
        val c = input.combineByKey(
          (x: Long) => {acc1.add(1); acc.add(1); racc1.add(1); racc.add(1); x},
          {(a: Long, b: Long) => acc2.add(1); acc.add(1); racc2.add(1); racc.add(1); (a + b)},
          {(a: Long, b: Long) => acc3.add(1); acc.add(1); racc3.add(1); racc.add(1); (a + b)},
          new HashPartitioner(10),
          mapSideCombine)
        val d = input.combineByKey(
          (x: Long) => {acc1.add(1); acc.add(1); x},
          {(a: Long, b: Long) => acc2.add(1); acc.add(1); (a + b)},
          {(a: Long, b: Long) => acc3.add(1); acc.add(1); (a + b)},
          new HashPartitioner(2),
          mapSideCombine)
        val e = d.map{x => acc.add(1); x}
        c.count()
        // If our partitioner is known then we should only create
        // one combiner for each key value. Otherwise we should
        // create at least that many combiners.
        if (input.partitioner.isDefined) {
          acc1.value.toInt should be (buckets)
        } else {
          acc1.value.toInt should be >= (buckets)
        }
        if (input.partitioner.isDefined) {
          acc2.value.toInt should be > (0)
        } else if (mapSideCombine) {
          acc3.value.toInt should be > (0)
        } else {
          acc2.value.toInt should be > (0)
          acc3.value.toInt should be (0)
        }
        acc.value should be (acc1.value + acc2.value + acc3.value)
        val oldValues = accs.map(_.value)
        // For one action the data property accumulators and regular should have the same value.
        accs.map(_.value) should be (raccs.map(_.value))
        c.count()
        accs.map(_.value) should be (oldValues)
        // Executing a second _different_ aggregation should count everything 2x
        d.count()
        accs.map(_.value) should be (oldValues.map(_ * 2))
        // Computing the mapped value on top and verify new changes are processed and old changes
        // are note double counted.
        val count = e.count()
        accs.tail.map(_.value) should be (oldValues.tail.map(_ * 2))
        acc.value should be (oldValues.head * 2 + count)
      }
    }
  }

  test("map + cache + first + count") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)

    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc.add(x); x}
    b.cache()
    b.first()
    acc.value.toInt should be > (0)
    b.collect()
    acc.value should be (210L)
  }

  test("coalesce acc on either side") {
    sc = new SparkContext("local[2]", "test")
    val List(acc1, acc2, acc3) = (1 to 3).map(x => sc.longAccumulator(dataProperty = true)).toList
    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc1.add(x); acc2.add(x); 2 * x}
    val c = b.coalesce(2).map{x => acc1.add(x); acc3.add(x); x}
    c.count()
    acc1.value should be (630L)
    acc2.value should be (210L)
    acc3.value should be (420L)
  }

  test("map + cache + map + first + count") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)

    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc.add(x); x}
    b.cache()
    val c = b.map{x => acc.add(x); x}
    c.first()
    c.count()
    acc.value should be (420L)
  }


  test("filter + cache + first + count") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)

    val a = sc.parallelize(1 to 20, 10)
    val b = a.filter{x =>
      if (x % 2 == 0) {
        acc.add(1)
        true
      } else {
        false
      }
    }
    b.cache()
    b.first()
    acc.value.toInt should be > (0)
    b.collect()
    acc.value should be (10L)
  }

  test ("basic accumulation") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)

    val d = sc.parallelize(1 to 20)
    d.map{x => acc.add(x)}.count()
    acc.value should be (210L)

    val longAcc = sc.longAccumulator(dataProperty = true)
    val maxInt = Integer.MAX_VALUE.toLong
    d.map{x => longAcc.add(maxInt + x); x}.count()
    longAcc.value should be (210L + maxInt * 20)
  }

  test ("basic accumulation flatMap") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)

    val d = sc.parallelize(1 to 20)
    d.map{x => acc.add(x)}.count()
    acc.value should be (210L)

    val longAcc = sc.longAccumulator(dataProperty = true)
    val maxInt = Integer.MAX_VALUE.toLong
    val c = d.flatMap{x =>
      longAcc.add(maxInt + x)
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
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)

    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc.add(x); x}
    val c = b.map{x => acc.add(x); x}
    c.count()
    acc.value should be (420L)
  }

  test("first + count") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)

    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc.add(x); x}
    b.first()
    b.count()
    acc.value should be (210L)
  }

  test("map + count + count + map + count") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)

    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc.add(x); x}
    b.count()
    acc.value should be (210L)
    b.count()
    acc.value should be (210L)
    val c = b.map{x => acc.add(x); x}
    c.count()
    acc.value should be (420L)
  }

  test ("map + toLocalIterator + count") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)

    val a = sc.parallelize(1 to 100, 10)
    val b = a.map{x => acc.add(x); x}
    // This depends on toLocalIterators per-partition fetch behaviour
    b.toLocalIterator.take(2).toList
    acc.value.toInt should be > (0)
    b.count()
    acc.value should be (5050L)
    b.count()
    acc.value should be (5050L)

    val c = b.map{x => acc.add(x); x}
    c.cache()
    c.toLocalIterator.take(2).toList
    acc.value.toInt should be > (5050)
    c.count()
    acc.value should be (10100L)
  }

  test("coalesce with partial partition reading") {
    // when coalescing, one task can completely read partitions of the input RDDs while not reading
    // complete partitions of the post-coalesce RDD.  We make sure that the accumulator has
    // data property semantics in these cases.
    sc = new SparkContext("local[2]", "test")
    val List(acc1, acc2, acc3) = 1.to(3).map(x => sc.longAccumulator(dataProperty = true)).toList
    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc1.add(x); acc2.add(x); 2 * x}
    val c = b.coalesce(2).map{x => acc1.add(x); acc3.add(x); x}
    // we read all of partition 1 from RDD's a & b, and part of partition 2
    // however, for RDD c, we don't read any of the partitions fully
    // so we should get updates for 1 partition from b, and nothing from c
    c.take(3) should be ((1 to 3).map(_*2).toArray)
    acc1.value should be (3L) // only elements 1 & 2 came from fully read partitions
    acc2.value should be (3L)
    acc3.value should be (0L)

    // now we read a few more parts:
    c.take(9) should be ((1 to 9).map(_*2).toArray)
    acc1.value should be (36L)
    acc2.value should be (36L)
    acc3.value should be (0L)

    // a couple more, this time we read 1 of c's partitions fully
    c.take(15) should be ((1 to 15).map(_*2).toArray)
    acc1.value should be (215L)
    acc2.value should be (105L)
    acc3.value should be (110L)

    // and if we read the entire data, we get the entire value
    c.count()
    acc1.value should be (630L)
    acc2.value should be (210L)
    acc3.value should be (420L)
  }

  test("async jobs same rdd with data property accumulator") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator( dataProperty = true)
    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc.add(x)}
    val futures = List(b, b).map(_.countAsync)
    futures.foreach(_.onComplete{_ => acc.value should be (210)})
    futures.foreach{_.get()}
    acc.value should be (210L)
  }

  test("async jobs same rdd with data property accumulator, new API") {
    sc = new SparkContext("local[2]", "test")
    val acc = sc.longAccumulator(dataProperty = true)
    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc.add(x)}
    val futures = List(b, b).map(_.countAsync)
    futures.foreach(_.onComplete{_ => acc.value should be (210)})
    futures.foreach{_.get()}
    acc.value should be (210L)
  }

  test("async jobs shared parent with data property accumulators") {
    // We want to ensure that two data property jobs with a shared parent
    // work with data property accumulators.
    sc = new SparkContext("local[2]", "test")
    val List(acc1, acc2, acc3, acc4) = 1.to(4).map(x =>
      sc.longAccumulator(dataProperty = true)).toList
    val a = sc.parallelize(1 to 20, 10)
    val b = a.map{x => acc1.add(x); acc2.add(x); 2 * x}
    val c = b.map{x => acc3.add(x); acc2.add(x)}
    val d = b.map{x => acc4.add(x + 1); acc2.add(x)}
    val future1 = c.countAsync()
    val future2 = d.countAsync()
    future1.onComplete({_ =>
      acc1.value should be (210L)
      acc3.value should be (420L)
    })
    future2.onComplete({_ =>
      acc1.value should be (210L)
      acc4.value should be (440L)
    })
    val futures = List(future1, future2)
    futures.foreach{f => f.get()}
    acc1.value should be (210L)
    acc2.value should be (1050L)
    acc3.value should be (420L)
    acc4.value should be (440L)
  }

  test ("garbage collection") {
    // Create an accumulator and let it go out of scope to test that it's properly garbage collected
    sc = new SparkContext("local[2]", "test")
    var acc = sc.longAccumulator(dataProperty = true)
    val accId = acc.id
    val ref = WeakReference(acc)

    // Ensure the accumulator is present
    assert(ref.get.isDefined)

    // Remove the explicit reference to it and allow weak reference to get garbage collected
    acc = null
    System.gc()
    assert(ref.get.isEmpty)

    AccumulatorContext.remove(accId)
    assert(!AccumulatorContext.get(accId).isDefined)
  }
}
