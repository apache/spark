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

object SortingSuite {
  case class TimeValue(time: Int, value: Double)
}

class SortingSuite extends FunSuite with SharedSparkContext with Matchers with Logging {

  test("sortByKey") {
    val pairs = sc.parallelize(Array((1, 0), (2, 0), (0, 0), (3, 0)), 2)
    assert(pairs.sortByKey().collect() === Array((0,0), (1,0), (2,0), (3,0)))
  }

  test("large array") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    val sorted = pairs.sortByKey()
    assert(sorted.partitions.size === 2)
    assert(sorted.collect() === pairArr.sortBy(_._1))
  }

  test("large array with one split") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    val sorted = pairs.sortByKey(true, 1)
    assert(sorted.partitions.size === 1)
    assert(sorted.collect() === pairArr.sortBy(_._1))
  }

  test("large array with many partitions") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    val sorted = pairs.sortByKey(true, 20)
    assert(sorted.partitions.size === 20)
    assert(sorted.collect() === pairArr.sortBy(_._1))
  }

  test("sort descending") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    assert(pairs.sortByKey(false).collect() === pairArr.sortWith((x, y) => x._1 > y._1))
  }

  test("sort descending with one split") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 1)
    assert(pairs.sortByKey(false, 1).collect() === pairArr.sortWith((x, y) => x._1 > y._1))
  }

  test("sort descending with many partitions") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 2)
    assert(pairs.sortByKey(false, 20).collect() === pairArr.sortWith((x, y) => x._1 > y._1))
  }

  test("more partitions than elements") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(10) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr, 30)
    assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))
  }

  test("empty RDD") {
    val pairArr = new Array[(Int, Int)](0)
    val pairs = sc.parallelize(pairArr, 2)
    assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))
  }

  test("partition balancing") {
    val pairArr = (1 to 1000).map(x => (x, x)).toArray
    val sorted = sc.parallelize(pairArr, 4).sortByKey()
    assert(sorted.collect() === pairArr.sortBy(_._1))
    val partitions = sorted.collectPartitions()
    logInfo("Partition lengths: " + partitions.map(_.length).mkString(", "))
    val lengthArr = partitions.map(_.length)
    lengthArr.foreach { len =>
      assert(len > 100 && len < 400)
    }
    partitions(0).last should be < partitions(1).head
    partitions(1).last should be < partitions(2).head
    partitions(2).last should be < partitions(3).head
  }

  test("partition balancing for descending sort") {
    val pairArr = (1 to 1000).map(x => (x, x)).toArray
    val sorted = sc.parallelize(pairArr, 4).sortByKey(false)
    assert(sorted.collect() === pairArr.sortBy(_._1).reverse)
    val partitions = sorted.collectPartitions()
    logInfo("partition lengths: " + partitions.map(_.length).mkString(", "))
    val lengthArr = partitions.map(_.length)
    lengthArr.foreach { len =>
      assert(len > 100 && len < 400)
    }
    partitions(0).last should be > partitions(1).head
    partitions(1).last should be > partitions(2).head
    partitions(2).last should be > partitions(3).head
  }

  test("groupByKeyAndSortValues") {
    import SortingSuite._
    implicit val tvOrd = Ordering.by[TimeValue, Int](_.time)

    val tseries = sc.parallelize(Array(
      (5, TimeValue(2, 0.5)), (1, TimeValue(1, 1.2)), (5, TimeValue(1, 1.0)), 
      (1, TimeValue(2, 2.0)), (1, TimeValue(3, 3.0))
    ))
    val emas = tseries.groupByKeyAndSortValues(2).mapValues(_
      .foldLeft(0.0){ case (acc, TimeValue(time, value)) => 0.8 * acc + 0.2 * value }
    )
    assert(emas.collect.toSet === Set((1, 1.0736), (5, 0.26)))

    val pairs = sc.parallelize(Array((5, "c"), (1, "a"), (5, "b"), (1, "c"), (1, "b")))
    val top2 = pairs.groupByKeyAndSortValues(2).mapValues(_.toIterator.take(2).mkString(""))
    assert(top2.collect.toSet === Set((1, "ab"), (5, "bc")))

    val collusionPairs = sc.parallelize(Array(("Aa", 1), ("BB", 1), ("Aa", 2), ("BB", 2))) // Aa and BB have same hashCode
    val sortedLists = collusionPairs.groupByKeyAndSortValues(2).mapValues(_.toList)
    assert(sortedLists.collect.toSet === Set(("Aa", List(1, 2)), ("BB", List(1, 2))))
  }
}

