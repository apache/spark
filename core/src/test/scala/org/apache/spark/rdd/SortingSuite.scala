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

import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.util.ArrayImplicits._

class SortingSuite extends SparkFunSuite with SharedSparkContext with Matchers {

  test("sortByKey") {
    val pairs = sc.parallelize(Seq((1, 0), (2, 0), (0, 0), (3, 0)), 2)
    assert(pairs.sortByKey().collect() === Array((0, 0), (1, 0), (2, 0), (3, 0)))
  }

  test("large array") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr.toImmutableArraySeq, 2)
    val sorted = pairs.sortByKey()
    assert(sorted.partitions.length === 2)
    assert(sorted.collect() === pairArr.sortBy(_._1))
  }

  test("large array with one split") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr.toImmutableArraySeq, 2)
    val sorted = pairs.sortByKey(true, 1)
    assert(sorted.partitions.length === 1)
    assert(sorted.collect() === pairArr.sortBy(_._1))
  }

  test("large array with many partitions") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr.toImmutableArraySeq, 2)
    val sorted = pairs.sortByKey(true, 20)
    assert(sorted.partitions.length === 20)
    assert(sorted.collect() === pairArr.sortBy(_._1))
  }

  test("sort descending") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr.toImmutableArraySeq, 2)
    assert(pairs.sortByKey(false).collect() === pairArr.sortWith((x, y) => x._1 > y._1))
  }

  test("sort descending with one split") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr.toImmutableArraySeq, 1)
    assert(pairs.sortByKey(false, 1).collect() === pairArr.sortWith((x, y) => x._1 > y._1))
  }

  test("sort descending with many partitions") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(1000) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr.toImmutableArraySeq, 2)
    assert(pairs.sortByKey(false, 20).collect() === pairArr.sortWith((x, y) => x._1 > y._1))
  }

  test("more partitions than elements") {
    val rand = new scala.util.Random()
    val pairArr = Array.fill(10) { (rand.nextInt(), rand.nextInt()) }
    val pairs = sc.parallelize(pairArr.toImmutableArraySeq, 30)
    assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))
  }

  test("empty RDD") {
    val pairArr = new Array[(Int, Int)](0)
    val pairs = sc.parallelize(pairArr.toImmutableArraySeq, 2)
    assert(pairs.sortByKey().collect() === pairArr.sortBy(_._1))
  }

  test("partition balancing") {
    val pairArr = (1 to 1000).map(x => (x, x)).toArray
    val sorted = sc.parallelize(pairArr.toImmutableArraySeq, 4).sortByKey()
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
    val sorted = sc.parallelize(pairArr.toImmutableArraySeq, 4).sortByKey(false)
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

  test("get a range of elements in a sorted RDD that is on one partition") {
    val pairArr = (1 to 1000).map(x => (x, x)).toArray
    val sorted = sc.parallelize(pairArr.toImmutableArraySeq, 10).sortByKey()
    val range = sorted.filterByRange(20, 40).collect()
    assert((20 to 40).toArray === range.map(_._1))
  }

  test("get a range of elements over multiple partitions in a descendingly sorted RDD") {
    val pairArr = (1000 to 1 by -1).map(x => (x, x)).toArray
    val sorted = sc.parallelize(pairArr.toImmutableArraySeq, 10).sortByKey(false)
    val range = sorted.filterByRange(200, 800).collect()
    assert((800 to 200 by -1).toArray === range.map(_._1))
  }

  test("get a range of elements in an array not partitioned by a range partitioner") {
    val pairArr = scala.util.Random.shuffle((1 to 1000).toList).map(x => (x, x))
    val pairs = sc.parallelize(pairArr, 10)
    val range = pairs.filterByRange(200, 800).collect()
    assert((800 to 200 by -1).toArray.sorted === range.map(_._1).sorted)
  }

  test("get a range of elements over multiple partitions but not taking up full partitions") {
    val pairArr = (1000 to 1 by -1).map(x => (x, x)).toArray
    val sorted = sc.parallelize(pairArr.toImmutableArraySeq, 10).sortByKey(false)
    val range = sorted.filterByRange(250, 850).collect()
    assert((850 to 250 by -1).toArray === range.map(_._1))
  }
}

