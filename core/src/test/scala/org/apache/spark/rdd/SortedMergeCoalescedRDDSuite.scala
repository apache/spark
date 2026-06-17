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

class SortedMergeCoalescedRDDSuite extends SparkFunSuite with SharedSparkContext {

  test("SPARK-55715: k-way merge maintains ordering - integers") {
    // Create RDD with 4 partitions, each sorted
    val data = Seq(
      Seq(1, 5, 9, 13),  // partition 0
      Seq(2, 6, 10, 14), // partition 1
      Seq(3, 7, 11, 15), // partition 2
      Seq(4, 8, 12, 16)  // partition 3
    )

    val rdd = sc.parallelize(data, data.size).flatMap(identity)

    // Coalesce to 2 partitions using sorted merge
    val coalescer = new TestPartitionCoalescer(Seq(Seq(0, 1), Seq(2, 3)))
    val merged = new SortedMergeCoalescedRDD[Int](rdd, 2, coalescer, Ordering.Int)

    // Verify per-partition contents: group (0,1) merges elements from partitions 0+1,
    // group (2,3) merges elements from partitions 2+3
    val partitionData = merged
      .mapPartitionsWithIndex { (idx, iter) => Iterator.single((idx, iter.toSeq)) }
      .collect().toMap

    assert(partitionData(0) === Seq(1, 2, 5, 6, 9, 10, 13, 14))
    assert(partitionData(1) === Seq(3, 4, 7, 8, 11, 12, 15, 16))
  }

  test("SPARK-55715: k-way merge handles empty partitions") {
    val data = Seq(
      Seq(1, 5, 9),   // partition 0
      Seq.empty[Int], // partition 1 - empty
      Seq(3, 7, 11),  // partition 2
      Seq.empty[Int]  // partition 3 - empty
    )

    val rdd = sc.parallelize(data, data.size).flatMap(identity)

    val coalescer = new TestPartitionCoalescer(Seq(Seq(0, 1, 2, 3)))
    val merged = new SortedMergeCoalescedRDD[Int]( rdd, 1, coalescer, Ordering.Int)

    val result = merged.collect()
    assert(result === Seq(1, 3, 5, 7, 9, 11))
  }

  test("SPARK-55715: k-way merge handles all-empty partitions in a group") {
    val data = Seq(
      Seq.empty[Int], // partition 0 - empty
      Seq.empty[Int], // partition 1 - empty
      Seq(1, 2, 3)    // partition 2 - non-empty (different group)
    )

    val rdd = sc.parallelize(data, data.size).flatMap(identity)

    val coalescer = new TestPartitionCoalescer(Seq(Seq(0, 1), Seq(2)))
    val merged = new SortedMergeCoalescedRDD[Int]( rdd, 2, coalescer, Ordering.Int)

    val partitionData = merged
      .mapPartitionsWithIndex { (idx, iter) => Iterator.single((idx, iter.toSeq)) }
      .collect().toMap

    assert(partitionData(0) === Seq.empty)
    assert(partitionData(1) === Seq(1, 2, 3))
  }

  test("SPARK-55715: k-way merge with single partition per group - no merge needed") {
    val data = Seq(1, 2, 3, 4, 5, 6)
    val rdd = sc.parallelize(data, 3)

    // Each group has only 1 partition - should just pass through
    val coalescer = new TestPartitionCoalescer(Seq(Seq(0), Seq(1), Seq(2)))
    val merged = new SortedMergeCoalescedRDD[Int](rdd, 3, coalescer, Ordering.Int)

    assert(merged.collect() === data)
  }

  test("SPARK-55715: k-way merge with reverse ordering") {
    val data = Seq(
      Seq(13, 9, 5, 1),  // partition 0 - descending
      Seq(14, 10, 6, 2), // partition 1 - descending
      Seq(15, 11, 7, 3), // partition 2 - descending
      Seq(16, 12, 8, 4)  // partition 3 - descending
    )

    val rdd = sc.parallelize(data, data.size).flatMap(identity)

    // Use reverse ordering
    val coalescer = new TestPartitionCoalescer(Seq(Seq(0, 1, 2, 3)))
    val merged = new SortedMergeCoalescedRDD[Int](
      rdd, 1, coalescer, Ordering.Int.reverse)

    val result = merged.collect()
    assert(result === (1 to 16).reverse)
  }

  test("SPARK-55715: k-way merge with many partitions") {
    val numPartitions = 20
    val rowsPerPartition = 50

    // Create sorted data where partition i starts at i and increments by numPartitions
    val data = (0 until numPartitions).map { partIdx =>
      (0 until rowsPerPartition).map(i => partIdx + i * numPartitions)
    }

    val rdd = sc.parallelize(data, numPartitions).flatMap(identity)

    // Coalesce all partitions into one
    val coalescer = new TestPartitionCoalescer(Seq((0 until numPartitions)))
    val merged = new SortedMergeCoalescedRDD[Int](rdd, 1, coalescer, Ordering.Int)

    val result = merged.collect()
    assert(result.length === numPartitions * rowsPerPartition)
    assert(result === result.sorted)
  }

  test("SPARK-55715: k-way merge preserves duplicate elements across partitions") {
    val data = Seq(
      Seq(1, 2, 3), // partition 0
      Seq(1, 2, 3), // partition 1 - identical to partition 0
      Seq(2, 2, 4)  // partition 2 - contains repeated value within partition
    )

    val rdd = sc.parallelize(data, data.size).flatMap(identity)

    val coalescer = new TestPartitionCoalescer(Seq(Seq(0, 1, 2)))
    val merged = new SortedMergeCoalescedRDD[Int](rdd, 1, coalescer, Ordering.Int)

    val result = merged.collect()
    assert(result === Seq(1, 1, 2, 2, 2, 2, 3, 3, 4))
  }

  test("SPARK-55715: k-way merge with strings") {
    val data = Seq(
      Seq("apple", "cherry", "grape"),
      Seq("banana", "date", "kiwi"),
      Seq("apricot", "fig", "mango")
    )

    val rdd = sc.parallelize(data, data.size).flatMap(identity)

    val coalescer = new TestPartitionCoalescer(Seq(Seq(0, 1, 2)))
    val merged = new SortedMergeCoalescedRDD[String](rdd, 1, coalescer, Ordering.String)

    val result = merged.collect()
    assert(result === result.sorted)
  }

  test("SPARK-55715: k-way merge with tuples") {
    val data = Seq(
      Seq((1, "a"), (3, "c"), (5, "e")),
      Seq((2, "b"), (4, "d"), (6, "f")),
      Seq((1, "z"), (4, "y"), (7, "x"))
    )

    val rdd = sc.parallelize(data, data.size).flatMap(identity)

    implicit val tupleOrdering: Ordering[(Int, String)] = Ordering.by[(Int, String), Int](_._1)

    val coalescer = new TestPartitionCoalescer(Seq(Seq(0, 1, 2)))
    val merged = new SortedMergeCoalescedRDD[(Int, String)](rdd, 1, coalescer, tupleOrdering)

    val result = merged.collect()
    val expected = data.flatten.sortBy(_._1)
    assert(result === expected)
  }

  test("SPARK-55715: SortedMergeIterator - next() on empty iterator throws " +
      "NoSuchElementException") {
    val iter = new SortedMergeIterator[Int](Seq.empty, Ordering.Int)
    assert(!iter.hasNext)
    intercept[NoSuchElementException] { iter.next() }
  }

  test("SPARK-55715: SortedMergeIterator - empty iterators list") {
    val iter = new SortedMergeIterator[Int](Seq(Iterator.empty, Iterator.empty), Ordering.Int)
    assert(!iter.hasNext)
    assert(iter.toSeq === Seq.empty)
  }

  test("SPARK-55715: SortedMergeIterator - single iterator passes through unchanged") {
    val iter = new SortedMergeIterator[Int](Seq(Iterator(3, 1, 2)), Ordering.Int)
    assert(iter.toSeq === Seq(3, 1, 2))
  }
}

/**
 * Test partition coalescer that groups partitions according to a predefined plan.
 */
class TestPartitionCoalescer(grouping: Seq[Seq[Int]]) extends PartitionCoalescer with Serializable {
  override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
    grouping.map { partitionIndices =>
      val pg = new PartitionGroup(None)
      partitionIndices.foreach { idx =>
        pg.partitions += parent.partitions(idx)
      }
      pg
    }.toArray
  }
}
