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

import java.io.IOException

import scala.collection.mutable.{ArrayBuffer, HashSet}
import scala.util.Random

import org.apache.commons.math3.distribution.{BinomialDistribution, PoissonDistribution}
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.{Job => NewJob, JobContext => NewJobContext,
  OutputCommitter => NewOutputCommitter, OutputFormat => NewOutputFormat,
  RecordWriter => NewRecordWriter, TaskAttemptContext => NewTaskAttempContext}
import org.apache.hadoop.util.Progressable
import org.scalatest.Assertions

import org.apache.spark._
import org.apache.spark.Partitioner
import org.apache.spark.util.Utils

class PairRDDFunctionsSuite extends SparkFunSuite with SharedSparkContext {
  test("aggregateByKey") {
    val pairs = sc.parallelize(Array((1, 1), (1, 1), (3, 2), (5, 1), (5, 3)), 2)

    val sets = pairs.aggregateByKey(new HashSet[Int]())(_ += _, _ ++= _).collect()
    assert(sets.size === 3)
    val valuesFor1 = sets.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1))
    val valuesFor3 = sets.find(_._1 == 3).get._2
    assert(valuesFor3.toList.sorted === List(2))
    val valuesFor5 = sets.find(_._1 == 5).get._2
    assert(valuesFor5.toList.sorted === List(1, 3))
  }

  test("groupByKey") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)))
    val groups = pairs.groupByKey().collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey with duplicates") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val groups = pairs.groupByKey().collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey with negative key hash codes") {
    val pairs = sc.parallelize(Array((-1, 1), (-1, 2), (-1, 3), (2, 1)))
    val groups = pairs.groupByKey().collect()
    assert(groups.size === 2)
    val valuesForMinus1 = groups.find(_._1 == -1).get._2
    assert(valuesForMinus1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey with many output partitions") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)))
    val groups = pairs.groupByKey(10).collect()
    assert(groups.size === 2)
    val valuesFor1 = groups.find(_._1 == 1).get._2
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("sampleByKey") {

    val defaultSeed = 1L

    // vary RDD size
    for (n <- List(100, 1000, 1000000)) {
      val data = sc.parallelize(1 to n, 2)
      val fractionPositive = 0.3
      val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))
      val samplingRate = 0.1
      StratifiedAuxiliary.testSample(stratifiedData, samplingRate, defaultSeed, n)
    }

    // vary fractionPositive
    for (fractionPositive <- List(0.1, 0.3, 0.5, 0.7, 0.9)) {
      val n = 100
      val data = sc.parallelize(1 to n, 2)
      val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))
      val samplingRate = 0.1
      StratifiedAuxiliary.testSample(stratifiedData, samplingRate, defaultSeed, n)
    }

    // Use the same data for the rest of the tests
    val fractionPositive = 0.3
    val n = 100
    val data = sc.parallelize(1 to n, 2)
    val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))

    // vary seed
    for (seed <- defaultSeed to defaultSeed + 5L) {
      val samplingRate = 0.1
      StratifiedAuxiliary.testSample(stratifiedData, samplingRate, seed, n)
    }

    // vary sampling rate
    for (samplingRate <- List(0.01, 0.05, 0.1, 0.5)) {
      StratifiedAuxiliary.testSample(stratifiedData, samplingRate, defaultSeed, n)
    }
  }

  test("sampleByKeyExact") {
    val defaultSeed = 1L

    // vary RDD size
    for (n <- List(100, 1000, 1000000)) {
      val data = sc.parallelize(1 to n, 2)
      val fractionPositive = 0.3
      val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))
      val samplingRate = 0.1
      StratifiedAuxiliary.testSampleExact(stratifiedData, samplingRate, defaultSeed, n)
    }

    // vary fractionPositive
    for (fractionPositive <- List(0.1, 0.3, 0.5, 0.7, 0.9)) {
      val n = 100
      val data = sc.parallelize(1 to n, 2)
      val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))
      val samplingRate = 0.1
      StratifiedAuxiliary.testSampleExact(stratifiedData, samplingRate, defaultSeed, n)
    }

    // Use the same data for the rest of the tests
    val fractionPositive = 0.3
    val n = 100
    val data = sc.parallelize(1 to n, 2)
    val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))

    // vary seed
    for (seed <- defaultSeed to defaultSeed + 5L) {
      val samplingRate = 0.1
      StratifiedAuxiliary.testSampleExact(stratifiedData, samplingRate, seed, n)
    }

    // vary sampling rate
    for (samplingRate <- List(0.01, 0.05, 0.1, 0.5)) {
      StratifiedAuxiliary.testSampleExact(stratifiedData, samplingRate, defaultSeed, n)
    }
  }

  test("reduceByKey") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val sums = pairs.reduceByKey(_ + _).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))
  }

  test("reduceByKey with collectAsMap") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val sums = pairs.reduceByKey(_ + _).collectAsMap()
    assert(sums.size === 2)
    assert(sums(1) === 7)
    assert(sums(2) === 1)
  }

  test("reduceByKey with many output partitions") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val sums = pairs.reduceByKey(_ + _, 10).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))
  }

  test("reduceByKey with partitioner") {
    val p = new Partitioner() {
      def numPartitions = 2
      def getPartition(key: Any) = key.asInstanceOf[Int]
    }
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 1), (0, 1))).partitionBy(p)
    val sums = pairs.reduceByKey(_ + _)
    assert(sums.collect().toSet === Set((1, 4), (0, 1)))
    assert(sums.partitioner === Some(p))
    // count the dependencies to make sure there is only 1 ShuffledRDD
    val deps = new HashSet[RDD[_]]()
    def visit(r: RDD[_]) {
      for (dep <- r.dependencies) {
        deps += dep.rdd
        visit(dep.rdd)
      }
    }
    visit(sums)
    assert(deps.size === 2) // ShuffledRDD, ParallelCollection.
  }

  test("countApproxDistinctByKey") {
    def error(est: Long, size: Long): Double = math.abs(est - size) / size.toDouble

    /* Since HyperLogLog unique counting is approximate, and the relative standard deviation is
     * only a statistical bound, the tests can fail for large values of relativeSD. We will be using
     * relatively tight error bounds to check correctness of functionality rather than checking
     * whether the approximation conforms with the requested bound.
     */
    val p = 20
    val sp = 0
    // When p = 20, the relative accuracy is about 0.001. So with high probability, the
    // relative error should be smaller than the threshold 0.01 we use here.
    val relativeSD = 0.01

    // For each value i, there are i tuples with first element equal to i.
    // Therefore, the expected count for key i would be i.
    val stacked = (1 to 100).flatMap(i => (1 to i).map(j => (i, j)))
    val rdd1 = sc.parallelize(stacked)
    val counted1 = rdd1.countApproxDistinctByKey(p, sp).collect()
    counted1.foreach { case (k, count) => assert(error(count, k) < relativeSD) }

    val rnd = new Random(42)

    // The expected count for key num would be num
    val randStacked = (1 to 100).flatMap { i =>
      val num = rnd.nextInt() % 500
      (1 to num).map(j => (num, j))
    }
    val rdd2 = sc.parallelize(randStacked)
    val counted2 = rdd2.countApproxDistinctByKey(relativeSD).collect()
    counted2.foreach { case (k, count) =>
      assert(error(count, k) < relativeSD, s"${error(count, k)} < $relativeSD")
    }
  }

  test("join") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))
  }

  test("join all-to-all") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (1, 3)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (1, 'y')))
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 6)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (1, 'y')),
      (1, (2, 'x')),
      (1, (2, 'y')),
      (1, (3, 'x')),
      (1, (3, 'y'))
    ))
  }

  test("leftOuterJoin") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.leftOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (1, Some('x'))),
      (1, (2, Some('x'))),
      (2, (1, Some('y'))),
      (2, (1, Some('z'))),
      (3, (1, None))
    ))
  }

  // See SPARK-9326
  test("cogroup with empty RDD") {
    import scala.reflect.classTag
    val intPairCT = classTag[(Int, Int)]

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.emptyRDD[(Int, Int)](intPairCT)

    val joined = rdd1.cogroup(rdd2).collect()
    assert(joined.size > 0)
  }

  // See SPARK-9326
  test("cogroup with groupByed RDD having 0 partitions") {
    import scala.reflect.classTag
    val intCT = classTag[Int]

    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.emptyRDD[Int](intCT).groupBy((x) => 5)
    val joined = rdd1.cogroup(rdd2).collect()
    assert(joined.size > 0)
  }

  // See SPARK-22465
  test("cogroup between multiple RDD " +
    "with an order of magnitude difference in number of partitions") {
    val rdd1 = sc.parallelize((1 to 1000).map(x => (x, x)), 1000)
    val rdd2 = sc
      .parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
      .partitionBy(new HashPartitioner(10))
    val joined = rdd1.cogroup(rdd2)
    assert(joined.getNumPartitions == rdd1.getNumPartitions)
  }

  // See SPARK-22465
  test("cogroup between multiple RDD with number of partitions similar in order of magnitude") {
    val rdd1 = sc.parallelize((1 to 1000).map(x => (x, x)), 20)
    val rdd2 = sc
      .parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
      .partitionBy(new HashPartitioner(10))
    val joined = rdd1.cogroup(rdd2)
    assert(joined.getNumPartitions == rdd2.getNumPartitions)
  }

  test("cogroup between multiple RDD when defaultParallelism is set without proper partitioner") {
    assert(!sc.conf.contains("spark.default.parallelism"))
    try {
      sc.conf.set("spark.default.parallelism", "4")
      val rdd1 = sc.parallelize((1 to 1000).map(x => (x, x)), 20)
      val rdd2 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)), 10)
      val joined = rdd1.cogroup(rdd2)
      assert(joined.getNumPartitions == sc.defaultParallelism)
    } finally {
      sc.conf.remove("spark.default.parallelism")
    }
  }

  test("cogroup between multiple RDD when defaultParallelism is set with proper partitioner") {
    assert(!sc.conf.contains("spark.default.parallelism"))
    try {
      sc.conf.set("spark.default.parallelism", "4")
      val rdd1 = sc.parallelize((1 to 1000).map(x => (x, x)), 20)
      val rdd2 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
        .partitionBy(new HashPartitioner(10))
      val joined = rdd1.cogroup(rdd2)
      assert(joined.getNumPartitions == rdd2.getNumPartitions)
    } finally {
      sc.conf.remove("spark.default.parallelism")
    }
  }

  test("cogroup between multiple RDD when defaultParallelism is set; with huge number of " +
    "partitions in upstream RDDs") {
    assert(!sc.conf.contains("spark.default.parallelism"))
    try {
      sc.conf.set("spark.default.parallelism", "4")
      val rdd1 = sc.parallelize((1 to 1000).map(x => (x, x)), 1000)
      val rdd2 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
        .partitionBy(new HashPartitioner(10))
      val joined = rdd1.cogroup(rdd2)
      assert(joined.getNumPartitions == rdd2.getNumPartitions)
    } finally {
      sc.conf.remove("spark.default.parallelism")
    }
  }

  test("rightOuterJoin") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.rightOuterJoin(rdd2).collect()
    assert(joined.size === 5)
    assert(joined.toSet === Set(
      (1, (Some(1), 'x')),
      (1, (Some(2), 'x')),
      (2, (Some(1), 'y')),
      (2, (Some(1), 'z')),
      (4, (None, 'w'))
    ))
  }

  test("fullOuterJoin") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.fullOuterJoin(rdd2).collect()
    assert(joined.size === 6)
    assert(joined.toSet === Set(
      (1, (Some(1), Some('x'))),
      (1, (Some(2), Some('x'))),
      (2, (Some(1), Some('y'))),
      (2, (Some(1), Some('z'))),
      (3, (Some(1), None)),
      (4, (None, Some('w')))
    ))
  }

  test("join with no matches") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((4, 'x'), (5, 'y'), (5, 'z'), (6, 'w')))
    val joined = rdd1.join(rdd2).collect()
    assert(joined.size === 0)
  }

  test("join with many output partitions") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.join(rdd2, 10).collect()
    assert(joined.size === 4)
    assert(joined.toSet === Set(
      (1, (1, 'x')),
      (1, (2, 'x')),
      (2, (1, 'y')),
      (2, (1, 'z'))
    ))
  }

  test("groupWith") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.groupWith(rdd2).collect()
    assert(joined.size === 4)
    val joinedSet = joined.map(x => (x._1, (x._2._1.toList, x._2._2.toList))).toSet
    assert(joinedSet === Set(
      (1, (List(1, 2), List('x'))),
      (2, (List(1), List('y', 'z'))),
      (3, (List(1), List())),
      (4, (List(), List('w')))
    ))
  }

  test("groupWith3") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val rdd3 = sc.parallelize(Array((1, 'a'), (3, 'b'), (4, 'c'), (4, 'd')))
    val joined = rdd1.groupWith(rdd2, rdd3).collect()
    assert(joined.size === 4)
    val joinedSet = joined.map(x => (x._1,
      (x._2._1.toList, x._2._2.toList, x._2._3.toList))).toSet
    assert(joinedSet === Set(
      (1, (List(1, 2), List('x'), List('a'))),
      (2, (List(1), List('y', 'z'), List())),
      (3, (List(1), List(), List('b'))),
      (4, (List(), List('w'), List('c', 'd')))
    ))
  }

  test("groupWith4") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val rdd3 = sc.parallelize(Array((1, 'a'), (3, 'b'), (4, 'c'), (4, 'd')))
    val rdd4 = sc.parallelize(Array((2, '@')))
    val joined = rdd1.groupWith(rdd2, rdd3, rdd4).collect()
    assert(joined.size === 4)
    val joinedSet = joined.map(x => (x._1,
      (x._2._1.toList, x._2._2.toList, x._2._3.toList, x._2._4.toList))).toSet
    assert(joinedSet === Set(
      (1, (List(1, 2), List('x'), List('a'), List())),
      (2, (List(1), List('y', 'z'), List(), List('@'))),
      (3, (List(1), List(), List('b'), List())),
      (4, (List(), List('w'), List('c', 'd'), List()))
    ))
  }

  test("zero-partition RDD") {
    withTempDir { emptyDir =>
      val file = sc.textFile(emptyDir.getAbsolutePath)
      assert(file.partitions.isEmpty)
      assert(file.collect().toList === Nil)
      // Test that a shuffle on the file works, because this used to be a bug
      assert(file.map(line => (line, 1)).reduceByKey(_ + _).collect().toList === Nil)
    }
  }

  test("keys and values") {
    val rdd = sc.parallelize(Array((1, "a"), (2, "b")))
    assert(rdd.keys.collect().toList === List(1, 2))
    assert(rdd.values.collect().toList === List("a", "b"))
  }

  test("default partitioner uses partition size") {
    // specify 2000 partitions
    val a = sc.makeRDD(Array(1, 2, 3, 4), 2000)
    // do a map, which loses the partitioner
    val b = a.map(a => (a, (a * 2).toString))
    // then a group by, and see we didn't revert to 2 partitions
    val c = b.groupByKey()
    assert(c.partitions.size === 2000)
  }

  test("default partitioner uses largest partitioner") {
    val a = sc.makeRDD(Array((1, "a"), (2, "b")), 2)
    val b = sc.makeRDD(Array((1, "a"), (2, "b")), 2000)
    val c = a.join(b)
    assert(c.partitions.size === 2000)
  }

  test("subtract") {
    val a = sc.parallelize(Array(1, 2, 3), 2)
    val b = sc.parallelize(Array(2, 3, 4), 4)
    val c = a.subtract(b)
    assert(c.collect().toSet === Set(1))
    assert(c.partitions.size === a.partitions.size)
  }

  test("subtract with narrow dependency") {
    // use a deterministic partitioner
    val p = new Partitioner() {
      def numPartitions = 5
      def getPartition(key: Any) = key.asInstanceOf[Int]
    }
    // partitionBy so we have a narrow dependency
    val a = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c"))).partitionBy(p)
    // more partitions/no partitioner so a shuffle dependency
    val b = sc.parallelize(Array((2, "b"), (3, "cc"), (4, "d")), 4)
    val c = a.subtract(b)
    assert(c.collect().toSet === Set((1, "a"), (3, "c")))
    // Ideally we could keep the original partitioner...
    assert(c.partitioner === None)
  }

  test("subtractByKey") {
    val a = sc.parallelize(Array((1, "a"), (1, "a"), (2, "b"), (3, "c")), 2)
    val b = sc.parallelize(Array((2, 20), (3, 30), (4, 40)), 4)
    val c = a.subtractByKey(b)
    assert(c.collect().toSet === Set((1, "a"), (1, "a")))
    assert(c.partitions.size === a.partitions.size)
  }

  test("subtractByKey with narrow dependency") {
    // use a deterministic partitioner
    val p = new Partitioner() {
      def numPartitions = 5
      def getPartition(key: Any) = key.asInstanceOf[Int]
    }
    // partitionBy so we have a narrow dependency
    val a = sc.parallelize(Array((1, "a"), (1, "a"), (2, "b"), (3, "c"))).partitionBy(p)
    // more partitions/no partitioner so a shuffle dependency
    val b = sc.parallelize(Array((2, "b"), (3, "cc"), (4, "d")), 4)
    val c = a.subtractByKey(b)
    assert(c.collect().toSet === Set((1, "a"), (1, "a")))
    assert(c.partitioner.get === p)
  }

  test("foldByKey") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val sums = pairs.foldByKey(0)(_ + _).collect()
    assert(sums.toSet === Set((1, 7), (2, 1)))
  }

  test("foldByKey with mutable result type") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val bufs = pairs.mapValues(v => ArrayBuffer(v)).cache()
    // Fold the values using in-place mutation
    val sums = bufs.foldByKey(new ArrayBuffer[Int])(_ ++= _).collect()
    assert(sums.toSet === Set((1, ArrayBuffer(1, 2, 3, 1)), (2, ArrayBuffer(1))))
    // Check that the mutable objects in the original RDD were not changed
    assert(bufs.collect().toSet === Set(
      (1, ArrayBuffer(1)),
      (1, ArrayBuffer(2)),
      (1, ArrayBuffer(3)),
      (1, ArrayBuffer(1)),
      (2, ArrayBuffer(1))))
  }

  test("saveNewAPIHadoopFile should call setConf if format is configurable") {
    val pairs = sc.parallelize(Array((Integer.valueOf(1), Integer.valueOf(1))))

    // No error, non-configurable formats still work
    pairs.saveAsNewAPIHadoopFile[NewFakeFormat]("ignored")

    /*
     * Check that configurable formats get configured:
     * ConfigTestFormat throws an exception if we try to write
     * to it when setConf hasn't been called first.
     * Assertion is in ConfigTestFormat.getRecordWriter.
     */
    pairs.saveAsNewAPIHadoopFile[ConfigTestFormat]("ignored")
  }

  test("The JobId on the driver and executors should be the same during the commit") {
    // Create more than one rdd to mimic stageId not equal to rddId
    val pairs = sc.parallelize(Array((1, 2), (2, 3)), 2)
      .map { p => (Integer.valueOf(p._1 + 1), Integer.valueOf(p._2 + 1)) }
      .filter { p => p._1 > 0 }
    pairs.saveAsNewAPIHadoopFile[YetAnotherFakeFormat]("ignored")
    assert(JobID.jobid != -1)
  }

  test("saveAsHadoopFile should respect configured output committers") {
    val pairs = sc.parallelize(Array((Integer.valueOf(1), Integer.valueOf(1))))
    val conf = new JobConf()
    conf.setOutputCommitter(classOf[FakeOutputCommitter])

    FakeOutputCommitter.ran = false
    pairs.saveAsHadoopFile(
      "ignored", pairs.keyClass, pairs.valueClass, classOf[FakeOutputFormat], conf)

    assert(FakeOutputCommitter.ran, "OutputCommitter was never called")
  }

  test("failure callbacks should be called before calling writer.close() in saveNewAPIHadoopFile") {
    val pairs = sc.parallelize(Array((Integer.valueOf(1), Integer.valueOf(2))), 1)

    FakeWriterWithCallback.calledBy = ""
    FakeWriterWithCallback.exception = null
    val e = intercept[SparkException] {
      pairs.saveAsNewAPIHadoopFile[NewFakeFormatWithCallback]("ignored")
    }
    assert(e.getCause.getMessage contains "failed to write")

    assert(FakeWriterWithCallback.calledBy === "write,callback,close")
    assert(FakeWriterWithCallback.exception != null, "exception should be captured")
    assert(FakeWriterWithCallback.exception.getMessage contains "failed to write")
  }

  test("failure callbacks should be called before calling writer.close() in saveAsHadoopFile") {
    val pairs = sc.parallelize(Array((Integer.valueOf(1), Integer.valueOf(2))), 1)
    val conf = new JobConf()

    FakeWriterWithCallback.calledBy = ""
    FakeWriterWithCallback.exception = null
    val e = intercept[SparkException] {
      pairs.saveAsHadoopFile(
        "ignored", pairs.keyClass, pairs.valueClass, classOf[FakeFormatWithCallback], conf)
    }
    assert(e.getCause.getMessage contains "failed to write")

    assert(FakeWriterWithCallback.calledBy === "write,callback,close")
    assert(FakeWriterWithCallback.exception != null, "exception should be captured")
    assert(FakeWriterWithCallback.exception.getMessage contains "failed to write")
  }

  test("saveAsNewAPIHadoopDataset should support invalid output paths when " +
    "there are no files to be committed to an absolute output location") {
    val pairs = sc.parallelize(Array((Integer.valueOf(1), Integer.valueOf(2))), 1)

    def saveRddWithPath(path: String): Unit = {
      val job = NewJob.getInstance(new Configuration(sc.hadoopConfiguration))
      job.setOutputKeyClass(classOf[Integer])
      job.setOutputValueClass(classOf[Integer])
      job.setOutputFormatClass(classOf[NewFakeFormat])
      if (null != path) {
        job.getConfiguration.set("mapred.output.dir", path)
      } else {
        job.getConfiguration.unset("mapred.output.dir")
      }
      val jobConfiguration = job.getConfiguration

      // just test that the job does not fail with java.lang.IllegalArgumentException.
      pairs.saveAsNewAPIHadoopDataset(jobConfiguration)
    }

    saveRddWithPath(null)
    saveRddWithPath("")
    saveRddWithPath("::invalid::")
  }

  // In spark 2.1, only null was supported - not other invalid paths.
  // org.apache.hadoop.mapred.FileOutputFormat.getOutputPath fails with IllegalArgumentException
  // for non-null invalid paths.
  test("saveAsHadoopDataset should respect empty output directory when " +
    "there are no files to be committed to an absolute output location") {
    val pairs = sc.parallelize(Array((Integer.valueOf(1), Integer.valueOf(2))), 1)

    val conf = new JobConf()
    conf.setOutputKeyClass(classOf[Integer])
    conf.setOutputValueClass(classOf[Integer])
    conf.setOutputFormat(classOf[FakeOutputFormat])
    conf.setOutputCommitter(classOf[FakeOutputCommitter])

    FakeOutputCommitter.ran = false
    pairs.saveAsHadoopDataset(conf)

    assert(FakeOutputCommitter.ran, "OutputCommitter was never called")
  }

  test("lookup") {
    val pairs = sc.parallelize(Array((1, 2), (3, 4), (5, 6), (5, 7)))

    assert(pairs.partitioner === None)
    assert(pairs.lookup(1) === Seq(2))
    assert(pairs.lookup(5) === Seq(6, 7))
    assert(pairs.lookup(-1) === Seq())

  }

  test("lookup with partitioner") {
    val pairs = sc.parallelize(Array((1, 2), (3, 4), (5, 6), (5, 7)))

    val p = new Partitioner {
      def numPartitions: Int = 2

      def getPartition(key: Any): Int = Math.abs(key.hashCode() % 2)
    }
    val shuffled = pairs.partitionBy(p)

    assert(shuffled.partitioner === Some(p))
    assert(shuffled.lookup(1) === Seq(2))
    assert(shuffled.lookup(5) === Seq(6, 7))
    assert(shuffled.lookup(-1) === Seq())
  }

  test("lookup with bad partitioner") {
    val pairs = sc.parallelize(Array((1, 2), (3, 4), (5, 6), (5, 7)))

    val p = new Partitioner {
      def numPartitions: Int = 2

      def getPartition(key: Any): Int = key.hashCode() % 2
    }
    val shuffled = pairs.partitionBy(p)

    assert(shuffled.partitioner === Some(p))
    assert(shuffled.lookup(1) === Seq(2))
    intercept[IllegalArgumentException] {shuffled.lookup(-1)}
  }

  private object StratifiedAuxiliary {
    def stratifier (fractionPositive: Double): (Int) => String = {
      (x: Int) => if (x % 10 < (10 * fractionPositive).toInt) "1" else "0"
    }

    def assertBinomialSample(
        exact: Boolean,
        actual: Int,
        trials: Int,
        p: Double): Unit = {
      if (exact) {
        assert(actual == math.ceil(p * trials).toInt)
      } else {
        val dist = new BinomialDistribution(trials, p)
        val q = dist.cumulativeProbability(actual)
        withClue(s"p = $p: trials = $trials") {
          assert(0.0 < q && q < 1.0)
        }
      }
    }

    def assertPoissonSample(
        exact: Boolean,
        actual: Int,
        trials: Int,
        p: Double): Unit = {
      if (exact) {
        assert(actual == math.ceil(p * trials).toInt)
      } else {
        val dist = new PoissonDistribution(p * trials)
        val q = dist.cumulativeProbability(actual)
        withClue(s"p = $p: trials = $trials") {
          assert(q >= 0.001 && q <= 0.999)
        }
      }
    }

    def testSampleExact(stratifiedData: RDD[(String, Int)],
        samplingRate: Double,
        seed: Long,
        n: Long): Unit = {
      testBernoulli(stratifiedData, true, samplingRate, seed, n)
      testPoisson(stratifiedData, true, samplingRate, seed, n)
    }

    def testSample(stratifiedData: RDD[(String, Int)],
        samplingRate: Double,
        seed: Long,
        n: Long): Unit = {
      testBernoulli(stratifiedData, false, samplingRate, seed, n)
      testPoisson(stratifiedData, false, samplingRate, seed, n)
    }

    // Without replacement validation
    def testBernoulli(stratifiedData: RDD[(String, Int)],
        exact: Boolean,
        samplingRate: Double,
        seed: Long,
        n: Long): Unit = {
      val trials = stratifiedData.countByKey()
      val fractions = Map("1" -> samplingRate, "0" -> samplingRate)
      val sample = if (exact) {
        stratifiedData.sampleByKeyExact(false, fractions, seed)
      } else {
        stratifiedData.sampleByKey(false, fractions, seed)
      }
      val sampleCounts = sample.countByKey()
      val takeSample = sample.collect()
      sampleCounts.foreach { case (k, v) =>
        assertBinomialSample(exact = exact, actual = v.toInt, trials = trials(k).toInt,
          p = samplingRate)
      }
      assert(takeSample.size === takeSample.toSet.size)
      takeSample.foreach { x => assert(1 <= x._2 && x._2 <= n, s"elements not in [1, $n]") }
    }

    // With replacement validation
    def testPoisson(stratifiedData: RDD[(String, Int)],
        exact: Boolean,
        samplingRate: Double,
        seed: Long,
        n: Long): Unit = {
      val trials = stratifiedData.countByKey()
      val expectedSampleSize = stratifiedData.countByKey().mapValues(count =>
        math.ceil(count * samplingRate).toInt)
      val fractions = Map("1" -> samplingRate, "0" -> samplingRate)
      val sample = if (exact) {
        stratifiedData.sampleByKeyExact(true, fractions, seed)
      } else {
        stratifiedData.sampleByKey(true, fractions, seed)
      }
      val sampleCounts = sample.countByKey()
      val takeSample = sample.collect()
      sampleCounts.foreach { case (k, v) =>
        assertPoissonSample(exact, actual = v.toInt, trials = trials(k).toInt, p = samplingRate)
      }
      val groupedByKey = takeSample.groupBy(_._1)
      for ((key, v) <- groupedByKey) {
        if (expectedSampleSize(key) >= 100 && samplingRate >= 0.1) {
          // sample large enough for there to be repeats with high likelihood
          assert(v.toSet.size < expectedSampleSize(key))
        } else {
          if (exact) {
            assert(v.toSet.size <= expectedSampleSize(key))
          } else {
            assertPoissonSample(false, actual = v.toSet.size, trials(key).toInt, p = samplingRate)
          }
        }
      }
      takeSample.foreach(x => assert(1 <= x._2 && x._2 <= n, s"elements not in [1, $n]"))
    }
  }

}

/*
  These classes are fakes for testing saveAsHadoopFile/saveNewAPIHadoopFile.
  Unfortunately, they have to be top level classes, and not defined in
  the test method, because otherwise Scala won't generate no-args constructors
  and the test will therefore throw InstantiationException when saveAsNewAPIHadoopFile
  tries to instantiate them with Class.newInstance.
 */

/*
 * Original Hadoop API
 */
class FakeWriter extends RecordWriter[Integer, Integer] {
  override def write(key: Integer, value: Integer): Unit = ()

  override def close(reporter: Reporter): Unit = ()
}

class FakeOutputCommitter() extends OutputCommitter() {
  override def setupJob(jobContext: JobContext): Unit = ()

  override def needsTaskCommit(taskContext: TaskAttemptContext): Boolean = true

  override def setupTask(taskContext: TaskAttemptContext): Unit = ()

  override def commitTask(taskContext: TaskAttemptContext): Unit = {
    FakeOutputCommitter.ran = true
    ()
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = ()
}

/*
 * Used to communicate state between the test harness and the OutputCommitter.
 */
object FakeOutputCommitter {
  var ran = false
}

class FakeOutputFormat() extends OutputFormat[Integer, Integer]() {
  override def getRecordWriter(
      ignored: FileSystem,
      job: JobConf, name: String,
      progress: Progressable): RecordWriter[Integer, Integer] = {
    new FakeWriter()
  }

  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = ()
}

/*
 * New-style Hadoop API
 */
class NewFakeWriter extends NewRecordWriter[Integer, Integer] {

  def close(p1: NewTaskAttempContext): Unit = ()

  def write(p1: Integer, p2: Integer): Unit = ()

}

class NewFakeCommitter extends NewOutputCommitter {
  def setupJob(p1: NewJobContext): Unit = ()

  def needsTaskCommit(p1: NewTaskAttempContext): Boolean = false

  def setupTask(p1: NewTaskAttempContext): Unit = ()

  def commitTask(p1: NewTaskAttempContext): Unit = ()

  def abortTask(p1: NewTaskAttempContext): Unit = ()
}

class NewFakeFormat() extends NewOutputFormat[Integer, Integer]() {

  def checkOutputSpecs(p1: NewJobContext): Unit = ()

  def getRecordWriter(p1: NewTaskAttempContext): NewRecordWriter[Integer, Integer] = {
    new NewFakeWriter()
  }

  def getOutputCommitter(p1: NewTaskAttempContext): NewOutputCommitter = {
    new NewFakeCommitter()
  }
}

object FakeWriterWithCallback {
  var calledBy: String = ""
  var exception: Throwable = _

  def onFailure(ctx: TaskContext, e: Throwable): Unit = {
    calledBy += "callback,"
    exception = e
  }
}

class FakeWriterWithCallback extends FakeWriter {

  override def close(p1: Reporter): Unit = {
    FakeWriterWithCallback.calledBy += "close"
  }

  override def write(p1: Integer, p2: Integer): Unit = {
    FakeWriterWithCallback.calledBy += "write,"
    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      FakeWriterWithCallback.onFailure(t, e)
    }
    throw new IOException("failed to write")
  }
}

class FakeFormatWithCallback() extends FakeOutputFormat {
  override def getRecordWriter(
    ignored: FileSystem,
    job: JobConf, name: String,
    progress: Progressable): RecordWriter[Integer, Integer] = {
    new FakeWriterWithCallback()
  }
}

class NewFakeWriterWithCallback extends NewFakeWriter {
  override def close(p1: NewTaskAttempContext): Unit = {
    FakeWriterWithCallback.calledBy += "close"
  }

  override def write(p1: Integer, p2: Integer): Unit = {
    FakeWriterWithCallback.calledBy += "write,"
    TaskContext.get().addTaskFailureListener { (t: TaskContext, e: Throwable) =>
      FakeWriterWithCallback.onFailure(t, e)
    }
    throw new IOException("failed to write")
  }
}

class NewFakeFormatWithCallback() extends NewFakeFormat {
  override def getRecordWriter(p1: NewTaskAttempContext): NewRecordWriter[Integer, Integer] = {
    new NewFakeWriterWithCallback()
  }
}

class YetAnotherFakeCommitter extends NewOutputCommitter with Assertions {
  def setupJob(j: NewJobContext): Unit = {
    JobID.jobid = j.getJobID().getId
  }

  def needsTaskCommit(t: NewTaskAttempContext): Boolean = false

  def setupTask(t: NewTaskAttempContext): Unit = {
    val jobId = t.getTaskAttemptID().getJobID().getId
    assert(jobId === JobID.jobid)
  }

  def commitTask(t: NewTaskAttempContext): Unit = {}

  def abortTask(t: NewTaskAttempContext): Unit = {}
}

class YetAnotherFakeFormat() extends NewOutputFormat[Integer, Integer]() {

  def checkOutputSpecs(j: NewJobContext): Unit = {}

  def getRecordWriter(t: NewTaskAttempContext): NewRecordWriter[Integer, Integer] = {
    new NewFakeWriter()
  }

  def getOutputCommitter(t: NewTaskAttempContext): NewOutputCommitter = {
    new YetAnotherFakeCommitter()
  }
}

object JobID {
  var jobid = -1
}

class ConfigTestFormat() extends NewFakeFormat() with Configurable {

  var setConfCalled = false
  def setConf(p1: Configuration): Unit = {
    setConfCalled = true
    ()
  }

  def getConf: Configuration = null

  override def getRecordWriter(p1: NewTaskAttempContext): NewRecordWriter[Integer, Integer] = {
    assert(setConfCalled, "setConf was never called")
    super.getRecordWriter(p1)
  }
}
