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
import org.scalatest.Matchers

import org.apache.spark.SparkContext._
import org.apache.spark.ShuffleSuite.NonJavaSerializableClass
import org.apache.spark.rdd.{CoGroupedRDD, OrderedRDDFunctions, RDD, ShuffledRDD, SubtractedRDD}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.MutablePair

abstract class ShuffleSuite extends FunSuite with Matchers with LocalSparkContext {

  val conf = new SparkConf(loadDefaults = false)

  test("groupByKey without compression") {
    try {
      System.setProperty("spark.shuffle.compress", "false")
      sc = new SparkContext("local", "test")
      val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 4)
      val groups = pairs.groupByKey(4).collect()
      assert(groups.size === 2)
      val valuesFor1 = groups.find(_._1 == 1).get._2
      assert(valuesFor1.toList.sorted === List(1, 2, 3))
      val valuesFor2 = groups.find(_._1 == 2).get._2
      assert(valuesFor2.toList.sorted === List(1))
    } finally {
      System.setProperty("spark.shuffle.compress", "true")
    }
  }

  test("shuffle non-zero block size") {
    sc = new SparkContext("local-cluster[2,1,512]", "test")
    val NUM_BLOCKS = 3

    val a = sc.parallelize(1 to 10, 2)
    val b = a.map { x =>
      (x, new NonJavaSerializableClass(x * 2))
    }
    // If the Kryo serializer is not used correctly, the shuffle would fail because the
    // default Java serializer cannot handle the non serializable class.
    val c = new ShuffledRDD[Int,
      NonJavaSerializableClass,
      NonJavaSerializableClass](b, new HashPartitioner(NUM_BLOCKS))
    c.setSerializer(new KryoSerializer(conf))
    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId

    assert(c.count === 10)

    // All blocks must have non-zero size
    (0 until NUM_BLOCKS).foreach { id =>
      val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, id)
      assert(statuses.forall(s => s._2 > 0))
    }
  }

  test("shuffle serializer") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,512]", "test")
    val a = sc.parallelize(1 to 10, 2)
    val b = a.map { x =>
      (x, new NonJavaSerializableClass(x * 2))
    }
    // If the Kryo serializer is not used correctly, the shuffle would fail because the
    // default Java serializer cannot handle the non serializable class.
    val c = new ShuffledRDD[Int,
      NonJavaSerializableClass,
      NonJavaSerializableClass](b, new HashPartitioner(3))
    c.setSerializer(new KryoSerializer(conf))
    assert(c.count === 10)
  }

  test("zero sized blocks") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,512]", "test")

    // 10 partitions from 4 keys
    val NUM_BLOCKS = 10
    val a = sc.parallelize(1 to 4, NUM_BLOCKS)
    val b = a.map(x => (x, x*2))

    // NOTE: The default Java serializer doesn't create zero-sized blocks.
    //       So, use Kryo
    val c = new ShuffledRDD[Int, Int, Int](b, new HashPartitioner(10))
      .setSerializer(new KryoSerializer(conf))

    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    assert(c.count === 4)

    val blockSizes = (0 until NUM_BLOCKS).flatMap { id =>
      val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, id)
      statuses.map(x => x._2)
    }
    val nonEmptyBlocks = blockSizes.filter(x => x > 0)

    // We should have at most 4 non-zero sized partitions
    assert(nonEmptyBlocks.size <= 4)
  }

  test("zero sized blocks without kryo") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,512]", "test")

    // 10 partitions from 4 keys
    val NUM_BLOCKS = 10
    val a = sc.parallelize(1 to 4, NUM_BLOCKS)
    val b = a.map(x => (x, x*2))

    // NOTE: The default Java serializer should create zero-sized blocks
    val c = new ShuffledRDD[Int, Int, Int](b, new HashPartitioner(10))

    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
    assert(c.count === 4)

    val blockSizes = (0 until NUM_BLOCKS).flatMap { id =>
      val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, id)
      statuses.map(x => x._2)
    }
    val nonEmptyBlocks = blockSizes.filter(x => x > 0)

    // We should have at most 4 non-zero sized partitions
    assert(nonEmptyBlocks.size <= 4)
  }

  test("shuffle on mutable pairs") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,512]", "test")
    def p[T1, T2](_1: T1, _2: T2) = MutablePair(_1, _2)
    val data = Array(p(1, 1), p(1, 2), p(1, 3), p(2, 1))
    val pairs: RDD[MutablePair[Int, Int]] = sc.parallelize(data, 2)
    val results = new ShuffledRDD[Int, Int, Int](pairs,
      new HashPartitioner(2)).collect()

    data.foreach { pair => results should contain ((pair._1, pair._2)) }
  }

  test("sorting on mutable pairs") {
    // This is not in SortingSuite because of the local cluster setup.
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,512]", "test")
    def p[T1, T2](_1: T1, _2: T2) = MutablePair(_1, _2)
    val data = Array(p(1, 11), p(3, 33), p(100, 100), p(2, 22))
    val pairs: RDD[MutablePair[Int, Int]] = sc.parallelize(data, 2)
    val results = new OrderedRDDFunctions[Int, Int, MutablePair[Int, Int]](pairs)
      .sortByKey().collect()
    results(0) should be ((1, 11))
    results(1) should be ((2, 22))
    results(2) should be ((3, 33))
    results(3) should be ((100, 100))
  }

  test("cogroup using mutable pairs") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,512]", "test")
    def p[T1, T2](_1: T1, _2: T2) = MutablePair(_1, _2)
    val data1 = Seq(p(1, 1), p(1, 2), p(1, 3), p(2, 1))
    val data2 = Seq(p(1, "11"), p(1, "12"), p(2, "22"), p(3, "3"))
    val pairs1: RDD[MutablePair[Int, Int]] = sc.parallelize(data1, 2)
    val pairs2: RDD[MutablePair[Int, String]] = sc.parallelize(data2, 2)
    val results = new CoGroupedRDD[Int](Seq(pairs1, pairs2), new HashPartitioner(2))
      .map(p => (p._1, p._2.map(_.toArray)))
      .collectAsMap()

    assert(results(1)(0).length === 3)
    assert(results(1)(0).contains(1))
    assert(results(1)(0).contains(2))
    assert(results(1)(0).contains(3))
    assert(results(1)(1).length === 2)
    assert(results(1)(1).contains("11"))
    assert(results(1)(1).contains("12"))
    assert(results(2)(0).length === 1)
    assert(results(2)(0).contains(1))
    assert(results(2)(1).length === 1)
    assert(results(2)(1).contains("22"))
    assert(results(3)(0).length === 0)
    assert(results(3)(1).contains("3"))
  }

  test("subtract mutable pairs") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,512]", "test")
    def p[T1, T2](_1: T1, _2: T2) = MutablePair(_1, _2)
    val data1 = Seq(p(1, 1), p(1, 2), p(1, 3), p(2, 1), p(3, 33))
    val data2 = Seq(p(1, "11"), p(1, "12"), p(2, "22"))
    val pairs1: RDD[MutablePair[Int, Int]] = sc.parallelize(data1, 2)
    val pairs2: RDD[MutablePair[Int, String]] = sc.parallelize(data2, 2)
    val results = new SubtractedRDD(pairs1, pairs2, new HashPartitioner(2)).collect()
    results should have length (1)
    // substracted rdd return results as Tuple2
    results(0) should be ((3, 33))
  }

  test("sort with Java non serializable class - Kryo") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName("test")
      .setMaster("local-cluster[2,1,512]")
    sc = new SparkContext(conf)
    val a = sc.parallelize(1 to 10, 2)
    val b = a.map { x =>
      (new NonJavaSerializableClass(x), x)
    }
    // If the Kryo serializer is not used correctly, the shuffle would fail because the
    // default Java serializer cannot handle the non serializable class.
    val c = b.sortByKey().map(x => x._2)
    assert(c.collect() === Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  }

  test("sort with Java non serializable class - Java") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    val conf = new SparkConf()
      .setAppName("test")
      .setMaster("local-cluster[2,1,512]")
    sc = new SparkContext(conf)
    val a = sc.parallelize(1 to 10, 2)
    val b = a.map { x =>
      (new NonJavaSerializableClass(x), x)
    }
    // default Java serializer cannot handle the non serializable class.
    val thrown = intercept[SparkException] {
      b.sortByKey().collect()
    }

    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getMessage.toLowerCase.contains("serializable"))
  }

  test("shuffle with different compression settings (SPARK-3426)") {
    for (
      shuffleSpillCompress <- Set(true, false);
      shuffleCompress <- Set(true, false)
    ) {
      val conf = new SparkConf()
        .setAppName("test")
        .setMaster("local")
        .set("spark.shuffle.spill.compress", shuffleSpillCompress.toString)
        .set("spark.shuffle.compress", shuffleCompress.toString)
        .set("spark.shuffle.memoryFraction", "0.001")
      resetSparkContext()
      sc = new SparkContext(conf)
      try {
        sc.parallelize(0 until 100000).map(i => (i / 4, i)).groupByKey().collect()
      } catch {
        case e: Exception =>
          val errMsg = s"Failed with spark.shuffle.spill.compress=$shuffleSpillCompress," +
            s" spark.shuffle.compress=$shuffleCompress"
          throw new Exception(errMsg, e)
      }
    }
  }
}

object ShuffleSuite {

  def mergeCombineException(x: Int, y: Int): Int = {
    throw new SparkException("Exception for map-side combine.")
    x + y
  }

  class NonJavaSerializableClass(val value: Int) extends Comparable[NonJavaSerializableClass] {
    override def compareTo(o: NonJavaSerializableClass): Int = {
      value - o.value
    }
  }
}
