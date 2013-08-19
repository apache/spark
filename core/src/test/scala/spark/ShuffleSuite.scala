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

package spark

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

import spark.rdd.ShuffledRDD
import spark.SparkContext._
import spark.ShuffleSuite.NonJavaSerializableClass
import spark.util.MutablePair


class ShuffleSuite extends FunSuite with ShouldMatchers with LocalSparkContext {
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
    val c = new ShuffledRDD[Int, NonJavaSerializableClass, (Int, NonJavaSerializableClass)](
      b, new HashPartitioner(NUM_BLOCKS)).setSerializer(classOf[spark.KryoSerializer].getName)
    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[Int, Int]].shuffleId

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
    val c = new ShuffledRDD[Int, NonJavaSerializableClass, (Int, NonJavaSerializableClass)](
      b, new HashPartitioner(3)).setSerializer(classOf[spark.KryoSerializer].getName)
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
    val c = new ShuffledRDD[Int, Int, (Int, Int)](b, new HashPartitioner(10))
      .setSerializer(classOf[spark.KryoSerializer].getName)

    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[Int, Int]].shuffleId
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
    val c = new ShuffledRDD[Int, Int, (Int, Int)](b, new HashPartitioner(10))

    val shuffleId = c.dependencies.head.asInstanceOf[ShuffleDependency[Int, Int]].shuffleId
    assert(c.count === 4)

    val blockSizes = (0 until NUM_BLOCKS).flatMap { id =>
      val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, id)
      statuses.map(x => x._2)
    }
    val nonEmptyBlocks = blockSizes.filter(x => x > 0)

    // We should have at most 4 non-zero sized partitions
    assert(nonEmptyBlocks.size <= 4)
  }

  test("shuffle using mutable pairs") {
    // Use a local cluster with 2 processes to make sure there are both local and remote blocks
    sc = new SparkContext("local-cluster[2,1,512]", "test")
    def p[T1, T2](_1: T1, _2: T2) = MutablePair(_1, _2)
    val data = Array(p(1, 1), p(1, 2), p(1, 3), p(2, 1))
    val pairs: RDD[MutablePair[Int, Int]] = sc.parallelize(data)
    val results = new ShuffledRDD[Int, Int, MutablePair[Int, Int]](pairs, new HashPartitioner(2))
      .collect()

    data.foreach { pair => results should contain (pair) }
  }
}

object ShuffleSuite {

  def mergeCombineException(x: Int, y: Int): Int = {
    throw new SparkException("Exception for map-side combine.")
    x + y
  }

  class NonJavaSerializableClass(val value: Int)
}
