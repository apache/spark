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

package org.apache.spark.mllib.random

import org.apache.spark.mllib.rdd.RandomRDDPartition
import org.scalatest.FunSuite

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.RDD

/**
 * Note: avoid including APIs that do not set the seed for the RNG in unit tests
 * in order to guarantee deterministic behavior.
 *
 * TODO update tests to use TestingUtils for floating point comparison after PR 1367 is merged
 */
class RandomRDDGeneratorsSuite extends FunSuite with LocalSparkContext {

  def testGeneratedRDD(rdd: RDD[Double],
      expectedSize: Long,
      expectedNumPartitions: Int,
      expectedMean: Double,
      expectedStddev: Double,
      epsilon: Double = 1e-2) {
    val stats = rdd.stats()
    println(stats.toString)
    assert(expectedSize === stats.count)
    assert(expectedNumPartitions === rdd.partitions.size)
    assert(math.abs(stats.mean - expectedMean) < epsilon)
    assert(math.abs(stats.stdev - expectedStddev) < epsilon)
  }

  test("uniformRDD") {
    val defaultSize = 1000000L
    val numPartitions = 100
    val defaultSeed = 1L

    // vary seeds
    for (seed <- 0 until 5) {
      val uniform = RandomRDDGenerators.uniformRDD(sc, defaultSize, numPartitions, seed)
      testGeneratedRDD(uniform, defaultSize, numPartitions, 0.5, 1 / math.sqrt(12))
    }

    // cases where size % numParts != 0
    for ((size, numPartitions) <- List((10000, 6), (12345, 1), (13000, 3))) {
      val uniform = RandomRDDGenerators.uniformRDD(sc, size, numPartitions, defaultSeed)
      uniform.partitions.foreach(p => println(p.asInstanceOf[RandomRDDPartition].size))
      testGeneratedRDD(uniform, size, numPartitions, 0.5, 1 / math.sqrt(12))
    }

    // default numPartitions = sc.defaultParallelism
    val uniform = RandomRDDGenerators.normalRDD(sc, defaultSize, defaultSeed)
    testGeneratedRDD(uniform, defaultSize, sc.defaultParallelism, 0.0, 1.0)
  }

  test("normalRDD") {
    val size = 1000000L
    val numPartitions = 100
    val defaultSeed = 1L

    for (seed <- 0 until 5) {
      val normal = RandomRDDGenerators.normalRDD(sc, size, numPartitions, seed)
      testGeneratedRDD(normal, size, numPartitions, 0.0, 1.0)
    }

    val normal2 = RandomRDDGenerators.normalRDD(sc, size, defaultSeed)
    testGeneratedRDD(normal2, size, sc.defaultParallelism, 0.0, 1.0)
  }

  test("poissonRDD") {
    val size = 1000000L
    val numPartitions = 100
    val defaultSeed = 1L
    val mean = 100.0

    for (seed <- 0 until 5) {
      val poisson = RandomRDDGenerators.poissonRDD(sc, size, numPartitions, mean, seed)
      testGeneratedRDD(poisson, size, numPartitions, mean, math.sqrt(mean), 1e-1)
    }

    val poisson2 = RandomRDDGenerators.poissonRDD(sc, size, mean, defaultSeed)
    testGeneratedRDD(poisson2, size, sc.defaultParallelism, mean, math.sqrt(mean), 1e-1)
  }
}
