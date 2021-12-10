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

package org.apache.spark.shuffle

import java.util.UUID

import org.junit.Test
import org.scalatest.Assertions._

import org.apache.spark._

class StarShuffleManagerTest {

  @Test
  def foldByKey(): Unit = {
    val conf = newSparkConf()
    runWithSparkConf(conf)
  }

  @Test
  def foldByKey_zeroBuffering(): Unit = {
    val conf = newSparkConf()
    conf.set("spark.reducer.maxSizeInFlight", "0")
    conf.set("spark.network.maxRemoteBlockSizeFetchToMem", "0")
    runWithSparkConf(conf)
  }

  private def runWithSparkConf(conf: SparkConf) = {
    var sc = new SparkContext(conf)

    try {
      val numValues = 10000
      val numMaps = 3
      val numPartitions = 5

      val rdd = sc.parallelize(0 until numValues, numMaps)
        .map(t => ((t / 2) -> (t * 2).longValue()))
        .foldByKey(0, numPartitions)((v1, v2) => v1 + v2)
      val result = rdd.collect()

      assert(result.size === numValues / 2)

      for (i <- 0 until result.size) {
        val key = result(i)._1
        val value = result(i)._2
        assert(key * 2 * 2 + (key * 2 + 1) * 2 === value)
      }

      val keys = result.map(_._1).distinct.sorted
      assert(keys.length === numValues / 2)
      assert(keys(0) === 0)
      assert(keys.last === (numValues - 1) / 2)
    } finally {
      sc.stop()
    }
  }

  def newSparkConf(): SparkConf = new SparkConf()
    .setAppName("testApp")
    .setMaster(s"local[2]")
    .set("spark.ui.enabled", "false")
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.app.id", "app-" + UUID.randomUUID())
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.StarShuffleManager")
}
