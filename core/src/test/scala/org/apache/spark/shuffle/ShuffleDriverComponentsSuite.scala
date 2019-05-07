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

import java.util

import com.google.common.collect.ImmutableMap

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.api.shuffle.{ShuffleDataIO, ShuffleDriverComponents, ShuffleExecutorComponents, ShuffleWriteSupport}
import org.apache.spark.internal.config.SHUFFLE_IO_PLUGIN_CLASS
import org.apache.spark.shuffle.sort.io.DefaultShuffleWriteSupport

class ShuffleDriverComponentsSuite extends SparkFunSuite with LocalSparkContext {
  test(s"test serialization of shuffle initialization conf to executors") {
    val testConf = new SparkConf()
      .setAppName("testing")
      .setMaster("local-cluster[2,1,1024]")
      .set(SHUFFLE_IO_PLUGIN_CLASS, "org.apache.spark.shuffle.TestShuffleDataIO")

    sc = new SparkContext(testConf)

    sc.parallelize(Seq((1, "one"), (2, "two"), (3, "three")), 3)
      .groupByKey()
      .collect()
  }
}

class TestShuffleDriverComponents extends ShuffleDriverComponents {
  override def initializeApplication(): util.Map[String, String] =
    ImmutableMap.of("test-key", "test-value")

  override def cleanupApplication(): Unit = {}

  override def removeShuffleData(shuffleId: Int, blocking: Boolean): Unit = {}
}

class TestShuffleDataIO(sparkConf: SparkConf) extends ShuffleDataIO {
  override def driver(): ShuffleDriverComponents = new TestShuffleDriverComponents()

  override def executor(): ShuffleExecutorComponents =
    new TestShuffleExecutorComponents(sparkConf)
}

class TestShuffleExecutorComponents(sparkConf: SparkConf) extends ShuffleExecutorComponents {
  override def initializeExecutor(appId: String, execId: String,
                                  extraConfigs: util.Map[String, String]): Unit = {
    assert(extraConfigs.get("test-key") == "test-value")
  }

  override def writes(): ShuffleWriteSupport = {
    val blockManager = SparkEnv.get.blockManager
    val blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager)
    new DefaultShuffleWriteSupport(sparkConf, blockResolver)
  }
}
