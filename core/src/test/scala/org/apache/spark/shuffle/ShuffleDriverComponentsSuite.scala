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

import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.collect.ImmutableMap
import org.scalatest.Assertions._
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.SHUFFLE_IO_PLUGIN_CLASS
import org.apache.spark.shuffle.api.{ShuffleDataIO, ShuffleDriverComponents, ShuffleExecutorComponents, ShuffleMapOutputWriter}
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO

class ShuffleDriverComponentsSuite
    extends SparkFunSuite with LocalSparkContext with BeforeAndAfterEach {

  test("test serialization of shuffle initialization conf to executors") {
    val testConf = new SparkConf()
      .setAppName("testing")
      .set(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX + "test-plugin-key", "user-set-value")
      .set(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX + "test-user-key", "user-set-value")
      .setMaster("local-cluster[2,1,1024]")
      .set(SHUFFLE_IO_PLUGIN_CLASS, "org.apache.spark.shuffle.TestShuffleDataIO")

    sc = new SparkContext(testConf)

    val out = sc.parallelize(Seq((1, "one"), (2, "two"), (3, "three")), 3)
      .groupByKey()
      .foreach { _ =>
        if (!TestShuffleExecutorComponentsInitialized.initialized.get()) {
          throw new RuntimeException("TestShuffleExecutorComponents wasn't initialized")
        }
      }
  }
}

class TestShuffleDataIO(sparkConf: SparkConf) extends ShuffleDataIO {
  private val delegate = new LocalDiskShuffleDataIO(sparkConf)

  override def driver(): ShuffleDriverComponents = new TestShuffleDriverComponents()

  override def executor(): ShuffleExecutorComponents =
    new TestShuffleExecutorComponentsInitialized(delegate.executor())
}

class TestShuffleDriverComponents extends ShuffleDriverComponents {
  override def initializeApplication(): JMap[String, String] = {
    ImmutableMap.of("test-plugin-key", "plugin-set-value")
  }

  override def cleanupApplication(): Unit = {}
}

object TestShuffleExecutorComponentsInitialized {
  val initialized = new AtomicBoolean(false)
}

class TestShuffleExecutorComponentsInitialized(delegate: ShuffleExecutorComponents)
    extends ShuffleExecutorComponents {

  override def initializeExecutor(
      appId: String,
      execId: String,
      extraConfigs: JMap[String, String]): Unit = {
    delegate.initializeExecutor(appId, execId, extraConfigs)
    assert(extraConfigs.get("test-plugin-key") == "plugin-set-value", extraConfigs)
    assert(extraConfigs.get("test-user-key") == "user-set-value")
    TestShuffleExecutorComponentsInitialized.initialized.set(true)
  }

  override def createMapOutputWriter(
      shuffleId: Int,
      mapTaskId: Long,
      numPartitions: Int): ShuffleMapOutputWriter = {
    delegate.createMapOutputWriter(shuffleId, mapTaskId, numPartitions)
  }
}
