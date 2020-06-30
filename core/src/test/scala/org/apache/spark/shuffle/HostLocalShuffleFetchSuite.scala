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

import org.scalatest.Matchers

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, SparkFunSuite, TestUtils}
import org.apache.spark.internal.config._
import org.apache.spark.network.netty.NettyBlockTransferService

/**
 * This test suite is used to test host local shuffle reading with external shuffle service disabled
 */
class HostLocalShuffleFetchSuite extends SparkFunSuite with Matchers with LocalSparkContext {
  test("read host local shuffle from disk with external shuffle service disabled") {
    val conf = new SparkConf()
      .set(SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED, true)
      .set(SHUFFLE_SERVICE_ENABLED, false)
      .set(DYN_ALLOCATION_ENABLED, false)
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    sc.getConf.get(SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED) should equal(true)
    sc.env.blockManager.externalShuffleServiceEnabled should equal(false)
    sc.env.blockManager.hostLocalDirManager.isDefined should equal(true)
    sc.env.blockManager.blockStoreClient.getClass should equal(classOf[NettyBlockTransferService])
    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

    val rdd = sc.parallelize(0 until 1000, 10)
      .map { i => (i, 1) }
      .reduceByKey(_ + _)

    assert(rdd.count() === 1000)
    assert(rdd.count() === 1000)

    val cachedExecutors = rdd.mapPartitions { _ =>
      SparkEnv.get.blockManager.hostLocalDirManager.map { localDirManager =>
        localDirManager.getCachedHostLocalDirs().keySet.iterator
      }.getOrElse(Iterator.empty)
    }.collect().toSet

    // both executors are caching the dirs of the other one
    cachedExecutors should equal(sc.getExecutorIds().toSet)

    rdd.collect().map(_._2).sum should equal(1000)
  }
}
