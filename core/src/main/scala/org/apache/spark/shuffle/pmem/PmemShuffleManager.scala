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

package org.apache.spark.shuffle.pmem

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.pmem._

private[spark] class PmemShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  override def registerShuffle[K, V, C](
    shuffleId: Int, numMaps: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val env: SparkEnv = SparkEnv.get
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val env: SparkEnv = SparkEnv.get
    val numMaps = handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps
    numMapsForShuffle.putIfAbsent(handle.shuffleId, numMaps)
    new PmemShuffleWriter(shuffleBlockResolver.asInstanceOf[PmemShuffleBlockResolver],
      handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context, env.conf, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    new PmemShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition,
      endPartition, context, metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
          .removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }

  override val shuffleBlockResolver: ShuffleBlockResolver = {
    new PmemShuffleBlockResolver(conf)
  }
}
