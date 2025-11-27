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

package org.apache.spark.shuffle.sort.remote

import java.util.Optional

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter, SingleSpillShuffleMapOutputWriter}
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents

class HybridShuffleExecutorComponents(sparkConf: SparkConf,
    localDiskShuffleExecutorComponents: LocalDiskShuffleExecutorComponents)
  extends ShuffleExecutorComponents {

  override def initializeExecutor(appId: String, execId: String,
                                  extraConfigs: java.util.Map[String, String]): Unit = {
    localDiskShuffleExecutorComponents.initializeExecutor(appId, execId, extraConfigs)
  }

  override def createMapOutputWriter(
      shuffleId: Int,
      mapTaskId: Long,
      numPartitions: Int): ShuffleMapOutputWriter = {
    val isRemote = TaskContext.get().getLocalProperties
      .getOrDefault("consolidation.write", "false").toString.toBoolean
    if (isRemote) {
      new RemoteShuffleMapOutputWriter(sparkConf, shuffleId, mapTaskId, numPartitions)
    } else {
      localDiskShuffleExecutorComponents.createMapOutputWriter(shuffleId, mapTaskId, numPartitions)
    }
  }

  override def createSingleFileMapOutputWriter(
      shuffleId: Int,
      mapId: Long): Optional[SingleSpillShuffleMapOutputWriter] = {
    Optional.empty()
  }
}
