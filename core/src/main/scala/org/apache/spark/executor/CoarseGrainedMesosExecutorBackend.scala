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

package org.apache.spark.executor

import org.apache.spark.executor.{CoarseGrainedExecutorBackendRunner, CoarseGrainedExecutorBackend}
import org.apache.spark.SecurityManager
import org.apache.spark.SparkConf
import org.apache.spark.deploy.worker.StandaloneWorkerShuffleService
import akka.actor.ActorSystem

private[spark] class CoarseGrainedMesosExecutorBackend(
    driverUrl: String,
    executorId: String,
    hostPort: String,
    cores: Int,
    sparkProperties: Seq[(String, String)],
    actorSystem: ActorSystem)
  extends CoarseGrainedExecutorBackend(driverUrl, executorId, hostPort,
                                       cores, sparkProperties, actorSystem) {

  lazy val shuffleService: StandaloneWorkerShuffleService = {
    val executorConf = new SparkConf
    new StandaloneWorkerShuffleService(executorConf, new SecurityManager(executorConf))
  }

  override def preStart() {
    shuffleService.startIfEnabled()
    super.preStart()
  }

  override def postStop() {
    shuffleService.stop()
    super.postStop()
  }
}

private[spark] object CoarseGrainedMesosExecutorBackend
  extends CoarseGrainedExecutorBackendRunner {

  def main(args: Array[String]) {
    args.length match {
      case x if x < 5 =>
        System.err.println(
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          "Usage: CoarseGrainedMesosExecutorBackend <driverUrl> <executorId> <hostname> " +
            "<cores> <appid> [<workerUrl>] ")
        System.exit(1)

      // NB: These arguments are provided by CoarseMesosSchedulerBackend (for mesos mode).
      case 5 =>
        run(args(0), args(1), args(2), args(3).toInt, args(4), None,
          classOf[CoarseGrainedMesosExecutorBackend])
      case x if x > 5 =>
        run(args(0), args(1), args(2), args(3).toInt, args(4), Some(args(5)),
          classOf[CoarseGrainedMesosExecutorBackend])
    }
  }
}
