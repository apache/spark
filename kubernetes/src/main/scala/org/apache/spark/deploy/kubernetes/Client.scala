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

package org.apache.spark.deploy.kubernetes

import java.util.concurrent.CountDownLatch

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.kubernetes.KubernetesClusterScheduler
import org.apache.spark.util.ShutdownHookManager

private[spark] class Client(val args: ClientArguments,
                            val hadoopConf: Configuration,
                            val sparkConf: SparkConf)
    extends Logging {
  private val scheduler = new KubernetesClusterScheduler(sparkConf)
  private val shutdownLatch = new CountDownLatch(1)

  def this(clientArgs: ClientArguments, spConf: SparkConf) =
    this(clientArgs, SparkHadoopUtil.get.newConfiguration(spConf), spConf)

  def start(): Unit = {
    scheduler.start(args)
  }

  def stop(): Unit = {
    scheduler.stop()
    shutdownLatch.countDown()

    System.clearProperty("SPARK_KUBERNETES_MODE")
  }

  def awaitShutdown(): Unit = {
    shutdownLatch.await()
  }
}

private object Client extends Logging {
  def main(argStrings: Array[String]) {
    val sparkConf = new SparkConf
    System.setProperty("SPARK_KUBERNETES_MODE", "true")
    val args = new ClientArguments(argStrings)
    val client = new Client(args, sparkConf)
    client.start()

    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook { () =>
      logInfo("Shutdown hook is shutting down dispatcher")
      client.stop()
      client.awaitShutdown()
    }
    client.awaitShutdown()
  }
}
