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
package org.apache.spark.scheduler.cluster.k8s.watch

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s.{KubernetesUtils, SparkKubernetesClientFactory}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.Utils

private[spark] object KubernetesWatchBackend extends Logging {

  case class Arguments(
      driverUrl: String,
      hostname: String,
      appId: String,
      cores: Int,
      namespace: String)

  def main(args: Array[String]): Unit = {
    run(parseArguments(args, this.getClass.getCanonicalName.stripSuffix("$")))
    System.exit(0)
  }

  def run(arguments: Arguments): Unit = {

    Utils.initDaemon(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Bootstrap to fetch the driver's Spark properties.
      val watchConf = new SparkConf
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        arguments.hostname,
        arguments.hostname,
        -1,
        watchConf,
        new SecurityManager(watchConf),
        numUsableCores = 0,
        clientMode = true)

      var driver: RpcEndpointRef = null
      val nTries = sys.env.getOrElse("WATCH_DRIVER_PROPS_FETCHER_MAX_ATTEMPTS", "3").toInt
      for (i <- 0 until nTries if driver == null) {
        try {
          driver = fetcher.setupEndpointRefByURI(arguments.driverUrl)
        } catch {
          case e: Throwable => if (i == nTries - 1) {
            throw e
          }
        }
      }

      val config = driver.askSync[SparkAppConfig](
        RetrieveSparkAppConfig(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))

      val props = config.sparkProperties ++ Seq[(String, String)](("spark.app.id", arguments.appId))

      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      val sparkConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          sparkConf.setIfMissing(key, value)
        } else {
          sparkConf.set(key, value)
        }
      }

      config.hadoopDelegationCreds.foreach { tokens =>
        SparkHadoopUtil.get.addDelegationTokens(tokens, sparkConf)
      }

      val env = SparkEnv.createWatchEnv(sparkConf, arguments.hostname, arguments.hostname,
        arguments.cores, config.ioEncryptionKey, isLocal = false)

      val master = KubernetesUtils.parseMasterUrl(sparkConf.get("spark.master"))

      Utils.tryWithResource(SparkKubernetesClientFactory.createKubernetesClient(
        master,
        Some(arguments.namespace),
        KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
        SparkKubernetesClientFactory.ClientType.Submission,
        sparkConf,
        None)) { kubernetesClient =>
        val backend = ExecutorPodsWatchRpc.createBackendEndpoint(env.rpcEnv,
          new SparkContext(sparkConf), kubernetesClient)

        env.rpcEnv.setupEndpoint("ClusterManagerWatcher", backend)
        env.rpcEnv.awaitTermination()
      }
    }
  }

  def parseArguments(args: Array[String], classNameForEntry: String): Arguments = {
    var driverUrl: String = null
    var hostname: String = null
    var appId: String = null
    var cores: Int = 0
    var namespace: String = null

    var argv = args.toList
    while (argv.nonEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          // entrypoint.sh sets SPARK_EXECUTOR_POD_IP without '[]'
          hostname = Utils.addBracketsIfNeeded(value)
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--namespace") :: value :: tail =>
          namespace = value
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit(classNameForEntry)
      }
    }

    if (driverUrl == null) {
      printUsageAndExit(classNameForEntry)
    }

    Arguments(driverUrl, hostname, appId, cores, namespace)
  }

  private def printUsageAndExit(classNameForEntry: String): Unit = {
    // scalastyle:off println
    System.err.println(
      s"""
      |Usage: $classNameForEntry [options]
      |
      | Options are:
      |   --driver-url <driverUrl>
      |   --hostname <hostname>
      |   --cores <cores>
      |   --app-id <appid>
      |   --namespace <namespace>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }
}
