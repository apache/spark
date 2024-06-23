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

package org.apache.spark.deploy.worker

import java.io.File

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{config, Logging, MDC}
import org.apache.spark.internal.LogKeys.RPC_ADDRESS
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util._

/**
 * Utility object for launching driver programs such that they share fate with the Worker process.
 * This is used in standalone cluster mode only.
 */
object DriverWrapper extends Logging {
  def main(args: Array[String]): Unit = {
    args.toList match {
      /*
       * IMPORTANT: Spark 1.3 provides a stable application submission gateway that is both
       * backward and forward compatible across future Spark versions. Because this gateway
       * uses this class to launch the driver, the ordering and semantics of the arguments
       * here must also remain consistent across versions.
       */
      case workerUrl :: userJar :: mainClass :: extraArgs =>
        val conf = new SparkConf()
        val host: String = Utils.localHostName()
        val port: Int = sys.props.getOrElse(config.DRIVER_PORT.key, "0").toInt
        val rpcEnv = RpcEnv.create("Driver", host, port, conf, new SecurityManager(conf))
        logInfo(log"Driver address: ${MDC(RPC_ADDRESS, rpcEnv.address)}")
        rpcEnv.setupEndpoint("workerWatcher", new WorkerWatcher(rpcEnv, workerUrl))

        val currentLoader = Thread.currentThread.getContextClassLoader
        val userJarUrl = new File(userJar).toURI().toURL()
        val loader =
          if (sys.props.getOrElse(config.DRIVER_USER_CLASS_PATH_FIRST.key, "false").toBoolean) {
            new ChildFirstURLClassLoader(Array(userJarUrl), currentLoader)
          } else {
            new MutableURLClassLoader(Array(userJarUrl), currentLoader)
          }
        Thread.currentThread.setContextClassLoader(loader)
        setupDependencies(loader, userJar)

        // Delegate to supplied main class
        val clazz = Utils.classForName(mainClass)
        val mainMethod = clazz.getMethod("main", classOf[Array[String]])
        mainMethod.invoke(null, extraArgs.toArray[String])

        rpcEnv.shutdown()

      case _ =>
        // scalastyle:off println
        System.err.println("Usage: DriverWrapper <workerUrl> <userJar> <driverMainClass> [options]")
        // scalastyle:on println
        System.exit(-1)
    }
  }

  private def setupDependencies(loader: MutableURLClassLoader, userJar: String): Unit = {
    val sparkConf = new SparkConf()
    val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)

    val ivyProperties = DependencyUtils.getIvyProperties()

    val resolvedMavenCoordinates = DependencyUtils.resolveMavenDependencies(true,
      ivyProperties.packagesExclusions, ivyProperties.packages, ivyProperties.repositories,
      ivyProperties.ivyRepoPath, Option(ivyProperties.ivySettingsPath))
    val jars = {
      val jarsProp = sys.props.get(config.JARS.key).orNull
      if (resolvedMavenCoordinates.nonEmpty) {
        DependencyUtils.mergeFileLists(jarsProp,
          DependencyUtils.mergeFileLists(resolvedMavenCoordinates: _*))
      } else {
        jarsProp
      }
    }
    val localJars = DependencyUtils.resolveAndDownloadJars(jars, userJar, sparkConf, hadoopConf)
    DependencyUtils.addJarsToClassPath(localJars, loader)
  }
}
