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

import org.apache.commons.lang3.StringUtils

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{DependencyUtils, SparkHadoopUtil, SparkSubmit}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util._

/**
 * Utility object for launching driver programs such that they share fate with the Worker process.
 * This is used in standalone cluster mode only.
 */
object DriverWrapper extends Logging {
  def main(args: Array[String]) {
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
        val port: Int = sys.props.getOrElse("spark.driver.port", "0").toInt
        val rpcEnv = RpcEnv.create("Driver", host, port, conf, new SecurityManager(conf))
        logInfo(s"Driver address: ${rpcEnv.address}")
        rpcEnv.setupEndpoint("workerWatcher", new WorkerWatcher(rpcEnv, workerUrl))

        val currentLoader = Thread.currentThread.getContextClassLoader
        val userJarUrl = new File(userJar).toURI().toURL()
        val loader =
          if (sys.props.getOrElse("spark.driver.userClassPathFirst", "false").toBoolean) {
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
    val secMgr = new SecurityManager(sparkConf)
    val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)

    val Seq(packagesExclusions, packages, repositories, ivyRepoPath, ivySettingsPath) =
      Seq(
        "spark.jars.excludes",
        "spark.jars.packages",
        "spark.jars.repositories",
        "spark.jars.ivy",
        "spark.jars.ivySettings"
      ).map(sys.props.get(_).orNull)

    val resolvedMavenCoordinates = DependencyUtils.resolveMavenDependencies(packagesExclusions,
      packages, repositories, ivyRepoPath, Option(ivySettingsPath))
    val jars = {
      val jarsProp = sys.props.get("spark.jars").orNull
      if (!StringUtils.isBlank(resolvedMavenCoordinates)) {
        DependencyUtils.mergeFileLists(jarsProp, resolvedMavenCoordinates)
      } else {
        jarsProp
      }
    }
    val localJars = DependencyUtils.resolveAndDownloadJars(jars, userJar, sparkConf, hadoopConf,
      secMgr)
    DependencyUtils.addJarsToClassPath(localJars, loader)
  }
}
