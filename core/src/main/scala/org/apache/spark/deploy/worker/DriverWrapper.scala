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
import java.nio.file.Files

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{SparkSubmit, SparkSubmitUtils}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, Utils}

/**
 * Utility object for launching driver programs such that they share fate with the Worker process.
 * This is used in standalone cluster mode only.
 */
object DriverWrapper {
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
        val rpcEnv = RpcEnv.create("Driver",
          Utils.localHostName(), 0, conf, new SecurityManager(conf))
        rpcEnv.setupEndpoint("workerWatcher", new WorkerWatcher(rpcEnv, workerUrl))

        val currentLoader = Thread.currentThread.getContextClassLoader
        val userJarUrl = new File(userJar).toURI.toURL
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

  // R or Python are not supported in cluster mode so download the jars to the driver side
  private def setupDependencies(loader: MutableURLClassLoader, userJar: String): Unit = {
    val packagesExclusions = sys.props.get("spark.jars.excludes").orNull
    val packages = sys.props.get("spark.jars.packages").orNull
    val repositories = sys.props.get("spark.jars.repositories").orNull
    val hadoopConf = new HadoopConfiguration()
    val childClasspath = new ArrayBuffer[String]()
    val ivyRepoPath = sys.props.get("spark.jars.ivy").orNull
    var jars = sys.props.get("spark.jars").orNull

    val exclusions: Seq[String] =
      if (!StringUtils.isBlank(packagesExclusions)) {
        packagesExclusions.split(",")
      } else {
        Nil
      }

    // Create the IvySettings, either load from file or build defaults
    val ivySettings = sys.props.get("spark.jars.ivySettings").map { ivySettingsFile =>
      SparkSubmitUtils.loadIvySettings(ivySettingsFile, Option(repositories),
        Option(ivyRepoPath))
    }.getOrElse {
      SparkSubmitUtils.buildIvySettings(Option(repositories), Option(ivyRepoPath))
    }

    val resolvedMavenCoordinates = SparkSubmitUtils.resolveMavenCoordinates(packages,
      ivySettings, exclusions = exclusions)

    if (!StringUtils.isBlank(resolvedMavenCoordinates)) {
      jars = SparkSubmit.mergeFileLists(jars, resolvedMavenCoordinates)
    }

    val targetDir = Files.createTempDirectory("tmp").toFile
    // scalastyle:off runtimeaddshutdownhook
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        FileUtils.deleteQuietly(targetDir)
      }
    })
    // scalastyle:on runtimeaddshutdownhook

    val sparkProperties = new mutable.HashMap[String, String]()
    val securityProperties = List("spark.ssl.fs.trustStore", "spark.ssl.trustStore",
      "spark.ssl.fs.trustStorePassword", "spark.ssl.trustStorePassword",
      "spark.ssl.fs.protocol", "spark.ssl.protocol")

    securityProperties
      .map {pName => sys.props.get(pName)
        .map{pValue => sparkProperties.put(pName, pValue)}}

    jars = Option(jars).map(SparkSubmit.resolveGlobPaths(_, hadoopConf)).orNull
    
    // filter out the user jar
    jars = jars.split(",").filterNot(_.contains(userJar.split("/").last)).mkString(",")
    jars = Option(jars)
      .map(SparkSubmit.downloadFileList(_, targetDir, sparkProperties, hadoopConf)).orNull

    if (jars != null) {childClasspath ++= jars.split(",")}

    for (jar <- childClasspath) {
        SparkSubmit.addJarToClasspath(jar, loader)
    }
  }
}
