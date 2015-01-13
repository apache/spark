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

package org.apache.spark.deploy.yarn

import java.net.URI

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.Utils

trait ExecutorRunnableUtil extends Logging {

  val yarnConf: YarnConfiguration
  val sparkConf: SparkConf
  lazy val env = prepareEnvironment

  def prepareCommand(
      masterAddress: String,
      slaveId: String,
      hostname: String,
      executorMemory: Int,
      executorCores: Int,
      appId: String,
      localResources: HashMap[String, LocalResource]): List[String] = {
    // Extra options for the JVM
    val javaOpts = ListBuffer[String]()

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    var prefixEnv: Option[String] = None

    // Set the JVM memory
    val executorMemoryString = executorMemory + "m"
    javaOpts += "-Xms" + executorMemoryString + " -Xmx" + executorMemoryString + " "

    // Set extra Java options for the executor, if defined
    sys.props.get("spark.executor.extraJavaOptions").foreach { opts =>
      javaOpts += opts
    }
    sys.env.get("SPARK_JAVA_OPTS").foreach { opts =>
      javaOpts += opts
    }
    sys.props.get("spark.executor.extraLibraryPath").foreach { p =>
      prefixEnv = Some(Utils.libraryPathEnvPrefix(Seq(p)))
    }

    javaOpts += "-Djava.io.tmpdir=" +
      new Path(Environment.PWD.$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR)

    // Certain configs need to be passed here because they are needed before the Executor
    // registers with the Scheduler and transfers the spark configs. Since the Executor backend
    // uses Akka to connect to the scheduler, the akka settings are needed as well as the
    // authentication settings.
    sparkConf.getAll
      .filter { case (k, v) => SparkConf.isExecutorStartupConf(k) }
      .foreach { case (k, v) => javaOpts += YarnSparkHadoopUtil.escapeForShell(s"-D$k=$v") }

    // Commenting it out for now - so that people can refer to the properties if required. Remove
    // it once cpuset version is pushed out.
    // The context is, default gc for server class machines end up using all cores to do gc - hence
    // if there are multiple containers in same node, spark gc effects all other containers
    // performance (which can also be other spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce spark behaves 'properly' in
    // multi-tenant environments. Not sure how default java gc behaves if it is limited to subset
    // of cores on a node.
    /*
        else {
          // If no java_opts specified, default to using -XX:+CMSIncrementalMode
          // It might be possible that other modes/config is being done in spark.executor.extraJavaOptions,
          // so we dont want to mess with it.
          // In our expts, using (default) throughput collector has severe perf ramnifications in
          // multi-tennent machines
          // The options are based on
          // http://www.oracle.com/technetwork/java/gc-tuning-5-138395.html#0.0.0.%20When%20to%20Use%20the%20Concurrent%20Low%20Pause%20Collector|outline
          javaOpts += " -XX:+UseConcMarkSweepGC "
          javaOpts += " -XX:+CMSIncrementalMode "
          javaOpts += " -XX:+CMSIncrementalPacing "
          javaOpts += " -XX:CMSIncrementalDutyCycleMin=0 "
          javaOpts += " -XX:CMSIncrementalDutyCycle=10 "
        }
    */

    // For log4j configuration to reference
    javaOpts += ("-Dspark.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)

    val commands = prefixEnv ++ Seq(Environment.JAVA_HOME.$() + "/bin/java",
      "-server",
      // Kill if OOM is raised - leverage yarn's failure handling to cause rescheduling.
      // Not killing the task leaves various aspects of the executor and (to some extent) the jvm in
      // an inconsistent state.
      // TODO: If the OOM is not recoverable by rescheduling it on different node, then do
      // 'something' to fail job ... akin to blacklisting trackers in mapred ?
      "-XX:OnOutOfMemoryError='kill %p'") ++
      javaOpts ++
      Seq("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      masterAddress.toString,
      slaveId.toString,
      hostname.toString,
      executorCores.toString,
      appId,
      "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
      "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    commands.map(s => if (s == null) "null" else s).toList
  }

  private def setupDistributedCache(
      file: String,
      rtype: LocalResourceType,
      localResources: HashMap[String, LocalResource],
      timestamp: String,
      size: String,
      vis: String): Unit = {
    val uri = new URI(file)
    val amJarRsrc = Records.newRecord(classOf[LocalResource])
    amJarRsrc.setType(rtype)
    amJarRsrc.setVisibility(LocalResourceVisibility.valueOf(vis))
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromURI(uri))
    amJarRsrc.setTimestamp(timestamp.toLong)
    amJarRsrc.setSize(size.toLong)
    localResources(uri.getFragment()) = amJarRsrc
  }

  def prepareLocalResources: HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    val localResources = HashMap[String, LocalResource]()

    if (System.getenv("SPARK_YARN_CACHE_FILES") != null) {
      val timeStamps = System.getenv("SPARK_YARN_CACHE_FILES_TIME_STAMPS").split(',')
      val fileSizes = System.getenv("SPARK_YARN_CACHE_FILES_FILE_SIZES").split(',')
      val distFiles = System.getenv("SPARK_YARN_CACHE_FILES").split(',')
      val visibilities = System.getenv("SPARK_YARN_CACHE_FILES_VISIBILITIES").split(',')
      for( i <- 0 to distFiles.length - 1) {
        setupDistributedCache(distFiles(i), LocalResourceType.FILE, localResources, timeStamps(i),
          fileSizes(i), visibilities(i))
      }
    }

    if (System.getenv("SPARK_YARN_CACHE_ARCHIVES") != null) {
      val timeStamps = System.getenv("SPARK_YARN_CACHE_ARCHIVES_TIME_STAMPS").split(',')
      val fileSizes = System.getenv("SPARK_YARN_CACHE_ARCHIVES_FILE_SIZES").split(',')
      val distArchives = System.getenv("SPARK_YARN_CACHE_ARCHIVES").split(',')
      val visibilities = System.getenv("SPARK_YARN_CACHE_ARCHIVES_VISIBILITIES").split(',')
      for( i <- 0 to distArchives.length - 1) {
        setupDistributedCache(distArchives(i), LocalResourceType.ARCHIVE, localResources,
          timeStamps(i), fileSizes(i), visibilities(i))
      }
    }

    logInfo("Prepared Local resources " + localResources)
    localResources
  }

  def prepareEnvironment: HashMap[String, String] = {
    val env = new HashMap[String, String]()
    val extraCp = sparkConf.getOption("spark.executor.extraClassPath")
    ClientBase.populateClasspath(null, yarnConf, sparkConf, env, extraCp)

    sparkConf.getExecutorEnv.foreach { case (key, value) =>
      // This assumes each executor environment variable set here is a path
      // This is kept for backward compatibility and consistency with hadoop
      YarnSparkHadoopUtil.addPathToEnvironment(env, key, value)
    }

    // Keep this for backwards compatibility but users should move to the config
    sys.env.get("SPARK_YARN_USER_ENV").foreach { userEnvs =>
      YarnSparkHadoopUtil.setEnvFromInputString(env, userEnvs)
    }

    System.getenv().filterKeys(_.startsWith("SPARK")).foreach { case (k, v) => env(k) = v }
    env
  }

}
