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

package org.apache.spark.deploy

/**
 * File is used to centralize references to configuration variables
 */
private[spark] object ConfigConstants {
  /**
   * The name of the application. This will appear in the UI and in log data.
   */
  val SparkAppName: String = "spark.app.name"

  /**
   * The main class to start executing
   */
  val SparkAppClass: String = "spark.app.class"

  /**
   * The cluster manager to connect to.
   */
  val SparkMaster: String = "spark.master"

  /**
   *  Whether to launch the driver program locally ("client") or
   * on one of the worker machines inside the cluster ("cluster")
   */
  val SparkDeployMode: String = "spark.deployMode"

  /**
   * Yarn client only: Number of executors to launch
   */
  val SparkExecutorInstances: String = "spark.executor.instances"

  /**
   * Spark standalone and Mesos only: Total cores for all executors.
   */
  val SparkCoresMax: String = "spark.cores.max"

  /**
   * Yarn client only:  Number of cores per executor
   */
  val SparkExecutorCores: String = "spark.executor.cores"

  /**
   * Memory per executor
   */
  val SparkExecutorMemory: String = "spark.executor.memory"

  /**
   * Standalone cluster only: Memory for driver
   */
  val SparkDriverMemory: String = "spark.driver.memory"

  /**
   * Standalone cluster only: Number of cores for driver
   */
  val SparkDriverCores: String = "spark.driver.cores"

  /**
   *  Extra class path entries to pass to the driver. Note that
   *  jars added with --jars are automatically included in the classpath.
   */
  val SparkDriverExtraClassPath: String = "spark.driver.extraClassPath"

  /**
   * Extra Java options to pass to the driver
   */
  val SparkDriverExtraJavaOptions: String = "spark.driver.extraJavaOptions"

  /**
   * Extra library path entries to pass to the driver.
   */
  val SparkDriverExtraLibraryPath: String = "spark.driver.extraLibraryPath"

  /**
   * Spark standalone with cluster deploy mode only:
   * restart driver application on failure
   */
  val SparkDriverSupervise: String = "spark.driver.supervise"

  /**
   * The YARN queue to submit to
   */
  val SparkYarnQueue: String = "spark.yarn.queue"

  /**
   * Comma-separated list of files to be placed in the working directory of each executor
   */
  val SparkFiles: String = "spark.files"

  /**
   * Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps.
   */
  val SparkSubmitPyFiles = "spark.submit.pyFiles"

  /**
   * Yarn only: Comma separated list of archives to be extracted into the
   * working directory of each executor
   */
  val SparkYarnDistArchives: String = "spark.yarn.dist.archives"

  /**
   * Comma-separated list of local jars to include on the driver and executor classpaths.
   */
  val SparkJars: String = "spark.jars"

  /**
   * Should spark-submit run in verbose mode
   */
  val SparkVerbose: String = "spark.verbose"

  /**
   * Main application to run
   */
  val SparkAppPrimaryResource: String =  "spark.app.primaryResource"

  /**
   * Arguments for application
   */
  val SparkAppArguments: String = "spark.app.arguments"

  /**
   * Path to an additional properties file to attempt to load at startup
   */
  val SparkPropertiesFile = "spark.propertiesFile"

  /**
   * Location of the spark home directory
   */
  val EnvSparkHome: String = "SPARK_HOME"

  /**
   * If present then all config files are read form this directory, rather then SPARK_HOME/conf
   */
  val EnvAltSparkConfPath: String = "SPARK_CONF_DIR"

  /**
   * sub directory of SPARK_HOME that configuration is stored in
   */
  val DirNameSparkConf: String = "conf"

  /**
   * If this file exists in $SPARK_HOME/conf or $SparkSubmitDefaults then its config
   * will be used
   */
  val FileNameSparkDefaultsConf: String = "spark-defaults.conf"

  /**
   * Name of the spark submit defaults resource
   */
  val ClassPathSparkSubmitDefaults: String = """org/apache/spark/deploy/spark-submit-defaults.prop"""

}
