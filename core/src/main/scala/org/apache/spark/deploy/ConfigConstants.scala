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
  val SPARK_APP_NAME: String = "spark.app.name"

  /**
   * The main class to start executing
   */
  val SPARK_APP_CLASS: String = "spark.app.class"

  /**
   * The cluster manager to connect to.
   */
  val SPARK_MASTER: String = "spark.master"

  /**
   *  Whether to launch the driver program locally ("client") or
   * on one of the worker machines inside the cluster ("cluster")
   */
  val SPARK_DEPLOY_MODE: String = "spark.deployMode"

  /**
   * Yarn client only: Number of executors to launch
   */
  val SPARK_EXECUTOR_INSTANCES: String = "spark.executor.instances"

  /**
   * Spark standalone and Mesos only: Total cores for all executors.
   */
  val SPARK_CORES_MAX: String = "spark.cores.max"

  /**
   * Yarn client only:  Number of cores per executor
   */
  val SPARK_EXECUTOR_CORES: String = "spark.executor.cores"

  /**
   * Memory per executor
   */
  val SPARK_EXECUTOR_MEMORY: String = "spark.executor.memory"

  /**
   * Standalone cluster only: Memory for driver
   */
  val SPARK_DRIVER_MEMORY: String = "spark.driver.memory"

  /**
   * Standalone cluster only: Number of cores for driver
   */
  val SPARK_DRIVER_CORES: String = "spark.driver.cores"

  /**
   *  Extra class path entries to pass to the driver. Note that
   *  jars added with --jars are automatically included in the classpath.
   */
  val SPARK_DRIVER_EXTRA_CLASSPATH: String = "spark.driver.extraClassPath"

  /**
   * Extra Java options to pass to the driver
   */
  val SPARK_DRIVER_EXTRA_JAVA_OPTIONS: String = "spark.driver.extraJavaOptions"

  /**
   * Extra library path entries to pass to the driver.
   */
  val SPARK_DRIVER_EXTRA_LIBRARY_PATH: String = "spark.driver.extraLibraryPath"

  /**
   * Spark standalone with cluster deploy mode only:
   * restart driver application on failure
   */
  val SPARK_DRIVER_SUPERVISE: String = "spark.driver.supervise"

  /**
   * The YARN queue to submit to
   */
  val SPARK_YARN_QUEUE: String = "spark.yarn.queue"

  /**
   * Comma-separated list of files to be placed in the working directory of each executor
   */
  val SPARK_FILES: String = "spark.files"

  /**
   * Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps.
   */
  val SPARK_SUBMIT_PYFILES = "spark.submit.pyFiles"

  /**
   * Yarn only: Comma separated list of archives to be extracted into the
   * working directory of each executor
   */
  val SPARK_YARN_DIST_ARCHIVES: String = "spark.yarn.dist.archives"

  /**
   * Yarn only: Comma separated list of files to be distributed to yarn cluster nodes
   */
  val SPARK_YARN_DIST_FILES: String = "spark.yarn.dist.files"

  /**
   * Comma-separated list of local jars to include on the driver and executor classpaths.
   */
  val SPARK_JARS: String = "spark.jars"

  /**
   * Should spark-submit run in verbose mode
   */
  val SPARK_VERBOSE: String = "spark.verbose"

  /**
   * Main application to run
   */
  val SPARK_APP_PRIMARY_RESOURCE: String =  "spark.app.primaryResource"

  /**
   * Arguments for application
   */
  val SPARK_APP_ARGUMENTS: String = "spark.app.arguments"

  /**
   * Property file to read in - usually set by SparkSubmitDriverBootstrap
   */
  val SPARK_PROPERTIES_FILE: String = "spark.properties.file"

  /**
   * Location of the spark home directory
   */
  val ENV_SPARK_HOME: String = "SPARK_HOME"

  /**
   * If present then all config files are read form this directory, rather then SPARK_HOME/conf
   */
  val ENV_ALT_SPARK_CONF_PATH: String = "SPARK_CONF_DIR"

  /**
   * sub directory of SPARK_HOME that configuration is stored in
   */
  val DIR_NAME_SPARK_CONF: String = "conf"

  /**
   * If this file exists in $SPARK_HOME/conf or $SparkSubmitDefaults then its config
   * will be used
   */
  val FILENAME_SPARK_DEFAULTS_CONF: String = "spark-defaults.conf"
}
